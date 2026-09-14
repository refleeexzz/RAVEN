package storage

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrOffsetOutOfRange is returned when reading beyond the high-water
// mark.
var ErrOffsetOutOfRange = errors.New("storage: offset out of range")

// maxRecordSize bounds one encoded record during scans/reads. Records
// arrive through frames capped at 4 MiB, so a "record" that does not
// decode within 4 MiB of bytes is garbage (corrupt length fields), not
// a torn write.
const maxRecordSize = 4 << 20

// Options tune partition behavior.
type Options struct {
	// MaxSegmentBytes: active segment rotates once it passes this size.
	// Rotation happens between batches, so a segment can overshoot by
	// one batch.
	MaxSegmentBytes int64
	// IndexIntervalBytes: one sparse index entry per this many log bytes.
	IndexIntervalBytes int64
}

// Partition is one ordered, append-only log. Appends go to the active
// (last) segment; reads may hit any segment. A single RWMutex guards
// segment state: writers hold it exclusively (behind the broker's
// per-partition writer goroutine), readers share it.
type Partition struct {
	mu                sync.RWMutex
	topic             string
	id                int32
	dir               string
	opts              Options
	segments          []*segment // sorted by base offset; last is active
	nextOffset        uint64     // high-water mark: next offset to assign
	recordsSinceFlush int
	dirty             bool // data written since last successful Flush
	dirtySegStart     int  // first segment that may hold unsynced data
	closed            bool
	log               *slog.Logger
}

// OpenPartition opens (or creates) the partition stored in dir and runs
// crash recovery on the active segment.
func OpenPartition(dir, topic string, id int32, opts Options, log *slog.Logger) (*Partition, error) {
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create partition dir: %w", err)
	}
	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, err
	}
	p := &Partition{
		topic:         topic,
		id:            id,
		dir:           dir,
		opts:          opts,
		dirtySegStart: 0,
		log:           log.With(slog.String("topic", topic), slog.Int("partition", int(id))),
	}
	if len(bases) == 0 {
		bases = []uint64{0}
	}
	for i, base := range bases {
		s, err := openSegment(dir, base, opts.IndexIntervalBytes)
		if err != nil {
			p.closeSegments()
			return nil, err
		}
		p.segments = append(p.segments, s)
		if i == len(bases)-1 {
			if err := p.recoverActive(s); err != nil {
				p.closeSegments()
				return nil, err
			}
		} else if len(s.entries) == 0 && s.size > 0 {
			// Index file was missing/corrupt for an inactive segment:
			// rebuild it by scanning (inactive segments are trusted, so
			// a corrupt tail here is reported, not truncated).
			if err := p.rebuildIndex(s); err != nil {
				p.closeSegments()
				return nil, err
			}
		}
	}
	p.dirtySegStart = len(p.segments) - 1
	return p, nil
}

// listSegmentBases returns the sorted base offsets of all .log files.
func listSegmentBases(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list partition dir: %w", err)
	}
	var bases []uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue // foreign file; ignore
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// recoverActive scans the active segment, verifies every record, drops
// a corrupt/torn tail by truncating at the last good byte, and rebuilds
// the sparse index. This is the crash-recovery path.
func (p *Partition) recoverActive(s *segment) error {
	s.entries = nil
	s.bytesSinceIndex = 0
	next := s.baseOffset
	expected := s.baseOffset
	var lastGood int64
	var scanErr error
	err := streamRecords(s.log, 0, s.size, true, func(r *Record, pos, size int64) bool {
		if r.Offset != expected {
			// Offsets must be contiguous from the segment base. A gap
			// means the file is corrupt: stop and truncate at the last
			// good record.
			scanErr = fmt.Errorf("offset gap: expected %d, found %d", expected, r.Offset)
			return false
		}
		if s.bytesSinceIndex >= s.indexInterval {
			s.addIndexEntry(indexEntry{relOffset: uint32(r.Offset - s.baseOffset), position: uint32(pos)})
			s.bytesSinceIndex = 0
		}
		s.bytesSinceIndex += size
		expected++
		next = r.Offset + 1
		lastGood = pos + size
		return true
	})
	if err != nil {
		return fmt.Errorf("scan active segment %d: %w", s.baseOffset, err)
	}
	if lastGood < s.size {
		p.log.Warn("recovery: truncating corrupt tail",
			slog.Uint64("segment_base", s.baseOffset),
			slog.Int64("from", s.size), slog.Int64("to", lastGood),
			slog.Any("cause", scanErr))
		if err := s.truncate(lastGood); err != nil {
			return err
		}
	}
	// Persist the rebuilt index so the next boot can trust it.
	if err := s.rewriteIndex(); err != nil {
		return err
	}
	p.nextOffset = next
	return nil
}

// rebuildIndex rescans an inactive segment to rebuild a lost index.
func (p *Partition) rebuildIndex(s *segment) error {
	s.entries = nil
	s.bytesSinceIndex = 0
	expected := s.baseOffset
	var scanErr error
	err := streamRecords(s.log, 0, s.size, true, func(r *Record, pos, size int64) bool {
		if r.Offset != expected {
			scanErr = fmt.Errorf("offset gap at %d", r.Offset)
			return false
		}
		if s.bytesSinceIndex >= s.indexInterval {
			s.addIndexEntry(indexEntry{relOffset: uint32(r.Offset - s.baseOffset), position: uint32(pos)})
			s.bytesSinceIndex = 0
		}
		s.bytesSinceIndex += size
		expected++
		return true
	})
	if err != nil {
		return fmt.Errorf("rebuild index for segment %d: %w", s.baseOffset, err)
	}
	if scanErr != nil {
		return fmt.Errorf("rebuild index for segment %d: %w", s.baseOffset, scanErr)
	}
	p.log.Warn("rebuilt index for inactive segment", slog.Uint64("segment_base", s.baseOffset))
	return s.rewriteIndex()
}

// Append writes records as one contiguous batch and returns the base
// (first) offset assigned. Offsets and timestamps are set on the
// records. Exactly one caller at a time reaches this (the partition's
// writer goroutine in the broker), serialized further by mu.
func (p *Partition) Append(records []Record) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, errors.New("storage: partition closed")
	}
	if len(records) == 0 {
		return 0, errors.New("storage: empty batch")
	}
	active := p.segments[len(p.segments)-1]
	if active.size >= p.opts.MaxSegmentBytes {
		ns, err := openSegment(p.dir, p.nextOffset, p.opts.IndexIntervalBytes)
		if err != nil {
			return 0, fmt.Errorf("rotate segment: %w", err)
		}
		p.segments = append(p.segments, ns)
		active = ns
	}
	buf := make([]byte, 0, 4096)
	positions := make([]recordPos, 0, len(records))
	base := p.nextOffset
	now := time.Now().UnixMilli()
	for i := range records {
		rec := &records[i]
		rec.Offset = base + uint64(i)
		if rec.TimestampMs == 0 {
			rec.TimestampMs = now
		}
		start := len(buf)
		var err error
		buf, err = EncodeRecord(buf, rec)
		if err != nil {
			return 0, err
		}
		positions = append(positions, recordPos{
			offset: rec.Offset,
			pos:    int64(start),
			size:   int64(len(buf) - start),
		})
	}
	if err := active.append(buf, positions); err != nil {
		return 0, err
	}
	activeIdx := len(p.segments) - 1
	if activeIdx < p.dirtySegStart {
		p.dirtySegStart = activeIdx
	}
	p.nextOffset += uint64(len(records))
	p.recordsSinceFlush += len(records)
	p.dirty = true
	return base, nil
}

// Read returns up to maxRecords records starting at offset, stopping
// once the accumulated key+value bytes pass maxBytes (at least one
// record is always returned so a large record can never stall a
// consumer). Every record's CRC is verified. Reading at the high-water
// mark returns an empty slice; reading beyond it returns
// ErrOffsetOutOfRange.
func (p *Partition) Read(offset uint64, maxRecords int, maxBytes int) ([]Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, errors.New("storage: partition closed")
	}
	if offset > p.nextOffset {
		return nil, ErrOffsetOutOfRange
	}
	if offset == p.nextOffset || maxRecords <= 0 {
		return nil, nil
	}
	idx := sort.Search(len(p.segments), func(i int) bool {
		return p.segments[i].baseOffset > offset
	}) - 1
	if idx < 0 {
		return nil, ErrOffsetOutOfRange
	}
	var out []Record
	total := 0
	for idx < len(p.segments) {
		s := p.segments[idx]
		pos := int64(0)
		if s.baseOffset <= offset {
			pos = s.lookup(uint32(offset - s.baseOffset))
		}
		err := streamRecords(s.log, pos, s.size, false, func(r *Record, _, _ int64) bool {
			if r.Offset < offset {
				return true // scanning forward from an index point
			}
			out = append(out, *r)
			total += len(r.Key) + len(r.Value)
			if len(out) >= maxRecords {
				return false
			}
			if total >= maxBytes {
				return false
			}
			return true
		})
		if err != nil {
			return nil, fmt.Errorf("read %s/%d at %d: %w", p.topic, p.id, offset, err)
		}
		if len(out) >= maxRecords || total >= maxBytes {
			break
		}
		idx++
	}
	return out, nil
}

// HighWatermark is the next offset to be assigned (one past the last
// record).
func (p *Partition) HighWatermark() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}

// ID returns the partition index within its topic.
func (p *Partition) ID() int32 { return p.id }

// SegmentCount reports how many segment log files the partition holds.
func (p *Partition) SegmentCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.segments)
}

// DiskUsageBytes sums the on-disk sizes of this partition's segment
// files (.log + .index). It reads the directory instead of trusting
// in-memory sizes, so it stays true after crashes and rebuilds. Cheap:
// two files per segment, called at metrics-scrape cadence.
func (p *Partition) DiskUsageBytes() int64 {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || (!strings.HasSuffix(name, ".log") && !strings.HasSuffix(name, ".index")) {
			continue
		}
		if fi, err := e.Info(); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// RecordsSinceFlush reports how many records were appended since the
// last Flush; the broker uses it for the fsync-every-N-records policy.
func (p *Partition) RecordsSinceFlush() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.recordsSinceFlush
}

// Flush fsyncs every segment touched since the last flush. It is a
// no-op when nothing was written.
func (p *Partition) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.dirty {
		return nil
	}
	for i := p.dirtySegStart; i < len(p.segments); i++ {
		if err := p.segments[i].sync(); err != nil {
			return err
		}
	}
	p.dirty = false
	p.recordsSinceFlush = 0
	p.dirtySegStart = len(p.segments) - 1
	return nil
}

// Close flushes and closes all segment files.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var firstErr error
	if p.dirty {
		for i := p.dirtySegStart; i < len(p.segments); i++ {
			if err := p.segments[i].sync(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, s := range p.segments {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (p *Partition) closeSegments() {
	for _, s := range p.segments {
		_ = s.close()
	}
	p.segments = nil
}

// streamRecords decodes records from ra starting at absolute position
// pos, calling fn per record until fn returns false, EOF, or a bad
// record. In tolerant mode (crash recovery) a corrupt record ends the
// stream silently; otherwise it is returned as ErrCorrupt. A torn tail
// (truncated final record) always ends the stream silently.
func streamRecords(ra io.ReaderAt, pos, fileSize int64, tolerant bool, fn func(r *Record, pos int64, size int64) bool) error {
	const chunk = 256 << 10
	buf := make([]byte, 0, chunk)
	cur := pos
	for {
		// Top up the buffer if the file has more bytes and we don't
		// already hold a max-size record worth of data.
		if cur+int64(len(buf)) < fileSize && len(buf) < maxRecordSize {
			want := int64(chunk)
			if rem := fileSize - cur - int64(len(buf)); rem < want {
				want = rem
			}
			tmp := make([]byte, want)
			n, err := ra.ReadAt(tmp, cur+int64(len(buf)))
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("stream read at %d: %w", cur, err)
			}
			buf = append(buf, tmp[:n]...)
		}
		if len(buf) == 0 {
			return nil // clean EOF
		}
		rec, consumed, err := DecodeRecord(buf)
		switch {
		case err == nil:
			if !fn(&rec, cur, int64(consumed)) {
				return nil
			}
			cur += int64(consumed)
			// Compact occasionally so long scans don't pin a huge
			// backing array.
			if consumed >= cap(buf)/2 {
				rest := make([]byte, len(buf)-consumed, chunk)
				copy(rest, buf[consumed:])
				buf = rest
			} else {
				buf = buf[consumed:]
			}
		case errors.Is(err, ErrTruncated):
			if cur+int64(len(buf)) < fileSize && len(buf) < maxRecordSize {
				continue // record spans unread data; top up and retry
			}
			return nil // torn tail (or absurd length): end of good data
		default:
			if tolerant {
				return nil
			}
			return ErrCorrupt
		}
	}
}
