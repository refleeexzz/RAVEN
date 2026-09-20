package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// ErrDivergedAppend rejects a replica append whose first offset does
// not continue the local log exactly. The replication loop resolves it
// by truncating to the leader's committed offset and re-fetching.
var ErrDivergedAppend = errors.New("storage: replica append not contiguous with local log")

// AppendReplica appends records produced by the partition leader,
// preserving their leader-assigned offsets and timestamps. Unlike
// Append (the client produce path), offsets are NOT assigned here: the
// leader owns the offset space and every replica stores the identical
// log, byte for byte at the record level.
//
// Contiguity is enforced: records[0].Offset must equal the local
// high-water mark and the batch must be gapless. A mismatch returns
// ErrDivergedAppend — the caller (replication loop) truncates and
// re-fetches; silently accepting a gap would fork the log.
//
// Exactly one caller at a time reaches this per partition (the
// partition's replication loop), serialized further by mu against
// client reads and the produce writer.
func (p *Partition) AppendReplica(records []Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("storage: partition closed")
	}
	if len(records) == 0 {
		return nil
	}
	if records[0].Offset != p.nextOffset {
		return fmt.Errorf("%w: local hwm %d, batch starts at %d",
			ErrDivergedAppend, p.nextOffset, records[0].Offset)
	}
	active := p.segments[len(p.segments)-1]
	if active.size >= p.opts.MaxSegmentBytes {
		ns, err := openSegment(p.dir, p.nextOffset, p.opts.IndexIntervalBytes)
		if err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
		p.segments = append(p.segments, ns)
		active = ns
	}
	buf := make([]byte, 0, 4096)
	positions := make([]recordPos, 0, len(records))
	for i := range records {
		rec := &records[i]
		if rec.Offset != p.nextOffset+uint64(i) {
			return fmt.Errorf("%w: batch gap at index %d (offset %d, want %d)",
				ErrDivergedAppend, i, rec.Offset, p.nextOffset+uint64(i))
		}
		if rec.TimestampMs > active.maxTs {
			active.maxTs = rec.TimestampMs
		}
		start := len(buf)
		var err error
		buf, err = EncodeRecord(buf, rec)
		if err != nil {
			return err
		}
		positions = append(positions, recordPos{
			offset: rec.Offset,
			pos:    int64(start),
			size:   int64(len(buf) - start),
		})
	}
	if err := active.append(buf, positions); err != nil {
		return err
	}
	activeIdx := len(p.segments) - 1
	if activeIdx < p.dirtySegStart {
		p.dirtySegStart = activeIdx
	}
	p.nextOffset += uint64(len(records))
	p.recordsSinceFlush += len(records)
	p.dirty = true
	return nil
}

// TruncateTo drops every record with Offset >= offset. It is the
// replication log-matching primitive: a follower whose log diverged
// from the leader's (records the leader never committed) cuts its tail
// back to the leader's committed offset and re-fetches.
//
// Two shapes:
//
//   - offset < nextOffset: cut the tail. The segment holding offset is
//     truncated at offset's exact byte position; every later segment is
//     closed and deleted from disk.
//   - offset > nextOffset: forward reset. The local log sits below a
//     hole on the leader (retention deleted records the follower never
//     fetched). All segments are removed and a fresh empty segment
//     starts at offset. Data between the old end and offset is lost
//     locally — the same loss window retention already allows.
//
// offset == nextOffset is a no-op.
func (p *Partition) TruncateTo(offset uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("storage: partition closed")
	}
	if offset == p.nextOffset {
		return nil
	}
	if offset > p.nextOffset {
		return p.forwardResetLocked(offset)
	}
	// Find the segment holding offset: the last one with base <= offset.
	idx := -1
	for i, s := range p.segments {
		if s.baseOffset <= offset {
			idx = i
		} else {
			break
		}
	}
	var cutPos int64
	switch {
	case idx == -1:
		// offset below every segment base: wipe everything.
		cutPos = 0
	case p.segments[idx].baseOffset == offset:
		cutPos = 0
	default:
		// Scan the segment for offset's byte position, tolerating
		// compaction gaps exactly like the index rebuild does.
		s := p.segments[idx]
		expected := s.baseOffset
		var scanErr error
		err := streamRecords(s.log, 0, s.size, true, func(r *Record, pos, size int64) bool {
			if r.Offset != expected && !(p.gapsAllowed && r.Offset > expected) {
				scanErr = fmt.Errorf("offset gap at %d", r.Offset)
				return false
			}
			expected = r.Offset + 1
			if r.Offset == offset {
				cutPos = pos
				return false
			}
			if r.Offset > offset {
				// A compaction gap jumped over offset: cut at the first
				// record above it.
				cutPos = pos
				return false
			}
			return true
		})
		if err != nil {
			return fmt.Errorf("truncate scan segment %d: %w", s.baseOffset, err)
		}
		if scanErr != nil {
			return fmt.Errorf("truncate scan segment %d: %w", s.baseOffset, scanErr)
		}
		if cutPos == 0 && expected <= offset {
			// offset lands past this segment's end (it is the base of a
			// later segment... but idx says none exists with base <=
			// offset). Keep the whole segment.
			cutPos = s.size
		}
	}
	// Close and delete every segment after idx. When idx == -1 the loop
	// removes all of them: offset sits below every segment base.
	for i := len(p.segments) - 1; i > idx; i-- {
		if err := p.removeSegmentLocked(i); err != nil {
			return err
		}
	}
	if idx < 0 {
		// Wiped below all bases: restart empty at offset.
		ns, err := openSegment(p.dir, offset, p.opts.IndexIntervalBytes)
		if err != nil {
			return fmt.Errorf("truncate: reopen empty segment: %w", err)
		}
		p.segments = []*segment{ns}
	} else if err := p.segments[idx].truncate(cutPos); err != nil {
		return err
	} else {
		p.refreshMaxTsLocked(p.segments[idx])
	}
	p.nextOffset = offset
	p.dirty = true
	if idx >= 0 && idx < p.dirtySegStart {
		p.dirtySegStart = idx
	}
	p.log.Warn("partition truncated for log matching",
		slog.Uint64("to_offset", offset))
	return nil
}

// forwardResetLocked wipes the log and restarts empty at offset.
// Caller holds mu and has checked offset > nextOffset.
func (p *Partition) forwardResetLocked(offset uint64) error {
	for i := len(p.segments) - 1; i >= 0; i-- {
		if err := p.removeSegmentLocked(i); err != nil {
			return err
		}
	}
	ns, err := openSegment(p.dir, offset, p.opts.IndexIntervalBytes)
	if err != nil {
		return fmt.Errorf("forward reset to %d: %w", offset, err)
	}
	p.segments = []*segment{ns}
	p.nextOffset = offset
	p.dirty = true
	p.dirtySegStart = 0
	p.log.Warn("partition jumped forward over a leader-side retention hole",
		slog.Uint64("to_offset", offset))
	return nil
}

// removeSegmentLocked closes segment i and deletes its files.
// Caller holds mu.
func (p *Partition) removeSegmentLocked(i int) error {
	s := p.segments[i]
	if err := s.close(); err != nil {
		return fmt.Errorf("close segment %d: %w", s.baseOffset, err)
	}
	for _, name := range []string{segmentLogName(s.baseOffset), segmentIndexName(s.baseOffset)} {
		if err := os.Remove(filepath.Join(p.dir, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}
	p.segments = append(p.segments[:i], p.segments[i+1:]...)
	return nil
}

// refreshMaxTsLocked recomputes a segment's max timestamp after a tail
// truncation, so time-based retention keeps working from real data.
// Caller holds mu.
func (p *Partition) refreshMaxTsLocked(s *segment) {
	s.maxTs = 0
	_ = streamRecords(s.log, 0, s.size, true, func(r *Record, _, _ int64) bool {
		if r.TimestampMs > s.maxTs {
			s.maxTs = r.TimestampMs
		}
		return true
	})
}
