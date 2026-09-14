package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// indexEntrySize: u32 relative offset + u32 file position.
const indexEntrySize = 8

// indexEntry maps a record's offset (relative to the segment base) to
// its byte position in the log file. Entries are sparse: one every
// indexIntervalBytes of log data.
type indexEntry struct {
	relOffset uint32
	position  uint32
}

// segment is one log file plus its sparse index. Writes use WriteAt at
// explicitly tracked positions, so there is no seek state to race over.
type segment struct {
	baseOffset uint64
	log        *os.File
	index      *os.File
	size       int64 // next write position in the log file
	entries    []indexEntry
	// indexInterval: one index entry per this many log bytes.
	indexInterval   int64
	bytesSinceIndex int64
}

func segmentLogName(base uint64) string   { return fmt.Sprintf("%020d.log", base) }
func segmentIndexName(base uint64) string { return fmt.Sprintf("%020d.index", base) }

// openSegment opens (or creates) the log and index files for base in dir
// and loads the index into memory. A missing or malformed index is not
// fatal: it is rebuilt from the log by rebuildIndex.
func openSegment(dir string, base uint64, indexInterval int64) (*segment, error) {
	logFile, err := os.OpenFile(filepath.Join(dir, segmentLogName(base)), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment log %d: %w", base, err)
	}
	indexFile, err := os.OpenFile(filepath.Join(dir, segmentIndexName(base)), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("open segment index %d: %w", base, err)
	}
	st, err := logFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("stat segment log %d: %w", base, err)
	}
	s := &segment{baseOffset: base, log: logFile, index: indexFile, size: st.Size(), indexInterval: indexInterval}
	if err := s.loadIndex(); err != nil {
		// Corrupt index file: start from an empty in-memory index; the
		// caller rebuilds it by scanning the log.
		s.entries = nil
	}
	return s, nil
}

// loadIndex reads the .index file into memory and validates it against
// the log. Beyond parse errors, an index is rejected when positions are
// not monotonic, point outside the log, or do not land on the record
// they claim to (verified by reading the offset field at each entry's
// position). Any failure makes the caller rebuild from the log, so an
// inconsistent index is always self-healing.
func (s *segment) loadIndex() error {
	if _, err := s.index.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(s.index)
	if err != nil {
		return fmt.Errorf("read index: %w", err)
	}
	if len(data)%indexEntrySize != 0 {
		return fmt.Errorf("index size %d not a multiple of %d", len(data), indexEntrySize)
	}
	entries := make([]indexEntry, 0, len(data)/indexEntrySize)
	for i := 0; i < len(data); i += indexEntrySize {
		entries = append(entries, indexEntry{
			relOffset: binary.BigEndian.Uint32(data[i : i+4]),
			position:  binary.BigEndian.Uint32(data[i+4 : i+8]),
		})
	}
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].relOffset < entries[j].relOffset }) {
		return fmt.Errorf("index entries out of order")
	}
	if err := s.validateEntries(entries); err != nil {
		return err
	}
	s.entries = entries
	return nil
}

// validateEntries checks every entry against the log file: positions
// must be monotonic, inside the log, and point at a record whose offset
// is exactly baseOffset+relOffset. One ReadAt per entry at open time;
// the index is sparse (one entry per indexInterval bytes), so this is
// cheap insurance against trusting a corrupt index.
func (s *segment) validateEntries(entries []indexEntry) error {
	var head [12]byte // crc(4) + offset(8): all we need to verify a record start
	prevPos := int64(-1)
	for _, e := range entries {
		pos := int64(e.position)
		if pos <= prevPos {
			return fmt.Errorf("index positions not strictly increasing at %d", pos)
		}
		prevPos = pos
		if pos < 0 || pos+int64(len(head)) > s.size {
			return fmt.Errorf("index position %d outside log size %d", pos, s.size)
		}
		if _, err := s.log.ReadAt(head[:], pos); err != nil {
			return fmt.Errorf("index probe at %d: %w", pos, err)
		}
		if got, want := binary.BigEndian.Uint64(head[4:12]), s.baseOffset+uint64(e.relOffset); got != want {
			return fmt.Errorf("index entry points at offset %d, want %d", got, want)
		}
	}
	return nil
}

// append writes data (one or more whole encoded records) at the current
// end of the log. records describes, for each record inside data, its
// offset and its byte position within data, so the sparse index can be
// maintained.
func (s *segment) append(data []byte, records []recordPos) error {
	if _, err := s.log.WriteAt(data, s.size); err != nil {
		return fmt.Errorf("append segment %d: %w", s.baseOffset, err)
	}
	for _, rp := range records {
		if s.bytesSinceIndex >= s.indexInterval {
			e := indexEntry{relOffset: uint32(rp.offset - s.baseOffset), position: uint32(s.size + rp.pos)}
			if err := s.writeIndexEntry(e); err != nil {
				return err
			}
			s.bytesSinceIndex = 0
		}
		s.bytesSinceIndex += rp.size
	}
	s.size += int64(len(data))
	return nil
}

// recordPos locates one record inside a batch buffer.
type recordPos struct {
	offset uint64 // absolute offset
	pos    int64  // byte position within the batch buffer
	size   int64  // encoded size of this record
}

// writeIndexEntry appends one entry to the in-memory index and the
// index file.
func (s *segment) writeIndexEntry(e indexEntry) error {
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], e.relOffset)
	binary.BigEndian.PutUint32(buf[4:8], e.position)
	pos := int64(len(s.entries)) * indexEntrySize
	if _, err := s.index.WriteAt(buf[:], pos); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}
	s.entries = append(s.entries, e)
	return nil
}

// lookup returns the byte position to start scanning from in order to
// find relOffset: the position of the greatest indexed entry whose
// relOffset is <= the target.
func (s *segment) lookup(relOffset uint32) int64 {
	i := sort.Search(len(s.entries), func(i int) bool {
		return s.entries[i].relOffset > relOffset
	})
	if i == 0 {
		return 0
	}
	return int64(s.entries[i-1].position)
}

// truncate cuts the log to size bytes and drops index entries pointing
// beyond it. Used by crash recovery to remove a corrupt tail.
func (s *segment) truncate(size int64) error {
	if err := s.log.Truncate(size); err != nil {
		return fmt.Errorf("truncate segment %d to %d: %w", s.baseOffset, size, err)
	}
	s.size = size
	kept := s.entries[:0]
	for _, e := range s.entries {
		if int64(e.position) < size {
			kept = append(kept, e)
		}
	}
	s.entries = kept
	return s.rewriteIndex()
}

// rewriteIndex replaces the whole index file with the in-memory
// entries. Used after truncate and after a full rebuild.
func (s *segment) rewriteIndex() error {
	if err := s.index.Truncate(0); err != nil {
		return fmt.Errorf("truncate index: %w", err)
	}
	buf := make([]byte, 0, len(s.entries)*indexEntrySize)
	for _, e := range s.entries {
		buf = binary.BigEndian.AppendUint32(buf, e.relOffset)
		buf = binary.BigEndian.AppendUint32(buf, e.position)
	}
	if _, err := s.index.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("rewrite index: %w", err)
	}
	return nil
}

// addIndexEntry records an entry without touching bytesSinceIndex;
// used by index rebuild during recovery.
func (s *segment) addIndexEntry(e indexEntry) {
	s.entries = append(s.entries, e)
}

func (s *segment) sync() error {
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("sync segment log %d: %w", s.baseOffset, err)
	}
	if err := s.index.Sync(); err != nil {
		return fmt.Errorf("sync segment index %d: %w", s.baseOffset, err)
	}
	return nil
}

func (s *segment) close() error {
	logErr := s.log.Close()
	indexErr := s.index.Close()
	if logErr != nil {
		return logErr
	}
	return indexErr
}
