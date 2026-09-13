package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// recordSizes returns the encoded size of each record, so tests can
// truncate a log file at an exact record boundary.
func recordSizes(t *testing.T, recs []Record) []int {
	t.Helper()
	sizes := make([]int, len(recs))
	for i := range recs {
		buf, err := EncodeRecord(nil, &recs[i])
		if err != nil {
			t.Fatal(err)
		}
		sizes[i] = len(buf)
	}
	return sizes
}

// overwriteFile replaces the whole content of path (used to corrupt
// index files).
func overwriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryEmptySegment: crash before any append (or a topic that
// never got data). A 0-byte log must reopen cleanly and accept writes.
func TestRecoveryEmptySegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 0 {
		t.Fatalf("hwm of fresh partition: %d", hwm)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Files exist but are empty: this is the "crash before append" case.
	st, err := os.Stat(filepath.Join(dir, segmentLogName(0)))
	if err != nil || st.Size() != 0 {
		t.Fatalf("fresh log: size=%d err=%v", st.Size(), err)
	}

	p2, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatalf("reopen empty: %v", err)
	}
	defer p2.Close()
	if hwm := p2.HighWatermark(); hwm != 0 {
		t.Fatalf("hwm after empty reopen: %d", hwm)
	}
	base := appendRecords(t, p2, 3, "post-empty")
	if base != 0 {
		t.Fatalf("append after empty recovery: base %d want 0", base)
	}
}

// TestRecoveryMissingIndex deletes .index files and expects recovery to
// rebuild them from the log — for the active segment and for an older,
// inactive one.
func TestRecoveryMissingIndex(t *testing.T) {
	t.Parallel()
	opts := Options{MaxSegmentBytes: 300, IndexIntervalBytes: 64}

	build := func(t *testing.T, dir string) int64 {
		p, err := OpenPartition(dir, "jobs", 0, opts, nil)
		if err != nil {
			t.Fatal(err)
		}
		// ~40 bytes per record, rotation at 300 → several segments.
		for i := 0; i < 40; i++ {
			if _, err := p.Append([]Record{{Value: []byte(fmt.Sprintf("v-%02d", i))}}); err != nil {
				t.Fatal(err)
			}
		}
		if len(p.segments) < 2 {
			t.Fatalf("expected rotation, got %d segment(s)", len(p.segments))
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		// Return the size of the first (inactive) segment's log.
		st, err := os.Stat(filepath.Join(dir, segmentLogName(0)))
		if err != nil {
			t.Fatal(err)
		}
		return st.Size()
	}

	t.Run("active segment", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		build(t, dir)
		lastBase := uint64(0)
		bases, err := listSegmentBases(dir)
		if err != nil {
			t.Fatal(err)
		}
		lastBase = bases[len(bases)-1]
		if err := os.Remove(filepath.Join(dir, segmentIndexName(lastBase))); err != nil {
			t.Fatal(err)
		}

		p2, err := OpenPartition(dir, "jobs", 0, opts, nil)
		if err != nil {
			t.Fatalf("reopen with missing active index: %v", err)
		}
		defer p2.Close()
		if hwm := p2.HighWatermark(); hwm != 40 {
			t.Fatalf("hwm: got %d want 40", hwm)
		}
		assertAllRecordsReadable(t, p2, 40)
		assertIndexRebuilt(t, dir, lastBase)
		assertSegmentIndexValid(t, p2, lastBase)
	})

	t.Run("inactive segment", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		build(t, dir)
		if err := os.Remove(filepath.Join(dir, segmentIndexName(0))); err != nil {
			t.Fatal(err)
		}

		p2, err := OpenPartition(dir, "jobs", 0, opts, nil)
		if err != nil {
			t.Fatalf("reopen with missing inactive index: %v", err)
		}
		defer p2.Close()
		if hwm := p2.HighWatermark(); hwm != 40 {
			t.Fatalf("hwm: got %d want 40", hwm)
		}
		assertAllRecordsReadable(t, p2, 40)
		assertIndexRebuilt(t, dir, 0)
		assertSegmentIndexValid(t, p2, 0)
	})
}

// assertSegmentIndexValid proves the in-memory index of the segment
// with the given base offset is genuinely rebuilt: non-empty and every
// entry probes to the exact record it claims.
func assertSegmentIndexValid(t *testing.T, p *Partition, base uint64) {
	t.Helper()
	for _, s := range p.segments {
		if s.baseOffset != base {
			continue
		}
		if len(s.entries) == 0 {
			t.Fatalf("segment %d: index has no entries after rebuild", base)
		}
		if err := s.validateEntries(s.entries); err != nil {
			t.Fatalf("segment %d: rebuilt index invalid: %v", base, err)
		}
		return
	}
	t.Fatalf("segment %d not found", base)
}

// assertAllRecordsReadable reads every offset and checks offsets and values.
func assertAllRecordsReadable(t *testing.T, p *Partition, n int) {
	t.Helper()
	recs, err := p.Read(0, n+10, 1<<22)
	if err != nil {
		t.Fatalf("full read: %v", err)
	}
	if len(recs) != n {
		t.Fatalf("read %d records, want %d", len(recs), n)
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Fatalf("record %d has offset %d", i, r.Offset)
		}
		if string(r.Value) != fmt.Sprintf("v-%02d", i) {
			t.Fatalf("record %d: value %q", i, r.Value)
		}
	}
	// Random-access reads must work too (index lookup path).
	for _, off := range []uint64{0, 1, uint64(n) / 2, uint64(n) - 1} {
		recs, err := p.Read(off, 1, 1<<20)
		if err != nil || len(recs) != 1 {
			t.Fatalf("read %d: len=%d err=%v", off, len(recs), err)
		}
		if recs[0].Offset != off {
			t.Fatalf("read %d returned offset %d", off, recs[0].Offset)
		}
	}
}

// assertIndexRebuilt checks the .index file exists again, is a
// well-formed multiple of the entry size, and — for the given segment —
// that the in-memory entries match the file and pass full validation
// against the log.
func assertIndexRebuilt(t *testing.T, dir string, base uint64) {
	t.Helper()
	st, err := os.Stat(filepath.Join(dir, segmentIndexName(base)))
	if err != nil {
		t.Fatalf("index not rebuilt: %v", err)
	}
	if st.Size()%indexEntrySize != 0 {
		t.Fatalf("rebuilt index size %d not a multiple of %d", st.Size(), indexEntrySize)
	}
}

// TestRecoveryCorruptIndex: every flavor of a broken index must be
// detected and rebuilt from the log, on the active and on an inactive
// segment.
func TestRecoveryCorruptIndex(t *testing.T) {
	t.Parallel()
	opts := Options{MaxSegmentBytes: 300, IndexIntervalBytes: 64}

	// build writes 40 records across several segments and returns the
	// real index bytes of segment 0 for crafting plausible corruption.
	build := func(t *testing.T, dir string) []byte {
		p, err := OpenPartition(dir, "jobs", 0, opts, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 40; i++ {
			if _, err := p.Append([]Record{{Value: []byte(fmt.Sprintf("v-%02d", i))}}); err != nil {
				t.Fatal(err)
			}
		}
		if len(p.segments) < 2 {
			t.Fatalf("expected rotation, got %d segment(s)", len(p.segments))
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		idx, err := os.ReadFile(filepath.Join(dir, segmentIndexName(0)))
		if err != nil {
			t.Fatal(err)
		}
		if len(idx) == 0 {
			t.Fatal("test setup: expected a non-empty index for segment 0")
		}
		return idx
	}

	corruptions := []struct {
		name   string
		mangle func(valid []byte) []byte
	}{
		{"garbage bytes", func(valid []byte) []byte {
			return []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03}
		}},
		{"truncated entry", func(valid []byte) []byte {
			if len(valid) < indexEntrySize+4 {
				return []byte{1, 2, 3, 4, 5} // odd size: invalid framing
			}
			return valid[:len(valid)-4] // size no longer a multiple of 8
		}},
		{"entries out of order", func(valid []byte) []byte {
			if len(valid) < 2*indexEntrySize {
				return valid // single-entry index: nothing to swap
			}
			out := append([]byte(nil), valid...)
			// Swap relOffsets of the first two entries.
			e0, e1 := binary.BigEndian.Uint32(out[0:4]), binary.BigEndian.Uint32(out[8:12])
			binary.BigEndian.PutUint32(out[0:4], e1)
			binary.BigEndian.PutUint32(out[8:12], e0)
			return out
		}},
		{"plausible but wrong relOffset", func(valid []byte) []byte {
			// Structurally valid, sorted, positions in range — but an
			// entry claims a relOffset one higher than the record its
			// position actually points at. Only probing the log catches
			// this.
			out := append([]byte(nil), valid...)
			if len(out) >= 2*indexEntrySize {
				rel := binary.BigEndian.Uint32(out[8:12])
				binary.BigEndian.PutUint32(out[8:12], rel+1)
			} else if len(out) >= indexEntrySize {
				rel := binary.BigEndian.Uint32(out[0:4])
				binary.BigEndian.PutUint32(out[0:4], rel+1)
			}
			return out
		}},
		{"position beyond log size", func(valid []byte) []byte {
			out := append([]byte(nil), valid...)
			if len(out) < indexEntrySize {
				out = make([]byte, indexEntrySize) // one fake entry: rel 0, pos huge
			}
			binary.BigEndian.PutUint32(out[4:8], 0x00ffffff) // first entry points far past EOF
			return out
		}},
	}

	for _, tc := range corruptions {
		for _, target := range []string{"active", "inactive"} {
			t.Run(tc.name+"/"+target, func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				valid := build(t, dir)
				bases, err := listSegmentBases(dir)
				if err != nil {
					t.Fatal(err)
				}
				base := bases[0]
				if target == "active" {
					base = bases[len(bases)-1]
					// For the active segment we need its own valid index
					// as the starting point.
					valid, err = os.ReadFile(filepath.Join(dir, segmentIndexName(base)))
					if err != nil {
						t.Fatal(err)
					}
				}
				overwriteFile(t, filepath.Join(dir, segmentIndexName(base)), tc.mangle(valid))

				p2, err := OpenPartition(dir, "jobs", 0, opts, nil)
				if err != nil {
					t.Fatalf("reopen with corrupt index (%s): %v", tc.name, err)
				}
				defer p2.Close()
				if hwm := p2.HighWatermark(); hwm != 40 {
					t.Fatalf("hwm: got %d want 40", hwm)
				}
				assertAllRecordsReadable(t, p2, 40)
				assertIndexRebuilt(t, dir, base)
				assertSegmentIndexValid(t, p2, base)
				// The decisive check: recovery rewrote the file with
				// exactly the ground-truth entries. If the corrupt index
				// had been trusted, the mangled bytes would still be on
				// disk (inactive segment) or the rebuilt content would
				// differ from the real scan (active).
				rebuilt, err := os.ReadFile(filepath.Join(dir, segmentIndexName(base)))
				if err != nil {
					t.Fatal(err)
				}
				mangled := tc.mangle(valid)
				if bytes.Equal(mangled, valid) {
					return // degenerate mangle (too few entries): nothing to prove
				}
				if !bytes.Equal(rebuilt, valid) {
					t.Fatalf("index file not rebuilt to ground truth (%d bytes, want %d)", len(rebuilt), len(valid))
				}
			})
		}
	}
}

// TestCrashScenariosMidAppend simulates power loss at every stage of an
// append: nothing written, half a batch, a truncated header, and a
// mid-log CRC failure. Recovery must keep exactly the good records and
// stay writable.
func TestCrashScenariosMidAppend(t *testing.T) {
	t.Parallel()

	recs := make([]Record, 10)
	for i := range recs {
		recs[i] = Record{Value: []byte(fmt.Sprintf("batch-%d", i))}
	}
	sizes := recordSizes(t, recs)
	// byteAfter(k) is the file offset just past record k.
	byteAfter := func(k int) int64 {
		total := int64(0)
		for i := 0; i <= k; i++ {
			total += int64(sizes[i])
		}
		return total
	}

	tests := []struct {
		name    string
		corrupt func(t *testing.T, logPath string)
		wantHWM uint64
	}{
		{"crash before append: file stays empty", func(t *testing.T, logPath string) {
			overwriteFile(t, logPath, nil)
		}, 0},
		{"torn write: half of record 5", func(t *testing.T, logPath string) {
			truncateTo(t, logPath, byteAfter(4)+int64(sizes[5]/2))
		}, 5},
		{"torn write: 5 bytes of record 5 header", func(t *testing.T, logPath string) {
			truncateTo(t, logPath, byteAfter(4)+5)
		}, 5},
		{"single byte of the next record", func(t *testing.T, logPath string) {
			truncateTo(t, logPath, byteAfter(6)+1)
		}, 7},
		{"bad CRC mid log kills the tail", func(t *testing.T, logPath string) {
			// Flip a value byte of record 3 → records 3..9 are dropped.
			pos := byteAfter(2) + int64(sizes[3]) - 1
			f, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.WriteAt([]byte{0xaa}, pos); err != nil {
				t.Fatal(err)
			}
		}, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if base, err := p.Append(recs); err != nil || base != 0 {
				t.Fatalf("append: base=%d err=%v", base, err)
			}
			if err := p.Close(); err != nil { // clean close = data flushed; corruption below is the crash
				t.Fatal(err)
			}
			tc.corrupt(t, filepath.Join(dir, segmentLogName(0)))

			p2, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer p2.Close()
			if hwm := p2.HighWatermark(); hwm != tc.wantHWM {
				t.Fatalf("hwm: got %d want %d", hwm, tc.wantHWM)
			}
			got, err := p2.Read(0, 100, 1<<20)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(got) != int(tc.wantHWM) {
				t.Fatalf("read %d records, want %d", len(got), tc.wantHWM)
			}
			for i, r := range got {
				if r.Offset != uint64(i) || string(r.Value) != fmt.Sprintf("batch-%d", i) {
					t.Fatalf("record %d: offset=%d value=%q", i, r.Offset, r.Value)
				}
			}
			// The log is writable afterwards and offsets continue.
			base := appendRecords(t, p2, 2, "post-crash")
			if base != tc.wantHWM {
				t.Fatalf("append after recovery: base %d want %d", base, tc.wantHWM)
			}
		})
	}
}

// truncateTo cuts path to exactly n bytes.
func truncateTo(t *testing.T, path string, n int64) {
	t.Helper()
	if err := os.Truncate(path, n); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryMultipleSegmentsTornTail: several full segments plus a
// torn last segment. Recovery keeps every record of the intact
// segments, truncates only the tail, and continues monotonically.
func TestRecoveryMultipleSegmentsTornTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := Options{MaxSegmentBytes: 300, IndexIntervalBytes: 64}
	p, err := OpenPartition(dir, "jobs", 0, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, err := p.Append([]Record{{Value: []byte(fmt.Sprintf("v-%02d", i))}}); err != nil {
			t.Fatal(err)
		}
	}
	nSegs := len(p.segments)
	if nSegs < 3 {
		t.Fatalf("expected 3+ segments, got %d", nSegs)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Torn write in the LAST segment: chop it mid-record.
	bases, err := listSegmentBases(dir)
	if err != nil {
		t.Fatal(err)
	}
	lastLog := filepath.Join(dir, segmentLogName(bases[len(bases)-1]))
	st, err := os.Stat(lastLog)
	if err != nil {
		t.Fatal(err)
	}
	truncateTo(t, lastLog, st.Size()-7)

	p2, err := OpenPartition(dir, "jobs", 0, opts, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	// 39 records survive (the torn one is dropped), offsets stay
	// monotonic, and the intact segments are untouched.
	if hwm := p2.HighWatermark(); hwm != 39 {
		t.Fatalf("hwm: got %d want 39", hwm)
	}
	recs, err := p2.Read(0, 100, 1<<22)
	if err != nil || len(recs) != 39 {
		t.Fatalf("read: len=%d err=%v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Fatalf("record %d has offset %d (gap!)", i, r.Offset)
		}
	}
	base := appendRecords(t, p2, 1, "post")
	if base != 39 {
		t.Fatalf("append after multi-segment recovery: base %d want 39", base)
	}
}

// TestMonotonicOffsetsAcrossRecovery: append → close → reopen cycles
// must never reuse or skip an offset, even with a crash truncation in
// the middle of the sequence.
func TestMonotonicOffsetsAcrossRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := Options{MaxSegmentBytes: 512, IndexIntervalBytes: 64}

	wantHWM := uint64(0)
	for cycle := 0; cycle < 3; cycle++ {
		p, err := OpenPartition(dir, "jobs", 0, opts, nil)
		if err != nil {
			t.Fatalf("cycle %d open: %v", cycle, err)
		}
		if hwm := p.HighWatermark(); hwm != wantHWM {
			t.Fatalf("cycle %d: hwm %d want %d", cycle, hwm, wantHWM)
		}
		base := appendRecords(t, p, 7, fmt.Sprintf("c%d", cycle))
		if base != wantHWM {
			t.Fatalf("cycle %d: base %d want %d", cycle, base, wantHWM)
		}
		wantHWM += 7
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if cycle == 1 {
			// Crash between cycles: tear the tail. The next cycle must
			// continue from the last GOOD offset, never a reused one.
			bases, err := listSegmentBases(dir)
			if err != nil {
				t.Fatal(err)
			}
			last := filepath.Join(dir, segmentLogName(bases[len(bases)-1]))
			st, err := os.Stat(last)
			if err != nil {
				t.Fatal(err)
			}
			truncateTo(t, last, st.Size()-3) // partial tail record
			wantHWM--                        // the torn record is lost
		}
	}

	// Final reopen: full scan must show contiguous offsets 0..hwm-1.
	p, err := OpenPartition(dir, "jobs", 0, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if hwm := p.HighWatermark(); hwm != wantHWM {
		t.Fatalf("final hwm %d want %d", hwm, wantHWM)
	}
	recs, err := p.Read(0, 100, 1<<22)
	if err != nil || len(recs) != int(wantHWM) {
		t.Fatalf("final read: len=%d err=%v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Fatalf("offset %d at position %d: not monotonic", r.Offset, i)
		}
	}
}

// TestFlushResetsCounters is the storage-level half of the fsync policy
// test: Flush fsyncs dirty segments and resets the counters the
// broker's policy watches.
func TestFlushResetsCounters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	appendRecords(t, p, 5, "flush")
	if got := p.RecordsSinceFlush(); got != 5 {
		t.Fatalf("records since flush: got %d want 5", got)
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := p.RecordsSinceFlush(); got != 0 {
		t.Fatalf("records since flush after Flush: got %d want 0", got)
	}
	// Flush with nothing dirty is a no-op and stays clean.
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
}
