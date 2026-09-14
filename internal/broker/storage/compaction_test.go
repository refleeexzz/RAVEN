package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// appendBatch appends one batch of key/value records. A "" key means a
// keyless record. Returns the base offset of the batch.
func appendBatch(t *testing.T, p *Partition, kvs ...string) uint64 {
	t.Helper()
	if len(kvs)%2 != 0 {
		t.Fatalf("appendBatch needs key/value pairs, got %d args", len(kvs))
	}
	recs := make([]Record, 0, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		recs = append(recs, Record{Key: []byte(kvs[i]), Value: []byte(kvs[i+1])})
	}
	base, err := p.Append(recs)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return base
}

// readAll reads every surviving record from the partition's low-water
// mark to the high-water mark.
func readAll(t *testing.T, p *Partition) []Record {
	t.Helper()
	recs, err := p.Read(p.FirstOffset(), 1<<20, 1<<26)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return recs
}

// byOffset indexes records by offset for assertions.
func byOffset(recs []Record) map[uint64]Record {
	out := make(map[uint64]Record, len(recs))
	for _, r := range recs {
		out[r.Offset] = r
	}
	return out
}

// fillCompactable appends batches with duplicate keys until the
// partition has at least wantSegs segments. Every batch is 4 records
// with keys a,b,c,d (plus one keyless record every other batch), so the
// closed segments always hold superseded values.
func fillCompactable(t *testing.T, p *Partition, wantSegs int) {
	t.Helper()
	for i := 0; p.SegmentCount() < wantSegs && i < 200; i++ {
		if i%2 == 0 {
			appendBatch(t, p, "a", fmt.Sprintf("a%d", i), "b", fmt.Sprintf("b%d", i),
				"a", fmt.Sprintf("a%d-b", i), "c", fmt.Sprintf("c%d", i))
		} else {
			appendBatch(t, p, "b", fmt.Sprintf("b%d", i), "", fmt.Sprintf("keyless%d", i),
				"c", fmt.Sprintf("c%d", i), "d", fmt.Sprintf("d%d", i))
		}
	}
	if p.SegmentCount() < wantSegs {
		t.Fatalf("never reached %d segments", wantSegs)
	}
}

func TestCompactKeepsLastValuePerKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Two closed segments with known contents, plus an active one.
	appendBatch(t, p, "a", "1", "b", "1", "a", "2", "c", "1") // offsets 0-3
	appendBatch(t, p, "b", "2", "a", "3", "d", "1", "c", "2") // offsets 4-7
	appendBatch(t, p, "e", "1", "f", "1", "g", "1", "h", "1") // offsets 8-11 (active)
	segsBefore := p.SegmentCount()
	if segsBefore < 2 {
		t.Fatalf("want >= 2 segments, got %d", segsBefore)
	}

	stats, err := p.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if stats.RecordsDropped == 0 {
		t.Fatal("nothing dropped, want superseded records removed")
	}
	got := byOffset(readAll(t, p))
	// Latest values inside the closed range: a→offset 5 ("3"), b→4 ("2"),
	// c→7 ("2"), d→6 ("1"). Everything older for those keys is gone.
	wantValues := map[uint64]string{5: "3", 4: "2", 7: "2", 6: "1"}
	for off, want := range wantValues {
		r, ok := got[off]
		if !ok {
			t.Fatalf("offset %d missing after compaction", off)
		}
		if string(r.Value) != want {
			t.Fatalf("offset %d = %q, want %q", off, r.Value, want)
		}
	}
	for _, dropped := range []uint64{0, 1, 2, 3} {
		if _, ok := got[dropped]; ok {
			t.Fatalf("offset %d should have been compacted away", dropped)
		}
	}
	// The active segment is untouched.
	for off := uint64(8); off < 12; off++ {
		if _, ok := got[off]; !ok {
			t.Fatalf("active segment offset %d missing", off)
		}
	}
}

func TestCompactPreservesOffsets(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Padded values make every batch cross MaxSegmentBytes, so each
	// batch is guaranteed to be its own segment.
	appendBatch(t, p, "", pad("kl"), "a", pad("2"), "a", pad("3"), "a", pad("4")) // seg 0: offsets 0-3
	appendBatch(t, p, "a", pad("5"), "b", pad("1"), "b", pad("2"), "b", pad("3")) // seg 1: offsets 4-7
	appendBatch(t, p, "x", pad("1"), "y", pad("1"), "z", pad("1"), "w", pad("1")) // active: 8-11
	if p.SegmentCount() != 3 {
		t.Fatalf("segments = %d, want 3", p.SegmentCount())
	}
	hwmBefore := p.HighWatermark()
	firstBefore := p.FirstOffset()

	if _, err := p.Compact(context.Background()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Offsets are never renumbered: HWM and low-water mark do not move
	// (segment 0 survives thanks to its keyless record).
	if p.HighWatermark() != hwmBefore {
		t.Fatalf("hwm moved: %d → %d", hwmBefore, p.HighWatermark())
	}
	if p.FirstOffset() != firstBefore {
		t.Fatalf("FirstOffset moved: %d → %d", firstBefore, p.FirstOffset())
	}
	// Kept records sit at their ORIGINAL offsets, with holes between:
	// last "a" is offset 4, last "b" is offset 7, the keyless record is
	// offset 0.
	got := byOffset(readAll(t, p))
	for _, off := range []uint64{0, 4, 7} {
		if _, ok := got[off]; !ok {
			t.Fatalf("survivor offset %d missing", off)
		}
	}
	for _, off := range []uint64{1, 2, 3, 5, 6} {
		if _, ok := got[off]; ok {
			t.Fatalf("offset %d should be compacted away", off)
		}
	}
	// A read starting inside a hole skips forward to the next survivor.
	recs, err := p.Read(1, 100, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) == 0 || recs[0].Offset != 4 {
		t.Fatalf("read from hole: first offset = %d, want 4", recs[0].Offset)
	}
	// Appends continue monotonically after the holes.
	base := appendBatch(t, p, "n", pad("1"), "n", pad("2"), "n", pad("3"), "n", pad("4"))
	if base != hwmBefore {
		t.Fatalf("append base = %d, want %d (monotonic)", base, hwmBefore)
	}
}

// pad makes a value long enough that a 4-record batch always crosses
// the 128-byte segment cap in tinyOpts, giving deterministic segment
// boundaries.
func pad(s string) string {
	for len(s) < 40 {
		s += "_"
	}
	return s
}

func TestCompactKeepsKeylessRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	appendBatch(t, p, "", "kless-0", "a", "1", "", "kless-1", "a", "2")
	appendBatch(t, p, "", "kless-2", "a", "3", "b", "1", "", "kless-3")
	appendBatch(t, p, "x", "1", "y", "1", "z", "1", "w", "1") // active

	stats, err := p.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got := byOffset(readAll(t, p))
	for _, off := range []uint64{0, 2, 4, 7} { // every keyless record
		if _, ok := got[off]; !ok {
			t.Fatalf("keyless offset %d was dropped; keyless records are incompatible with compaction and must stay", off)
		}
	}
	if stats.RecordsDropped != 2 { // only the two superseded "a" values
		t.Fatalf("dropped %d records, want 2", stats.RecordsDropped)
	}
}

func TestCompactEmptyValueIsNotTombstone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Documented rule: there are no tombstones in v1. The record format
	// cannot tell "null" from "empty", so an empty value is just the
	// latest value for the key and is KEPT like any other. The padded
	// non-empty values push the batch past the segment cap so it lands
	// in its own (closed) segment.
	appendBatch(t, p, "a", pad("1"), "a", "", "b", pad("1"), "b", "")
	appendBatch(t, p, "x", pad("1"), "y", pad("1"), "z", pad("1"), "w", pad("1")) // active
	if p.SegmentCount() != 2 {
		t.Fatalf("segments = %d, want 2", p.SegmentCount())
	}

	stats, err := p.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got := byOffset(readAll(t, p))
	r, ok := got[1]
	if !ok {
		t.Fatal("latest a (empty value) was dropped; empty values are not tombstones")
	}
	if len(r.Value) != 0 {
		t.Fatalf("offset 1 value = %q, want empty", r.Value)
	}
	if _, ok := got[3]; !ok {
		t.Fatal("latest b (empty value) was dropped")
	}
	if stats.RecordsDropped != 2 {
		t.Fatalf("dropped %d, want 2 (only the superseded non-empty values)", stats.RecordsDropped)
	}
}

func TestCompactNeverTouchesActiveSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	appendBatch(t, p, "a", "closed-1", "b", "closed-1", "c", "closed-1", "d", "closed-1")
	// The active segment is full of duplicate keys — all must survive.
	appendBatch(t, p, "a", "active-1", "a", "active-2", "a", "active-3", "b", "active-1")
	if p.SegmentCount() < 2 {
		t.Fatalf("want >= 2 segments, got %d", p.SegmentCount())
	}

	stats, err := p.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got := byOffset(readAll(t, p))
	for _, off := range []uint64{4, 5, 6, 7} {
		if _, ok := got[off]; !ok {
			t.Fatalf("active-segment offset %d missing", off)
		}
	}
	if stats.RecordsDropped != 0 {
		t.Fatalf("dropped %d records, want 0 (no duplicates inside closed segments here)", stats.RecordsDropped)
	}
}

func TestCompactZeroSurvivorSegmentDeleted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Segment 0: only keys a,b,c,d. Segment 1: the same keys with newer
	// values. Segment 0 ends up with zero survivors and must be deleted.
	appendBatch(t, p, "a", "1", "b", "1", "c", "1", "d", "1") // 0-3
	appendBatch(t, p, "a", "2", "b", "2", "c", "2", "d", "2") // 4-7
	appendBatch(t, p, "e", "1", "f", "1", "g", "1", "h", "1") // active
	segsBefore := p.SegmentCount()

	stats, err := p.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if stats.SegmentsDeleted != 1 {
		t.Fatalf("SegmentsDeleted = %d, want 1 (zero survivors)", stats.SegmentsDeleted)
	}
	if p.SegmentCount() != segsBefore-1 {
		t.Fatalf("SegmentCount = %d, want %d", p.SegmentCount(), segsBefore-1)
	}
	// The first surviving segment now starts at offset 4.
	if got := p.FirstOffset(); got != 4 {
		t.Fatalf("FirstOffset = %d, want 4", got)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentLogName(0))); !os.IsNotExist(err) {
		t.Fatalf("zero-survivor segment file still on disk (err=%v)", err)
	}
	// Reads below the new low-water mark get OFFSET_OUT_OF_RANGE.
	if _, err := p.Read(0, 10, 1<<20); err == nil {
		t.Fatal("Read(0) after zero-survivor deletion should fail")
	}
	got := byOffset(readAll(t, p))
	if len(got) != 8 { // offsets 4-11
		t.Fatalf("survivors = %d, want 8", len(got))
	}
}

func TestCompactReopenAndIndexRebuild(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}

	fillCompactable(t, p, 4)
	hwm := p.HighWatermark()
	stats, err := p.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if stats.RecordsDropped == 0 {
		t.Fatal("nothing dropped")
	}
	want := readAll(t, p)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// The marker file must exist now.
	if _, err := os.Stat(filepath.Join(dir, compactedMarkerName)); err != nil {
		t.Fatalf("compaction marker missing: %v", err)
	}
	// Delete a compacted segment's index to force a rebuild at open —
	// with offset gaps, only a gap-tolerant rebuild can succeed.
	bases, err := listSegmentBases(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, segmentIndexName(bases[0]))); err != nil {
		t.Fatal(err)
	}

	p2, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	defer p2.Close()
	if p2.HighWatermark() != hwm {
		t.Fatalf("hwm after reopen = %d, want %d", p2.HighWatermark(), hwm)
	}
	got := readAll(t, p2)
	if len(got) != len(want) {
		t.Fatalf("read %d records after reopen, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].Offset != want[i].Offset || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("record %d: got (%d,%q), want (%d,%q)", i,
				got[i].Offset, got[i].Value, want[i].Offset, want[i].Value)
		}
	}
	// A second compaction pass finds nothing to rewrite.
	stats2, err := p2.Compact(context.Background())
	if err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	if stats2.SegmentsRewritten != 0 || stats2.RecordsDropped != 0 {
		t.Fatalf("second pass rewrote %+v; repeated compaction must be a no-op", stats2)
	}
}

func TestCompactCrashBeforeSwapDiscardsSwapFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	appendBatch(t, p, "a", "1", "b", "1", "c", "1", "d", "1") // segment 0
	appendBatch(t, p, "e", "1", "f", "1", "g", "1", "h", "1") // active
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash AFTER writing swap files but BEFORE touching the
	// real ones: both pairs present on disk.
	if err := os.WriteFile(filepath.Join(dir, segmentLogSwapName(0)), []byte("garbage swap"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, segmentIndexSwapName(0)), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	p2, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	// The old pair is authoritative: the original data is intact.
	got := byOffset(readAll(t, p2))
	if len(got) != 8 {
		t.Fatalf("read %d records, want 8 (original data intact)", len(got))
	}
	// Swap leftovers are gone.
	if _, err := os.Stat(filepath.Join(dir, segmentLogSwapName(0))); !os.IsNotExist(err) {
		t.Fatalf("swap file survived boot resolution (err=%v)", err)
	}
}

func TestCompactCrashMidSwapCompletesSwap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	appendBatch(t, p, "a", "1", "a", "2", "a", "3", "a", "4") // segment 0: offsets 0-3
	appendBatch(t, p, "b", "1", "c", "1", "d", "1", "e", "1") // active
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash MID-SWAP: the old pair of segment 0 is gone, the
	// compacted swap pair waits to be renamed, the marker is durable
	// (phase 4 writes it first). The compacted content keeps only the
	// latest "a" (offset 3, value "4").
	survivor := Record{Offset: 3, TimestampMs: time.Now().UnixMilli(), Key: []byte("a"), Value: []byte("4")}
	buf, err := EncodeRecord(nil, &survivor)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{segmentLogName(0), segmentIndexName(0)} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, segmentLogSwapName(0)), buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, segmentIndexSwapName(0)), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeMarker(dir); err != nil {
		t.Fatal(err)
	}

	p2, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatalf("reopen must complete the interrupted swap: %v", err)
	}
	defer p2.Close()
	got := byOffset(readAll(t, p2))
	// Segment 0 now holds exactly one survivor at its original offset.
	r, ok := got[3]
	if !ok || string(r.Value) != "4" {
		t.Fatalf("survivor at offset 3 = %v, want value \"4\"", ok)
	}
	for _, off := range []uint64{0, 1, 2} {
		if _, ok := got[off]; ok {
			t.Fatalf("offset %d should be compacted away", off)
		}
	}
	// The swap completed: no .swap files remain.
	if _, err := os.Stat(filepath.Join(dir, segmentLogSwapName(0))); !os.IsNotExist(err) {
		t.Fatalf("swap file left behind (err=%v)", err)
	}
	// New appends land after the hole, monotonic as ever.
	base := appendBatch(t, p2, "n", "1", "n", "2", "n", "3", "n", "4")
	if base != 8 {
		t.Fatalf("append base = %d, want 8", base)
	}
}

func TestCompactAbortsOnTooManyKeys(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	appendBatch(t, p, "k1", "1", "k2", "1", "k3", "1", "k4", "1")
	appendBatch(t, p, "k5", "1", "k6", "1", "k7", "1", "k8", "1")

	old := maxCompactionKeys
	maxCompactionKeys = 2
	defer func() { maxCompactionKeys = old }()

	before := readAll(t, p)
	if _, err := p.Compact(context.Background()); err == nil {
		t.Fatal("Compact should abort past the key cap")
	}
	// Aborted compaction changes nothing.
	after := readAll(t, p)
	if len(before) != len(after) {
		t.Fatalf("aborted compaction changed data: %d → %d records", len(before), len(after))
	}
	for _, name := range []string{segmentLogSwapName(0), segmentIndexSwapName(0)} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("aborted compaction left %s behind", name)
		}
	}
}

func TestSweepOnceRunsCompaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := OpenStore(dir, tinyOpts(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.CreateTopic("state", 1); err != nil {
		t.Fatal(err)
	}
	topic, _ := store.Topic("state")
	p := topic.Partitions[0]
	fillCompactable(t, p, 4)

	var observed []CleanupStats
	store.SweepOnce(context.Background(),
		func(name string) Policy { return Policy{Compact: true} },
		nil,
		func(cs CleanupStats) { observed = append(observed, cs) })
	if len(observed) != 1 {
		t.Fatalf("observer got %d stats, want 1", len(observed))
	}
	if observed[0].Compaction.RecordsDropped == 0 {
		t.Fatal("sweep with Compact policy dropped nothing")
	}
	// The partition still serves correct data afterwards.
	got := readAll(t, p)
	if len(got) == 0 {
		t.Fatal("no records after compaction sweep")
	}
}

func TestCompactConcurrentWithAppendAndRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "state", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	fillCompactable(t, p, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// One producer hammering appends (may trigger rotations).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			recs := []Record{
				{Key: []byte(fmt.Sprintf("k%d", i%50)), Value: []byte(fmt.Sprintf("v%d", i))},
				{Key: []byte(fmt.Sprintf("k%d", (i+1)%50)), Value: []byte(fmt.Sprintf("v%d", i))},
			}
			if _, err := p.Append(recs); err != nil {
				select {
				case errCh <- fmt.Errorf("append: %w", err):
				default:
				}
				return
			}
		}
	}()
	// One reader hammering reads.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if _, err := p.Read(p.FirstOffset(), 100, 1<<20); err != nil {
				select {
				case errCh <- fmt.Errorf("read: %w", err):
				default:
				}
				return
			}
		}
	}()
	// Compaction passes while the others work.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if _, err := p.Compact(ctx); err != nil && ctx.Err() == nil {
				select {
				case errCh <- fmt.Errorf("compact: %w", err):
				default:
				}
				return
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	// Data still consistent at the end.
	recs := readAll(t, p)
	for i := 1; i < len(recs); i++ {
		if recs[i].Offset <= recs[i-1].Offset {
			t.Fatalf("offsets not increasing at %d: %d after %d", i, recs[i].Offset, recs[i-1].Offset)
		}
	}
}

func BenchmarkCompactPartition(b *testing.B) {
	// Measures the I/O cost of one full compaction pass (offset-map scan
	// + survivor rewrite + swap) over a partition with heavy key reuse.
	dir := b.TempDir()
	const keys = 1000
	const batchesPerSeg = 250 // 4 records each ≈ 1000 records per segment
	setup := func() *Partition {
		p, err := OpenPartition(dir, "bench", 0, Options{MaxSegmentBytes: 512 << 10, IndexIntervalBytes: 4096}, nil)
		if err != nil {
			b.Fatal(err)
		}
		i := 0
		for p.SegmentCount() < 5 { // 4 closed segments + active
			recs := make([]Record, 0, batchesPerSeg*4)
			for j := 0; j < batchesPerSeg*4; j++ {
				recs = append(recs, Record{
					Key:   []byte(fmt.Sprintf("key-%d", (i+j)%keys)),
					Value: []byte(fmt.Sprintf("value-%d-payload-padding", i+j)),
				})
			}
			if _, err := p.Append(recs); err != nil {
				b.Fatal(err)
			}
			i += len(recs)
		}
		return p
	}
	p := setup()
	var totalBytes int64
	p.mu.RLock()
	for _, s := range p.segments[:len(p.segments)-1] {
		totalBytes += s.size
	}
	p.mu.RUnlock()
	_ = p.Close()

	b.SetBytes(totalBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Fresh copy of the same data each iteration: compaction is
		// destructive, so we rebuild the partition between runs.
		p, err := OpenPartition(dir, "bench", 0, Options{MaxSegmentBytes: 512 << 10, IndexIntervalBytes: 4096}, nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, err := p.Compact(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if stats.RecordsDropped == 0 && i == 0 {
			b.Fatal("benchmark dropped nothing; data setup is wrong")
		}
		_ = p.Close()
		// Restore duplicate-rich data for the next iteration by
		// rewriting the partition dir from scratch.
		for _, e := range mustReadDir(b, dir) {
			_ = os.Remove(filepath.Join(dir, e))
		}
		_ = os.Remove(filepath.Join(dir, compactedMarkerName))
		p = setup()
		_ = p.Close()
		b.StartTimer()
	}
}

func mustReadDir(b *testing.B, dir string) []string {
	b.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
