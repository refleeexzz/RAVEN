package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// tinyOpts rotates segments after roughly a handful of records so tests
// get many segments with few appends.
func tinyOpts() Options {
	return Options{MaxSegmentBytes: 128, IndexIntervalBytes: 64}
}

// appendTimed appends n records with an explicit timestamp and returns
// the base offset assigned.
func appendTimed(t *testing.T, p *Partition, n int, ts time.Time) uint64 {
	t.Helper()
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{
			TimestampMs: ts.UnixMilli(),
			Key:         []byte(fmt.Sprintf("k%d", i)),
			Value:       []byte(fmt.Sprintf("v%d", i)),
		}
	}
	base, err := p.Append(recs)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return base
}

// fillSegments appends batches until the partition has at least wantSegs
// segments and returns the base offsets of the segment files on disk.
func fillSegments(t *testing.T, p *Partition, dir string, wantSegs int, ts time.Time) []uint64 {
	t.Helper()
	for i := 0; i < 200; i++ {
		appendTimed(t, p, 4, ts)
		if p.SegmentCount() >= wantSegs {
			bases, err := listSegmentBases(dir)
			if err != nil {
				t.Fatalf("listSegmentBases: %v", err)
			}
			return bases
		}
	}
	t.Fatalf("never reached %d segments", wantSegs)
	return nil
}

func TestRetentionTimeBasedDeletesOldSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	old := time.Now().Add(-2 * time.Hour)
	bases := fillSegments(t, p, dir, 4, old)
	stats, err := p.ApplyRetention(Policy{RetentionMaxAge: time.Hour}, math.MaxUint64, time.Now())
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	// Every segment is expired, but the active one must survive.
	wantDeleted := len(bases) - 1
	if stats.SegmentsDeleted != wantDeleted {
		t.Fatalf("deleted %d segments, want %d", stats.SegmentsDeleted, wantDeleted)
	}
	if stats.BytesFreed <= 0 {
		t.Fatalf("BytesFreed = %d, want > 0", stats.BytesFreed)
	}
	if got := p.SegmentCount(); got != 1 {
		t.Fatalf("SegmentCount = %d, want 1 (active survives)", got)
	}
	if got := p.FirstOffset(); got != bases[len(bases)-1] {
		t.Fatalf("FirstOffset = %d, want %d (active segment base)", got, bases[len(bases)-1])
	}
	// The surviving segment must still be fully readable.
	recs, err := p.Read(bases[len(bases)-1], 100, 1<<20)
	if err != nil || len(recs) == 0 {
		t.Fatalf("Read active segment: len=%d err=%v", len(recs), err)
	}
}

func TestRetentionSizeBasedDeletesOldest(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	now := time.Now()
	bases := fillSegments(t, p, dir, 5, now)
	// Fresh data: time-based rule must NOT fire. Cap the partition at
	// roughly two segments' worth of bytes.
	segBytes := p.segments[len(p.segments)-1].size
	stats, err := p.ApplyRetention(Policy{RetentionMaxBytes: 2 * segBytes}, math.MaxUint64, now)
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatal("size-based retention deleted nothing")
	}
	if stats.SegmentsDeleted >= len(bases) {
		t.Fatalf("deleted %d of %d segments: active must survive", stats.SegmentsDeleted, len(bases))
	}
	// The oldest segments go first: new FirstOffset is the base of the
	// first surviving segment.
	first := p.FirstOffset()
	if first <= bases[0] {
		t.Fatalf("FirstOffset = %d, want > %d (oldest deleted)", first, bases[0])
	}
	for _, b := range bases {
		if b < first {
			if _, err := os.Stat(filepath.Join(dir, segmentLogName(b))); !os.IsNotExist(err) {
				t.Fatalf("segment %d still on disk (err=%v)", b, err)
			}
		}
	}
	// Total remaining log bytes must fit the cap (one segment may stay
	// over-cap by itself — the active one is untouchable).
	var remaining int64
	for _, s := range p.segments {
		remaining += s.size
	}
	if remaining > 2*segBytes+segBytes {
		t.Fatalf("remaining bytes %d way over cap %d", remaining, 2*segBytes)
	}
}

func TestRetentionNeverDeletesActiveSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// One single (active) segment, everything ancient, size cap tiny:
	// nothing may be deleted.
	old := time.Now().Add(-24 * time.Hour)
	appendTimed(t, p, 10, old)
	hwmBefore := p.HighWatermark()
	stats, err := p.ApplyRetention(Policy{RetentionMaxAge: time.Hour, RetentionMaxBytes: 1}, math.MaxUint64, time.Now())
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if stats.SegmentsDeleted != 0 {
		t.Fatalf("deleted %d segments, want 0 (only the active segment exists)", stats.SegmentsDeleted)
	}
	if p.HighWatermark() != hwmBefore {
		t.Fatalf("hwm moved: %d → %d", hwmBefore, p.HighWatermark())
	}
	recs, err := p.Read(0, 100, 1<<20)
	if err != nil || len(recs) != 10 {
		t.Fatalf("Read: len=%d err=%v, want 10 records intact", len(recs), err)
	}
}

func TestRetentionRespectsMinSafeOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	old := time.Now().Add(-2 * time.Hour)
	bases := fillSegments(t, p, dir, 5, old)
	// A group has committed offset bases[2]+1: it still needs segment
	// bases[1] (which contains that offset... no: bases[2] is where the
	// offset lives). Everything strictly below bases[2] may go; segment
	// bases[2] itself contains offsets >= the commit and must stay.
	minSafe := bases[2] + 1
	stats, err := p.ApplyRetention(Policy{RetentionMaxAge: time.Hour}, minSafe, time.Now())
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if stats.SegmentsDeleted != 2 {
		t.Fatalf("deleted %d segments, want 2 (bases %d and %d)", stats.SegmentsDeleted, bases[0], bases[1])
	}
	if got := p.FirstOffset(); got != bases[2] {
		t.Fatalf("FirstOffset = %d, want %d", got, bases[2])
	}
	// The protected record must still be readable.
	recs, err := p.Read(minSafe, 10, 1<<20)
	if err != nil || len(recs) == 0 || recs[0].Offset != minSafe {
		t.Fatalf("Read(%d): len=%d err=%v", minSafe, len(recs), err)
	}

	// Zero protection (no group committed): now everything closed goes.
	stats, err = p.ApplyRetention(Policy{RetentionMaxAge: time.Hour}, math.MaxUint64, time.Now())
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatal("without a committed offset, retention should delete freely")
	}
	if p.SegmentCount() != 1 {
		t.Fatalf("SegmentCount = %d, want 1", p.SegmentCount())
	}
}

func TestRetentionReopenAfterDeletion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-2 * time.Hour)
	bases := fillSegments(t, p, dir, 4, old)
	hwm := p.HighWatermark()
	if _, err := p.ApplyRetention(Policy{RetentionMaxAge: time.Hour}, math.MaxUint64, time.Now()); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	// Recovery after cleanup: offsets stay monotonic, nothing rewinds.
	if p2.HighWatermark() != hwm {
		t.Fatalf("hwm after reopen = %d, want %d", p2.HighWatermark(), hwm)
	}
	if got := p2.FirstOffset(); got != bases[len(bases)-1] {
		t.Fatalf("FirstOffset after reopen = %d, want %d", got, bases[len(bases)-1])
	}
	// Fetching a deleted offset returns OFFSET_OUT_OF_RANGE (documented
	// rule, same as Kafka's behavior for offsets below the low-water
	// mark).
	if _, err := p2.Read(bases[0], 10, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read deleted offset: err=%v, want ErrOffsetOutOfRange", err)
	}
	// Fetching at the low-water mark works.
	recs, err := p2.Read(bases[len(bases)-1], 100, 1<<20)
	if err != nil || len(recs) == 0 {
		t.Fatalf("Read at low-water mark: len=%d err=%v", len(recs), err)
	}
	// Appends continue monotonically from the old high-water mark.
	base := appendTimed(t, p2, 3, time.Now())
	if base != hwm {
		t.Fatalf("append base = %d, want %d (monotonic across cleanup)", base, hwm)
	}
}

func TestRetentionFreedBytesAccounting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, dir, 4, time.Now())
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// Measure with closed files: on Windows a directory listing reports
	// stale (zero) sizes for files that are still open, so both
	// measurement points need the partition closed.
	before := dirUsage(t, dir)

	p, err = OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	// Size-based retention: independent of timestamps and mtimes.
	stats, err := p.ApplyRetention(Policy{RetentionMaxBytes: 1}, math.MaxUint64, time.Now())
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatal("expected deletions with a 1-byte cap")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	after := dirUsage(t, dir)
	if stats.BytesFreed != before-after {
		t.Fatalf("BytesFreed = %d, real disk drop = %d", stats.BytesFreed, before-after)
	}
}

// dirUsage sums .log/.index sizes in dir straight from the directory.
func dirUsage(t *testing.T, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".index") {
			if fi, err := e.Info(); err == nil {
				total += fi.Size()
			}
		}
	}
	return total
}

func TestRetentionDisabledByDefault(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, tinyOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	old := time.Now().Add(-2 * time.Hour)
	fillSegments(t, p, dir, 4, old)
	// Zero policy: both rules off. Nothing may happen, ever.
	stats, err := p.ApplyRetention(Policy{}, math.MaxUint64, time.Now())
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if stats.SegmentsDeleted != 0 || stats.BytesFreed != 0 {
		t.Fatalf("zero policy deleted %+v", stats)
	}
}

func TestSweepOnceStoreLevel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := OpenStore(dir, tinyOpts(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	old := time.Now().Add(-2 * time.Hour)
	for _, name := range []string{"hot", "cold"} {
		if _, err := store.CreateTopic(name, 1); err != nil {
			t.Fatal(err)
		}
		topic, _ := store.Topic(name)
		fillSegments(t, topic.Partitions[0], filepath.Join(dir, "topics", name, "partition-0"), 4, old)
	}

	policies := map[string]Policy{
		"cold": {RetentionMaxAge: time.Hour},
		"hot":  {}, // retention off for this topic
	}
	var observed []CleanupStats
	store.SweepOnce(context.Background(),
		func(topic string) Policy { return policies[topic] },
		nil, // no consumer group commits anywhere
		func(cs CleanupStats) { observed = append(observed, cs) },
	)

	if len(observed) != 2 {
		t.Fatalf("observer got %d stats, want 2 (one per partition)", len(observed))
	}
	byTopic := map[string]CleanupStats{}
	for _, cs := range observed {
		byTopic[cs.Topic] = cs
	}
	if byTopic["cold"].Retention.SegmentsDeleted == 0 {
		t.Fatal("cold topic: retention deleted nothing")
	}
	if byTopic["hot"].Retention.SegmentsDeleted != 0 {
		t.Fatalf("hot topic: deleted %d segments with retention off", byTopic["hot"].Retention.SegmentsDeleted)
	}
	cold, _ := store.Topic("cold")
	if cold.Partitions[0].SegmentCount() != 1 {
		t.Fatalf("cold segments = %d, want 1", cold.Partitions[0].SegmentCount())
	}
	hot, _ := store.Topic("hot")
	if hot.Partitions[0].SegmentCount() < 4 {
		t.Fatalf("hot segments = %d, want >= 4", hot.Partitions[0].SegmentCount())
	}
}

func TestSweepOnceHonorsSafetyMap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := OpenStore(dir, tinyOpts(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.CreateTopic("jobs", 1); err != nil {
		t.Fatal(err)
	}
	topic, _ := store.Topic("jobs")
	p := topic.Partitions[0]
	partDir := filepath.Join(dir, "topics", "jobs", "partition-0")
	old := time.Now().Add(-2 * time.Hour)
	bases := fillSegments(t, p, partDir, 4, old)

	// Safety map: smallest committed offset sits in segment bases[2]:
	// segments before it may go, segment bases[2] must stay.
	safety := func() map[string]map[int32]uint64 {
		return map[string]map[int32]uint64{"jobs": {0: bases[2]}}
	}
	store.SweepOnce(context.Background(),
		func(string) Policy { return Policy{RetentionMaxAge: time.Hour} },
		safety, nil)
	if got := p.FirstOffset(); got != bases[2] {
		t.Fatalf("FirstOffset = %d, want %d (protected by committed offset)", got, bases[2])
	}
}

func TestRunCleanerStopsOnContext(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := OpenStore(dir, tinyOpts(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("jobs", 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.RunCleaner(ctx, 5*time.Millisecond,
			func(string) Policy { return Policy{RetentionMaxAge: time.Hour} },
			nil, nil)
	}()
	// Let a few sweeps run, then cancel: the loop must exit promptly —
	// this is what makes broker shutdown graceful.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunCleaner did not stop within 2s of cancel")
	}
}
