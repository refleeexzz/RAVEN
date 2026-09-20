package storage

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func openTestPartition(t *testing.T, maxSeg int64) *Partition {
	t.Helper()
	p, err := OpenPartition(t.TempDir(), "t", 0, Options{
		MaxSegmentBytes:    maxSeg,
		IndexIntervalBytes: 64,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func replicaBatch(base uint64, n int) []Record {
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{Offset: base + uint64(i), TimestampMs: 1000 + int64(i), Value: []byte(strings.Repeat("x", 8))}
	}
	return recs
}

func TestAppendReplicaPreservesOffsets(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	if err := p.AppendReplica(replicaBatch(0, 10)); err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 10 {
		t.Fatalf("hwm: got %d want 10", hwm)
	}
	recs, err := p.Read(0, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 {
		t.Fatalf("read %d records, want 10", len(recs))
	}
	for i, r := range recs {
		if r.Offset != uint64(i) || r.TimestampMs != 1000+int64(i) {
			t.Fatalf("rec %d: offset=%d ts=%d", i, r.Offset, r.TimestampMs)
		}
	}
}

func TestAppendReplicaRejectsDivergence(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	if err := p.AppendReplica(replicaBatch(0, 5)); err != nil {
		t.Fatal(err)
	}
	// Batch starts past the local end.
	err := p.AppendReplica(replicaBatch(7, 3))
	if !errors.Is(err, ErrDivergedAppend) {
		t.Fatalf("got %v want ErrDivergedAppend", err)
	}
	// Batch starts before the local end (overlap).
	err = p.AppendReplica(replicaBatch(3, 3))
	if !errors.Is(err, ErrDivergedAppend) {
		t.Fatalf("overlap: got %v want ErrDivergedAppend", err)
	}
	// Gap inside the batch.
	bad := replicaBatch(5, 3)
	bad[1].Offset = 99
	err = p.AppendReplica(bad)
	if !errors.Is(err, ErrDivergedAppend) {
		t.Fatalf("gap: got %v want ErrDivergedAppend", err)
	}
	// The failed batches must not have moved the log.
	if hwm := p.HighWatermark(); hwm != 5 {
		t.Fatalf("hwm after failed appends: got %d want 5", hwm)
	}
}

func TestAppendReplicaEmptyIsNoop(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	if err := p.AppendReplica(nil); err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 0 {
		t.Fatalf("hwm: got %d", hwm)
	}
}

func TestAppendReplicaRotatesSegments(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 128) // tiny segments
	// Rotation happens between batches, so feed several small ones.
	for base := uint64(0); base < 30; base += 2 {
		if err := p.AppendReplica(replicaBatch(base, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if p.SegmentCount() < 2 {
		t.Fatalf("want rotation, got %d segment(s)", p.SegmentCount())
	}
	recs, err := p.Read(0, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 30 {
		t.Fatalf("read %d, want 30", len(recs))
	}
}

func TestTruncateTailCut(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	if err := p.AppendReplica(replicaBatch(0, 100)); err != nil {
		t.Fatal(err)
	}
	if err := p.TruncateTo(60); err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 60 {
		t.Fatalf("hwm: got %d want 60", hwm)
	}
	recs, err := p.Read(0, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 60 {
		t.Fatalf("read %d, want 60", len(recs))
	}
	// Reading at/above the new end behaves like the normal end-of-log.
	if _, err := p.Read(61, 10, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read past new end: got %v", err)
	}
	// Re-applying the divergent records at 60 must work (log matching).
	if err := p.AppendReplica(replicaBatch(60, 5)); err != nil {
		t.Fatalf("re-append after truncate: %v", err)
	}
	if hwm := p.HighWatermark(); hwm != 65 {
		t.Fatalf("hwm after re-append: got %d want 65", hwm)
	}
}

func TestTruncateDeletesLaterSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "t", 0, Options{MaxSegmentBytes: 64, IndexIntervalBytes: 32}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	for base := uint64(0); base < 40; base += 2 {
		if err := p.AppendReplica(replicaBatch(base, 2)); err != nil {
			t.Fatal(err)
		}
	}
	segsBefore := p.SegmentCount()
	if segsBefore < 3 {
		t.Fatalf("setup: want >=3 segments, got %d", segsBefore)
	}
	if err := p.TruncateTo(10); err != nil {
		t.Fatal(err)
	}
	if got := p.SegmentCount(); got >= segsBefore {
		t.Fatalf("segments not deleted: %d -> %d", segsBefore, got)
	}
	// On-disk listing must agree: no .log files above the cut.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	logs := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logs++
		}
	}
	if logs != p.SegmentCount() {
		t.Fatalf("disk holds %d logs, in-memory %d", logs, p.SegmentCount())
	}
	// Crash-recovery view must agree too.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2, err := OpenPartition(dir, "t", 0, Options{MaxSegmentBytes: 64, IndexIntervalBytes: 32}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p2.Close() }()
	if hwm := p2.HighWatermark(); hwm != 10 {
		t.Fatalf("reopened hwm: got %d want 10", hwm)
	}
}

func TestTruncateNoopAtEnd(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	if err := p.AppendReplica(replicaBatch(0, 10)); err != nil {
		t.Fatal(err)
	}
	if err := p.TruncateTo(10); err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 10 {
		t.Fatalf("hwm: got %d", hwm)
	}
}

func TestTruncateToZero(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	if err := p.AppendReplica(replicaBatch(0, 50)); err != nil {
		t.Fatal(err)
	}
	if err := p.TruncateTo(0); err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 0 {
		t.Fatalf("hwm: got %d want 0", hwm)
	}
	recs, err := p.Read(0, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("read %d after wipe", len(recs))
	}
	if err := p.AppendReplica(replicaBatch(0, 3)); err != nil {
		t.Fatal(err)
	}
}

func TestTruncateForwardReset(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	if err := p.AppendReplica(replicaBatch(0, 50)); err != nil {
		t.Fatal(err)
	}
	// The leader deleted records 50..499 (retention) before we fetched
	// them: jump forward.
	if err := p.TruncateTo(500); err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 500 {
		t.Fatalf("hwm: got %d want 500", hwm)
	}
	if first := p.FirstOffset(); first != 500 {
		t.Fatalf("first offset: got %d want 500", first)
	}
	if err := p.AppendReplica(replicaBatch(500, 2)); err != nil {
		t.Fatal(err)
	}
	recs, err := p.Read(500, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("read %d want 2", len(recs))
	}
}

func TestTruncateThenNormalAppend(t *testing.T) {
	t.Parallel()
	p := openTestPartition(t, 1<<20)
	// The client-produce path must keep working after a truncate.
	if _, err := p.Append([]Record{{Value: []byte("a")}, {Value: []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	if err := p.TruncateTo(1); err != nil {
		t.Fatal(err)
	}
	base, err := p.Append([]Record{{Value: []byte("c")}})
	if err != nil {
		t.Fatal(err)
	}
	if base != 1 {
		t.Fatalf("append base after truncate: got %d want 1", base)
	}
}
