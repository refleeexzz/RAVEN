package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func testOpts() Options {
	return Options{MaxSegmentBytes: 64 << 20, IndexIntervalBytes: 4096}
}

func appendRecords(t *testing.T, p *Partition, n int, valuePrefix string) uint64 {
	t.Helper()
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{Key: []byte(fmt.Sprintf("key-%s-%d", valuePrefix, i)), Value: []byte(fmt.Sprintf("val-%s-%d", valuePrefix, i))}
	}
	base, err := p.Append(recs)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return base
}

func TestPartitionWriteReadReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	base := appendRecords(t, p, 10, "a")
	if base != 0 {
		t.Fatalf("first base offset: got %d want 0", base)
	}
	base = appendRecords(t, p, 5, "b")
	if base != 10 {
		t.Fatalf("second base offset: got %d want 10", base)
	}
	if hwm := p.HighWatermark(); hwm != 15 {
		t.Fatalf("hwm: got %d want 15", hwm)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: recovery must restore state without data loss.
	p2, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	if hwm := p2.HighWatermark(); hwm != 15 {
		t.Fatalf("hwm after reopen: got %d want 15", hwm)
	}
	recs, err := p2.Read(0, 100, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 15 {
		t.Fatalf("read %d records, want 15", len(recs))
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Fatalf("record %d has offset %d", i, r.Offset)
		}
	}
	// Middle-offset read.
	recs, err = p2.Read(10, 100, 1<<20)
	if err != nil || len(recs) != 5 || string(recs[0].Value) != "val-b-0" {
		t.Fatalf("read from 10: len=%d err=%v", len(recs), err)
	}
}

func TestPartitionCorruptTailTruncated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	appendRecords(t, p, 10, "a")
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	goodSize, err := os.Stat(filepath.Join(dir, segmentLogName(0)))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-write: garbage bytes after the good records.
	f, err := os.OpenFile(filepath.Join(dir, segmentLogName(0)), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	p2, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatalf("reopen with corrupt tail: %v", err)
	}
	defer p2.Close()
	if hwm := p2.HighWatermark(); hwm != 10 {
		t.Fatalf("hwm after truncation: got %d want 10", hwm)
	}
	st, _ := os.Stat(filepath.Join(dir, segmentLogName(0)))
	if st.Size() != goodSize.Size() {
		t.Fatalf("log not truncated: size %d, want %d", st.Size(), goodSize.Size())
	}
	// Log stays usable after recovery.
	base := appendRecords(t, p2, 3, "c")
	if base != 10 {
		t.Fatalf("append after recovery: base %d want 10", base)
	}
	recs, err := p2.Read(0, 100, 1<<20)
	if err != nil || len(recs) != 13 {
		t.Fatalf("read after recovery: len=%d err=%v", len(recs), err)
	}
}

func TestPartitionTornWriteTruncated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	appendRecords(t, p, 5, "a")
	p.Close()

	// Append half a record: valid-looking header, missing body.
	f, _ := os.OpenFile(filepath.Join(dir, segmentLogName(0)), os.O_APPEND|os.O_WRONLY, 0o644)
	half := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 'k'}
	f.Write(half)
	f.Close()

	p2, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer p2.Close()
	if hwm := p2.HighWatermark(); hwm != 5 {
		t.Fatalf("hwm: got %d want 5", hwm)
	}
	recs, err := p2.Read(0, 100, 1<<20)
	if err != nil || len(recs) != 5 {
		t.Fatalf("len=%d err=%v", len(recs), err)
	}
}

func TestPartitionBadCRCTruncated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	appendRecords(t, p, 6, "a")
	p.Close()

	// Corrupt the last record's CRC by flipping its value bytes.
	logPath := filepath.Join(dir, segmentLogName(0))
	st, _ := os.Stat(logPath)
	f, _ := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if _, err := f.WriteAt([]byte{0xff, 0xfe, 0xfd}, st.Size()-3); err != nil {
		t.Fatal(err)
	}
	f.Close()

	p2, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatalf("reopen with bad CRC: %v", err)
	}
	defer p2.Close()
	if hwm := p2.HighWatermark(); hwm != 5 {
		t.Fatalf("hwm after CRC truncation: got %d want 5", hwm)
	}
}

func TestSegmentRotation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := Options{MaxSegmentBytes: 256, IndexIntervalBytes: 64}
	p, err := OpenPartition(dir, "jobs", 0, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Each record is ~40+ bytes; 40 records must rotate several times.
	big := bytes.Repeat([]byte("x"), 32)
	for i := 0; i < 40; i++ {
		if _, err := p.Append([]Record{{Value: big}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.segments) < 2 {
		t.Fatalf("expected rotation, got %d segment(s)", len(p.segments))
	}
	if hwm := p.HighWatermark(); hwm != 40 {
		t.Fatalf("hwm %d want 40", hwm)
	}
	// Read across the segment boundary.
	recs, err := p.Read(0, 100, 1<<20)
	if err != nil || len(recs) != 40 {
		t.Fatalf("full read: len=%d err=%v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Fatalf("record %d offset %d", i, r.Offset)
		}
	}

	// Reopen: recovery sees multiple segments, next offset continues.
	p.Close()
	p2, err := OpenPartition(dir, "jobs", 0, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	base, err := p2.Append([]Record{{Value: big}})
	if err != nil || base != 40 {
		t.Fatalf("append after reopen: base=%d err=%v", base, err)
	}
}

func TestSparseIndexLookup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := Options{MaxSegmentBytes: 64 << 20, IndexIntervalBytes: 128}
	p, err := OpenPartition(dir, "jobs", 0, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 200; i++ {
		if _, err := p.Append([]Record{{Value: []byte(fmt.Sprintf("v-%03d", i))}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.segments[0].entries) < 2 {
		t.Fatalf("expected sparse index entries, got %d", len(p.segments[0].entries))
	}
	// Every offset must read back the right record regardless of index
	// granularity.
	for _, off := range []uint64{0, 1, 7, 63, 64, 127, 199} {
		recs, err := p.Read(off, 1, 1<<20)
		if err != nil || len(recs) != 1 {
			t.Fatalf("read %d: len=%d err=%v", off, len(recs), err)
		}
		if string(recs[0].Value) != fmt.Sprintf("v-%03d", off) {
			t.Fatalf("offset %d: got %q", off, recs[0].Value)
		}
	}
}

func TestReadLimits(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := OpenPartition(dir, "jobs", 0, testOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	appendRecords(t, p, 10, "lim")

	// maxRecords caps the batch.
	recs, _ := p.Read(0, 3, 1<<20)
	if len(recs) != 3 {
		t.Fatalf("maxRecords: got %d", len(recs))
	}
	// maxBytes caps too, but at least one record always comes back.
	recs, _ = p.Read(0, 100, 1)
	if len(recs) != 1 {
		t.Fatalf("maxBytes min-1: got %d", len(recs))
	}
	// Reading at hwm is empty, beyond hwm is an error.
	recs, err = p.Read(10, 100, 1<<20)
	if err != nil || len(recs) != 0 {
		t.Fatalf("read at hwm: len=%d err=%v", len(recs), err)
	}
	if _, err = p.Read(11, 100, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read beyond hwm: %v", err)
	}
}

func TestStoreCreateAndRecover(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := OpenStore(dir, testOpts(), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	topic, err := s.CreateTopic("jobs", 0)
	if err != nil {
		t.Fatal(err)
	}
	if topic.NumPartitions() != 3 {
		t.Fatalf("default partitions: got %d want 3", topic.NumPartitions())
	}
	if _, err := s.CreateTopic("jobs", 0); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	if _, err := s.CreateTopic("bad name!", 0); err == nil {
		t.Fatal("invalid name accepted")
	}
	for i := 0; i < 3; i++ {
		if _, err := topic.Partitions[i].Append([]Record{{Value: []byte("x")}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: topic rediscovered from disk with partition count intact.
	s2, err := OpenStore(dir, testOpts(), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	t2, err := s2.Topic("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if t2.NumPartitions() != 3 {
		t.Fatalf("recovered partitions: got %d want 3", t2.NumPartitions())
	}
	for i := 0; i < 3; i++ {
		if hwm := t2.Partitions[i].HighWatermark(); hwm != 1 {
			t.Fatalf("partition %d hwm %d want 1", i, hwm)
		}
	}
	if _, err := s2.Topic("nope"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("missing topic: %v", err)
	}
}

func TestPickPartition(t *testing.T) {
	t.Parallel()
	topic := &Topic{Name: "t", Partitions: make([]*Partition, 4)}
	// Keyed: deterministic, within range.
	a := topic.PickPartition([]byte("order-123"))
	b := topic.PickPartition([]byte("order-123"))
	if a != b {
		t.Fatalf("same key routed differently: %d vs %d", a, b)
	}
	if a < 0 || a >= 4 {
		t.Fatalf("partition %d out of range", a)
	}
	// Keyless: round-robin covers all partitions.
	seen := map[int32]bool{}
	for i := 0; i < 8; i++ {
		seen[topic.PickPartition(nil)] = true
	}
	if len(seen) != 4 {
		t.Fatalf("round-robin covered %d partitions, want 4", len(seen))
	}
}

func TestRecordRoundTripAndCRC(t *testing.T) {
	t.Parallel()
	r := Record{
		Offset: 42, TimestampMs: 1700000000000,
		Key: []byte("k"), Value: []byte("payload"),
		Headers: []Header{{Key: "trace", Value: []byte("abc")}},
	}
	buf, err := EncodeRecord(nil, &r)
	if err != nil {
		t.Fatal(err)
	}
	got, consumed, err := DecodeRecord(buf)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(buf) {
		t.Fatalf("consumed %d of %d", consumed, len(buf))
	}
	if got.Offset != r.Offset || got.TimestampMs != r.TimestampMs ||
		!bytes.Equal(got.Key, r.Key) || !bytes.Equal(got.Value, r.Value) ||
		len(got.Headers) != 1 || got.Headers[0].Key != "trace" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	// Flip one payload byte: CRC must fail.
	buf[len(buf)-1] ^= 0xff
	if _, _, err := DecodeRecord(buf); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt record: got %v want ErrCorrupt", err)
	}
	// Truncated record must report ErrTruncated.
	if _, _, err := DecodeRecord(buf[:len(buf)-2]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("truncated record: got %v want ErrTruncated", err)
	}
	_ = io.EOF // keep io import if unused in future edits
}
