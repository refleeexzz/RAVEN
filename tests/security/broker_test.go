// Package security holds the offensive security regression suite. This
// file covers the message broker (internal/broker): every test maps to a
// finding in docs/security/broker.md (BRKR-xx).
//
// The attacker model: an unauthenticated client with a raw TCP
// connection to the broker port. Tests talk raw wire frames (no client
// library) because a hostile client is not bound by our client code.
package security

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/storage"
)

// ---- harness (broker-prefixed names: this package is shared with the
// auth/gateway/jobs/infra auditors) ----

func brokerSecLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type brokerSecHarness struct {
	b   *broker.Broker
	dir string
}

// startSecBroker boots an in-process broker on a random port with a
// throwaway data dir. Long session timeout so the reaper never
// interferes mid-test.
func startSecBroker(t *testing.T, mutate func(*broker.Config)) *brokerSecHarness {
	t.Helper()
	dir := t.TempDir()
	cfg := broker.Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        dir,
		FsyncEvery:     50 * time.Millisecond,
		FsyncRecords:   1000,
		SessionTimeout: 30 * time.Second,
		DrainTimeout:   2 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	b, err := broker.New(cfg, brokerSecLogger(), nil)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("broker did not stop within 15s")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if addr := b.Addr(); addr != "" {
			if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
				_ = c.Close()
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("broker did not start listening in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return &brokerSecHarness{b: b, dir: dir}
}

// secDial opens a raw connection (registered for cleanup).
func secDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// secRoundTrip writes one frame and reads one response, bounded by a
// deadline so a hung broker fails the test instead of the suite.
func secRoundTrip(t *testing.T, c net.Conn, op protocol.Opcode, payload []byte, corr uint64) *protocol.Frame {
	t.Helper()
	if err := protocol.WriteFrame(c, &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: payload}); err != nil {
		t.Fatalf("write %s: %v", op, err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := protocol.ReadFrame(c)
	if err != nil {
		t.Fatalf("read %s response: %v", op, err)
	}
	if f.CorrelationID != corr {
		t.Fatalf("correlation id mismatch: sent %d got %d", corr, f.CorrelationID)
	}
	return f
}

// secJSON marshals a control payload.
func secJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// secExpectError asserts the frame is OpError with one of the codes.
func secExpectError(t *testing.T, f *protocol.Frame, codes ...string) *protocol.Error {
	t.Helper()
	if f.Opcode != protocol.OpError {
		t.Fatalf("expected ERROR frame, got %s (payload %d bytes)", f.Opcode, len(f.Payload))
	}
	e := protocol.DecodeErrorFrame(f.Payload)
	for _, c := range codes {
		if e.Code == c {
			return e
		}
	}
	t.Fatalf("error code %q, want one of %v (message: %s)", e.Code, codes, e.Message)
	return nil
}

// secExpectOK asserts the frame is a success response for op.
func secExpectOK(t *testing.T, f *protocol.Frame, op protocol.Opcode) {
	t.Helper()
	if f.Opcode == protocol.OpError {
		e := protocol.DecodeErrorFrame(f.Payload)
		t.Fatalf("expected %s, got ERROR %s: %s", op, e.Code, e.Message)
	}
	if f.Opcode != op {
		t.Fatalf("expected %s, got %s", op, f.Opcode)
	}
}

// secCreateTopic sends CREATE_TOPIC and returns the raw response frame.
func secCreateTopic(t *testing.T, c net.Conn, topic string, partitions int32, corr uint64) *protocol.Frame {
	t.Helper()
	return secRoundTrip(t, c, protocol.OpCreateTopic,
		secJSON(t, protocol.CreateTopicRequest{Topic: topic, Partitions: partitions}), corr)
}

// secJoin sends JOIN_GROUP and returns the raw response frame.
func secJoin(t *testing.T, c net.Conn, group, member string, topics []string, corr uint64) *protocol.Frame {
	t.Helper()
	return secRoundTrip(t, c, protocol.OpJoinGroup,
		secJSON(t, protocol.JoinGroupRequest{Group: group, MemberID: member, Topics: topics}), corr)
}

// secJoinOK joins successfully or fails the test; returns the generation.
func secJoinOK(t *testing.T, c net.Conn, group, member string, topics []string, corr uint64) (int32, []protocol.Assignment) {
	t.Helper()
	f := secJoin(t, c, group, member, topics, corr)
	secExpectOK(t, f, protocol.OpJoinGroup)
	var resp protocol.JoinGroupResponse
	if err := json.Unmarshal(f.Payload, &resp); err != nil {
		t.Fatalf("decode join response: %v", err)
	}
	return resp.Generation, resp.Assignments
}

// secProduce appends records to an explicit partition and returns the
// high-water mark after the batch.
func secProduce(t *testing.T, c net.Conn, topic string, partition int32, records []protocol.Message, corr uint64) uint64 {
	t.Helper()
	f := secRoundTrip(t, c, protocol.OpProduce,
		protocol.EncodeProduceRequest(&protocol.ProduceRequest{Topic: topic, Partition: partition, Records: records}), corr)
	secExpectOK(t, f, protocol.OpProduce)
	resp, err := protocol.DecodeProduceResponse(f.Payload)
	if err != nil {
		t.Fatalf("decode produce response: %v", err)
	}
	if len(resp.Results) != len(records) {
		t.Fatalf("produce results %d, want %d", len(resp.Results), len(records))
	}
	return resp.Results[len(resp.Results)-1].Offset + 1
}

// secCommit sends COMMIT_OFFSET and returns the raw response frame.
func secCommit(t *testing.T, c net.Conn, group, member, topic string, partition int32, offset uint64, gen int32, corr uint64) *protocol.Frame {
	t.Helper()
	return secRoundTrip(t, c, protocol.OpCommitOffset,
		secJSON(t, protocol.CommitOffsetRequest{
			Group: group, MemberID: member, Topic: topic,
			Partition: partition, Offset: offset, Generation: gen,
		}), corr)
}

// ---- BRKR-01: path traversal via topic names ----

// TestSecBrokerTopicTraversalRejected: every traversal flavor must be
// rejected with BAD_REQUEST, and nothing may appear on disk outside
// $DATA_DIR/topics/<name>.
func TestSecBrokerTopicTraversalRejected(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	bad := []string{
		"../../../tmp/evil",
		`..\..\..\tmp\evil`,
		"a/b",
		`a\b`,
		"/etc/passwd",
		`C:\evil`,
		"..",
		".",
		"../",
		`..\`,
		"...",    // all-dots: Windows refuses to create it (trailing dot)
		"a.",     // trailing dot: stripped by Windows, platform-dependent
		"a\x00b", // NUL byte
		"a\nb",   // log injection
		"a\rb",
		"a\tb",
		"a b",
		"",                       // empty
		strings.Repeat("x", 129), // over the 128-char cap
	}
	var corr uint64 = 100
	for _, name := range bad {
		corr++
		f := secCreateTopic(t, c, name, 1, corr)
		secExpectError(t, f, protocol.CodeBadRequest)
	}

	// Sane names still work, including dot-containing (but contained)
	// ones and the 128-char boundary.
	for _, name := range []string{"jobs", "a.b_c-1", "a..b", strings.Repeat("x", 128)} {
		corr++
		f := secCreateTopic(t, c, name, 1, corr)
		secExpectOK(t, f, protocol.OpCreateTopic)
	}

	// Disk layout: the data dir root may only hold topics/ and
	// offsets.jsonl; every child of topics/ must be a clean name.
	root, err := os.ReadDir(h.dir)
	if err != nil {
		t.Fatalf("read data dir: %v", err)
	}
	for _, e := range root {
		if e.Name() != "topics" && e.Name() != "offsets.jsonl" {
			t.Errorf("unexpected entry in data dir root: %q (traversal escaped topics/)", e.Name())
		}
	}
	topicEntries, err := os.ReadDir(filepath.Join(h.dir, "topics"))
	if err != nil {
		t.Fatalf("read topics dir: %v", err)
	}
	for _, e := range topicEntries {
		n := e.Name()
		if n == "." || n == ".." || strings.ContainsAny(n, "/\\\x00\n\r\t ") {
			t.Errorf("unsafe topic dir on disk: %q", n)
		}
	}
}

// TestSecStoreDotNamesEscape: store-level proof. "." and ".." pass the
// character whitelist but resolve outside topics/ after filepath.Clean.
// ".." lands partitions in the data-dir root; "." lands them directly in
// topics/ (where the next boot re-opens "partition-0" as a topic with
// zero partitions — a later PRODUCE to it divides by zero and panics).
func TestSecStoreDotNamesEscape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := storage.OpenStore(dir, storage.Options{MaxSegmentBytes: 1 << 20, IndexIntervalBytes: 4096}, 3, brokerSecLogger())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	for _, name := range []string{"..", ".", "..."} {
		_, err := s.CreateTopic(name, 1)
		if !errors.Is(err, storage.ErrInvalidTopicName) {
			t.Errorf("CreateTopic(%q): got %v, want ErrInvalidTopicName", name, err)
		}
	}
	if _, statErr := os.Stat(filepath.Join(dir, "partition-0")); !os.IsNotExist(statErr) {
		t.Error("topic \"..\" created partition-0 in the data-dir root")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "topics", "partition-0")); !os.IsNotExist(statErr) {
		t.Error("topic \".\" created partition-0 inside topics/ (becomes a phantom topic on reboot)")
	}

	// A contained dotted name is still fine.
	if _, err := s.CreateTopic("a..b", 1); err != nil {
		t.Errorf("CreateTopic(\"a..b\"): %v", err)
	}
}

// TestSecStoreRejectsPartitionlessTopic: defense in depth. A topic dir
// without any partition-N subdirectory is corrupt (it can only appear
// via the "." traversal or manual edits). Opening it must fail loudly at
// boot instead of exposing a zero-partition topic whose PickPartition
// divides by zero on the first PRODUCE.
func TestSecStoreRejectsPartitionlessTopic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "topics", "phantom"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := storage.OpenStore(dir, storage.Options{MaxSegmentBytes: 1 << 20, IndexIntervalBytes: 4096}, 3, brokerSecLogger())
	if err == nil {
		t.Fatal("OpenStore accepted a partitionless topic dir (zero-partition topic is a divide-by-zero time bomb)")
	}
	if !strings.Contains(err.Error(), "no partitions") && !strings.Contains(err.Error(), "phantom") {
		t.Fatalf("unexpected error text: %v", err)
	}
}

// ---- BRKR-02: offset manipulation ----

// TestSecBrokerCommitOffsetBeyondHighWatermark: a joined member with the
// current generation must not commit past the high-water mark. A
// committed offset beyond the HWM wedges the group forever: every later
// FETCH answers OFFSET_OUT_OF_RANGE.
func TestSecBrokerCommitOffsetBeyondHighWatermark(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	secExpectOK(t, secCreateTopic(t, c, "jobs", 1, 1), protocol.OpCreateTopic)
	recs := make([]protocol.Message, 5)
	for i := range recs {
		recs[i] = protocol.Message{Value: []byte(fmt.Sprintf("v%d", i))}
	}
	hwm := secProduce(t, c, "jobs", 0, recs, 2)
	if hwm != 5 {
		t.Fatalf("hwm after produce: %d, want 5", hwm)
	}
	gen, _ := secJoinOK(t, c, "g", "m1", []string{"jobs"}, 3)

	// Boundary: offset == HWM is the normal "consumed everything" commit.
	secExpectOK(t, secCommit(t, c, "g", "m1", "jobs", 0, 5, gen, 4), protocol.OpCommitOffset)

	// Past the HWM, near or absurd, must be rejected and must not move
	// the stored offset.
	secExpectError(t, secCommit(t, c, "g", "m1", "jobs", 0, 6, gen, 5), protocol.CodeOffsetOutOfRange)
	secExpectError(t, secCommit(t, c, "g", "m1", "jobs", 0, math.MaxUint64, gen, 6), protocol.CodeOffsetOutOfRange)

	f := secRoundTrip(t, c, protocol.OpFetchOffset, secJSON(t, map[string]any{
		"group": "g", "member_id": "m1", "topic": "jobs", "partition": 0, "generation": gen,
	}), 7)
	secExpectOK(t, f, protocol.OpFetchOffset)
	var fo protocol.FetchOffsetResponse
	if err := json.Unmarshal(f.Payload, &fo); err != nil {
		t.Fatalf("decode fetch-offset: %v", err)
	}
	if fo.Offset != 5 {
		t.Fatalf("committed offset moved by rejected commits: got %d want 5", fo.Offset)
	}
}

// TestSecBrokerFetchOutOfRangeOffsets: FETCH at or beyond the high-water
// mark is bounds-checked and clean (no panic, no garbage). Clean bill —
// pinned as a regression.
func TestSecBrokerFetchOutOfRangeOffsets(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	secExpectOK(t, secCreateTopic(t, c, "jobs", 1, 1), protocol.OpCreateTopic)
	secProduce(t, c, "jobs", 0, []protocol.Message{{Value: []byte("v")}}, 2)

	fetch := func(offset uint64, corr uint64) *protocol.Frame {
		t.Helper()
		return secRoundTrip(t, c, protocol.OpFetch, protocol.EncodeFetchRequest(&protocol.FetchRequest{
			Topic: "jobs", Partition: 0, Offset: offset, MaxRecords: 10, MaxBytes: 1 << 20,
		}), corr)
	}

	// At the HWM: empty page, not an error.
	f := fetch(1, 3)
	secExpectOK(t, f, protocol.OpFetch)
	resp, err := protocol.DecodeFetchResponse(f.Payload)
	if err != nil {
		t.Fatalf("decode fetch: %v", err)
	}
	if len(resp.Records) != 0 || resp.HighWatermark != 1 {
		t.Fatalf("fetch at hwm: records=%d hwm=%d, want 0/1", len(resp.Records), resp.HighWatermark)
	}

	// Beyond the HWM, including u64 max: clean OFFSET_OUT_OF_RANGE.
	secExpectError(t, fetch(2, 4), protocol.CodeOffsetOutOfRange)
	secExpectError(t, fetch(math.MaxUint64, 5), protocol.CodeOffsetOutOfRange)
}

// ---- BRKR-03: consumer-group isolation ----

// TestSecBrokerFetchOffsetRequiresMembership: FETCH_OFFSET used to trust
// any group_id the client named — no member, no generation, nothing.
// Any client could read any group's committed offsets. Offset reads must
// be bound to a member that is currently joined and in-generation.
func TestSecBrokerFetchOffsetRequiresMembership(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	secExpectOK(t, secCreateTopic(t, c, "jobs", 1, 1), protocol.OpCreateTopic)
	secProduce(t, c, "jobs", 0, []protocol.Message{{Value: []byte("v")}}, 2)
	gen, _ := secJoinOK(t, c, "g1", "m1", []string{"jobs"}, 3)
	secExpectOK(t, secCommit(t, c, "g1", "m1", "jobs", 0, 1, gen, 4), protocol.OpCommitOffset)

	// A second group whose member will try to read g1's offsets.
	gen2, _ := secJoinOK(t, c, "g2", "m9", []string{"jobs"}, 5)

	fetchOffset := func(payload map[string]any, corr uint64) *protocol.Frame {
		t.Helper()
		return secRoundTrip(t, c, protocol.OpFetchOffset, secJSON(t, payload), corr)
	}

	// (a) Legacy/foreign shape: no member_id, no generation.
	secExpectError(t, fetchOffset(map[string]any{
		"group": "g1", "topic": "jobs", "partition": 0,
	}, 10), protocol.CodeUnknownMember, protocol.CodeRebalance, protocol.CodeBadRequest)

	// (b) Member that never joined the group.
	secExpectError(t, fetchOffset(map[string]any{
		"group": "g1", "member_id": "ghost", "generation": gen, "topic": "jobs", "partition": 0,
	}, 11), protocol.CodeUnknownMember, protocol.CodeRebalance)

	// (c) Right member, wrong (stale) generation.
	secExpectError(t, fetchOffset(map[string]any{
		"group": "g1", "member_id": "m1", "generation": gen + 5, "topic": "jobs", "partition": 0,
	}, 12), protocol.CodeRebalance)

	// (d) A member of ANOTHER group naming g1 (cross-group read).
	secExpectError(t, fetchOffset(map[string]any{
		"group": "g1", "member_id": "m9", "generation": gen2, "topic": "jobs", "partition": 0,
	}, 13), protocol.CodeUnknownMember, protocol.CodeRebalance)

	// (e) Legit: joined member, current generation → the committed value.
	f := fetchOffset(map[string]any{
		"group": "g1", "member_id": "m1", "generation": gen, "topic": "jobs", "partition": 0,
	}, 14)
	secExpectOK(t, f, protocol.OpFetchOffset)
	var fo protocol.FetchOffsetResponse
	if err := json.Unmarshal(f.Payload, &fo); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if fo.Offset != 1 {
		t.Fatalf("legit fetch-offset: got %d want 1", fo.Offset)
	}
}

// TestSecBrokerCommitUnassignedPartitionRejected: generation fencing
// stops zombies, but a LIVE member could commit offsets for partitions
// it does not own — clobbering another member's progress (message skip
// or mass redelivery) or writing offsets under topics the group never
// subscribed to. Commits must be bound to the member's assignment.
func TestSecBrokerCommitUnassignedPartitionRejected(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	secExpectOK(t, secCreateTopic(t, c, "jobs", 3, 1), protocol.OpCreateTopic)
	secExpectOK(t, secCreateTopic(t, c, "other", 1, 2), protocol.OpCreateTopic)

	// Fill the partitions so the offset bounds check passes and the
	// assignment check is the one under test.
	rec := []protocol.Message{{Value: []byte("v")}, {Value: []byte("w")}}
	secProduce(t, c, "jobs", 0, rec, 30)
	secProduce(t, c, "jobs", 2, rec, 31)
	secProduce(t, c, "other", 0, rec, 32)

	// Range assignor: with 2 members on a 3-partition topic, m1 gets
	// {0,1} and m2 gets {2}. m1's first join (alone) returned all three;
	// re-join to learn its assignment in the current generation (an
	// unchanged re-join does not bump the generation).
	secJoinOK(t, c, "g", "m1", []string{"jobs"}, 3)
	gen2, as2 := secJoinOK(t, c, "g", "m2", []string{"jobs"}, 4)
	gen3, as1 := secJoinOK(t, c, "g", "m1", []string{"jobs"}, 40)
	if gen3 != gen2 {
		t.Fatalf("unchanged re-join bumped generation: %d → %d", gen2, gen3)
	}
	owns := func(as []protocol.Assignment, topic string, p int32) bool {
		for _, a := range as {
			if a.Topic != topic {
				continue
			}
			for _, id := range a.Partitions {
				if id == p {
					return true
				}
			}
		}
		return false
	}
	if !owns(as1, "jobs", 0) || owns(as1, "jobs", 2) {
		t.Fatalf("m1 assignment unexpected: %+v", as1)
	}
	if !owns(as2, "jobs", 2) || owns(as2, "jobs", 0) {
		t.Fatalf("m2 assignment unexpected: %+v", as2)
	}

	// m2 commits over m1's partition 0: must be rejected.
	secExpectError(t, secCommit(t, c, "g", "m2", "jobs", 0, 1, gen2, 5), protocol.CodeBadRequest)
	// m1 commits over m2's partition 2: must be rejected.
	secExpectError(t, secCommit(t, c, "g", "m1", "jobs", 2, 1, gen2, 6), protocol.CodeBadRequest)
	// m1 commits under a topic the group never subscribed to.
	secExpectError(t, secCommit(t, c, "g", "m1", "other", 0, 1, gen2, 7), protocol.CodeBadRequest)

	// Legit commits for owned partitions still land.
	secExpectOK(t, secCommit(t, c, "g", "m2", "jobs", 2, 1, gen2, 8), protocol.OpCommitOffset)
	secExpectOK(t, secCommit(t, c, "g", "m1", "jobs", 0, 1, gen2, 9), protocol.OpCommitOffset)

	// Nothing leaked into the rejected slots: read offsets back as m1.
	for _, tc := range []struct {
		topic string
		part  int32
		want  uint64
	}{{"jobs", 1, 0}, {"jobs", 2, 1}, {"other", 0, 0}} {
		f := secRoundTrip(t, c, protocol.OpFetchOffset, secJSON(t, map[string]any{
			"group": "g", "member_id": "m1", "topic": tc.topic, "partition": tc.part, "generation": gen2,
		}), 20)
		secExpectOK(t, f, protocol.OpFetchOffset)
		var fo protocol.FetchOffsetResponse
		if err := json.Unmarshal(f.Payload, &fo); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if fo.Offset != tc.want {
			t.Errorf("offset %s/%d: got %d want %d", tc.topic, tc.part, fo.Offset, tc.want)
		}
	}
}

// ---- BRKR-04: topic/group metadata hardening ----

// TestSecBrokerPartitionCountCap: partition counts are bounded. 0 (or
// negative) means "broker default"; the hard cap is 64 per topic.
func TestSecBrokerPartitionCountCap(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	var corr uint64
	// 0 and negative fall back to the broker default (3).
	for _, n := range []int32{0, -1, -8} {
		corr++
		f := secCreateTopic(t, c, fmt.Sprintf("cap-def-%d", corr), n, corr)
		secExpectOK(t, f, protocol.OpCreateTopic)
		var resp protocol.CreateTopicResponse
		if err := json.Unmarshal(f.Payload, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Partitions != 3 {
			t.Fatalf("partitions=%d: got %d, want broker default 3", n, resp.Partitions)
		}
	}
	// Boundaries: 1 and 64 pass.
	for _, n := range []int32{1, 64} {
		corr++
		f := secCreateTopic(t, c, fmt.Sprintf("cap-ok-%d", n), n, corr)
		secExpectOK(t, f, protocol.OpCreateTopic)
	}
	// Over the cap: clean BAD_REQUEST, no 65/100000-partition topics.
	for _, n := range []int32{65, 1000, 100000, math.MaxInt32} {
		corr++
		f := secCreateTopic(t, c, fmt.Sprintf("cap-no-%d", corr), n, corr)
		secExpectError(t, f, protocol.CodeBadRequest)
	}
}

// TestSecBrokerIDValidation: group and member IDs ride into log lines
// and offsets.jsonl. Control characters forge log lines; absurd lengths
// bloat the offsets file. Both must be rejected at JOIN.
func TestSecBrokerIDValidation(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())
	secExpectOK(t, secCreateTopic(t, c, "jobs", 1, 1), protocol.OpCreateTopic)

	var corr uint64 = 10
	bad := [][2]string{
		{"bad\ngroup", "m1"},             // log forging via newline
		{"bad\rgroup", "m1"},             //
		{"bad\x00group", "m1"},           // NUL
		{"ok", "mem\tber"},               // control char in member id
		{"ok", ""},                       // empty member
		{"", "m1"},                       // empty group
		{strings.Repeat("g", 200), "m1"}, // over 128 chars
		{"ok", strings.Repeat("m", 200)}, //
		{"g/r", "m1"},                    // separators
		{`g\r`, "m1"},                    //
	}
	for _, gm := range bad {
		corr++
		f := secJoin(t, c, gm[0], gm[1], []string{"jobs"}, corr)
		secExpectError(t, f, protocol.CodeBadRequest)
	}

	corr++
	f := secJoin(t, c, "workers", "host-1-ab12cd", []string{"jobs"}, corr)
	secExpectOK(t, f, protocol.OpJoinGroup)
}

// TestSecBrokerTopicCountCap: topic creation is bounded (BROKER_MAX_TOPICS).
// Past the cap the broker answers BAD_REQUEST, not a silently growing
// pile of directories, file handles and writer goroutines.
func TestSecBrokerTopicCountCap(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, func(c *broker.Config) { c.MaxTopics = 2 })
	c := secDial(t, h.b.Addr())

	secExpectOK(t, secCreateTopic(t, c, "t1", 1, 1), protocol.OpCreateTopic)
	secExpectOK(t, secCreateTopic(t, c, "t2", 1, 2), protocol.OpCreateTopic)
	secExpectError(t, secCreateTopic(t, c, "t3", 1, 3), protocol.CodeBadRequest)
	// Creating an existing topic stays TOPIC_EXISTS (cap does not mask it).
	secExpectError(t, secCreateTopic(t, c, "t1", 1, 4), protocol.CodeTopicExists)
}

// TestSecBrokerGroupCountCap: group creation is bounded
// (BROKER_MAX_GROUPS), and groups whose last member left are
// garbage-collected so the cap cannot wedge the broker after churn.
// Committed offsets survive the GC of the empty group state.
func TestSecBrokerGroupCountCap(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, func(c *broker.Config) { c.MaxGroups = 2 })
	c := secDial(t, h.b.Addr())
	secExpectOK(t, secCreateTopic(t, c, "jobs", 1, 1), protocol.OpCreateTopic)

	secJoinOK(t, c, "g1", "m1", []string{"jobs"}, 2)
	secJoinOK(t, c, "g2", "m1", []string{"jobs"}, 3)
	// Third distinct group: rejected.
	secExpectError(t, secJoin(t, c, "g3", "m1", []string{"jobs"}, 4), protocol.CodeBadRequest)

	// The last member of g1 leaves: the empty group state is GC'd…
	secExpectOK(t, secRoundTrip(t, c, protocol.OpLeaveGroup,
		secJSON(t, protocol.LeaveGroupRequest{Group: "g1", MemberID: "m1"}), 5), protocol.OpLeaveGroup)
	// …so a new group fits under the cap again.
	secJoinOK(t, c, "g3", "m1", []string{"jobs"}, 6)

	// Offsets committed before the GC survive it (they live in the
	// offset store, not the group state). m1 owns partition 0 in g2.
	secProduce(t, c, "jobs", 0, []protocol.Message{{Value: []byte("v")}}, 7)
	gen, _ := secJoinOK(t, c, "g2", "m2", []string{"jobs"}, 8)
	secExpectOK(t, secCommit(t, c, "g2", "m1", "jobs", 0, 1, gen, 9), protocol.OpCommitOffset)
	secExpectOK(t, secRoundTrip(t, c, protocol.OpLeaveGroup,
		secJSON(t, protocol.LeaveGroupRequest{Group: "g2", MemberID: "m1"}), 10), protocol.OpLeaveGroup)
	secExpectOK(t, secRoundTrip(t, c, protocol.OpLeaveGroup,
		secJSON(t, protocol.LeaveGroupRequest{Group: "g2", MemberID: "m2"}), 11), protocol.OpLeaveGroup)
	// g2 is now empty and collected. Re-joining starts a fresh group…
	gen2, _ := secJoinOK(t, c, "g2", "m3", []string{"jobs"}, 12)
	// …and the committed offset is still there for the fresh member.
	f := secRoundTrip(t, c, protocol.OpFetchOffset, secJSON(t, map[string]any{
		"group": "g2", "member_id": "m3", "topic": "jobs", "partition": 0, "generation": gen2,
	}), 13)
	secExpectOK(t, f, protocol.OpFetchOffset)
	var fo protocol.FetchOffsetResponse
	if err := json.Unmarshal(f.Payload, &fo); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if fo.Offset != 1 {
		t.Fatalf("offset lost across group GC: got %d want 1", fo.Offset)
	}
}

// ---- BRKR-05: protocol desync / robustness ----

// TestSecBrokerMutatedFramesOverWire: deterministic mutation table. Take
// valid frames, damage them in every interesting way, and fire each over
// its own connection. The broker must answer with a well-formed frame,
// close the connection, or keep waiting for the rest of a truncated
// frame — never panic, never answer garbage. Afterwards the broker must
// still serve a normal request.
func TestSecBrokerMutatedFramesOverWire(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	addr := h.b.Addr()

	frameBytes := func(op protocol.Opcode, payload []byte) []byte {
		t.Helper()
		var buf bytes.Buffer
		if err := protocol.WriteFrame(&buf, &protocol.Frame{Opcode: op, CorrelationID: 7, Payload: payload}); err != nil {
			t.Fatalf("build seed frame: %v", err)
		}
		return buf.Bytes()
	}

	seeds := map[string][]byte{
		"create": frameBytes(protocol.OpCreateTopic, secJSON(t, protocol.CreateTopicRequest{Topic: "mut", Partitions: 1})),
		"list":   frameBytes(protocol.OpListTopics, nil),
		"produce": frameBytes(protocol.OpProduce, protocol.EncodeProduceRequest(&protocol.ProduceRequest{
			Topic: "mut", Partition: 0, Records: []protocol.Message{{Key: []byte("k"), Value: []byte("v")}},
		})),
		"fetch": frameBytes(protocol.OpFetch, protocol.EncodeFetchRequest(&protocol.FetchRequest{
			Topic: "mut", Partition: 0, MaxRecords: 1, MaxBytes: 1024,
		})),
		"join": frameBytes(protocol.OpJoinGroup, secJSON(t, protocol.JoinGroupRequest{Group: "g", MemberID: "m", Topics: []string{"mut"}})),
	}

	type outcome int
	const (
		outFrame   outcome = iota // got a well-formed frame back
		outClosed                 // connection closed / write failed
		outWaiting                // broker waits for more bytes (truncated frame)
	)

	runMutation := func(name string, data []byte) outcome {
		t.Helper()
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatalf("%s: dial: %v", name, err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := c.Write(data); err != nil {
			return outClosed
		}
		var hdr [4]byte
		if _, err := io.ReadFull(c, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
				return outClosed
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return outWaiting
			}
			return outClosed
		}
		ln := binary.BigEndian.Uint32(hdr[:])
		if ln > protocol.MaxPayloadSize+9 {
			t.Fatalf("%s: broker emitted nonsense frame (declared payload %d bytes)", name, ln)
		}
		return outFrame
	}

	for seedName, seed := range seeds {
		muts := map[string][]byte{
			"truncated-half":   seed[:len(seed)/2],
			"truncated-header": seed[:5],
			"opcode-0xff":      flip(seed, 4, 0xff),
			"len-0xffffffff":   withLen(seed, 0xffffffff),
			"len-shrunk":       withLen(seed, 1),
			"payload-zeroed":   zeroPayload(seed),
			"payload-0xff":     fillPayload(seed, 0xff),
		}
		if len(seed) > 14 {
			muts["flip-first-payload-byte"] = flip(seed, 13, seed[13]^0xff)
			muts["flip-last-byte"] = flip(seed, len(seed)-1, seed[len(seed)-1]^0xff)
		}
		for mutName, data := range muts {
			got := runMutation(seedName+"/"+mutName, data)
			_ = got // any of the three outcomes is acceptable; the asserts are inside
		}
	}

	// The broker must be unharmed: a normal request still works.
	c := secDial(t, addr)
	f := secRoundTrip(t, c, protocol.OpListTopics, nil, 1)
	secExpectOK(t, f, protocol.OpListTopics)
}

func flip(b []byte, at int, v byte) []byte {
	out := append([]byte(nil), b...)
	out[at] = v
	return out
}

func withLen(b []byte, n uint32) []byte {
	out := append([]byte(nil), b...)
	binary.BigEndian.PutUint32(out[0:4], n)
	return out
}

func zeroPayload(b []byte) []byte { return fillPayload(b, 0x00) }

func fillPayload(b []byte, v byte) []byte {
	out := append([]byte(nil), b...)
	for i := 13; i < len(out); i++ {
		out[i] = v
	}
	return out
}

// TestSecBrokerPipelinedCorrelationIntegrity: pipelined requests on one
// connection, including unknown opcodes and error cases, must each get
// exactly one response carrying the request's own correlation id — no
// cross-talk, no dropped responses, and the connection stays usable
// after errors (there is no "ERROR state" that bricks it).
func TestSecBrokerPipelinedCorrelationIntegrity(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	type req struct {
		corr uint64
		op   protocol.Opcode
		body []byte
	}
	var reqs []req
	for i := 0; i < 8; i++ {
		base := uint64(1000 + i*4)
		reqs = append(reqs,
			req{base, protocol.OpCreateTopic, secJSON(t, protocol.CreateTopicRequest{Topic: fmt.Sprintf("p%d", i), Partitions: 1})},
			req{base + 1, protocol.OpListTopics, nil},
			req{base + 2, protocol.Opcode(0x77), []byte("garbage")},    // unknown opcode
			req{base + 3, protocol.OpCreateTopic, []byte("{not json")}, // bad payload
		)
	}
	for _, r := range reqs {
		if err := protocol.WriteFrame(c, &protocol.Frame{Opcode: r.op, CorrelationID: r.corr, Payload: r.body}); err != nil {
			t.Fatalf("write corr %d: %v", r.corr, err)
		}
	}
	wantErr := map[uint64]string{}
	for _, r := range reqs {
		switch r.op {
		case protocol.Opcode(0x77):
			wantErr[r.corr] = protocol.CodeUnknownOpcode
		default:
			if string(r.body) == "{not json" {
				wantErr[r.corr] = protocol.CodeBadRequest
			}
		}
	}
	seen := make(map[uint64]*protocol.Frame, len(reqs))
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < len(reqs); i++ {
		f, err := protocol.ReadFrame(c)
		if err != nil {
			t.Fatalf("response %d of %d lost: %v", i+1, len(reqs), err)
		}
		if _, dup := seen[f.CorrelationID]; dup {
			t.Fatalf("duplicate response for correlation id %d", f.CorrelationID)
		}
		seen[f.CorrelationID] = f
	}
	if len(seen) != len(reqs) {
		t.Fatalf("got %d responses for %d requests", len(seen), len(reqs))
	}
	for _, r := range reqs {
		f, ok := seen[r.corr]
		if !ok {
			t.Fatalf("no response for correlation id %d", r.corr)
		}
		if want, isErr := wantErr[r.corr]; isErr {
			if f.Opcode != protocol.OpError {
				t.Fatalf("corr %d: expected ERROR(%s), got %s", r.corr, want, f.Opcode)
			}
			if e := protocol.DecodeErrorFrame(f.Payload); e.Code != want {
				t.Fatalf("corr %d: error code %q, want %q", r.corr, e.Code, want)
			}
		} else if f.Opcode != r.op {
			t.Fatalf("corr %d: expected %s, got %s", r.corr, r.op, f.Opcode)
		}
	}
}

// FuzzBrokerFrame mutates raw frame bytes and feeds them through the
// whole decode path: frame reader, PRODUCE/FETCH binary decoders, the
// JSON control payloads, and the on-disk record decoder (which also
// parses untrusted bytes after corruption). Must never panic and never
// accept an oversize payload.
//
// Run: go test -run '^$' -fuzz FuzzBrokerFrame -fuzztime 30s ./tests/security/
func FuzzBrokerFrame(f *testing.F) {
	mk := func(op protocol.Opcode, payload []byte) []byte {
		var buf bytes.Buffer
		if err := protocol.WriteFrame(&buf, &protocol.Frame{Opcode: op, CorrelationID: 1, Payload: payload}); err != nil {
			panic(err)
		}
		return buf.Bytes()
	}
	f.Add(mk(protocol.OpProduce, protocol.EncodeProduceRequest(&protocol.ProduceRequest{
		Topic: "jobs", Partition: -1,
		Records: []protocol.Message{
			{Key: []byte("k"), Value: []byte("v"), Headers: []protocol.Header{{Key: "traceparent", Value: []byte("00-abc-def-01")}}},
			{Value: []byte{}},
		},
	})))
	f.Add(mk(protocol.OpFetch, protocol.EncodeFetchRequest(&protocol.FetchRequest{
		Topic: "jobs", Partition: 2, Offset: 42, MaxRecords: 100, MaxBytes: 1 << 20,
		Group: "g", MemberID: "m", Generation: 3,
	})))
	f.Add(mk(protocol.OpCreateTopic, []byte(`{"topic":"jobs","partitions":3}`)))
	f.Add(mk(protocol.OpJoinGroup, []byte(`{"group":"g","member_id":"m","topics":["jobs"]}`)))
	f.Add(mk(protocol.OpCommitOffset, []byte(`{"group":"g","member_id":"m","topic":"jobs","partition":0,"offset":18446744073709551615,"generation":-1}`)))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})                      // zero-length payload, header cut
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 3, 0, 0}) // oversize length word
	f.Add(bytes.Repeat([]byte{0x41}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := protocol.ReadFrame(bytes.NewReader(data))
		if err != nil {
			return
		}
		if len(frame.Payload) > protocol.MaxPayloadSize {
			t.Fatalf("oversize payload accepted: %d", len(frame.Payload))
		}
		switch frame.Opcode {
		case protocol.OpProduce:
			if req, derr := protocol.DecodeProduceRequest(frame.Payload); derr == nil && req.Topic == "" {
				t.Fatal("produce decoded with empty topic")
			}
		case protocol.OpFetch:
			if req, derr := protocol.DecodeFetchRequest(frame.Payload); derr == nil && req.Topic == "" {
				t.Fatal("fetch decoded with empty topic")
			}
		case protocol.OpCreateTopic:
			var v protocol.CreateTopicRequest
			_ = json.Unmarshal(frame.Payload, &v)
		case protocol.OpJoinGroup:
			var v protocol.JoinGroupRequest
			_ = json.Unmarshal(frame.Payload, &v)
		case protocol.OpCommitOffset:
			var v protocol.CommitOffsetRequest
			_ = json.Unmarshal(frame.Payload, &v)
		case protocol.OpFetchOffset:
			var v protocol.FetchOffsetRequest
			_ = json.Unmarshal(frame.Payload, &v)
		case protocol.OpHeartbeat:
			var v protocol.HeartbeatRequest
			_ = json.Unmarshal(frame.Payload, &v)
		case protocol.OpLeaveGroup:
			var v protocol.LeaveGroupRequest
			_ = json.Unmarshal(frame.Payload, &v)
		}
		// The record decoder parses untrusted bytes too (corrupt segment
		// files): feed both the whole input and any decoded payload.
		_, _, _ = storage.DecodeRecord(data)
		_, _, _ = storage.DecodeRecord(frame.Payload)
	})
}

// ---- BRKR-07: resource-exhaustion guards (clean bills, pinned) ----

// TestSecBrokerFetchMaxBytesClamped: FETCH max_bytes is unbounded on the
// wire (u32 up to 4 GiB) but the broker must clamp it so one response
// never exceeds the 4 MiB frame budget. 16 × 256 KiB records live; a
// max_bytes=MaxUint32 fetch must come back with exactly 12 (3 MiB clamp).
func TestSecBrokerFetchMaxBytesClamped(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, nil)
	c := secDial(t, h.b.Addr())

	secExpectOK(t, secCreateTopic(t, c, "big", 1, 1), protocol.OpCreateTopic)
	val := bytes.Repeat([]byte("x"), 256<<10)
	recs := make([]protocol.Message, 8)
	for i := range recs {
		recs[i] = protocol.Message{Value: val}
	}
	// Two batches of 8: one frame would exceed the 4 MiB payload cap.
	secProduce(t, c, "big", 0, recs, 2)
	hwm := secProduce(t, c, "big", 0, recs, 3)
	if hwm != 16 {
		t.Fatalf("hwm: got %d want 16", hwm)
	}

	f := secRoundTrip(t, c, protocol.OpFetch, protocol.EncodeFetchRequest(&protocol.FetchRequest{
		Topic: "big", Partition: 0, Offset: 0, MaxRecords: 1000, MaxBytes: math.MaxUint32,
	}), 4)
	secExpectOK(t, f, protocol.OpFetch)
	resp, err := protocol.DecodeFetchResponse(f.Payload)
	if err != nil {
		t.Fatalf("decode fetch: %v", err)
	}
	var total int
	for _, r := range resp.Records {
		total += len(r.Key) + len(r.Value)
	}
	if len(resp.Records) != 12 {
		t.Fatalf("records: got %d, want exactly 12 (3 MiB clamp / 256 KiB)", len(resp.Records))
	}
	if total > 3<<20 {
		t.Fatalf("fetch returned %d bytes, clamp is 3 MiB", total)
	}
	if resp.HighWatermark != 16 {
		t.Fatalf("hwm in response: got %d want 16", resp.HighWatermark)
	}
}

// TestSecBrokerConnectionCap: BROKER_MAX_CONNECTIONS is enforced with a
// clean BROKER_BUSY error frame, not a silent drop.
func TestSecBrokerConnectionCap(t *testing.T) {
	t.Parallel()
	h := startSecBroker(t, func(c *broker.Config) { c.MaxConnections = 3 })
	addr := h.b.Addr()

	hold := make([]net.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		c := secDial(t, addr)
		// Round-trip so the accept loop has tracked this connection
		// before the next dial; a bare TCP handshake can sit in the
		// backlog and make the cap check race.
		secExpectOK(t, secRoundTrip(t, c, protocol.OpListTopics, nil, uint64(i+1)), protocol.OpListTopics)
		hold = append(hold, c)
	}
	// The fourth connection gets a BROKER_BUSY error frame and a close.
	c4, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial 4th: %v", err)
	}
	defer c4.Close()
	_ = c4.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := protocol.ReadFrame(c4)
	if err != nil {
		t.Fatalf("4th conn: expected a BROKER_BUSY frame, got %v", err)
	}
	secExpectError(t, f, protocol.CodeBrokerBusy)
	// One held conn works fine — the cap rejects only the overflow.
	secExpectOK(t, secRoundTrip(t, hold[0], protocol.OpListTopics, nil, 1), protocol.OpListTopics)
	for _, c := range hold[1:] {
		_ = c.Close()
	}
}
