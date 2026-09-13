package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// rawClient is a minimal synchronized wire client for tests: writes and
// reads are serialized (one outstanding request at a time), like the
// real client transport does with writeMu + readLoop. Driving the same
// connection from a heartbeat goroutine AND the test main line without
// this lock would interleave frame bytes on the wire.
type rawClient struct {
	conn net.Conn
	mu   sync.Mutex
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return &rawClient{conn: conn}
}

func (r *rawClient) Close() { _ = r.conn.Close() }

// call sends one request and waits for its response. Returns nil on
// connection trouble (best effort, for heartbeat loops).
func (r *rawClient) call(op protocol.Opcode, payload any) *protocol.Frame {
	var body []byte
	switch p := payload.(type) {
	case []byte:
		body = p
	default:
		b, err := json.Marshal(payload)
		if err != nil {
			return nil
		}
		body = b
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := protocol.WriteFrame(r.conn, &protocol.Frame{Opcode: op, CorrelationID: 1, Payload: body}); err != nil {
		return nil
	}
	r.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(r.conn)
	if err != nil {
		return nil
	}
	return f
}

// mustCall is call for the test main line: any failure is fatal.
func (r *rawClient) mustCall(t *testing.T, op protocol.Opcode, payload any) *protocol.Frame {
	t.Helper()
	f := r.call(op, payload)
	if f == nil {
		t.Fatalf("%s: no response (connection trouble)", op)
	}
	return f
}

// joinResp is a JOIN_GROUP over the wire; fails the test on error frames.
func joinResp(t *testing.T, c *rawClient, group, member string, topics ...string) *protocol.JoinGroupResponse {
	t.Helper()
	f := c.mustCall(t, protocol.OpJoinGroup, protocol.JoinGroupRequest{Group: group, MemberID: member, Topics: topics})
	if err := errorFromFrame(f); err != nil {
		t.Fatalf("join %s: %v", member, err)
	}
	var resp protocol.JoinGroupResponse
	if err := json.Unmarshal(f.Payload, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp
}

func errorFromFrame(f *protocol.Frame) error {
	if f.Opcode == protocol.OpError {
		return protocol.DecodeErrorFrame(f.Payload)
	}
	return nil
}

// ownedPartitions extracts the partition ids of one topic from an assignment list.
func ownedPartitions(as []protocol.Assignment, topic string) []int32 {
	for _, a := range as {
		if a.Topic == topic {
			return a.Partitions
		}
	}
	return nil
}

func assertErrorCode(t *testing.T, f *protocol.Frame, want string) {
	t.Helper()
	if f.Opcode != protocol.OpError {
		t.Fatalf("expected ERROR frame, got %v", f.Opcode)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != want {
		t.Fatalf("error code: got %s want %s", e.Code, want)
	}
}

func assertErrorCodeAny(t *testing.T, f *protocol.Frame, wants ...string) {
	t.Helper()
	if f.Opcode != protocol.OpError {
		t.Fatalf("expected ERROR frame, got %v", f.Opcode)
	}
	e := protocol.DecodeErrorFrame(f.Payload)
	for _, w := range wants {
		if e.Code == w {
			return
		}
	}
	t.Fatalf("error code %s, want one of %v", e.Code, wants)
}

func generationOfGroup(b *broker.Broker, id string) int32 {
	for _, g := range b.Status().Groups {
		if g.ID == id {
			return g.Generation
		}
	}
	return -1
}

// TestE2EZombieMemberFencedOut drives the whole broker over the wire:
// a consumer that stops heartbeating mid-processing loses its
// partitions; while it is a zombie, every FETCH and COMMIT with the old
// generation is rejected, so it can never process a partition at the
// same time as the new owner. Re-joining restores it.
func TestE2EZombieMemberFencedOut(t *testing.T) {
	t.Parallel()
	finish := checkLeaks(t, 15*time.Second)
	defer finish()

	b, cancel, brokerDone := startBroker(t, func(c *broker.Config) {
		c.SessionTimeout = 400 * time.Millisecond
	})
	defer func() {
		cancel()
		<-brokerDone
	}()

	ctx := context.Background()
	admin := client.NewAdmin(b.Addr())
	defer admin.Close()
	if _, err := admin.CreateTopic(ctx, "jobs", 3); err != nil {
		t.Fatal(err)
	}
	producer := client.NewProducer(b.Addr(), client.WithProducerLogger(quietLogger()))
	defer producer.Close()
	for i := 0; i < 30; i++ {
		if _, err := producer.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Two raw protocol connections: the zombie-to-be and the survivor.
	zombie := dialRaw(t, b.Addr())
	defer zombie.Close()
	survivor := dialRaw(t, b.Addr())
	defer survivor.Close()

	// Phase 1: zombie joins alone and owns everything; it can fetch.
	zj := joinResp(t, zombie, "workers", "zombie", "jobs")
	if len(ownedPartitions(zj.Assignments, "jobs")) != 3 {
		t.Fatalf("zombie initial assignment: %+v", zj.Assignments)
	}
	fetchReq := protocol.EncodeFetchRequest(&protocol.FetchRequest{
		Topic: "jobs", Partition: 0, Offset: 0, MaxRecords: 10, MaxBytes: 1 << 20,
		Group: "workers", MemberID: "zombie", Generation: zj.Generation,
	})
	f := zombie.mustCall(t, protocol.OpFetch, fetchReq)
	if err := errorFromFrame(f); err != nil {
		t.Fatalf("zombie fetch while healthy: %v", err)
	}
	fr, err := protocol.DecodeFetchResponse(f.Payload)
	if err != nil || len(fr.Records) == 0 {
		t.Fatalf("zombie fetch: %d records, err %v", len(fr.Records), err)
	}

	// Phase 2: survivor joins → rebalance (gen2). "m2" < "zombie", so
	// the survivor gets partitions {0,1}, the zombie keeps {2}.
	sj := joinResp(t, survivor, "workers", "m2", "jobs")
	if sj.Generation != zj.Generation+1 {
		t.Fatalf("generation after second join: %d → %d", zj.Generation, sj.Generation)
	}
	if got := ownedPartitions(sj.Assignments, "jobs"); len(got) != 2 {
		t.Fatalf("survivor assignment: %+v", sj.Assignments)
	}

	// The zombie is still a member but its generation is stale: a commit
	// with gen1 is rejected. (Stale ACK rejection, mid-processing.)
	f = zombie.mustCall(t, protocol.OpCommitOffset, protocol.CommitOffsetRequest{
		Group: "workers", MemberID: "zombie", Topic: "jobs", Partition: 2, Offset: 5, Generation: zj.Generation,
	})
	assertErrorCode(t, f, protocol.CodeRebalance)

	// Phase 3: the zombie stops heartbeating (GC pause / hang). The
	// survivor keeps heartbeating so only the zombie is expelled.
	hbStop := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-hbStop:
				return
			case <-tick.C:
				survivor.call(protocol.OpHeartbeat, protocol.HeartbeatRequest{
					Group: "workers", MemberID: "m2", Generation: generationOfGroup(b, "workers"),
				}) // best effort; the test asserts via Status
			}
		}
	}()
	defer func() { close(hbStop); <-hbDone }()

	waitFor(t, "zombie expelled by session timeout", 10*time.Second, func() bool {
		for _, g := range b.Status().Groups {
			if g.ID == "workers" {
				return g.Members == 1
			}
		}
		return false
	})
	genAfterExpel := generationOfGroup(b, "workers")
	if genAfterExpel <= sj.Generation {
		t.Fatalf("generation did not bump on expulsion: %d → %d", sj.Generation, genAfterExpel)
	}

	// The survivor now owns all three partitions (fencing proves the
	// zombie can't; the survivor's fetch proves it can).
	var lastFetchErr error
	owned := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		f := survivor.call(protocol.OpFetch, protocol.EncodeFetchRequest(&protocol.FetchRequest{
			Topic: "jobs", Partition: 2, Offset: 0, MaxRecords: 1, MaxBytes: 1 << 20,
			Group: "workers", MemberID: "m2", Generation: genAfterExpel,
		}))
		lastFetchErr = nil
		if f == nil {
			lastFetchErr = fmt.Errorf("no response (conn trouble)")
		} else {
			lastFetchErr = errorFromFrame(f)
		}
		if lastFetchErr == nil {
			owned = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !owned {
		t.Fatalf("survivor never fetched partition 2 after expulsion; last error: %v", lastFetchErr)
	}

	// The zombie wakes up and tries to keep working partition 2 with its
	// ancient generation. Both fetch and commit are fenced out — it
	// cannot process the partition the survivor now owns.
	f = zombie.mustCall(t, protocol.OpFetch, protocol.EncodeFetchRequest(&protocol.FetchRequest{
		Topic: "jobs", Partition: 2, Offset: 0, MaxRecords: 10, MaxBytes: 1 << 20,
		Group: "workers", MemberID: "zombie", Generation: zj.Generation,
	}))
	assertErrorCode(t, f, protocol.CodeRebalance)
	f = zombie.mustCall(t, protocol.OpCommitOffset, protocol.CommitOffsetRequest{
		Group: "workers", MemberID: "zombie", Topic: "jobs", Partition: 2, Offset: 5, Generation: zj.Generation,
	})
	assertErrorCodeAny(t, f, protocol.CodeRebalance, protocol.CodeUnknownMember)

	// No zombie state landed.
	f = survivor.mustCall(t, protocol.OpFetchOffset, protocol.FetchOffsetRequest{Group: "workers", Topic: "jobs", Partition: 2})
	var offResp protocol.FetchOffsetResponse
	if err := json.Unmarshal(f.Payload, &offResp); err != nil {
		t.Fatal(err)
	}
	if offResp.Offset != 0 {
		t.Fatalf("zombie commit landed: offset %d, want 0", offResp.Offset)
	}

	// Phase 4: the zombie re-joins and is a healthy member again.
	zj2 := joinResp(t, zombie, "workers", "zombie", "jobs")
	if zj2.Generation <= genAfterExpel {
		t.Fatalf("re-join after expulsion should bump: %d → %d", genAfterExpel, zj2.Generation)
	}
	f = zombie.mustCall(t, protocol.OpFetch, protocol.EncodeFetchRequest(&protocol.FetchRequest{
		Topic: "jobs", Partition: 2, Offset: 0, MaxRecords: 10, MaxBytes: 1 << 20,
		Group: "workers", MemberID: "zombie", Generation: zj2.Generation,
	}))
	if err := errorFromFrame(f); err != nil {
		t.Fatalf("zombie fetch after re-join: %v", err)
	}
}
