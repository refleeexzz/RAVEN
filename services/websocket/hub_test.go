package websocket

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testHub(t *testing.T) (*Hub, *Metrics) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	met := NewMetrics()
	return NewHub(log, met), met
}

// newTestConn builds a conn with no socket and no pumps: hub logic only.
// trySend and the maps work without a network connection.
func newTestConn(hub *Hub, userID string) *conn {
	return &conn{
		id:     uuid.NewString(),
		userID: userID,
		hub:    hub,
		send:   make(chan []byte, sendBuffer),
		done:   make(chan struct{}),
		rooms:  make(map[string]struct{}),
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func recvFrame(t *testing.T, c *conn) []byte {
	t.Helper()
	select {
	case f := <-c.send:
		return f
	case <-time.After(time.Second):
		t.Fatal("expected frame, got none")
		return nil
	}
}

func expectNoFrame(t *testing.T, c *conn) {
	t.Helper()
	select {
	case f := <-c.send:
		t.Fatalf("unexpected frame: %s", f)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRegisterMultiConnPerUser(t *testing.T) {
	hub, _ := testHub(t)
	c1 := newTestConn(hub, "alice")
	c2 := newTestConn(hub, "alice")

	if first := hub.Register(c1); !first {
		t.Error("first connection of a user must report first=true")
	}
	if first := hub.Register(c2); first {
		t.Error("second connection of a user must report first=false")
	}
	if got := hub.Stats(); got.Connections != 2 || got.Users != 1 || got.Rooms != 1 {
		t.Fatalf("stats after registers: %+v (want 2 conns, 1 user, 1 auto room)", got)
	}

	if last := hub.Unregister(c1); last {
		t.Error("user still has one connection; last must be false")
	}
	if last := hub.Unregister(c2); !last {
		t.Error("last connection closing must report last=true")
	}
	if got := hub.Stats(); got.Connections != 0 || got.Users != 0 || got.Rooms != 0 {
		t.Fatalf("stats after unregisters: %+v (want all zero)", got)
	}
}

func TestJoinLeaveBroadcast(t *testing.T) {
	hub, _ := testHub(t)
	a := newTestConn(hub, "alice")
	b := newTestConn(hub, "bob")
	outsider := newTestConn(hub, "carol")
	hub.Register(a)
	hub.Register(b)
	hub.Register(outsider)

	if !hub.Join(a, "jobs") || !hub.Join(b, "jobs") {
		t.Fatal("fresh joins must report true")
	}
	if hub.Join(a, "jobs") {
		t.Error("re-joining must report false")
	}
	if !hub.InRoom(a, "jobs") || hub.InRoom(outsider, "jobs") {
		t.Error("InRoom mismatch")
	}

	frame := []byte(`{"op":"msg","room":"jobs"}`)
	hub.BroadcastRoom("jobs", frame, opMsg)
	if got := recvFrame(t, a); string(got) != string(frame) {
		t.Errorf("alice got %s", got)
	}
	if got := recvFrame(t, b); string(got) != string(frame) {
		t.Errorf("bob got %s", got)
	}
	expectNoFrame(t, outsider)

	if !hub.Leave(b, "jobs") {
		t.Error("leave must report true for a member")
	}
	if hub.Leave(b, "jobs") {
		t.Error("leaving twice must report false")
	}
	hub.BroadcastRoom("jobs", frame, opMsg)
	recvFrame(t, a)
	expectNoFrame(t, b)
}

func TestDeliverUserDM(t *testing.T) {
	hub, _ := testHub(t)
	c1 := newTestConn(hub, "alice")
	c2 := newTestConn(hub, "alice")
	hub.Register(c1)
	hub.Register(c2)

	frame := []byte(`{"op":"msg","from":"bob"}`)
	if n := hub.DeliverUser("alice", frame, opMsg); n != 2 {
		t.Fatalf("DeliverUser = %d, want 2 (both tabs)", n)
	}
	recvFrame(t, c1)
	recvFrame(t, c2)

	if n := hub.DeliverUser("nobody", frame, opMsg); n != 0 {
		t.Fatalf("DeliverUser to unknown user = %d, want 0", n)
	}
}

func TestSlowConsumerDrop(t *testing.T) {
	hub, met := testHub(t)
	c := newTestConn(hub, "alice")
	hub.Register(c)
	hub.Join(c, "jobs")

	// Fill the 256-frame outbound buffer without draining it.
	junk := []byte(`{"op":"event"}`)
	for i := 0; i < sendBuffer; i++ {
		if !c.trySend(junk) {
			t.Fatalf("buffer not full after %d frames", i)
		}
	}
	if c.trySend(junk) {
		t.Fatal("full buffer must reject")
	}

	// Next broadcast overflows the buffer → slow-consumer eviction.
	hub.BroadcastRoom("jobs", junk, opMsg)

	if got := hub.Stats(); got.Connections != 0 || got.Rooms != 0 || got.Users != 0 {
		t.Fatalf("dropped conn still registered: %+v", got)
	}
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("dropped conn was not closed")
	}
	if got := testutil.ToFloat64(met.Dropped); got != 1 {
		t.Fatalf("dropped_total = %v, want 1", got)
	}
}

func TestEmptyRoomCleanup(t *testing.T) {
	hub, _ := testHub(t)
	c := newTestConn(hub, "alice")
	hub.Register(c)

	hub.Join(c, "jobs")
	if got := hub.Stats().Rooms; got != 2 {
		t.Fatalf("rooms = %d, want 2 (jobs + user:alice)", got)
	}
	hub.Leave(c, "jobs")
	if got := hub.Stats().Rooms; got != 1 {
		t.Fatalf("rooms after leave = %d, want 1 (empty room deleted)", got)
	}
	hub.Unregister(c)
	if got := hub.Stats().Rooms; got != 0 {
		t.Fatalf("rooms after unregister = %d, want 0 (auto room deleted)", got)
	}
}

func TestUnregisterIdempotent(t *testing.T) {
	hub, _ := testHub(t)
	c := newTestConn(hub, "alice")
	hub.Register(c)

	if last := hub.Unregister(c); !last {
		t.Fatal("first unregister must report last=true")
	}
	if last := hub.Unregister(c); last {
		t.Fatal("second unregister must be a no-op returning last=false")
	}
	if got := hub.Stats().Connections; got != 0 {
		t.Fatalf("connections = %d after double unregister", got)
	}
	if got := testutil.ToFloat64(hub.met.Connections); got != 0 {
		t.Fatalf("connections gauge = %v, want 0", got)
	}
}

func TestShutdownClosesConnections(t *testing.T) {
	hub, _ := testHub(t)
	c := newTestConn(hub, "alice")
	hub.Register(c)

	// No pumps are running in this test, so the conn never unregisters:
	// Shutdown must still close it and honor the context deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	hub.Shutdown(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Shutdown ignored its deadline: %v", elapsed)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("Shutdown must close live connections")
	}
}
