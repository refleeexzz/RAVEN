package server_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/raven/platform/internal/broker/protocol"
	"github.com/raven/platform/internal/broker/server"
)

// fakeBackend implements server.Backend with canned responses.
type fakeBackend struct{}

func (fakeBackend) CreateTopic(context.Context, *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	return &protocol.CreateTopicResponse{Topic: "t", Partitions: 3}, nil
}
func (fakeBackend) ListTopics(context.Context, *protocol.ListTopicsRequest) (*protocol.ListTopicsResponse, error) {
	return &protocol.ListTopicsResponse{Topics: []protocol.TopicInfo{{Name: "jobs"}}}, nil
}
func (fakeBackend) Produce(context.Context, *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	return &protocol.ProduceResponse{Results: []protocol.ProduceResult{{Partition: 0, Offset: 1}}}, nil
}
func (fakeBackend) Fetch(context.Context, *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	return &protocol.FetchResponse{HighWatermark: 1}, nil
}
func (fakeBackend) CommitOffset(context.Context, *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error) {
	return &protocol.CommitOffsetResponse{}, nil
}
func (fakeBackend) FetchOffset(context.Context, *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error) {
	return &protocol.FetchOffsetResponse{Offset: 7}, nil
}
func (fakeBackend) JoinGroup(context.Context, *protocol.JoinGroupRequest) (*protocol.JoinGroupResponse, error) {
	return &protocol.JoinGroupResponse{Generation: 1, MemberID: "m"}, nil
}
func (fakeBackend) LeaveGroup(context.Context, *protocol.LeaveGroupRequest) (*protocol.LeaveGroupResponse, error) {
	return &protocol.LeaveGroupResponse{}, nil
}
func (fakeBackend) Heartbeat(context.Context, *protocol.HeartbeatRequest) (*protocol.HeartbeatResponse, error) {
	return &protocol.HeartbeatResponse{}, nil
}

func startServer(t *testing.T) (*server.Server, context.CancelFunc) {
	t.Helper()
	s := server.New("127.0.0.1:0", fakeBackend{}, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop in time")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for s.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return s, cancel
}

func TestPipelinedRequests(t *testing.T) {
	t.Parallel()
	s, _ := startServer(t)
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Fire 10 pipelined requests without waiting between them.
	for i := uint64(1); i <= 10; i++ {
		payload, _ := json.Marshal(protocol.FetchOffsetRequest{Group: "g", Topic: "jobs", Partition: 0})
		f := &protocol.Frame{Opcode: protocol.OpFetchOffset, CorrelationID: i, Payload: payload}
		if err := protocol.WriteFrame(conn, f); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Every request gets exactly one response, matched by correlation id.
	got := make(map[uint64]bool)
	for i := 0; i < 10; i++ {
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if f.Opcode != protocol.OpFetchOffset {
			t.Fatalf("corr %d: opcode %v", f.CorrelationID, f.Opcode)
		}
		var resp protocol.FetchOffsetResponse
		if err := json.Unmarshal(f.Payload, &resp); err != nil || resp.Offset != 7 {
			t.Fatalf("corr %d: bad payload", f.CorrelationID)
		}
		got[f.CorrelationID] = true
	}
	if len(got) != 10 {
		t.Fatalf("got %d distinct correlation ids, want 10", len(got))
	}
}

func TestUnknownOpcode(t *testing.T) {
	t.Parallel()
	s, _ := startServer(t)
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: 0x7f, CorrelationID: 1, Payload: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if f.Opcode != protocol.OpError {
		t.Fatalf("opcode: got %v want ERROR", f.Opcode)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeUnknownOpcode {
		t.Fatalf("code: got %s want UNKNOWN_OPCODE", e.Code)
	}
}

func TestOversizeFrameDropsConnection(t *testing.T) {
	t.Parallel()
	s, _ := startServer(t)
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Announce a 5 MiB payload: over the 4 MiB cap.
	head := []byte{0x00, 0x50, 0x00, 0x00}
	if _, err := conn.Write(head); err != nil {
		t.Fatal(err)
	}
	// The server must close the connection.
	buf := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("connection still open after oversize frame")
	}
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	t.Parallel()
	s, cancel := startServer(t)
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Warmup roundtrip: guarantees serveConn is running before we test
	// the drain. (A connection accepted only after shutdown begins is
	// refused instead — it was never in-flight.)
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpListTopics, CorrelationID: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(conn); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	// In-flight request while shutdown starts.
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpListTopics, CorrelationID: 9}); err != nil {
		t.Fatal(err)
	}
	cancel()
	// The response should still arrive (drained, not dropped).
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("in-flight request lost during shutdown: %v", err)
	}
	if f.CorrelationID != 9 || f.Opcode != protocol.OpListTopics {
		t.Fatalf("unexpected frame: %+v", f)
	}
}
