package server_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
)

// startServerOpts is startServer plus hardening options.
func startServerOpts(t *testing.T, backend server.Backend, opts ...server.Option) *server.Server {
	t.Helper()
	if backend == nil {
		backend = fakeBackend{}
	}
	s := server.New("127.0.0.1:0", backend, 2*time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)), opts...)
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
	return s
}

func dial(t *testing.T, s *server.Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// roundtrip sends one LIST_TOPICS and expects its response.
func roundtrip(t *testing.T, conn net.Conn, corr uint64) {
	t.Helper()
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpListTopics, CorrelationID: corr}); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if f.Opcode != protocol.OpListTopics || f.CorrelationID != corr {
		t.Fatalf("unexpected frame: %+v", f)
	}
}

// frameBytes serializes a frame the way WriteFrame does, so tests can
// slice and concat the bytes freely.
func frameBytes(t *testing.T, f *protocol.Frame) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := protocol.WriteFrame(&buf, f); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestPartialFrameDripFeed sends frames in tiny pieces with pauses —
// the slow-writer case. The server must reassemble and answer every
// one. Chunk size 1 is the byte-by-byte drip of both header and
// payload.
func TestPartialFrameDripFeed(t *testing.T) {
	t.Parallel()
	payload, _ := json.Marshal(protocol.FetchOffsetRequest{Group: "g", Topic: "jobs", Partition: 0})
	tests := []struct {
		name  string
		chunk int
	}{
		{"byte-by-byte drip", 1},
		{"header in 3-byte pieces", 3},
		{"header/payload split", 13}, // exactly the header, then the payload
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := startServerOpts(t, nil)
			conn := dial(t, s)
			defer conn.Close()
			data := frameBytes(t, &protocol.Frame{Opcode: protocol.OpFetchOffset, CorrelationID: 7, Payload: payload})
			for off := 0; off < len(data); off += tc.chunk {
				end := off + tc.chunk
				if end > len(data) {
					end = len(data)
				}
				if _, err := conn.Write(data[off:end]); err != nil {
					t.Fatalf("drip write at %d: %v", off, err)
				}
				time.Sleep(2 * time.Millisecond)
			}
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			f, err := protocol.ReadFrame(conn)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if f.Opcode != protocol.OpFetchOffset || f.CorrelationID != 7 {
				t.Fatalf("unexpected frame: %+v", f)
			}
			var resp protocol.FetchOffsetResponse
			if err := json.Unmarshal(f.Payload, &resp); err != nil || resp.Offset != 7 {
				t.Fatalf("bad payload: %s", f.Payload)
			}
		})
	}
}

// TestMultipleFramesOneWrite pipelines several frames in a SINGLE TCP
// write. The read loop must parse them one by one and answer each.
func TestMultipleFramesOneWrite(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil)
	conn := dial(t, s)
	defer conn.Close()

	var buf bytes.Buffer
	for i := uint64(1); i <= 5; i++ {
		payload, _ := json.Marshal(protocol.FetchOffsetRequest{Group: "g", Topic: "jobs", Partition: 0})
		buf.Write(frameBytes(t, &protocol.Frame{Opcode: protocol.OpFetchOffset, CorrelationID: i, Payload: payload}))
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	got := map[uint64]bool{}
	for i := 0; i < 5; i++ {
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if f.Opcode != protocol.OpFetchOffset {
			t.Fatalf("corr %d: opcode %v", f.CorrelationID, f.Opcode)
		}
		got[f.CorrelationID] = true
	}
	if len(got) != 5 {
		t.Fatalf("got %d responses, want 5", len(got))
	}
}

// TestEOFMidFrame: the client vanishes (clean FIN) in the middle of a
// frame. The server must drop the connection quietly and keep serving.
func TestEOFMidFrame(t *testing.T) {
	t.Parallel()
	full := frameBytes(t, &protocol.Frame{
		Opcode:        protocol.OpFetchOffset,
		CorrelationID: 1,
		Payload:       []byte(`{"group":"g","topic":"jobs","partition":0}`),
	})
	tests := []struct {
		name  string
		piece []byte
	}{
		{"EOF after 2 header bytes", full[:2]},
		{"EOF mid-header", full[:7]},   // length + opcode + 2 bytes of correlation id
		{"EOF mid-payload", full[:20]}, // full header, half the payload
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := startServerOpts(t, nil)
			conn := dial(t, s)
			if _, err := conn.Write(tc.piece); err != nil {
				t.Fatal(err)
			}
			conn.Close() // clean EOF mid-frame

			// The server is alive and serves the next connection.
			conn2 := dial(t, s)
			defer conn2.Close()
			roundtrip(t, conn2, 99)
		})
	}
}

// TestDisconnectMidPayload: abrupt close (RST via linger 0) while the
// payload is in flight. Same contract as EOF: no panic, no wedged
// server.
func TestDisconnectMidPayload(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil)
	full := frameBytes(t, &protocol.Frame{
		Opcode:        protocol.OpFetchOffset,
		CorrelationID: 1,
		Payload:       []byte(`{"group":"g","topic":"jobs","partition":0}`),
	})
	conn := dial(t, s)
	tc := conn.(*net.TCPConn)
	if err := tc.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(full[:16]); err != nil { // header + 3 payload bytes
		t.Fatal(err)
	}
	conn.Close()

	conn2 := dial(t, s)
	defer conn2.Close()
	roundtrip(t, conn2, 42)
}

// TestMalformedPayloads: frames with a known opcode but a payload that
// does not decode must get BAD_REQUEST, not a panic or a dropped conn.
// The connection stays usable afterwards.
func TestMalformedPayloads(t *testing.T) {
	t.Parallel()
	goodProduce := protocol.EncodeProduceRequest(&protocol.ProduceRequest{
		Topic: "jobs", Partition: 0,
		Records: []protocol.Message{{Value: []byte("v")}},
	})
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload []byte
	}{
		{"garbage JSON on JOIN_GROUP", protocol.OpJoinGroup, []byte("{not json")},
		{"empty payload on PRODUCE", protocol.OpProduce, nil},
		{"truncated PRODUCE payload", protocol.OpProduce, goodProduce[:len(goodProduce)/2]},
		{"garbage binary on PRODUCE", protocol.OpProduce, []byte{0xff, 0xff, 0xff, 0xff, 0xff}},
		{"garbage JSON on COMMIT_OFFSET", protocol.OpCommitOffset, []byte("[1,2,3]")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := startServerOpts(t, nil)
			conn := dial(t, s)
			defer conn.Close()
			if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: tc.opcode, CorrelationID: 1, Payload: tc.payload}); err != nil {
				t.Fatal(err)
			}
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			f, err := protocol.ReadFrame(conn)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if f.Opcode != protocol.OpError {
				t.Fatalf("opcode: got %v want ERROR", f.Opcode)
			}
			if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeBadRequest {
				t.Fatalf("code: got %s want BAD_REQUEST", e.Code)
			}
			// Protocol is not desynchronized: next request answers fine.
			roundtrip(t, conn, 2)
		})
	}
}

// TestMaxConnections: past the cap, a new connection gets a clean
// BROKER_BUSY error frame and a close. Freed slots are reusable.
func TestMaxConnections(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithMaxConnections(2))

	c1 := dial(t, s)
	defer c1.Close()
	roundtrip(t, c1, 1) // warm up so the conn is tracked
	c2 := dial(t, s)
	defer c2.Close()
	roundtrip(t, c2, 1)

	// Third connection: refused with a readable error, then closed.
	c3 := dial(t, s)
	defer c3.Close()
	c3.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(c3)
	if err != nil {
		t.Fatalf("read refusal frame: %v", err)
	}
	if f.Opcode != protocol.OpError {
		t.Fatalf("refusal opcode: got %v want ERROR", f.Opcode)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeBrokerBusy {
		t.Fatalf("refusal code: got %s want BROKER_BUSY", e.Code)
	}
	// After the error frame the server closes the connection.
	if _, err := c3.Read(make([]byte, 1)); err == nil {
		t.Fatal("refused connection still open")
	}

	// Capped connections still work while the refusal happened.
	roundtrip(t, c1, 2)

	// Free a slot: a new connection is accepted again.
	c1.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c4, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatal(err)
		}
		c4.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if err := protocol.WriteFrame(c4, &protocol.Frame{Opcode: protocol.OpListTopics, CorrelationID: 1}); err == nil {
			if f, err := protocol.ReadFrame(c4); err == nil && f.Opcode == protocol.OpListTopics {
				c4.Close()
				return // slot was freed and reused
			}
		}
		c4.Close()
		if time.Now().After(deadline) {
			t.Fatal("freed connection slot was never reusable")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestIdleTimeoutReapsSilentConn: a connection that never sends is
// closed after the idle timeout; one that keeps sending survives.
func TestIdleTimeoutReapsSilentConn(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithIdleTimeout(300*time.Millisecond))

	silent := dial(t, s)
	defer silent.Close()
	active := dial(t, s)
	defer active.Close()

	// Keep the active conn busy while the silent one idles out.
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		roundtrip(t, active, 1)
		time.Sleep(100 * time.Millisecond)
	}

	// Silent conn must be closed server-side by now.
	silent.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	if _, err := silent.Read(make([]byte, 1)); err == nil {
		t.Fatal("silent connection not reaped after idle timeout")
	}

	// Active conn never tripped the deadline.
	roundtrip(t, active, 2)
}

// TestSlowReaderDoesNotBlockOthers: one connection floods its
// pipelining cap and never reads a single response. Other connections
// must be served unaffected, and once the slow conn closes every
// goroutine it pinned must be reaped.
//
// Not parallel on purpose: it asserts on process-wide goroutine counts,
// which parallel sibling tests would pollute.
func TestSlowReaderDoesNotBlockOthers(t *testing.T) {
	s := startServerOpts(t, nil)
	base := goroutineBaseline(t, s)

	slow := dial(t, s)
	// Fire 64 pipelined requests (= the per-conn in-flight cap) and
	// never read. The server may pin only its bounded per-conn
	// goroutines on this client.
	payload, _ := json.Marshal(protocol.FetchOffsetRequest{Group: "g", Topic: "jobs", Partition: 0})
	for i := uint64(1); i <= 64; i++ {
		if err := protocol.WriteFrame(slow, &protocol.Frame{Opcode: protocol.OpFetchOffset, CorrelationID: i, Payload: payload}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Give the server a moment to actually accept and work the flood,
	// so the assertions below aren't vacuous.
	time.Sleep(150 * time.Millisecond)

	// A second client gets answers immediately, throughout.
	fast := dial(t, s)
	for i := 0; i < 5; i++ {
		roundtrip(t, fast, uint64(i+1))
		time.Sleep(20 * time.Millisecond)
	}

	// Closing both conns must return the process to baseline: the slow
	// reader pinned nothing permanently.
	slow.Close()
	fast.Close()
	waitGoroutinesSettle(t, base, 5*time.Second)
}

// bigFetchBackend returns a ~3.9 MiB FETCH response. On Windows
// loopback one such frame is fully absorbed by kernel buffers even when
// the peer never reads, but the stack will not queue a second one —
// so pipelining several of these makes the server's write genuinely
// block.
type bigFetchBackend struct{ fakeBackend }

func (bigFetchBackend) Fetch(context.Context, *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	val := make([]byte, 64<<10) // 64 KiB per record
	recs := make([]protocol.FetchedMessage, 0, 62)
	for i := 0; i < 62; i++ { // 62 × 64 KiB ≈ 3.9 MiB payload, just under the 4 MiB frame cap
		recs = append(recs, protocol.FetchedMessage{Offset: uint64(i), Timestamp: 1, Value: val})
	}
	return &protocol.FetchResponse{HighWatermark: 62, Records: recs}, nil
}

// TestWriteDeadlineDropsStalledReader: the client pipelines three huge
// fetches and never reads a byte. Kernel buffers absorb the first
// response; the second write blocks and the write deadline must fire:
// the writer closes the conn, the read loop unblocks, and every
// goroutine pinned on this client exits — other clients are unaffected.
//
// Not parallel on purpose: goroutine-count assertion.
func TestWriteDeadlineDropsStalledReader(t *testing.T) {
	s := startServerOpts(t, bigFetchBackend{}, server.WithWriteTimeout(500*time.Millisecond))
	base := goroutineBaseline(t, s)

	stalled := dial(t, s)
	defer stalled.Close()
	// Shrink the client receive buffer too, so kernel absorption is as
	// small as the platform allows.
	if err := stalled.(*net.TCPConn).SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	req := protocol.EncodeFetchRequest(&protocol.FetchRequest{Topic: "jobs", Partition: 0, MaxRecords: 100, MaxBytes: 4 << 20})
	for i := uint64(1); i <= 3; i++ {
		if err := protocol.WriteFrame(stalled, &protocol.Frame{Opcode: protocol.OpFetch, CorrelationID: i, Payload: req}); err != nil {
			t.Fatal(err)
		}
	}
	// The conn must first be accepted and handling: its serveConn +
	// writer goroutines appear (+2)...
	waitGoroutinesAbove(t, base+2, 3*time.Second)
	// ...then the 500 ms write deadline reaps the whole connection and
	// the count returns to exactly baseline. Never reading is the point.
	waitGoroutinesSettle(t, base, 5*time.Second)

	// Server is healthy for everyone else.
	good := dial(t, s)
	defer good.Close()
	roundtrip(t, good, 1)
}

// waitGoroutinesAbove polls until the goroutine count exceeds min —
// proof that the server actually spawned the connection machinery a
// test is about to assert something about.
func waitGoroutinesAbove(t *testing.T, min int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() >= min {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines never rose above %d (now %d)", min, runtime.NumGoroutine())
}

// goroutineBaseline returns a trustworthy goroutine baseline for a
// freshly started server: it does one warmup roundtrip first so every
// long-lived server goroutine (accept loop, drain watcher) definitely
// exists, waits for the warmup conn to be reaped, then samples.
func goroutineBaseline(t *testing.T, s *server.Server) int {
	t.Helper()
	warm := dial(t, s)
	roundtrip(t, warm, 1)
	warm.Close()
	// Let the warmup conn's goroutines exit, then take a stable sample:
	// two equal readings 100 ms apart.
	deadline := time.Now().Add(3 * time.Second)
	prev := -1
	for time.Now().Before(deadline) {
		cur := runtime.NumGoroutine()
		if cur == prev {
			return cur
		}
		prev = cur
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("goroutine count never stabilized for a baseline")
	return 0
}

// waitGoroutinesSettle polls until the goroutine count drops to max or
// the timeout expires.
func waitGoroutinesSettle(t *testing.T, max int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= max {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("goroutines did not settle: baseline-max %d, now %d", max, runtime.NumGoroutine())
}

// TestOversizeHeaderOnlyDrip: an oversize announcement arriving
// byte-by-byte is still rejected as soon as the length word is complete
// (the server must not wait for the rest of the header).
func TestOversizeHeaderOnlyDrip(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil)
	conn := dial(t, s)
	defer conn.Close()
	head := make([]byte, 4)
	binary.BigEndian.PutUint32(head, protocol.MaxPayloadSize+1)
	for _, b := range head {
		if _, err := conn.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection still open after dripped oversize header")
	}
}
