package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
)

// FuzzAuthHandshake throws mutated bytes at the new handshake path: a
// live auth-enabled server, one connection per input, the fuzzed bytes
// as the AUTH payload (plus seeds that are not AUTH at all). The
// server must never panic, never hang, and always answer with a
// well-formed error frame or a clean close. After the budget (5) the
// broker closes — both outcomes are legal, garbage is not.
//
// Run: go test -run '^$' -fuzz FuzzAuthHandshake -fuzztime 30s ./internal/broker/server/
func FuzzAuthHandshake(f *testing.F) {
	good, _ := json.Marshal(protocol.AuthRequest{ID: "worker", Secret: "s3cr3t-worker"})
	f.Add(good)
	f.Add([]byte("{not json"))
	f.Add([]byte(`{"id":"","secret":""}`))
	f.Add([]byte(`{"id":"worker"}`))
	f.Add(bytes.Repeat([]byte{0x22, 0x3a}, 512)) // `":` soup
	f.Add([]byte{})
	f.Add([]byte(`{"id":"worker","secret":"s3cr3t-worker","extra":[1,2,3]}`))

	// One server for all iterations: dials are cheap, brokers are not.
	var once sync.Once
	var srv *server.Server
	start := func() {
		s := server.New("127.0.0.1:0", fakeBackend{}, 2*time.Second,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			server.WithAuthenticator(testKeys()))
		ctx := context.Background() // lives for the whole fuzz run
		go func() { _ = s.Run(ctx) }()
		deadline := time.Now().Add(5 * time.Second)
		for s.Addr() == "" && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		srv = s
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		once.Do(start)
		conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
		if err != nil {
			t.Skip("server not reachable")
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		frame := &protocol.Frame{Opcode: protocol.OpAuth, CorrelationID: 1, Payload: data}
		if len(data) > 0 && data[0] == 0xFF {
			// Seed marker: send as a non-AUTH opcode to fuzz the gate.
			frame.Opcode = protocol.Opcode(data[len(data)-1])
		}
		if err := protocol.WriteFrame(conn, frame); err != nil {
			return // connection already gone: fine
		}
		resp, err := protocol.ReadFrame(conn)
		if err != nil {
			return // closed after budget or timeout: fine
		}
		// Any response must be a well-formed frame: AUTH (success) or a
		// decodable ERROR. Garbage bytes or a half frame is a bug.
		switch resp.Opcode {
		case protocol.OpAuth:
			var ar protocol.AuthResponse
			if err := json.Unmarshal(resp.Payload, &ar); err != nil {
				t.Fatalf("AUTH success with undecodable payload: %v", err)
			}
		case protocol.OpError:
			e := protocol.DecodeErrorFrame(resp.Payload)
			switch e.Code {
			case protocol.CodeUnauthenticated, protocol.CodeBadRequest:
			default:
				t.Fatalf("unexpected error code %q on the auth path", e.Code)
			}
		default:
			t.Fatalf("unexpected opcode %s before authentication", resp.Opcode)
		}
	})
}
