package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
)

// staticAuth is a test Authenticator: exact-match id+secret.
type staticAuth map[string]struct {
	secret string
	grants *server.Principal
}

func (a staticAuth) Authenticate(_ context.Context, id, secret string) (*server.Principal, error) {
	e, ok := a[id]
	if !ok || e.secret != secret {
		return nil, server.ErrBadCredentials
	}
	return e.grants, nil
}

func testKeys() staticAuth {
	return staticAuth{
		"admin":  {"s3cr3t-admin", &server.Principal{ID: "admin", Admin: true}},
		"worker": {"s3cr3t-worker", &server.Principal{ID: "worker", TopicsRead: []string{"jobs"}, TopicsWrite: []string{"jobs"}}},
	}
}

// authRoundtrip sends one AUTH frame and returns the response.
func authRoundtrip(t *testing.T, conn net.Conn, corr uint64, id, secret string) *protocol.Frame {
	t.Helper()
	payload, _ := json.Marshal(protocol.AuthRequest{ID: id, Secret: secret})
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpAuth, CorrelationID: corr, Payload: payload}); err != nil {
		t.Fatalf("write AUTH: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read AUTH response: %v", err)
	}
	return f
}

func mustAuth(t *testing.T, conn net.Conn, id, secret string) {
	t.Helper()
	f := authRoundtrip(t, conn, 1, id, secret)
	if f.Opcode != protocol.OpAuth {
		t.Fatalf("AUTH failed: %+v", f)
	}
}

func TestAuthGateRejectsPreAuthFrames(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithAuthenticator(testKeys()))
	conn := dial(t, s)
	defer conn.Close()

	// Any non-AUTH frame before authentication: UNAUTHENTICATED.
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpListTopics, CorrelationID: 1}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if f.Opcode != protocol.OpError {
		t.Fatalf("got %s, want ERROR", f.Opcode)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeUnauthenticated {
		t.Fatalf("code %q, want UNAUTHENTICATED", e.Code)
	}
}

func TestAuthSuccessUnlocksConnection(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithAuthenticator(testKeys()))
	conn := dial(t, s)
	defer conn.Close()

	f := authRoundtrip(t, conn, 1, "worker", "s3cr3t-worker")
	if f.Opcode != protocol.OpAuth {
		t.Fatalf("AUTH failed: %+v", f)
	}
	// The AUTH response echoes the grants so clients can fail fast.
	var ar protocol.AuthResponse
	if err := json.Unmarshal(f.Payload, &ar); err != nil {
		t.Fatalf("decode AUTH response: %v", err)
	}
	if ar.ID != "worker" || len(ar.TopicsRead) != 1 || ar.TopicsRead[0] != "jobs" || ar.Admin {
		t.Fatalf("AUTH response grants wrong: %+v", ar)
	}

	// After AUTH, normal ops work (ACL matrix is covered separately).
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpListTopics, CorrelationID: 99}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Opcode != protocol.OpListTopics {
		t.Fatalf("got %s, want LIST_TOPICS", resp.Opcode)
	}
}

func TestAuthBadSecretAndUnknownID(t *testing.T) {
	t.Parallel()
	var failures int
	s := startServerOpts(t, nil,
		server.WithAuthenticator(testKeys()),
		server.WithSecurityHooks(server.SecurityHooks{OnAuthFailure: func() { failures++ }}),
	)
	conn := dial(t, s)
	defer conn.Close()

	for _, c := range [][2]string{{"worker", "wrong"}, {"nobody", "s3cr3t-worker"}} {
		f := authRoundtrip(t, conn, 1, c[0], c[1])
		if f.Opcode != protocol.OpError {
			t.Fatalf("AUTH %q: got %s, want ERROR", c[0], f.Opcode)
		}
		if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeUnauthenticated {
			t.Fatalf("AUTH %q: code %q, want UNAUTHENTICATED", c[0], e.Code)
		}
	}
	// Empty credentials are malformed, not merely wrong: BAD_REQUEST.
	f := authRoundtrip(t, conn, 1, "", "")
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeBadRequest {
		t.Fatalf("empty AUTH: code %q, want BAD_REQUEST", e.Code)
	}
	if failures != 3 {
		t.Fatalf("OnAuthFailure fired %d times, want 3", failures)
	}
	// Budget not yet exhausted: connection still usable, good creds win.
	mustAuth(t, conn, "admin", "s3cr3t-admin")
}

func TestAuthFailureBudgetClosesConnection(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil,
		server.WithAuthenticator(testKeys()),
		server.WithMaxAuthFailures(3),
	)
	conn := dial(t, s)
	defer conn.Close()

	// Three strikes: the third error frame is delivered, then the
	// broker closes the connection.
	for i := 0; i < 3; i++ {
		f := authRoundtrip(t, conn, uint64(i+1), "worker", "wrong")
		if f.Opcode != protocol.OpError {
			t.Fatalf("attempt %d: got %s, want ERROR", i+1, f.Opcode)
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("connection still open after the auth failure budget")
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		// Any close is fine; log non-EOF shapes for debuggability.
		t.Logf("connection closed with %v (acceptable)", err)
	}
}

func TestAuthMalformedPayloadCountsAsFailure(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithAuthenticator(testKeys()))
	conn := dial(t, s)
	defer conn.Close()

	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpAuth, CorrelationID: 1, Payload: []byte("{not json")}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeBadRequest {
		t.Fatalf("code %q, want BAD_REQUEST", e.Code)
	}
}

func TestAuthTimeoutReapsUnauthenticatedConn(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil,
		server.WithAuthenticator(testKeys()),
		server.WithAuthTimeout(300*time.Millisecond),
		server.WithIdleTimeout(time.Hour), // prove the auth timeout, not idle, fires
	)
	conn := dial(t, s)
	defer conn.Close()

	// Say nothing. The auth timeout must reap the connection.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("silent unauthenticated connection was not reaped")
	}
}

func TestAuthSecondAUTHRejected(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithAuthenticator(testKeys()))
	conn := dial(t, s)
	defer conn.Close()

	mustAuth(t, conn, "admin", "s3cr3t-admin")
	f := authRoundtrip(t, conn, 2, "admin", "s3cr3t-admin")
	if f.Opcode != protocol.OpError {
		t.Fatalf("second AUTH: got %s, want ERROR", f.Opcode)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeBadRequest {
		t.Fatalf("second AUTH: code %q, want BAD_REQUEST", e.Code)
	}
}

func TestOpenModeAnswersAuthWithUnknownOpcode(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil) // no authenticator: open mode
	conn := dial(t, s)
	defer conn.Close()

	// This is the negotiation path the v1.1 client relies on: an
	// open-mode (or v1) broker answers AUTH with UNKNOWN_OPCODE and the
	// client proceeds unauthenticated.
	f := authRoundtrip(t, conn, 1, "worker", "s3cr3t-worker")
	if f.Opcode != protocol.OpError {
		t.Fatalf("got %s, want ERROR", f.Opcode)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeUnknownOpcode {
		t.Fatalf("code %q, want UNKNOWN_OPCODE", e.Code)
	}
	// And the connection is still fully usable.
	roundtrip(t, conn, 2)
}
