package server_test

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
	"github.com/refleeexzz/RAVEN/internal/broker/testcert"
)

// tlsServerConfig builds a server-side TLS config from the bundle:
// TLS 1.2 floor, static certificate (hot-reload lives in the broker
// package and is tested there).
func tlsServerConfig(t *testing.T, b *testcert.Bundle) *tls.Config {
	t.Helper()
	cert := b.ServerCert(t)
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
}

// tlsClientConfig trusts only the bundle CA.
func tlsClientConfig(t *testing.T, b *testcert.Bundle) *tls.Config {
	t.Helper()
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    b.CAPool(t),
		ServerName: "localhost",
	}
}

// dialTLS performs the TLS handshake and returns the conn.
func dialTLS(t *testing.T, s *server.Server, cfg *tls.Config) *tls.Conn {
	t.Helper()
	conn, err := tls.Dial("tcp", s.Addr(), cfg)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestTLSHandshakeRoundTrip(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	s := startServerOpts(t, nil, server.WithTLS(tlsServerConfig(t, b)))

	conn := dialTLS(t, s, tlsClientConfig(t, b))
	if got := conn.ConnectionState().Version; got != tls.VersionTLS13 {
		t.Fatalf("negotiated TLS 0x%04x, want TLS 1.3 with a modern client", got)
	}
	roundtrip(t, conn, 1)
}

func TestTLS12FloorStillWorks(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	s := startServerOpts(t, nil, server.WithTLS(tlsServerConfig(t, b)))

	// A client capped at TLS 1.2 must still connect (the floor), even
	// though 1.3 is preferred when offered.
	cfg := tlsClientConfig(t, b)
	cfg.MaxVersion = tls.VersionTLS12
	conn := dialTLS(t, s, cfg)
	if got := conn.ConnectionState().Version; got != tls.VersionTLS12 {
		t.Fatalf("negotiated TLS 0x%04x, want TLS 1.2", got)
	}
	roundtrip(t, conn, 1)
}

func TestTLSRejectsBelowFloor(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	s := startServerOpts(t, nil, server.WithTLS(tlsServerConfig(t, b)))

	// A TLS 1.1-only client must never get a session. (Whether the
	// error surfaces in Dial or on the first frame read depends on who
	// aborts first; both are a rejection.)
	cfg := tlsClientConfig(t, b)
	cfg.MinVersion = tls.VersionTLS10
	cfg.MaxVersion = tls.VersionTLS11
	conn, err := tls.Dial("tcp", s.Addr(), cfg)
	if err != nil {
		return // handshake rejected outright
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("TLS 1.1 client got a usable connection; server floor is broken")
	}
}

func TestTLSRejectsPlaintextClient(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	s := startServerOpts(t, nil, server.WithTLS(tlsServerConfig(t, b)))

	// A plaintext frame must not be parsed as a ClientHello; the
	// connection dies without any protocol response.
	conn := dial(t, s)
	defer conn.Close()
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpListTopics, CorrelationID: 1}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("plaintext client got a response from a TLS listener")
	}
}

func TestMTLSAcceptsVerifiedClient(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	cfg := tlsServerConfig(t, b)
	cfg.ClientCAs = b.CAPool(t)
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	s := startServerOpts(t, nil, server.WithTLS(cfg))

	ccfg := tlsClientConfig(t, b)
	ccfg.Certificates = []tls.Certificate{b.ClientCert(t)}
	conn := dialTLS(t, s, ccfg)
	if n := len(conn.ConnectionState().PeerCertificates); n != 1 {
		// Client-side this is the server's chain: exactly its cert.
		t.Fatalf("client connection sees %d peer certs, want 1 (server)", n)
	}
	roundtrip(t, conn, 1)
}

func TestMTLSRejectsClientWithoutCert(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	cfg := tlsServerConfig(t, b)
	cfg.ClientCAs = b.CAPool(t)
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	s := startServerOpts(t, nil, server.WithTLS(cfg))

	conn, err := tls.Dial("tcp", s.Addr(), tlsClientConfig(t, b))
	if err != nil {
		return // rejected during handshake
	}
	defer conn.Close()
	// TLS 1.3 defers the client-cert request, so the failure can surface
	// on the first read instead of the handshake.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("cert-less client got a response from an mTLS listener")
	}
}

func TestMTLSRejectsCertFromWrongCA(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	other := testcert.New(t) // different CA
	cfg := tlsServerConfig(t, b)
	cfg.ClientCAs = b.CAPool(t)
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	s := startServerOpts(t, nil, server.WithTLS(cfg))

	ccfg := tlsClientConfig(t, b)
	ccfg.Certificates = []tls.Certificate{other.ClientCert(t)}
	conn, err := tls.Dial("tcp", s.Addr(), ccfg)
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("client with foreign-CA cert got a response from an mTLS listener")
	}
}

// TestTLSServerAcceptsConcurrentHandshakes guards the lazy-handshake
// design: a stalled handshake must not block other clients (the accept
// loop never handshakes synchronously).
func TestTLSServerAcceptsConcurrentHandshakes(t *testing.T) {
	t.Parallel()
	b := testcert.New(t)
	s := startServerOpts(t, nil, server.WithTLS(tlsServerConfig(t, b)),
		server.WithHandshakeTimeout(500*time.Millisecond))

	// Park one raw TCP connection that never speaks TLS.
	raw := dial(t, s)
	defer raw.Close()

	// A real client still gets served immediately.
	conn := dialTLS(t, s, tlsClientConfig(t, b))
	roundtrip(t, conn, 1)

	// The stalled conn is reaped by the handshake timeout.
	_ = raw.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := raw.Read(make([]byte, 1)); err == nil {
		t.Fatal("stalled pre-handshake connection was not reaped")
	}
}
