// Package server is the broker's TCP front end. It owns the listener,
// per-connection goroutines, request dispatch and graceful shutdown.
// It knows the wire protocol but nothing about storage or groups: all
// work goes through the Backend interface.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// Backend handles one decoded request per op. Errors map onto OpError
// frames: *protocol.Error keeps its code, anything else becomes
// INTERNAL.
type Backend interface {
	CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error)
	ListTopics(ctx context.Context, req *protocol.ListTopicsRequest) (*protocol.ListTopicsResponse, error)
	Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error)
	Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error)
	CommitOffset(ctx context.Context, req *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error)
	FetchOffset(ctx context.Context, req *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error)
	JoinGroup(ctx context.Context, req *protocol.JoinGroupRequest) (*protocol.JoinGroupResponse, error)
	LeaveGroup(ctx context.Context, req *protocol.LeaveGroupRequest) (*protocol.LeaveGroupResponse, error)
	Heartbeat(ctx context.Context, req *protocol.HeartbeatRequest) (*protocol.HeartbeatResponse, error)
}

// maxInFlightPerConn caps pipelined requests being handled at once per
// connection. Past that, the reader blocks on the semaphore, which
// applies TCP backpressure to the client.
const maxInFlightPerConn = 64

// Server hardening defaults. All three are overridable through the
// functional options below (the broker wires them to env vars; see
// internal/broker/config.go).
const (
	// defaultMaxConnections caps simultaneous client connections. Past
	// the cap, new connections get a clean BROKER_BUSY error frame and
	// are closed — file descriptors stay bounded no matter how hostile
	// the network gets.
	defaultMaxConnections = 1024
	// defaultIdleTimeout closes connections that sent nothing for this
	// long. It is a read deadline refreshed before every frame read, so
	// active connections never trip it. It reaps half-open connections
	// (client crashed without a FIN) that TCP keepalive alone would
	// take hours to notice.
	defaultIdleTimeout = 5 * time.Minute
	// defaultWriteTimeout bounds a single frame write so a stalled
	// client cannot pin a connection goroutine forever.
	defaultWriteTimeout = 30 * time.Second
	// defaultHandshakeTimeout bounds the TLS handshake. Without it a
	// slow-loris client could park a connection goroutine before the
	// first frame for the whole idle timeout.
	defaultHandshakeTimeout = 10 * time.Second
	// defaultMaxAuthFailures is how many rejected AUTH attempts (or
	// pre-auth frames) one connection gets before the broker answers a
	// final UNAUTHENTICATED and closes it.
	defaultMaxAuthFailures = 5
	// defaultAuthTimeout is how long a connection may stay
	// unauthenticated. Much shorter than the idle timeout so half-open
	// unauth connections cannot hold connection slots.
	defaultAuthTimeout = 10 * time.Second
)

// Option customizes a Server. Options exist so tests and the broker can
// tune hardening limits without changing the New signature again.
type Option func(*Server)

// WithMaxConnections sets the simultaneous-connection cap (0 keeps the
// default of 1024).
func WithMaxConnections(n int) Option {
	return func(s *Server) {
		if n > 0 {
			s.maxConns = n
		}
	}
}

// WithIdleTimeout sets the per-connection idle read deadline (0 keeps
// the default of 5 minutes).
func WithIdleTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.idleTimeout = d
		}
	}
}

// WithWriteTimeout sets the per-frame write deadline (0 keeps the
// default of 30 seconds).
func WithWriteTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.writeTimeout = d
		}
	}
}

// WithTLS wraps the listener in TLS. A nil config keeps the listener
// plaintext (the default, for local/dev compatibility). The config
// owns version policy, cipher suites and client-cert (mTLS) rules;
// hot-reload belongs in a GetCertificate callback on the config.
func WithTLS(cfg *tls.Config) Option {
	return func(s *Server) {
		s.tlsCfg = cfg
	}
}

// WithHandshakeTimeout sets the TLS handshake deadline (0 keeps the
// default of 10 seconds). Only used when WithTLS is set.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.handshakeTimeout = d
		}
	}
}

// Server is the TCP listener for the broker protocol.
type Server struct {
	addr    string
	backend Backend
	log     *slog.Logger
	drain   time.Duration

	maxConns     int
	idleTimeout  time.Duration
	writeTimeout time.Duration

	tlsCfg           *tls.Config
	handshakeTimeout time.Duration

	authenticator    Authenticator
	maxAuthFailures  int
	authTimeout      time.Duration
	hooks            SecurityHooks

	ln      net.Listener
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup // connection goroutines
	closing atomic.Bool
}

// New creates a server; Run starts it.
func New(addr string, backend Backend, drainTimeout time.Duration, log *slog.Logger, opts ...Option) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		addr:            addr,
		backend:         backend,
		log:             log,
		drain:           drainTimeout,
		maxConns:        defaultMaxConnections,
		idleTimeout:     defaultIdleTimeout,
		writeTimeout:    defaultWriteTimeout,
		handshakeTimeout: defaultHandshakeTimeout,
		maxAuthFailures: defaultMaxAuthFailures,
		authTimeout:     defaultAuthTimeout,
		conns:           make(map[net.Conn]struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Addr returns the actual bound address (useful with ":0").
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// ActiveConnections reports how many client connections are currently
// tracked (accepted and not yet closed). Feeds the broker's
// raven_broker_active_connections gauge.
func (s *Server) ActiveConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Run listens and serves until ctx is cancelled, then drains in-flight
// handlers with a deadline and force-closes whatever is left. It
// returns only after every connection goroutine has exited, so callers
// can safely tear down the backend afterwards.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("broker listen %s: %w", s.addr, err)
	}
	if s.tlsCfg != nil {
		// The handshake runs lazily inside serveConn (bounded by
		// handshakeTimeout), so a slow peer never stalls Accept.
		ln = tls.NewListener(ln, s.tlsCfg)
		s.log.Info("broker TLS enabled", slog.String("addr", ln.Addr().String()))
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.log.Info("broker TCP listening", slog.String("addr", ln.Addr().String()))

	// Shutdown watcher: stop accepting, drain, then force-close.
	// drainDone closes when all connection goroutines have exited.
	drainDone := make(chan struct{})
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		defer close(drainDone)
		select {
		case <-ctx.Done():
		case <-stopped:
			return // listener died on its own; nothing to drain here
		}
		s.mu.Lock()
		s.closing.Store(true) // under mu: the accept loop decides wg.Add under the same lock
		s.mu.Unlock()
		_ = ln.Close()
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(s.drain):
			s.log.Warn("drain deadline hit, closing remaining connections")
			s.closeAllConns()
			select {
			case <-done:
			case <-time.After(s.drain):
				s.log.Warn("connections still draining after force close; giving up")
			}
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closing.Load() || errors.Is(err, net.ErrClosed) {
				// Shutdown path: wait for the drain to finish.
				<-drainDone
				return nil
			}
			// Temporary accept failure (fd exhaustion...): back off
			// briefly instead of hot-looping.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return fmt.Errorf("broker accept: %w", err)
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
		}
		s.mu.Lock()
		if s.closing.Load() {
			// Shutdown started between Accept and here. Never wg.Add
			// after the watcher began waiting on wg.
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		if len(s.conns) >= s.maxConns {
			// At capacity: reject cleanly (an ERROR frame the client can
			// read, then a close) instead of silently dropping. Never
			// tracked, never counted against the WaitGroup.
			s.mu.Unlock()
			s.rejectConn(conn)
			continue
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(ctx, conn)
	}
}

// rejectConn tells a refused client why (BROKER_BUSY) and closes. Best
// effort: a 2-second write deadline so a hostile client that never
// reads cannot stall the accept loop. Under TLS there is no safe
// cleartext error channel — the client is mid-handshake and would read
// the frame as handshake garbage — so the connection is just closed.
func (s *Server) rejectConn(conn net.Conn) {
	s.log.Warn("connection refused: at max connections",
		slog.String("remote", conn.RemoteAddr().String()),
		slog.Int("max", s.maxConns))
	if s.tlsCfg != nil {
		_ = conn.Close()
		return
	}
	f := protocol.ErrorFrame(0, protocol.NewError(protocol.CodeBrokerBusy, "too many connections"))
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = protocol.WriteFrame(conn, f)
	_ = conn.Close()
}

func (s *Server) closeAllConns() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.Close()
	}
}

func (s *Server) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

// serveConn runs one connection: a reader loop, one writer goroutine,
// and one handler goroutine per in-flight request (bounded by the
// per-connection semaphore).
//
// Exit paths: read error/EOF, oversize frame, or connection close at
// shutdown. The writer exits when resCh closes, which happens after all
// in-flight handlers finished. Handler contexts live until serveConn
// returns, so in-flight requests complete during a graceful drain
// instead of being cancelled mid-flight.
func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer s.untrack(conn)
	defer func() { _ = conn.Close() }()

	// TLS first, when enabled: no frame is read before the handshake
	// completes. The deadline keeps a stalled handshake from parking the
	// goroutine for the whole idle timeout.
	if tc, ok := conn.(*tls.Conn); ok {
		_ = conn.SetReadDeadline(time.Now().Add(s.handshakeTimeout))
		if err := tc.Handshake(); err != nil {
			if !s.closing.Load() {
				s.log.Debug("TLS handshake failed",
					slog.String("remote", conn.RemoteAddr().String()),
					slog.Any("err", err))
			}
			return
		}
		// Clear the handshake deadline; the read loop installs its own
		// per-frame deadlines from here on.
		_ = conn.SetReadDeadline(time.Time{})
		if cs := tc.ConnectionState(); len(cs.PeerCertificates) > 0 {
			s.log.Debug("mTLS client certificate verified",
				slog.String("remote", conn.RemoteAddr().String()),
				slog.String("subject", cs.PeerCertificates[0].Subject.String()))
		}
	}

	// connCtx is cancelled only when the connection itself goes away,
	// not when the server starts shutting down: a draining server still
	// completes in-flight requests (that is what draining means).
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	resCh := make(chan *protocol.Frame, maxInFlightPerConn)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for f := range resCh {
			_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			if err := protocol.WriteFrame(conn, f); err != nil {
				// Stalled reader (or dead conn). Close so the blocked
				// read loop wakes up and the whole connection is reaped
				// now, not at idle timeout. The read loop's own close
				// is a harmless no-op after this.
				_ = conn.Close()
				return
			}
		}
	}()

	sem := make(chan struct{}, maxInFlightPerConn)
	var handlers sync.WaitGroup
	// connDone aborts handler response sends if the connection dies
	// mid-dispatch. resCh is sized to the in-flight cap, so sends can
	// never block anyway; this is belt-and-suspenders.
	connDone := make(chan struct{})
	defer close(connDone)

	// Per-connection auth state. principal is nil until a successful
	// AUTH (or forever in open mode); it is written once by the read
	// loop and read by handler goroutines via dispatch, so an atomic
	// pointer keeps it race-free. failures budgets rejected attempts.
	var principal atomic.Pointer[Principal]
	authFailures := 0

	for {
		// Idle deadline: refreshed before every frame, so only genuinely
		// silent connections trip it. This is what reaps half-open
		// connections (client crashed without closing the socket).
		// Unauthenticated connections get the much shorter auth timeout
		// instead: AUTH must come promptly after connect.
		readDeadline := s.idleTimeout
		if s.authenticator != nil && principal.Load() == nil {
			readDeadline = s.authTimeout
		}
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			var ne net.Error
			switch {
			case errors.Is(err, protocol.ErrFrameTooLarge):
				s.log.Warn("oversize frame, dropping connection",
					slog.String("remote", conn.RemoteAddr().String()))
			case errors.As(err, &ne) && ne.Timeout():
				s.log.Debug("connection idle timeout",
					slog.String("remote", conn.RemoteAddr().String()))
			case !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) &&
				!errors.Is(err, net.ErrClosed) && !s.closing.Load():
				s.log.Debug("connection closed",
					slog.String("remote", conn.RemoteAddr().String()),
					slog.Any("err", err))
			}
			break
		}
		// Pre-auth gate: until AUTH succeeds, only AUTH frames pass.
		// AUTH is handled synchronously here (never pipelined to
		// handlers), so authentication is strictly ordered before every
		// dispatched frame. No handlers are running at this point, so
		// the read loop is the only sender to resCh besides the writer
		// drain — the terminal failure path (keepGoing == false) breaks
		// to the drain epilogue, which flushes the error frame through
		// the writer before the connection closes.
		if s.authenticator != nil && principal.Load() == nil {
			resp, keepGoing := s.handlePreAuth(connCtx, conn, f, &authFailures, &principal)
			select {
			case resCh <- resp:
			case <-connDone:
				return
			}
			if !keepGoing {
				s.log.Warn("auth failure budget exhausted, closing connection",
					slog.String("remote", conn.RemoteAddr().String()),
					slog.Int("failures", authFailures))
				break
			}
			continue
		}
		// Prefer a free in-flight slot over the shutdown signal: any
		// frame we managed to read gets dispatched, even mid-drain.
		// Only when the cap is genuinely full do we honor ctx.
		select {
		case sem <- struct{}{}:
		default:
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				// Shutdown while the in-flight cap is full: stop
				// reading. The epilogue drains running handlers.
				goto drain
			}
		}
		p := principal.Load() // nil in open mode; set before any dispatch otherwise
		handlers.Add(1)
		go func(f *protocol.Frame) {
			defer handlers.Done()
			defer func() { <-sem }()
			resp := s.dispatch(connCtx, f, p)
			select {
			case resCh <- resp:
			case <-connDone:
			}
		}(f)
		if s.closing.Load() {
			break // draining: finish in-flight requests, take no new ones
		}
	}

drain:
	handlers.Wait()
	close(resCh)
	<-writerDone
}

// dispatch routes one frame to the backend and always returns exactly
// one response frame. p is the connection's authenticated principal
// (nil in open mode); authorize() consults it per operation.
func (s *Server) dispatch(ctx context.Context, f *protocol.Frame, p *Principal) *protocol.Frame {
	resp := &protocol.Frame{Opcode: f.Opcode, CorrelationID: f.CorrelationID}
	fail := func(err error) *protocol.Frame {
		return protocol.ErrorFrame(f.CorrelationID, err)
	}
	badReq := func(err error) *protocol.Frame {
		return fail(protocol.NewError(protocol.CodeBadRequest, err.Error()))
	}

	switch f.Opcode {
	case protocol.OpAuth:
		// AUTH is handled by the read-loop gate. Reaching dispatch
		// means either open mode (no authenticator: tell the client
		// auth is unsupported so it proceeds, mirroring a v1 server) or
		// a second AUTH on an authenticated connection.
		if s.authenticator == nil {
			return fail(protocol.NewError(protocol.CodeUnknownOpcode, "auth not required: server runs in open mode"))
		}
		return fail(protocol.NewError(protocol.CodeBadRequest, "connection already authenticated"))
	case protocol.OpCreateTopic:
		if err := s.authorize(p, accessAdmin, ""); err != nil {
			return fail(err)
		}
		var req protocol.CreateTopicRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return badReq(err)
		}
		r, err := s.backend.CreateTopic(ctx, &req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = mustJSON(r)
	case protocol.OpListTopics:
		if err := s.authorize(p, accessAdmin, ""); err != nil {
			return fail(err)
		}
		r, err := s.backend.ListTopics(ctx, &protocol.ListTopicsRequest{})
		if err != nil {
			return fail(err)
		}
		resp.Payload = mustJSON(r)
	case protocol.OpProduce:
		req, err := protocol.DecodeProduceRequest(f.Payload)
		if err != nil {
			return badReq(err)
		}
		if err := s.authorize(p, accessWrite, req.Topic); err != nil {
			return fail(err)
		}
		r, err := s.backend.Produce(ctx, req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = protocol.EncodeProduceResponse(r)
	case protocol.OpFetch:
		req, err := protocol.DecodeFetchRequest(f.Payload)
		if err != nil {
			return badReq(err)
		}
		if err := s.authorize(p, accessRead, req.Topic); err != nil {
			return fail(err)
		}
		r, err := s.backend.Fetch(ctx, req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = protocol.EncodeFetchResponse(r)
	case protocol.OpCommitOffset:
		var req protocol.CommitOffsetRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return badReq(err)
		}
		if err := s.authorize(p, accessRead, req.Topic); err != nil {
			return fail(err)
		}
		r, err := s.backend.CommitOffset(ctx, &req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = mustJSON(r)
	case protocol.OpFetchOffset:
		var req protocol.FetchOffsetRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return badReq(err)
		}
		if err := s.authorize(p, accessRead, req.Topic); err != nil {
			return fail(err)
		}
		r, err := s.backend.FetchOffset(ctx, &req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = mustJSON(r)
	case protocol.OpJoinGroup:
		var req protocol.JoinGroupRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return badReq(err)
		}
		// Joining subscribes the member to every listed topic: each one
		// needs a read grant.
		for _, topic := range req.Topics {
			if err := s.authorize(p, accessRead, topic); err != nil {
				return fail(err)
			}
		}
		r, err := s.backend.JoinGroup(ctx, &req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = mustJSON(r)
	case protocol.OpLeaveGroup:
		var req protocol.LeaveGroupRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return badReq(err)
		}
		r, err := s.backend.LeaveGroup(ctx, &req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = mustJSON(r)
	case protocol.OpHeartbeat:
		var req protocol.HeartbeatRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return badReq(err)
		}
		r, err := s.backend.Heartbeat(ctx, &req)
		if err != nil {
			return fail(err)
		}
		resp.Payload = mustJSON(r)
	default:
		return fail(protocol.NewError(protocol.CodeUnknownOpcode, fmt.Sprintf("opcode 0x%02x not supported", uint8(f.Opcode))))
	}
	return resp
}

// mustJSON marshals a control response. All control response types are
// plain structs of JSON-safe fields, so a failure here is a bug, not a
// runtime condition — encode the empty object and let tests catch it.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
