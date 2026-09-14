// Package server is the broker's TCP front end. It owns the listener,
// per-connection goroutines, request dispatch and graceful shutdown.
// It knows the wire protocol but nothing about storage or groups: all
// work goes through the Backend interface.
package server

import (
	"context"
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

// Server is the TCP listener for the broker protocol.
type Server struct {
	addr    string
	backend Backend
	log     *slog.Logger
	drain   time.Duration

	maxConns     int
	idleTimeout  time.Duration
	writeTimeout time.Duration

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
		addr:         addr,
		backend:      backend,
		log:          log,
		drain:        drainTimeout,
		maxConns:     defaultMaxConnections,
		idleTimeout:  defaultIdleTimeout,
		writeTimeout: defaultWriteTimeout,
		conns:        make(map[net.Conn]struct{}),
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
// reads cannot stall the accept loop.
func (s *Server) rejectConn(conn net.Conn) {
	s.log.Warn("connection refused: at max connections",
		slog.String("remote", conn.RemoteAddr().String()),
		slog.Int("max", s.maxConns))
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
	defer conn.Close()

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

	for {
		// Idle deadline: refreshed before every frame, so only genuinely
		// silent connections trip it. This is what reaps half-open
		// connections (client crashed without closing the socket).
		_ = conn.SetReadDeadline(time.Now().Add(s.idleTimeout))
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
		handlers.Add(1)
		go func(f *protocol.Frame) {
			defer handlers.Done()
			defer func() { <-sem }()
			resp := s.dispatch(connCtx, f)
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
// one response frame.
func (s *Server) dispatch(ctx context.Context, f *protocol.Frame) *protocol.Frame {
	resp := &protocol.Frame{Opcode: f.Opcode, CorrelationID: f.CorrelationID}
	fail := func(err error) *protocol.Frame {
		return protocol.ErrorFrame(f.CorrelationID, err)
	}
	badReq := func(err error) *protocol.Frame {
		return fail(protocol.NewError(protocol.CodeBadRequest, err.Error()))
	}

	switch f.Opcode {
	case protocol.OpCreateTopic:
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
