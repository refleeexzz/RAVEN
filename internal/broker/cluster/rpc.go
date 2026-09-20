package cluster

import (
	"context"
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

// This file is the node-to-node transport: a small frame server on the
// cluster listener plus one persistent, serialized connection per peer.
//
// The Listener and Dialer abstractions exist so the chaos suite can put
// a virtual network (drop / delay / duplicate / partition) between
// nodes while the client-facing listener keeps using plain TCP.

// Listener is the accept side of the cluster transport. net.Listener
// does not satisfy it directly because Addr returns net.Addr; the
// tcpListener adapter below bridges that.
type Listener interface {
	Accept() (net.Conn, error)
	Close() error
	Addr() string
}

// tcpListener adapts a net.Listener to the cluster Listener interface.
type tcpListener struct{ net.Listener }

func (l tcpListener) Addr() string { return l.Listener.Addr().String() }

// Dialer is the connect side of the cluster transport.
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// TCPDialer is the production Dialer: plain TCP with a connect timeout.
type TCPDialer struct{ Timeout time.Duration }

// Dial opens a TCP connection, honoring ctx and the configured timeout.
func (d TCPDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	to := d.Timeout
	if to <= 0 {
		to = 5 * time.Second
	}
	nd := net.Dialer{Timeout: to}
	return nd.DialContext(ctx, "tcp", addr)
}

// Handler handles one inbound cluster frame and returns the response
// frame. Every request gets exactly one response, matched by the
// correlation id the peer chose.
type Handler func(ctx context.Context, f *protocol.Frame) *protocol.Frame

// connIdleTimeout reaps half-open cluster connections. Cluster traffic
// is chatty (heartbeats every 500ms), so anything silent for 2 minutes
// is dead, not idle.
const connIdleTimeout = 2 * time.Minute

// nodeServer is the cluster listener: one accept loop, one goroutine
// per connection, sequential request/response per connection (the peer
// client serializes calls, so pipelining never happens in v1).
type nodeServer struct {
	ln      Listener
	handler Handler
	log     *slog.Logger
	closing atomic.Bool
	wg      sync.WaitGroup

	// conns tracks accepted connections so close can force them out of
	// a blocked ReadFrame instead of waiting out the idle deadline.
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

func newNodeServer(ln Listener, h Handler, log *slog.Logger) *nodeServer {
	return &nodeServer{ln: ln, handler: h, log: log, conns: make(map[net.Conn]struct{})}
}

// serve accepts until the listener is closed, then waits for the
// connection goroutines to exit.
func (s *nodeServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.closing.Load() || errors.Is(err, net.ErrClosed) {
				s.wg.Wait()
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			s.log.Error("cluster accept failed", slog.Any("err", err))
			s.wg.Wait()
			return
		}
		s.wg.Add(1)
		s.connsMu.Lock()
		s.conns[conn] = struct{}{}
		s.connsMu.Unlock()
		go s.serveConn(conn)
	}
}

func (s *nodeServer) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()
	defer func() {
		s.connsMu.Lock()
		delete(s.conns, conn)
		s.connsMu.Unlock()
	}()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(connIdleTimeout))
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			return
		}
		resp := s.handler(context.Background(), f)
		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err := protocol.WriteFrame(conn, resp); err != nil {
			return
		}
		if s.closing.Load() {
			return
		}
	}
}

// close stops accepting, force-closes every open connection (waking
// blocked ReadFrames) and waits for connection goroutines.
func (s *nodeServer) close() {
	s.closing.Store(true)
	_ = s.ln.Close()
	s.connsMu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.connsMu.Unlock()
	s.wg.Wait()
}

// peer is one persistent connection to another node. Calls are
// serialized: one outstanding request at a time (request/response, no
// pipelining). A failed call drops the connection; the next call
// reconnects. This keeps the replication loop and the heartbeat loop
// independent of each other's stalls.
type peer struct {
	info   NodeInfo
	dialer Dialer
	mu     sync.Mutex
	conn   net.Conn
	nextID uint64
}

func newPeer(info NodeInfo, dialer Dialer) *peer {
	return &peer{info: info, dialer: dialer, nextID: 1}
}

// call sends one frame and waits for its response. It is safe for
// concurrent use, but calls execute one at a time by design.
func (p *peer) call(ctx context.Context, f *protocol.Frame) (*protocol.Frame, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureConn(ctx); err != nil {
		return nil, err
	}
	p.nextID++
	f.CorrelationID = p.nextID
	deadline := time.Now().Add(30 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = p.conn.SetDeadline(deadline)
	if err := protocol.WriteFrame(p.conn, f); err != nil {
		p.dropConn()
		return nil, fmt.Errorf("write to %s: %w", p.info.ID, err)
	}
	resp, err := protocol.ReadFrame(p.conn)
	if err != nil {
		p.dropConn()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("peer %s closed the connection", p.info.ID)
		}
		return nil, fmt.Errorf("read from %s: %w", p.info.ID, err)
	}
	if resp.CorrelationID != f.CorrelationID {
		p.dropConn()
		return nil, fmt.Errorf("peer %s: correlation id mismatch (got %d, want %d)",
			p.info.ID, resp.CorrelationID, f.CorrelationID)
	}
	return resp, nil
}

func (p *peer) ensureConn(ctx context.Context) error {
	if p.conn != nil {
		return nil
	}
	conn, err := p.dialer.Dial(ctx, p.info.Addr)
	if err != nil {
		return fmt.Errorf("dial %s (%s): %w", p.info.ID, p.info.Addr, err)
	}
	p.conn = conn
	return nil
}

// dropConn closes the current connection (caller holds mu).
func (p *peer) dropConn() {
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

// close shuts the peer connection down for good.
func (p *peer) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropConn()
}
