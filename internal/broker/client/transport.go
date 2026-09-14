// Package client is the Go client for the RAVEN broker. Other services
// (jobs, worker) use it to produce and consume; it hides framing,
// correlation ids, reconnects, batching, heartbeats and rebalances.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// transport is a reconnecting, pipelined connection to the broker.
// Concurrent calls are safe: writes are serialized and responses are
// routed by correlation id.
type transport struct {
	addr        string
	dialTimeout time.Duration
	log         *slog.Logger

	// tlsCfg, when non-nil, upgrades every dialed connection to TLS
	// before any frame is written. Set via the With*TLS options.
	tlsCfg *tls.Config

	mu      sync.Mutex // guards conn, pending
	conn    net.Conn
	pending map[uint64]chan callResult

	writeMu sync.Mutex // serializes frame writes
	corr    atomic.Uint64
	closed  atomic.Bool
}

type callResult struct {
	frame *protocol.Frame
	err   error
}

// ErrClosed is returned by calls after Close.
var ErrClosed = errors.New("client: transport closed")

func newTransport(addr string, dialTimeout time.Duration, log *slog.Logger) *transport {
	if log == nil {
		log = slog.Default()
	}
	return &transport{
		addr:        addr,
		dialTimeout: dialTimeout,
		log:         log,
		pending:     make(map[uint64]chan callResult),
	}
}

// call sends one request and waits for its response. If the connection
// is down it reconnects first, with exponential backoff bounded by ctx.
// A request whose write already started is never retried silently: a
// mid-write failure is returned to the caller (retrying produce blindly
// would duplicate records).
func (t *transport) call(ctx context.Context, op protocol.Opcode, payload []byte) (*protocol.Frame, error) {
	if t.closed.Load() {
		return nil, ErrClosed
	}
	conn, err := t.ensureConnected(ctx)
	if err != nil {
		return nil, err
	}
	id := t.corr.Add(1)
	ch := make(chan callResult, 1)
	t.mu.Lock()
	if t.closed.Load() {
		t.mu.Unlock()
		return nil, ErrClosed
	}
	t.pending[id] = ch
	t.mu.Unlock()

	frame := &protocol.Frame{Opcode: op, CorrelationID: id, Payload: payload}
	t.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	werr := protocol.WriteFrame(conn, frame)
	t.writeMu.Unlock()
	if werr != nil {
		t.removePending(id)
		t.dropConn(conn)
		return nil, fmt.Errorf("client: write %s: %w", op, werr)
	}

	select {
	case res := <-ch:
		return res.frame, res.err
	case <-ctx.Done():
		t.removePending(id)
		return nil, ctx.Err()
	}
}

// ensureConnected returns the live connection, dialing with
// exponential backoff (50ms → 2s cap) until ctx gives up.
func (t *transport) ensureConnected(ctx context.Context) (net.Conn, error) {
	backoff := 50 * time.Millisecond
	for {
		t.mu.Lock()
		c := t.conn
		t.mu.Unlock()
		if c != nil {
			return c, nil
		}
		if t.closed.Load() {
			return nil, ErrClosed
		}
		d := &net.Dialer{Timeout: t.dialTimeout, KeepAlive: 30 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", t.addr)
		if err != nil {
			t.log.Debug("broker dial failed, retrying",
				slog.String("addr", t.addr), slog.Any("err", err))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			backoff *= 2
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			continue
		}
		if t.tlsCfg != nil {
			// Clone per connection: ServerName defaults to the dial
			// host, and a caller-owned config must never be mutated.
			cfg := t.tlsCfg.Clone()
			if cfg.ServerName == "" {
				if host, _, err := net.SplitHostPort(t.addr); err == nil {
					cfg.ServerName = host
				}
			}
			tc := tls.Client(conn, cfg)
			if err := tc.HandshakeContext(ctx); err != nil {
				_ = conn.Close()
				t.log.Debug("broker TLS handshake failed, retrying",
					slog.String("addr", t.addr), slog.Any("err", err))
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				backoff *= 2
				if backoff > 2*time.Second {
					backoff = 2 * time.Second
				}
				continue
			}
			conn = tc
		}
		t.mu.Lock()
		if t.conn != nil {
			// Another goroutine won the dial race.
			t.mu.Unlock()
			_ = conn.Close()
			continue
		}
		t.conn = conn
		t.mu.Unlock()
		go t.readLoop(conn)
		return conn, nil
	}
}

// readLoop routes responses to pending calls. On any error it tears
// down the connection and fails every pending call; the next call
// reconnects. The goroutine exits when the connection dies or Close
// runs, so it never leaks.
func (t *transport) readLoop(conn net.Conn) {
	for {
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			t.mu.Lock()
			if t.conn == conn {
				t.conn = nil
			}
			pending := t.pending
			t.pending = make(map[uint64]chan callResult)
			t.mu.Unlock()
			if !t.closed.Load() {
				t.log.Debug("broker connection lost", slog.String("addr", t.addr), slog.Any("err", err))
			}
			for _, ch := range pending {
				ch <- callResult{err: fmt.Errorf("client: connection lost: %w", err)}
			}
			_ = conn.Close()
			return
		}
		t.mu.Lock()
		ch, ok := t.pending[f.CorrelationID]
		delete(t.pending, f.CorrelationID)
		t.mu.Unlock()
		if ok {
			ch <- callResult{frame: f}
		}
		// Unknown correlation id: a call that timed out meanwhile.
		// Drop it; pipelining stays correct.
	}
}

func (t *transport) removePending(id uint64) {
	t.mu.Lock()
	delete(t.pending, id)
	t.mu.Unlock()
}

// dropConn tears down conn if it is still the live one (a write error
// means the read loop may be stuck on a dead socket).
func (t *transport) dropConn(conn net.Conn) {
	t.mu.Lock()
	cur := t.conn
	if cur == conn {
		t.conn = nil
	}
	pending := make([]chan callResult, 0, len(t.pending))
	if cur == conn {
		for id, ch := range t.pending {
			delete(t.pending, id)
			pending = append(pending, ch)
		}
	}
	t.mu.Unlock()
	if cur == conn {
		for _, ch := range pending {
			ch <- callResult{err: errors.New("client: connection lost on write")}
		}
		_ = conn.Close()
	}
}

// Close shuts the transport down and fails pending calls.
func (t *transport) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	t.mu.Lock()
	conn := t.conn
	t.conn = nil
	pending := t.pending
	t.pending = make(map[uint64]chan callResult)
	t.mu.Unlock()
	for _, ch := range pending {
		ch <- callResult{err: ErrClosed}
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// checkError turns an OpError frame into a *protocol.Error.
func checkError(f *protocol.Frame) error {
	if f.Opcode == protocol.OpError {
		return protocol.DecodeErrorFrame(f.Payload)
	}
	return nil
}
