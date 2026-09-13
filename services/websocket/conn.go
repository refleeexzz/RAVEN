package websocket

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	gws "github.com/gorilla/websocket"

	ravenerrors "github.com/raven/platform/pkg/errors"
)

const (
	// writeWait bounds a single write to the client.
	writeWait = 10 * time.Second
	// pongWait is how long readPump waits for a protocol-level pong before
	// declaring the connection dead.
	pongWait = 60 * time.Second
	// pingPeriod is the server heartbeat cadence; must stay below pongWait.
	pingPeriod = 30 * time.Second
	// maxMessageSize caps one inbound frame.
	maxMessageSize = 32 * 1024
	// sendBuffer is the per-connection outbound queue. Full = slow consumer.
	sendBuffer = 256
)

// conn is one WebSocket connection. A user may hold several conns (tabs).
//
// Goroutine lifecycle (one reader + one writer per conn, both exit on close):
//
//   - readPump: exits when the socket read fails — heartbeat deadline, peer
//     close frame, network error, or ws.Close from writePump during teardown.
//     Its defer is the cleanup coordinator: hub.Unregister + presence
//     teardown run exactly once there.
//   - writePump: exits when c.done closes (teardown) after emitting a
//     protocol close frame, or when a write fails. Its defer closes the
//     socket, which is what unblocks readPump on server-initiated shutdown.
type conn struct {
	id     string
	userID string

	hub      *Hub
	ws       *gws.Conn
	presence *Presence
	fanout   *Fanout
	limiter  *tokenBucket
	log      *slog.Logger

	send chan []byte   // outbound queue, drained by writePump
	done chan struct{} // closed exactly once via closeWithReason

	closeOnce sync.Once
	// closeCode/closeReason are written before done closes; the channel
	// close gives writePump happens-before visibility when it reads them.
	closeCode   int
	closeReason string
	dropped     atomic.Bool // metrics/log fired at most once per conn

	// rooms this connection joined. Guarded by hub.mu — only Hub methods
	// touch it.
	rooms map[string]struct{}
}

func newConn(userID string, ws *gws.Conn, hub *Hub, presence *Presence, fanout *Fanout, log *slog.Logger) *conn {
	return &conn{
		id:       uuid.NewString(),
		userID:   userID,
		hub:      hub,
		ws:       ws,
		presence: presence,
		fanout:   fanout,
		limiter:  newTokenBucket(ratePerSecond, rateBurst),
		log:      log.With(slog.String("user", userID)),
		send:     make(chan []byte, sendBuffer),
		done:     make(chan struct{}),
		rooms:    make(map[string]struct{}),
	}
}

// trySend queues frame without blocking. It reports false when the
// connection is closing or its buffer is full (slow consumer).
func (c *conn) trySend(frame []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.send <- frame:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

// closeWithReason initiates teardown exactly once; later calls are no-ops.
func (c *conn) closeWithReason(code int, reason string) {
	c.closeOnce.Do(func() {
		c.closeCode = code
		c.closeReason = reason
		close(c.done)
	})
}

// sendOut queues a server-initiated frame and counts it. If our own buffer
// is full we are the slow consumer and the hub evicts us.
func (c *conn) sendOut(frame []byte, op string) {
	if c.trySend(frame) {
		c.hub.met.countOut(op)
		return
	}
	select {
	case <-c.done: // already closing; nothing to do
	default:
		c.hub.dropSlow(c)
	}
}

// sendError is a convenience wrapper for protocol error frames.
func (c *conn) sendError(message string) {
	c.sendOut(errorFrame(message), opError)
}

// writePump is the ONLY writer to the socket. Exit paths: c.done closes
// (teardown: emit close frame, then return) or a write fails (peer gone).
// The defer closes the socket, unblocking readPump.
func (c *conn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.ws.Close() // second Close is a harmless error
	}()
	for {
		select {
		case frame := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(gws.TextMessage, frame); err != nil {
				c.log.Debug("write failed, closing connection",
					slog.String("conn_id", c.id), slog.Any("error", err))
				return
			}
		case <-ticker.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(gws.PingMessage, nil); err != nil {
				c.log.Debug("ping failed, closing connection",
					slog.String("conn_id", c.id), slog.Any("error", err))
				return
			}
		case <-c.done:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.ws.WriteMessage(gws.CloseMessage,
				gws.FormatCloseMessage(c.closeCode, c.closeReason))
			return
		}
	}
}

// readPump is the ONLY reader from the socket. Exit paths: heartbeat
// deadline, peer close frame, network error, or ws.Close from writePump
// during teardown. Its defer coordinates cleanup exactly once.
func (c *conn) readPump() {
	defer func() {
		if last := c.hub.Unregister(c); last {
			c.presence.OnDisconnect(c.userID, true)
		}
		c.closeWithReason(gws.CloseNormalClosure, "")
		_ = c.ws.Close()
		c.log.Debug("connection closed", slog.String("conn_id", c.id))
	}()

	c.ws.SetReadLimit(maxMessageSize)
	_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		c.handleFrame(raw)
	}
}

// handleFrame rate-limits, decodes and dispatches one client frame.
func (c *conn) handleFrame(raw []byte) {
	if !c.limiter.allow() {
		c.hub.met.countIn("rate_limited")
		c.sendError("rate limit exceeded")
		return
	}

	m, err := decodeInbound(raw)
	if err != nil {
		c.hub.met.countIn("unknown")
		c.sendError(ravenerrors.MessageOf(err))
		return
	}
	c.hub.met.countIn(m.Op)

	switch m.Op {
	case opJoin:
		if reservedRoom(m.Room) {
			c.sendError(`room prefix "user:" is reserved`)
			return
		}
		if c.hub.Join(c, m.Room) {
			frame, err := joinedFrame(m.Room)
			if err != nil {
				c.log.Error("joined frame encode failed", slog.Any("error", err))
				return
			}
			c.sendOut(frame, opJoined)
		}
	case opLeave:
		if reservedRoom(m.Room) {
			c.sendError(`room prefix "user:" is reserved`)
			return
		}
		c.hub.Leave(c, m.Room)
	case opMsg:
		if reservedRoom(m.Room) {
			c.sendError(`room prefix "user:" is reserved`)
			return
		}
		if !c.hub.InRoom(c, m.Room) {
			c.sendError("join the room before publishing to it")
			return
		}
		frame, err := msgFrame(m.Room, c.userID, m.Data, time.Now())
		if err != nil {
			c.log.Error("msg frame encode failed", slog.Any("error", err))
			return
		}
		c.hub.BroadcastRoom(m.Room, frame, opMsg)
		c.fanout.PublishRoom(m.Room, c.userID, m.Data)
	case opDM:
		frame, err := msgFrame("", c.userID, m.Data, time.Now())
		if err != nil {
			c.log.Error("dm frame encode failed", slog.Any("error", err))
			return
		}
		if c.hub.DeliverUser(m.To, frame, opMsg) == 0 {
			c.sendError("recipient is not connected")
		}
	case opPing:
		c.sendOut(pongFrame(), opPong)
	}
}
