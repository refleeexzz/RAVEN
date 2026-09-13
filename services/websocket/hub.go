package websocket

import (
	"context"
	"log/slog"
	"sync"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the service-specific Prometheus collectors. They work
// unregistered, so unit tests can use them without a registry.
type Metrics struct {
	Connections prometheus.Gauge
	Rooms       prometheus.Gauge
	Messages    *prometheus.CounterVec // labels: direction(in|out), op
	Dropped     prometheus.Counter
}

// NewMetrics builds the raven_websocket_* collectors.
func NewMetrics() *Metrics {
	return &Metrics{
		Connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "raven", Subsystem: "websocket", Name: "connections",
			Help: "Open WebSocket connections.",
		}),
		Rooms: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "raven", Subsystem: "websocket", Name: "rooms",
			Help: "Rooms with at least one local member.",
		}),
		Messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven", Subsystem: "websocket", Name: "messages_total",
			Help: "Frames processed, by direction (in|out) and op.",
		}, []string{"direction", "op"}),
		Dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven", Subsystem: "websocket", Name: "dropped_total",
			Help: "Connections dropped as slow consumers (full outbound buffer).",
		}),
	}
}

// Collectors lists every collector so main can register them with the
// shared metrics registry.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{m.Connections, m.Rooms, m.Messages, m.Dropped}
}

func (m *Metrics) countIn(op string)  { m.Messages.WithLabelValues("in", op).Inc() }
func (m *Metrics) countOut(op string) { m.Messages.WithLabelValues("out", op).Inc() }

// Stats is the point-in-time snapshot served by GET /debug/stats (the
// console UI polls it).
type Stats struct {
	Connections int `json:"connections"`
	Rooms       int `json:"rooms"`
	Users       int `json:"users"`
}

// Hub owns every connection/room map. Concurrency strategy: the Hub is the
// ONLY writer to the maps and every Hub method takes the lock — there is no
// separate actor goroutine and no hub-owned channels. Conn pumps and Redis
// subscribers call Hub methods directly. Delivery never blocks: a full
// outbound buffer evicts the connection (dropSlow) instead of stalling the
// hub.
type Hub struct {
	log *slog.Logger
	met *Metrics

	mu    sync.RWMutex
	users map[string]map[*conn]struct{} // userID → live connections (multiple tabs allowed)
	rooms map[string]map[*conn]struct{} // room → member connections
	total int                           // total open connections
}

// NewHub builds an empty hub. met must not be nil; use NewMetrics().
func NewHub(log *slog.Logger, met *Metrics) *Hub {
	return &Hub{
		log:   log,
		met:   met,
		users: make(map[string]map[*conn]struct{}),
		rooms: make(map[string]map[*conn]struct{}),
	}
}

// Register adds c under its user ID and auto-joins the personal room
// user:<id>. It reports whether this is the user's first connection (used
// to fire presence exactly once per user).
func (h *Hub) Register(c *conn) (first bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	set, ok := h.users[c.userID]
	if !ok {
		set = make(map[*conn]struct{})
		h.users[c.userID] = set
	}
	first = len(set) == 0
	set[c] = struct{}{}
	h.total++
	h.met.Connections.Inc()
	h.joinLocked(c, userRoom(c.userID))
	return first
}

// Unregister removes c from the user index and every room it joined, and
// deletes empty rooms. It is idempotent and reports whether the user went
// fully offline (its last connection closed) — callers use that to drive
// presence teardown exactly once.
func (h *Hub) Unregister(c *conn) (last bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	set, ok := h.users[c.userID]
	if !ok {
		return false
	}
	if _, ok := set[c]; !ok {
		return false
	}
	delete(set, c)
	if len(set) == 0 {
		delete(h.users, c.userID)
		last = true
	}
	h.total--
	h.met.Connections.Dec()
	for room := range c.rooms {
		h.leaveLocked(c, room)
	}
	return last
}

// Join adds c to room, reporting false when c was already a member.
func (h *Hub) Join(c *conn, room string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.joinLocked(c, room)
}

func (h *Hub) joinLocked(c *conn, room string) bool {
	if _, ok := c.rooms[room]; ok {
		return false
	}
	members, ok := h.rooms[room]
	if !ok {
		members = make(map[*conn]struct{})
		h.rooms[room] = members
		h.met.Rooms.Set(float64(len(h.rooms)))
	}
	members[c] = struct{}{}
	c.rooms[room] = struct{}{}
	return true
}

// Leave removes c from room and deletes the room when it becomes empty.
// Reports false when c was not a member.
func (h *Hub) Leave(c *conn, room string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.leaveLocked(c, room)
}

func (h *Hub) leaveLocked(c *conn, room string) bool {
	if _, ok := c.rooms[room]; !ok {
		return false
	}
	delete(c.rooms, room)
	if members, ok := h.rooms[room]; ok {
		delete(members, c)
		if len(members) == 0 {
			delete(h.rooms, room)
			h.met.Rooms.Set(float64(len(h.rooms)))
		}
	}
	return true
}

// InRoom reports whether c joined room.
func (h *Hub) InRoom(c *conn, room string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := c.rooms[room]
	return ok
}

// BroadcastRoom delivers frame to every local member of room. Members whose
// outbound buffer is full are evicted as slow consumers after the lock is
// released. op is used for the messages_total metric.
func (h *Hub) BroadcastRoom(room string, frame []byte, op string) {
	h.mu.RLock()
	var dropped []*conn
	for c := range h.rooms[room] {
		if c.trySend(frame) {
			h.met.countOut(op)
		} else {
			dropped = append(dropped, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range dropped {
		h.dropSlow(c)
	}
}

// BroadcastAll delivers frame to every local connection (presence fan-in).
func (h *Hub) BroadcastAll(frame []byte, op string) {
	h.mu.RLock()
	var dropped []*conn
	for _, set := range h.users {
		for c := range set {
			if c.trySend(frame) {
				h.met.countOut(op)
			} else {
				dropped = append(dropped, c)
			}
		}
	}
	h.mu.RUnlock()
	for _, c := range dropped {
		h.dropSlow(c)
	}
}

// DeliverUser delivers frame to every connection of userID (direct
// messages) and reports how many connections accepted it.
func (h *Hub) DeliverUser(userID string, frame []byte, op string) int {
	h.mu.RLock()
	delivered := 0
	var dropped []*conn
	for c := range h.users[userID] {
		if c.trySend(frame) {
			h.met.countOut(op)
			delivered++
		} else {
			dropped = append(dropped, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range dropped {
		h.dropSlow(c)
	}
	return delivered
}

// dropSlow evicts a connection whose 256-frame outbound buffer is full. The
// hub never blocks on a slow consumer; the connection gets a policy-violation
// close frame instead. readPump's cleanup is idempotent, so the later
// read-error teardown is a no-op.
func (h *Hub) dropSlow(c *conn) {
	if c.dropped.CompareAndSwap(false, true) {
		h.met.Dropped.Inc()
		h.log.Warn("dropping slow consumer",
			slog.String("conn_id", c.id),
			slog.String("user", c.userID),
		)
	}
	if last := h.Unregister(c); last {
		c.presence.OnDisconnect(c.userID, true)
	}
	c.closeWithReason(gws.ClosePolicyViolation, "slow consumer")
}

// Stats snapshots the registry for GET /debug/stats.
func (h *Hub) Stats() Stats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return Stats{Connections: h.total, Rooms: len(h.rooms), Users: len(h.users)}
}

// Shutdown asks every connection to close (protocol close frame) and waits
// until all pumps have unregistered or ctx expires. Each writePump closes
// its socket on done, which fails the matching readPump, which unregisters
// the connection — so a zero total means every goroutine exited.
func (h *Hub) Shutdown(ctx context.Context) {
	h.mu.RLock()
	conns := make([]*conn, 0, h.total)
	for _, set := range h.users {
		for c := range set {
			conns = append(conns, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range conns {
		c.closeWithReason(gws.CloseGoingAway, "server shutting down")
	}

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if remaining := h.Stats().Connections; remaining == 0 {
			return
		} else {
			select {
			case <-ctx.Done():
				h.log.Warn("shutdown deadline hit with connections still open",
					slog.Int("remaining", remaining))
				return
			case <-ticker.C:
			}
		}
	}
}
