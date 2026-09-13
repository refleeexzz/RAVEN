package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	ravenerrors "github.com/refleeexzz/RAVEN/pkg/errors"
)

const (
	// fanoutChannel carries room broadcasts between websocket nodes.
	fanoutChannel = "raven:ws:fanout"
	// presenceChannel carries online/offline transitions between nodes.
	presenceChannel = "raven:ws:presence"
	// jobEventsChannel is published by workers (see docs/contracts/ports-and-env.md).
	jobEventsChannel = "raven:events:jobs"

	presenceKeyPrefix = "presence:user:"
	presenceTTL       = 90 * time.Second
	presenceRefresh   = 30 * time.Second
	redisOpTimeout    = 2 * time.Second
)

// fanoutEnvelope is the inter-node room broadcast format. Origin identifies
// the publishing node so it can skip its own message (already delivered
// locally) and avoid double delivery.
type fanoutEnvelope struct {
	Origin string          `json:"origin"`
	Room   string          `json:"room"`
	From   string          `json:"from,omitempty"`
	Data   json.RawMessage `json:"data"`
	At     time.Time       `json:"at"`
}

// presenceEvent is the inter-node presence transition format.
type presenceEvent struct {
	Origin string    `json:"origin"`
	User   string    `json:"user"`
	Online bool      `json:"online"`
	At     time.Time `json:"at"`
}

// jobEvent mirrors the worker payload on raven:events:jobs.
type jobEvent struct {
	Type     string    `json:"type"`
	JobID    string    `json:"job_id"`
	Status   string    `json:"status"`
	WorkerID string    `json:"worker_id"`
	OwnerID  string    `json:"owner_id,omitempty"`
	At       time.Time `json:"at"`
}

// Fanout bridges the local hub and Redis pub/sub so every replica delivers
// room broadcasts, presence transitions and job events to its own clients.
// A nil Redis client is supported (tests, single-node dev): publishing
// no-ops and Run just waits for ctx.
type Fanout struct {
	rdb    *redis.Client
	nodeID string
	hub    *Hub
	log    *slog.Logger
}

// NewFanout builds a fanout bridge; nodeID identifies this replica.
func NewFanout(rdb *redis.Client, hub *Hub, log *slog.Logger) *Fanout {
	return &Fanout{
		rdb:    rdb,
		nodeID: uuid.NewString(),
		hub:    hub,
		log:    log,
	}
}

// NodeID identifies this replica in fanout envelopes.
func (f *Fanout) NodeID() string { return f.nodeID }

// PublishRoom fans a room message out to the other nodes. Local delivery
// already happened; a failure only degrades remote nodes and is logged.
func (f *Fanout) PublishRoom(room, from string, data json.RawMessage) {
	if f == nil || f.rdb == nil {
		return
	}
	payload, err := json.Marshal(fanoutEnvelope{
		Origin: f.nodeID, Room: room, From: from, Data: data, At: time.Now(),
	})
	if err != nil {
		f.log.Error("fanout marshal failed", slog.String("room", room), slog.Any("error", err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
	defer cancel()
	if err := f.rdb.Publish(ctx, fanoutChannel, payload).Err(); err != nil {
		f.log.Warn("fanout publish failed; remote nodes will miss this message",
			slog.String("room", room), slog.Any("error", err))
	}
}

// Run subscribes to the fanout, presence and job-event channels until ctx is
// cancelled, resubscribing with backoff when the connection drops.
// Exit path: ctx cancelled → subscription closed → nil return.
func (f *Fanout) Run(ctx context.Context) error {
	if f == nil || f.rdb == nil {
		<-ctx.Done()
		return nil
	}
	backoff := 500 * time.Millisecond
	for {
		err := f.subscribe(ctx)
		if ctx.Err() != nil {
			return nil // graceful shutdown
		}
		f.log.Warn("redis subscription lost; resubscribing",
			slog.Any("error", err), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// subscribe blocks on one pub/sub connection until ctx ends or the
// subscription breaks.
func (f *Fanout) subscribe(ctx context.Context) error {
	sub := f.rdb.Subscribe(ctx, fanoutChannel, presenceChannel, jobEventsChannel)
	defer func() { _ = sub.Close() }()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return ravenerrors.E(ravenerrors.KindUnavailable,
					"redis_sub_closed", "subscription channel closed unexpectedly", nil)
			}
			f.dispatch(msg)
		}
	}
}

func (f *Fanout) dispatch(msg *redis.Message) {
	switch msg.Channel {
	case fanoutChannel:
		var env fanoutEnvelope
		if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
			f.log.Warn("bad fanout envelope", slog.Any("error", err))
			return
		}
		if env.Origin == f.nodeID {
			return // our own broadcast, already delivered locally
		}
		frame, err := msgFrame(env.Room, env.From, env.Data, env.At)
		if err != nil {
			f.log.Warn("fanout frame encode failed", slog.Any("error", err))
			return
		}
		f.hub.BroadcastRoom(env.Room, frame, opMsg)
	case presenceChannel:
		var ev presenceEvent
		if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
			f.log.Warn("bad presence event", slog.Any("error", err))
			return
		}
		if ev.Origin == f.nodeID {
			return
		}
		frame, err := presenceFrame(ev.User, ev.Online, ev.At)
		if err != nil {
			f.log.Warn("presence frame encode failed", slog.Any("error", err))
			return
		}
		f.hub.BroadcastAll(frame, opPresence)
	case jobEventsChannel:
		var ev jobEvent
		if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
			f.log.Warn("bad job event", slog.Any("error", err))
			return
		}
		at := ev.At
		if at.IsZero() {
			at = time.Now()
		}
		raw := json.RawMessage(msg.Payload)
		frame, err := eventFrame("jobs", raw, at)
		if err != nil {
			f.log.Warn("job event frame encode failed", slog.Any("error", err))
			return
		}
		f.hub.BroadcastRoom("jobs", frame, opEvent)
		if ev.OwnerID != "" {
			uf, err := eventFrame(userRoom(ev.OwnerID), raw, at)
			if err != nil {
				f.log.Warn("job owner frame encode failed", slog.Any("error", err))
				return
			}
			f.hub.BroadcastRoom(userRoom(ev.OwnerID), uf, opEvent)
		}
	}
}

// Presence tracks who is online in Redis (presence:user:<id>, 90s TTL,
// refreshed every 30s while connected) and announces transitions on
// raven:ws:presence.
//
// Multi-node caveat: transitions fire on the first LOCAL connect / last
// LOCAL disconnect of a user. A user connected on two nodes may flap
// offline briefly when one node loses its last connection; the surviving
// node's refresher re-creates the key within presenceRefresh. Acceptable
// for the learning platform; a per-node counting scheme would be exact.
type Presence struct {
	rdb    *redis.Client
	nodeID string
	hub    *Hub
	log    *slog.Logger
}

// NewPresence builds the presence tracker. nodeID matches the fanout origin
// so nodes skip their own presence re-deliveries.
func NewPresence(rdb *redis.Client, hub *Hub, nodeID string, log *slog.Logger) *Presence {
	return &Presence{rdb: rdb, nodeID: nodeID, hub: hub, log: log}
}

func presenceKey(userID string) string { return presenceKeyPrefix + userID }

// OnConnect sets the presence key and, on the user's first local
// connection, announces the transition locally and to other nodes.
func (p *Presence) OnConnect(userID string, first bool) {
	if p == nil || p.rdb == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
	defer cancel()
	if err := p.rdb.Set(ctx, presenceKey(userID), 1, presenceTTL).Err(); err != nil {
		p.log.Warn("presence set failed", slog.String("user", userID), slog.Any("error", err))
	}
	if first {
		p.announce(userID, true)
	}
}

// OnDisconnect deletes the key and announces the transition when the user's
// last local connection closed.
func (p *Presence) OnDisconnect(userID string, last bool) {
	if p == nil || p.rdb == nil || !last {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
	defer cancel()
	if err := p.rdb.Del(ctx, presenceKey(userID)).Err(); err != nil {
		p.log.Warn("presence delete failed", slog.String("user", userID), slog.Any("error", err))
	}
	p.announce(userID, false)
}

// announce delivers the transition to local clients and publishes it so
// other nodes can do the same for theirs.
func (p *Presence) announce(userID string, online bool) {
	at := time.Now()
	if frame, err := presenceFrame(userID, online, at); err == nil {
		p.hub.BroadcastAll(frame, opPresence)
	} else {
		p.log.Error("presence frame encode failed", slog.Any("error", err))
	}
	payload, err := json.Marshal(presenceEvent{Origin: p.nodeID, User: userID, Online: online, At: at})
	if err != nil {
		p.log.Error("presence event marshal failed", slog.Any("error", err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
	defer cancel()
	if err := p.rdb.Publish(ctx, presenceChannel, payload).Err(); err != nil {
		p.log.Warn("presence publish failed", slog.String("user", userID), slog.Any("error", err))
	}
}

// RefreshLoop keeps the presence key alive while the connection is open.
// Exit path: c.done closes (connection teardown). One goroutine per
// connection; multiple conns of one user refresh the same key, which is
// idempotent. Returns immediately when presence is disabled (nil receiver
// or nil Redis client).
func (p *Presence) RefreshLoop(c *conn) {
	if p == nil || p.rdb == nil {
		return
	}
	ticker := time.NewTicker(presenceRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
			err := p.rdb.Set(ctx, presenceKey(c.userID), 1, presenceTTL).Err()
			cancel()
			if err != nil {
				p.log.Warn("presence refresh failed",
					slog.String("user", c.userID), slog.Any("error", err))
			}
		}
	}
}
