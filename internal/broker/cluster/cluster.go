package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// Assignment describes who holds one partition: the full replica set
// and the current leader (always one of the replicas).
type Assignment struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Leader    NodeID   `json:"leader"`
	Replicas  []NodeID `json:"replicas"`
}

// Cluster is the node-to-node control plane. It owns the cluster
// listener, the membership table and the heartbeat/failure-detection
// loops. The data plane (replication, elections, reassignment) plugs in
// through Handle; Cluster itself only speaks NODE_PING.
type Cluster struct {
	cfg    Config
	log    *slog.Logger
	dialer Dialer

	mu      sync.RWMutex
	ln      Listener
	srv     *nodeServer
	peers   map[NodeID]*peer
	members *Membership

	// handlers routes inbound opcodes; Cluster registers NODE_PING
	// itself, the broker registers the rest.
	handlersMu sync.RWMutex
	handlers   map[protocol.Opcode]Handler

	// overlay holds operator-driven reassignments (see assignment.go).
	overlay *overlay

	metrics *Metrics

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// onPeerDead is invoked when the failure detector flips a peer to
	// DEAD. The election layer uses it to start elections for the
	// partitions that peer led. Set by the broker; nil-safe.
	onPeerDead func(NodeID)

	// noLeaveAnnounce skips the graceful leaving announce in Run:
	// tests and the chaos suite use it to simulate a hard kill
	// (SIGKILL), where the process dies without saying goodbye.
	noLeaveAnnounce atomic.Bool
}

// Option customizes a Cluster for tests and the chaos suite.
type Option func(*Cluster)

// WithDialer overrides the peer dialer (default: TCP with 5s timeout).
func WithDialer(d Dialer) Option {
	return func(c *Cluster) { c.dialer = d }
}

// WithListener overrides the cluster listener (default: TCP bound to
// the configured self address).
func WithListener(l Listener) Option {
	return func(c *Cluster) { c.ln = l }
}

// WithClock overrides the membership clock (tests only).
func WithClock(now func() time.Time) Option {
	return func(c *Cluster) {
		c.members.now = now
	}
}

// WithPeerDeadHook installs the callback fired when a peer goes DEAD.
func WithPeerDeadHook(fn func(NodeID)) Option {
	return func(c *Cluster) { c.onPeerDead = fn }
}

// New builds the cluster control plane. The listener is bound eagerly
// (a bad address fails the broker boot, not the first heartbeat). reg
// may be nil (tests); metrics registration is the broker's job via
// RegisterMetrics.
func New(cfg Config, log *slog.Logger, opts ...Option) (*Cluster, error) {
	if log == nil {
		log = slog.Default()
	}
	if cfg.NodeID == "" || len(cfg.Nodes) == 0 {
		return nil, errors.New("cluster: Config is not enabled (missing NodeID/Nodes)")
	}
	cfg = cfg.withDefaults()
	c := &Cluster{
		cfg:      cfg,
		log:      log.With(slog.String("node", string(cfg.NodeID))),
		dialer:   TCPDialer{Timeout: 5 * time.Second},
		peers:    make(map[NodeID]*peer, len(cfg.Nodes)-1),
		handlers: make(map[protocol.Opcode]Handler),
		overlay:  newOverlay(),
		metrics:  newMetrics(),
	}
	c.members = NewMembership(cfg, c.onMemberChange)
	for _, o := range opts {
		o(c)
	}
	if c.ln == nil {
		self := cfg.Self()
		ln, err := net.Listen("tcp", self.Addr)
		if err != nil {
			return nil, fmt.Errorf("cluster listen %s: %w", self.Addr, err)
		}
		c.ln = tcpListener{ln}
	}
	for _, n := range cfg.Peers() {
		c.peers[n.ID] = newPeer(n, c.dialer)
	}
	c.Handle(protocol.OpNodePing, c.handlePing)
	c.srv = newNodeServer(c.ln, c.dispatch, c.log)
	return c, nil
}

// Handle registers the handler for one cluster opcode. It exists so the
// broker can plug the data plane (replicate, vote, status, reassign)
// into the control plane's listener without the cluster package
// importing the broker.
func (c *Cluster) Handle(op protocol.Opcode, h Handler) {
	c.handlersMu.Lock()
	c.handlers[op] = h
	c.handlersMu.Unlock()
}

// dispatch routes one inbound frame to its opcode handler. Unknown
// opcodes get UNKNOWN_OPCODE, exactly like the client-facing server.
func (c *Cluster) dispatch(ctx context.Context, f *protocol.Frame) *protocol.Frame {
	c.handlersMu.RLock()
	h, ok := c.handlers[f.Opcode]
	c.handlersMu.RUnlock()
	if !ok {
		return errFrame(f.CorrelationID, protocol.CodeUnknownOpcode,
			fmt.Sprintf("cluster opcode 0x%02x not supported", uint8(f.Opcode)))
	}
	return h(ctx, f)
}

// Run starts the accept loop and the heartbeat/failure-detection loops,
// and blocks until ctx is cancelled. Peers are left as-is on return;
// call Close to release the listener and peer connections.
func (c *Cluster) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		c.srv.serve()
	}()
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop(ctx)
	}()
	c.log.Info("cluster listener up",
		slog.String("addr", c.ln.Addr()),
		slog.Int("nodes", len(c.cfg.Nodes)),
		slog.Int("replication_factor", c.cfg.ReplicationFactor))
	<-ctx.Done()
	// Announce a graceful departure so peers mark us LEAVING instead of
	// waiting out the DEAD timeout. Best effort, short deadline. A hard
	// crash (noLeaveAnnounce) skips this — that is the whole point of
	// simulating SIGKILL.
	if !c.noLeaveAnnounce.Load() {
		c.announceLeaving()
	}
	c.srv.close()
	c.wg.Wait()
	return nil
}

// Close releases the listener and every peer connection. Run must have
// returned already (or never been started).
func (c *Cluster) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	for _, p := range c.peers {
		p.close()
	}
	return nil
}

// Addr returns the bound cluster listener address.
func (c *Cluster) Addr() string { return c.ln.Addr() }

// Members returns the membership snapshot (ops surface).
func (c *Cluster) Members() []Member { return c.members.Snapshot() }

// MemberState reports this node's view of one member.
func (c *Cluster) MemberState(id NodeID) MemberState { return c.members.State(id) }

// Self returns this node's id.
func (c *Cluster) Self() NodeID { return c.cfg.NodeID }

// Config returns the resolved cluster configuration.
func (c *Cluster) Config() Config { return c.cfg }

// callPeer sends one frame to a peer and returns the decoded response
// payload. A successful exchange marks the peer seen; an OpError frame
// is returned as a *protocol.Error.
func (c *Cluster) callPeer(ctx context.Context, id NodeID, op protocol.Opcode, payload []byte) ([]byte, error) {
	p, ok := c.peers[id]
	if !ok {
		return nil, fmt.Errorf("cluster: unknown peer %s", id)
	}
	resp, err := p.call(ctx, &protocol.Frame{Opcode: op, Payload: payload})
	if err != nil {
		return nil, err
	}
	c.members.MarkSeen(id)
	if resp.Opcode == protocol.OpError {
		return nil, protocol.DecodeErrorFrame(resp.Payload)
	}
	return resp.Payload, nil
}

// ---- heartbeats + failure detection ----

// heartbeatLoop pings every peer on the heartbeat cadence and advances
// the failure detector on the same ticker.
func (c *Cluster) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.HeartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.pingAll(ctx, false)
		c.members.Tick()
	}
}

// pingAll sends one NODE_PING to every peer (concurrently; a stalled
// peer must not delay the others).
func (c *Cluster) pingAll(ctx context.Context, leaving bool) {
	req := NodePingRequest{ID: c.cfg.NodeID, Leaving: leaving}
	payload, _ := json.Marshal(req)
	var wg sync.WaitGroup
	for id, p := range c.peers {
		wg.Add(1)
		go func(id NodeID, p *peer) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, 2*c.cfg.HeartbeatEvery)
			defer cancel()
			resp, err := p.call(pctx, &protocol.Frame{Opcode: protocol.OpNodePing, Payload: payload})
			if err != nil {
				c.metrics.heartbeats.WithLabelValues(string(id), "fail").Inc()
				return
			}
			c.members.MarkSeen(id)
			if resp.Opcode == protocol.OpError {
				c.metrics.heartbeats.WithLabelValues(string(id), "fail").Inc()
				return
			}
			c.metrics.heartbeats.WithLabelValues(string(id), "ok").Inc()
		}(id, p)
	}
	wg.Wait()
}

// announceLeaving notifies every peer of the graceful shutdown. It uses
// its own short-lived context: Run's ctx is already done at this point.
func (c *Cluster) announceLeaving() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c.pingAll(ctx, true)
}

// handlePing answers NODE_PING: mark the sender seen (or leaving), then
// prove we are alive.
func (c *Cluster) handlePing(_ context.Context, f *protocol.Frame) *protocol.Frame {
	var req NodePingRequest
	if err := json.Unmarshal(f.Payload, &req); err != nil {
		return errFrame(f.CorrelationID, protocol.CodeBadRequest, err.Error())
	}
	if req.ID == "" {
		return errFrame(f.CorrelationID, protocol.CodeBadRequest, "missing node id")
	}
	if req.Leaving {
		c.members.MarkLeaving(req.ID)
	} else {
		c.members.MarkSeen(req.ID)
	}
	payload, _ := json.Marshal(NodePingResponse{ID: c.cfg.NodeID})
	return &protocol.Frame{Opcode: protocol.OpNodePing, CorrelationID: f.CorrelationID, Payload: payload}
}

// onMemberChange feeds membership transitions into logs and metrics,
// and fires the election hook when a peer dies.
func (c *Cluster) onMemberChange(mem Member) {
	c.metrics.transitions.WithLabelValues(mem.State.String()).Inc()
	switch mem.State {
	case StateActive:
		c.log.Info("cluster member ACTIVE", slog.String("member", string(mem.ID)))
	case StateSuspect:
		c.log.Warn("cluster member SUSPECT (heartbeat timeout)",
			slog.String("member", string(mem.ID)))
	case StateDead:
		c.log.Warn("cluster member DEAD (heartbeat timeout)",
			slog.String("member", string(mem.ID)))
		if c.onPeerDead != nil {
			c.onPeerDead(mem.ID)
		}
	case StateLeaving:
		c.log.Info("cluster member LEAVING (graceful)", slog.String("member", string(mem.ID)))
		if c.onPeerDead != nil {
			// A leaving leader must trigger elections too: from the
			// cluster's point of view its partitions need new leaders,
			// they just get them faster than the DEAD timeout.
			c.onPeerDead(mem.ID)
		}
	}
}

// ---- placement ----

// AssignmentFor computes the deterministic v1 placement for one
// partition: replicas walk the sorted node ring starting at
// hash(topic)+partition, and the leader is the first replica. Every
// node computes the same assignment from the same inputs, so no
// assignment traffic is needed for ordinary operation. Reassignments
// recorded in the overlay (see reassignment.go) take precedence.
func (c *Cluster) AssignmentFor(topic string, partition int32) Assignment {
	if a, ok := c.assignmentOverlay(topic, partition); ok {
		return a
	}
	ids := c.cfg.sortedIDs()
	n := len(ids)
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	start := (int(h.Sum32()) + int(partition)) % n
	rf := c.cfg.ReplicationFactor
	if rf > n {
		rf = n
	}
	replicas := make([]NodeID, 0, rf)
	for i := 0; i < rf; i++ {
		replicas = append(replicas, ids[(start+i)%n])
	}
	return Assignment{Topic: topic, Partition: partition, Leader: replicas[0], Replicas: replicas}
}

// IsLeaderFor reports whether this node currently leads the partition.
func (c *Cluster) IsLeaderFor(topic string, partition int32) bool {
	return c.AssignmentFor(topic, partition).Leader == c.cfg.NodeID
}

// IsReplicaFor reports whether this node holds a replica of the
// partition (leader or follower).
func (c *Cluster) IsReplicaFor(topic string, partition int32) bool {
	for _, id := range c.AssignmentFor(topic, partition).Replicas {
		if id == c.cfg.NodeID {
			return true
		}
	}
	return false
}
