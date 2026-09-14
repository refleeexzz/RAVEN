package group

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// Sentinel errors; the broker maps them onto wire error codes.
// ErrRebalance covers every "your generation is stale / you are not a
// member anymore" case: the only correct client reaction is to re-join.
var (
	ErrUnknownTopic  = errors.New("group: unknown topic")
	ErrUnknownGroup  = errors.New("group: unknown group")
	ErrUnknownMember = errors.New("group: unknown member")
	ErrRebalance     = errors.New("group: stale generation, rebalance in progress or done")
	// ErrNotAssigned rejects a commit for a partition the member does not
	// own in the current generation (BRKR-03).
	ErrNotAssigned = errors.New("group: partition not assigned to member")
	// ErrTooManyGroups rejects the creation of a new group past the cap
	// (BRKR-04). Empty groups are garbage-collected, so the cap only
	// ever counts groups with state worth keeping.
	ErrTooManyGroups = errors.New("group: too many groups")
	// ErrInvalidID rejects group/member ids with control characters,
	// separators or absurd length: ids flow into log lines and the
	// offsets file, so they get the same charset as topic names.
	ErrInvalidID = errors.New("group: invalid group/member id")
)

// idRe is the allowed charset for group and member ids. They are not
// file paths, but the restriction kills log forging via control
// characters and bounds every line in offsets.jsonl (BRKR-06).
var idRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,128}$`)

func validID(kind, id string) error {
	if !idRe.MatchString(id) {
		return fmt.Errorf("%w: %s %q: allowed [a-zA-Z0-9._-], max 128 chars", ErrInvalidID, kind, id)
	}
	return nil
}

// defaultMaxGroups bounds live group states when no explicit cap is
// configured.
const defaultMaxGroups = 1024

// member is one live consumer in a group.
type member struct {
	id       string
	topics   []string
	lastSeen time.Time
}

// groupState is the coordinator's view of one group.
type groupState struct {
	id          string
	generation  int32
	members     map[string]*member
	assignments map[string][]protocol.Assignment
}

// GroupInfo is a snapshot for the ops API.
type GroupInfo struct {
	ID         string
	Generation int32
	Members    int
}

// Coordinator owns all consumer groups. Every state change goes through
// mu; rebalances are computed synchronously inside Join/Leave/expire,
// which keeps the protocol trivially consistent: a Join response always
// carries the assignment of the generation it created.
type Coordinator struct {
	mu             sync.Mutex
	groups         map[string]*groupState
	sessionTimeout time.Duration
	partitionsFor  func(topic string) (int, bool)
	offsets        *offsetStore
	maxGroups      int
	log            *slog.Logger

	reaperCancel context.CancelFunc
	reaperDone   chan struct{}
}

// Option customizes a Coordinator.
type Option func(*Coordinator)

// WithMaxGroups caps the number of live group states (0 keeps the
// default of 1024). Groups whose last member left or expired are
// garbage-collected — committed offsets live in the offset store and
// survive — so the cap only bounds groups that actually hold members.
func WithMaxGroups(n int) Option {
	return func(c *Coordinator) {
		if n > 0 {
			c.maxGroups = n
		}
	}
}

// NewCoordinator builds a coordinator. partitionsFor resolves topic
// partition counts (the broker injects storage access here).
// sessionTimeout is how long a member may go without a heartbeat before
// it is expelled and the group rebalances.
func NewCoordinator(dataDir string, partitionsFor func(topic string) (int, bool), sessionTimeout time.Duration, log *slog.Logger, opts ...Option) (*Coordinator, error) {
	if log == nil {
		log = slog.Default()
	}
	offsets, err := openOffsetStore(dataDir)
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		groups:         make(map[string]*groupState),
		sessionTimeout: sessionTimeout,
		partitionsFor:  partitionsFor,
		offsets:        offsets,
		maxGroups:      defaultMaxGroups,
		log:            log,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Start launches the reaper goroutine that expels dead members. It
// exits when ctx is cancelled; Close waits for it.
func (c *Coordinator) Start(ctx context.Context) {
	rctx, cancel := context.WithCancel(context.Background())
	c.reaperCancel = cancel
	c.reaperDone = make(chan struct{})
	interval := c.sessionTimeout / 3
	if interval <= 0 || interval > time.Second {
		interval = time.Second
	}
	go func() {
		defer close(c.reaperDone)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-rctx.Done():
				return
			case <-ctx.Done():
				return
			case now := <-t.C:
				c.expireDeadMembers(now)
			}
		}
	}()
}

// Close stops the reaper and compacts/closes the offsets file.
func (c *Coordinator) Close() error {
	if c.reaperCancel != nil {
		c.reaperCancel()
		<-c.reaperDone
	}
	return c.offsets.Close()
}

// Join adds (or updates) a member and returns the current generation
// and this member's assignment.
//
// Rebalance invariant: the generation moves ONLY when the membership
// or a subscription actually changes (new member, leave, timeout,
// topic change). A re-join by an existing member with an unchanged
// subscription is a no-op for the group: it just refreshes liveness
// and hands back the current generation and assignment. Bumping the
// generation here would invalidate every other member's heartbeat and
// cascade into a rebalance storm (see docs/broker-internals.md).
func (c *Coordinator) Join(groupID, memberID string, topics []string) (int32, []protocol.Assignment, error) {
	if groupID == "" || memberID == "" || len(topics) == 0 {
		return 0, nil, fmt.Errorf("group: join requires group, member_id and topics")
	}
	if err := validID("group", groupID); err != nil {
		return 0, nil, err
	}
	if err := validID("member", memberID); err != nil {
		return 0, nil, err
	}
	for _, t := range topics {
		if _, ok := c.partitionsFor(t); !ok {
			return 0, nil, fmt.Errorf("%w: %s", ErrUnknownTopic, t)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		if len(c.groups) >= c.maxGroups {
			return 0, nil, fmt.Errorf("%w (limit %d)", ErrTooManyGroups, c.maxGroups)
		}
		g = &groupState{id: groupID, members: make(map[string]*member), assignments: make(map[string][]protocol.Assignment)}
		c.groups[groupID] = g
	}
	if m, ok := g.members[memberID]; ok && sameTopics(m.topics, topics) {
		m.lastSeen = time.Now()
		return g.generation, g.assignments[memberID], nil
	}
	m, ok := g.members[memberID]
	if !ok {
		m = &member{id: memberID}
		g.members[memberID] = m
	}
	m.topics = append([]string(nil), topics...)
	m.lastSeen = time.Now()
	c.rebalanceLocked(g)
	return g.generation, g.assignments[memberID], nil
}

// sameTopics compares two subscriptions as sets (order is not a change).
func sameTopics(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, t := range a {
		seen[t]++
	}
	for _, t := range b {
		seen[t]--
		if seen[t] < 0 {
			return false
		}
	}
	return true
}

// Leave removes a member and rebalances. Leaving a group you are not in
// is a no-op (idempotent leave keeps shutdown paths simple). A group
// whose last member left is dropped: committed offsets live in the
// offset store and survive, and a re-join simply starts a fresh group
// (BRKR-04: group states must not accumulate forever).
func (c *Coordinator) Leave(groupID, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return nil
	}
	if _, ok := g.members[memberID]; !ok {
		return nil
	}
	delete(g.members, memberID)
	if len(g.members) == 0 {
		delete(c.groups, groupID)
		return nil
	}
	c.rebalanceLocked(g)
	return nil
}

// Heartbeat refreshes a member's liveness. A stale generation or an
// unknown member/group returns ErrRebalance so the client re-joins.
func (c *Coordinator) Heartbeat(groupID, memberID string, generation int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return ErrRebalance
	}
	m, ok := g.members[memberID]
	if !ok {
		return ErrRebalance
	}
	if generation != g.generation {
		return ErrRebalance
	}
	m.lastSeen = time.Now()
	return nil
}

// CheckFetch validates group context on FETCH: the group and member
// must exist and the generation must be current.
func (c *Coordinator) CheckFetch(groupID, memberID string, generation int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return ErrRebalance
	}
	if _, ok := g.members[memberID]; !ok {
		return ErrRebalance
	}
	if generation != g.generation {
		return ErrRebalance
	}
	return nil
}

// Commit persists the next-to-consume offset for (group, topic,
// partition). Stale generations are rejected with ErrRebalance so a
// zombie member from an old generation cannot rewind committed state.
// The (topic, partition) must be part of the member's assignment in the
// current generation (BRKR-03): without that binding any live member
// could clobber another member's progress or write offsets under topics
// the group never subscribed to.
func (c *Coordinator) Commit(groupID, memberID, topic string, partition int32, offset uint64, generation int32) error {
	c.mu.Lock()
	g, ok := c.groups[groupID]
	if !ok {
		c.mu.Unlock()
		return ErrRebalance
	}
	if _, ok := g.members[memberID]; !ok {
		c.mu.Unlock()
		return ErrUnknownMember
	}
	if generation != g.generation {
		c.mu.Unlock()
		return ErrRebalance
	}
	assigned := false
	for _, a := range g.assignments[memberID] {
		if a.Topic != topic {
			continue
		}
		for _, p := range a.Partitions {
			if p == partition {
				assigned = true
				break
			}
		}
	}
	c.mu.Unlock()
	if !assigned {
		return ErrNotAssigned
	}
	// Disk write outside the lock: offsets have their own mutex.
	return c.offsets.Set(groupID, topic, partition, offset)
}

// CheckOffsetAccess authorizes a FETCH_OFFSET (BRKR-03): the group must
// exist, the member must be joined to it, and the generation must be
// current. Reading offsets is intentionally NOT assignment-bound —
// members legitimately inspect sibling partitions — but it is always
// membership-bound, so a client can no longer read a foreign group's
// offsets just by naming its id.
func (c *Coordinator) CheckOffsetAccess(groupID, memberID string, generation int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return ErrRebalance
	}
	if _, ok := g.members[memberID]; !ok {
		return ErrUnknownMember
	}
	if generation != g.generation {
		return ErrRebalance
	}
	return nil
}

// Committed returns the next-to-consume offset, 0 when never committed.
func (c *Coordinator) Committed(groupID, topic string, partition int32) uint64 {
	off, _ := c.offsets.Get(groupID, topic, partition)
	return off
}

// GroupOffsets returns all committed offsets of a group.
func (c *Coordinator) GroupOffsets(groupID string) map[string]map[int32]uint64 {
	return c.offsets.Snapshot(groupID)
}

// OffsetGroupIDs lists groups with at least one committed offset
// (includes currently memberless groups).
func (c *Coordinator) OffsetGroupIDs() []string {
	return c.offsets.GroupIDs()
}

// ActiveGroups counts groups with at least one live member.
func (c *Coordinator) ActiveGroups() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, g := range c.groups {
		if len(g.members) > 0 {
			n++
		}
	}
	return n
}

// GroupsInfo snapshots group state for the ops API.
func (c *Coordinator) GroupsInfo() []GroupInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]GroupInfo, 0, len(c.groups))
	for _, g := range c.groups {
		out = append(out, GroupInfo{ID: g.id, Generation: g.generation, Members: len(g.members)})
	}
	return out
}

// expireDeadMembers removes members whose heartbeat expired and
// rebalances their groups. Called by the reaper goroutine. Groups that
// end up memberless are dropped (offsets survive in the offset store).
func (c *Coordinator) expireDeadMembers(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, g := range c.groups {
		changed := false
		for mid, m := range g.members {
			if now.Sub(m.lastSeen) > c.sessionTimeout {
				c.log.Warn("member heartbeat expired, expelling",
					slog.String("group", g.id), slog.String("member", mid))
				delete(g.members, mid)
				changed = true
			}
		}
		if !changed {
			continue
		}
		if len(g.members) == 0 {
			delete(c.groups, id)
			continue
		}
		c.rebalanceLocked(g)
	}
}

// rebalanceLocked bumps the generation and recomputes every member's
// assignment. Caller holds mu.
func (c *Coordinator) rebalanceLocked(g *groupState) {
	g.generation++
	members := make([]memberInfo, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, memberInfo{id: m.id, topics: m.topics})
	}
	g.assignments = rangeAssign(members, c.partitionsFor)
	c.log.Info("group rebalanced",
		slog.String("group", g.id),
		slog.Int("generation", int(g.generation)),
		slog.Int("members", len(g.members)))
}
