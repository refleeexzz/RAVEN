package cluster

import (
	"sync"
	"time"
)

// MemberState is the lifecycle state of a cluster member as seen by
// this node.
type MemberState int

const (
	// StateJoining: configured but never contacted successfully yet.
	StateJoining MemberState = iota
	// StateActive: heartbeats flowing; the member participates in
	// elections and ISR quorums.
	StateActive
	// StateSuspect: silent for longer than SuspectAfter. Still counted
	// for quorums (it may be slow, not dead) but elections may start.
	StateSuspect
	// StateDead: silent for longer than DeadAfter. Removed from ISR
	// quorums; its leader partitions elect replacements.
	StateDead
	// StateLeaving: the member announced a graceful shutdown. Treated
	// like DEAD for placement, but without alarm: it is expected.
	StateLeaving
)

// String renders the state for logs, metrics labels and the ops API.
func (s MemberState) String() string {
	switch s {
	case StateJoining:
		return "JOINING"
	case StateActive:
		return "ACTIVE"
	case StateSuspect:
		return "SUSPECT"
	case StateDead:
		return "DEAD"
	case StateLeaving:
		return "LEAVING"
	default:
		return "UNKNOWN"
	}
}

// Member is the local view of one cluster member.
type Member struct {
	NodeInfo
	State MemberState `json:"state"`
	// LastSeen is when the last valid frame arrived from this member
	// (zero for never). It is wall-clock local time: good enough for
	// timeout comparisons, never compared across nodes.
	LastSeen time.Time `json:"last_seen,omitempty"`
	// Since is when the current state began.
	Since time.Time `json:"since"`
}

// Membership is this node's table of every configured member. It is a
// pure state machine: the heartbeat loop feeds it observations
// (MarkSeen / MarkLeaving) and a ticker advances timeouts (Tick). All
// placement and election decisions read Snapshot/State.
type Membership struct {
	mu       sync.RWMutex
	self     NodeID
	members  map[NodeID]*Member
	suspect  time.Duration
	dead     time.Duration
	now      func() time.Time // test hook
	onChange func(Member)     // metrics/log hook, called outside the lock
}

// NewMembership builds the table from the static config. Every peer
// starts JOINING; self is always ACTIVE.
func NewMembership(cfg Config, onChange func(Member)) *Membership {
	m := &Membership{
		self:     cfg.NodeID,
		members:  make(map[NodeID]*Member, len(cfg.Nodes)),
		suspect:  cfg.SuspectAfter,
		dead:     cfg.DeadAfter,
		now:      time.Now,
		onChange: onChange,
	}
	now := m.now()
	for _, n := range cfg.Nodes {
		st := StateJoining
		if n.ID == cfg.NodeID {
			st = StateActive
		}
		m.members[n.ID] = &Member{NodeInfo: n, State: st, Since: now}
		if st == StateActive {
			m.members[n.ID].LastSeen = now
		}
	}
	return m
}

// MarkSeen records a valid frame from id: JOINING/SUSPECT/DEAD/LEAVING
// all transition back to ACTIVE. (A LEAVING node that speaks again is
// treated as rejoining — v1 has no incarnation counter, documented.)
func (m *Membership) MarkSeen(id NodeID) {
	m.mu.Lock()
	mem, ok := m.members[id]
	if !ok {
		m.mu.Unlock()
		return
	}
	changed := mem.State != StateActive
	mem.LastSeen = m.now()
	if changed {
		mem.State = StateActive
		mem.Since = mem.LastSeen
	}
	snap := *mem
	m.mu.Unlock()
	if changed {
		m.notify(snap)
	}
}

// MarkLeaving records a graceful-shutdown announcement from id. Any
// prior state moves to LEAVING; the failure detector leaves LEAVING
// nodes alone afterwards.
func (m *Membership) MarkLeaving(id NodeID) {
	m.mu.Lock()
	mem, ok := m.members[id]
	if !ok {
		m.mu.Unlock()
		return
	}
	changed := mem.State != StateLeaving
	if changed {
		mem.State = StateLeaving
		mem.Since = m.now()
	}
	snap := *mem
	m.mu.Unlock()
	if changed {
		m.notify(snap)
	}
}

// Tick advances timeout-based transitions: an ACTIVE member silent for
// longer than SuspectAfter becomes SUSPECT; silent for longer than
// DeadAfter becomes DEAD. LEAVING is terminal (until a MarkSeen).
func (m *Membership) Tick() {
	now := m.now()
	var changed []Member
	m.mu.Lock()
	for _, mem := range m.members {
		if mem.NodeInfo.ID == m.self {
			continue
		}
		switch mem.State {
		case StateActive:
			if d := now.Sub(mem.LastSeen); d > m.dead {
				mem.State, mem.Since = StateDead, now
				changed = append(changed, *mem)
			} else if d > m.suspect {
				mem.State, mem.Since = StateSuspect, now
				changed = append(changed, *mem)
			}
		case StateSuspect:
			if now.Sub(mem.LastSeen) > m.dead {
				mem.State, mem.Since = StateDead, now
				changed = append(changed, *mem)
			}
		}
	}
	m.mu.Unlock()
	for _, mem := range changed {
		m.notify(mem)
	}
}

// State reports the current state of id (JOINING for unknown ids).
func (m *Membership) State(id NodeID) MemberState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if mem, ok := m.members[id]; ok {
		return mem.State
	}
	return StateJoining
}

// Alive reports whether id is reachable enough to count in a quorum:
// ACTIVE or SUSPECT. DEAD and LEAVING members do not count.
func (m *Membership) Alive(id NodeID) bool {
	s := m.State(id)
	return s == StateActive || s == StateSuspect
}

// Snapshot returns a copy of the member table in configuration order.
func (m *Membership) Snapshot() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Member, 0, len(m.members))
	for _, mem := range m.members {
		out = append(out, *mem)
	}
	// Stable order: by id. The table is tiny (static v1 clusters).
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].ID < out[j-1].ID; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func (m *Membership) notify(mem Member) {
	if m.onChange != nil {
		m.onChange(mem)
	}
}
