package cluster

import (
	"testing"
	"time"
)

func testMembershipConfig() Config {
	return Config{
		NodeID: "n1",
		Nodes: []NodeInfo{
			{ID: "n1", Addr: "a:1"},
			{ID: "n2", Addr: "a:2"},
			{ID: "n3", Addr: "a:3"},
		},
		SuspectAfter: 3 * time.Second,
		DeadAfter:    10 * time.Second,
	}.withDefaults()
}

func TestMembershipStartsJoining(t *testing.T) {
	t.Parallel()
	m := NewMembership(testMembershipConfig(), nil)
	if got := m.State("n1"); got != StateActive {
		t.Fatalf("self: got %s want ACTIVE", got)
	}
	for _, id := range []NodeID{"n2", "n3"} {
		if got := m.State(id); got != StateJoining {
			t.Fatalf("%s: got %s want JOINING", id, got)
		}
	}
}

func TestMembershipMarkSeenActivates(t *testing.T) {
	t.Parallel()
	var flips []Member
	m := NewMembership(testMembershipConfig(), func(mem Member) { flips = append(flips, mem) })
	m.MarkSeen("n2")
	if got := m.State("n2"); got != StateActive {
		t.Fatalf("n2: got %s want ACTIVE", got)
	}
	if len(flips) != 1 || flips[0].ID != "n2" || flips[0].State != StateActive {
		t.Fatalf("flips: got %+v", flips)
	}
	// Unknown ids are ignored, never panics.
	m.MarkSeen("nobody")
}

func TestMembershipTimeoutTransitions(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &now
	m := NewMembership(testMembershipConfig(), nil)
	m.now = func() time.Time { return *clock }
	m.MarkSeen("n2")

	// Just before SUSPECT: still ACTIVE.
	*clock = now.Add(2 * time.Second)
	m.Tick()
	if got := m.State("n2"); got != StateActive {
		t.Fatalf("t+2s: got %s want ACTIVE", got)
	}
	// Past SUSPECT.
	*clock = now.Add(4 * time.Second)
	m.Tick()
	if got := m.State("n2"); got != StateSuspect {
		t.Fatalf("t+4s: got %s want SUSPECT", got)
	}
	// Past DEAD.
	*clock = now.Add(11 * time.Second)
	m.Tick()
	if got := m.State("n2"); got != StateDead {
		t.Fatalf("t+11s: got %s want DEAD", got)
	}
	// Recovery: any contact reactivates.
	m.MarkSeen("n2")
	if got := m.State("n2"); got != StateActive {
		t.Fatalf("after contact: got %s want ACTIVE", got)
	}
}

func TestMembershipLeavingIsSticky(t *testing.T) {
	t.Parallel()
	now := time.Now()
	clock := &now
	m := NewMembership(testMembershipConfig(), nil)
	m.now = func() time.Time { return *clock }
	m.MarkSeen("n2")
	m.MarkLeaving("n2")
	if got := m.State("n2"); got != StateLeaving {
		t.Fatalf("got %s want LEAVING", got)
	}
	// The failure detector never flips a LEAVING member to DEAD.
	*clock = now.Add(time.Hour)
	m.Tick()
	if got := m.State("n2"); got != StateLeaving {
		t.Fatalf("after 1h: got %s want LEAVING", got)
	}
	// A rejoining node comes back ACTIVE on contact.
	m.MarkSeen("n2")
	if got := m.State("n2"); got != StateActive {
		t.Fatalf("rejoin: got %s want ACTIVE", got)
	}
}

func TestMembershipAlive(t *testing.T) {
	t.Parallel()
	m := NewMembership(testMembershipConfig(), nil)
	if m.Alive("n2") {
		t.Fatal("JOINING must not count as alive")
	}
	m.MarkSeen("n2")
	if !m.Alive("n2") {
		t.Fatal("ACTIVE must count as alive")
	}
	m.MarkLeaving("n2")
	if m.Alive("n2") {
		t.Fatal("LEAVING must not count as alive")
	}
}

func TestMembershipSnapshotStableOrder(t *testing.T) {
	t.Parallel()
	m := NewMembership(testMembershipConfig(), nil)
	snap := m.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("len: got %d want 3", len(snap))
	}
	for i, want := range []NodeID{"n1", "n2", "n3"} {
		if snap[i].ID != want {
			t.Fatalf("snap[%d]: got %s want %s", i, snap[i].ID, want)
		}
	}
	// Mutating the snapshot must not corrupt the table.
	snap[0].State = StateDead
	if m.State("n1") != StateActive {
		t.Fatal("snapshot mutation leaked into the table")
	}
}

func TestMemberStateString(t *testing.T) {
	t.Parallel()
	want := map[MemberState]string{
		StateJoining: "JOINING", StateActive: "ACTIVE", StateSuspect: "SUSPECT",
		StateDead: "DEAD", StateLeaving: "LEAVING", MemberState(99): "UNKNOWN",
	}
	for st, s := range want {
		if st.String() != s {
			t.Errorf("state %d: got %q want %q", int(st), st.String(), s)
		}
	}
}
