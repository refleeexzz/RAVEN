package group

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestDuplicateCommitIdempotent: committing the same offset twice is a
// no-op, not an error. Retried commits (client got a timeout but the
// broker did persist) must never corrupt state.
func TestDuplicateCommitIdempotent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)
	gen, _, err := c.Join("workers", "m1", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Commit("workers", "m1", "jobs", 0, 42, gen); err != nil {
			t.Fatalf("duplicate commit %d: %v", i, err)
		}
	}
	if got := c.Committed("workers", "jobs", 0); got != 42 {
		t.Fatalf("committed after duplicates: got %d want 42", got)
	}

	// Same member, same generation, newer offset: last write wins.
	if err := c.Commit("workers", "m1", "jobs", 0, 50, gen); err != nil {
		t.Fatal(err)
	}
	if got := c.Committed("workers", "jobs", 0); got != 50 {
		t.Fatalf("committed after advance: got %d want 50", got)
	}

	// A stale-generation replay of an old offset must NOT rewind state.
	if err := c.Commit("workers", "m1", "jobs", 0, 42, gen+99); !errors.Is(err, ErrRebalance) {
		t.Fatalf("future-gen commit: %v", err)
	}
	if got := c.Committed("workers", "jobs", 0); got != 50 {
		t.Fatalf("committed after rejected replay: got %d want 50", got)
	}
}

// TestStaleGenerationFencing: after a rebalance, every operation
// carrying the old generation is rejected — commits, fetches and
// heartbeats alike. This is what stops a zombie from an old generation
// from consuming or committing over the new generation's work.
func TestStaleGenerationFencing(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)

	gen1, _, err := c.Join("workers", "m1", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	gen2, _, err := c.Join("workers", "m2", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if gen2 != gen1+1 {
		t.Fatalf("gen1=%d gen2=%d", gen1, gen2)
	}

	// m1 is still a member, but gen1 is history: everything stale fails.
	staleOps := []struct {
		name string
		op   func() error
	}{
		{"commit", func() error { return c.Commit("workers", "m1", "jobs", 0, 10, gen1) }},
		{"fetch", func() error { return c.CheckFetch("workers", "m1", gen1) }},
		{"heartbeat", func() error { return c.Heartbeat("workers", "m1", gen1) }},
	}
	for _, so := range staleOps {
		if err := so.op(); !errors.Is(err, ErrRebalance) {
			t.Fatalf("stale %s: got %v want ErrRebalance", so.name, err)
		}
	}

	// Nothing was committed by the stale attempts.
	if got := c.Committed("workers", "jobs", 0); got != 0 {
		t.Fatalf("stale commit landed: got %d want 0", got)
	}

	// m1 re-joins (unchanged subscription: no bump) and can work again
	// with the current generation.
	gen3, _, err := c.Join("workers", "m1", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if gen3 != gen2 {
		t.Fatalf("re-join bumped generation: %d → %d", gen2, gen3)
	}
	if err := c.CheckFetch("workers", "m1", gen3); err != nil {
		t.Fatalf("fetch with current gen: %v", err)
	}
	if err := c.Commit("workers", "m1", "jobs", 0, 10, gen3); err != nil {
		t.Fatalf("commit with current gen: %v", err)
	}

	// Unknown group/member fetches are fenced too.
	if err := c.CheckFetch("nope", "m1", gen3); !errors.Is(err, ErrRebalance) {
		t.Fatalf("unknown group fetch: %v", err)
	}
	if err := c.CheckFetch("workers", "ghost", gen3); !errors.Is(err, ErrRebalance) {
		t.Fatalf("unknown member fetch: %v", err)
	}
}

// TestAssignmentExclusivityAcrossRebalances hammers the coordinator
// with a deterministic pseudo-random sequence of joins, leaves and
// subscription changes, and after EVERY operation asserts the safety
// property: no partition is ever owned by two members at once, and no
// assigned partition is out of range.
func TestAssignmentExclusivityAcrossRebalances(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)

	members := []string{"m1", "m2", "m3", "m4", "m5"}
	topicSets := [][]string{{"jobs"}, {"jobs.retry"}, {"jobs", "jobs.retry"}}
	joined := map[string]bool{}

	// xorshift PRNG: deterministic, no math/rand seeding worries.
	seed := uint32(0x9e3779b9)
	rnd := func() uint32 {
		seed ^= seed << 13
		seed ^= seed >> 17
		seed ^= seed << 5
		return seed
	}

	checkExclusivity := func(step int) {
		t.Helper()
		c.mu.Lock()
		defer c.mu.Unlock()
		for gid, g := range c.groups {
			type tp struct {
				topic string
				part  int32
			}
			owner := map[tp]string{}
			for memberID, as := range g.assignments {
				if _, live := g.members[memberID]; !live {
					t.Errorf("step %d: group %s: assignment for non-member %s", step, gid, memberID)
				}
				for _, a := range as {
					n, ok := partitions3(a.Topic)
					if !ok {
						t.Errorf("step %d: assignment for unknown topic %s", step, a.Topic)
						continue
					}
					for _, p := range a.Partitions {
						if p < 0 || p >= int32(n) {
							t.Errorf("step %d: %s/%d out of range (topic has %d)", step, a.Topic, p, n)
						}
						key := tp{a.Topic, p}
						if other, dup := owner[key]; dup {
							t.Errorf("step %d: group %s gen %d: %s/%d owned by BOTH %s and %s",
								step, gid, g.generation, a.Topic, p, other, memberID)
						}
						owner[key] = memberID
					}
				}
			}
		}
	}

	for step := 0; step < 300; step++ {
		m := members[rnd()%uint32(len(members))]
		switch rnd() % 3 {
		case 0, 1: // join (new or re-join)
			topics := topicSets[rnd()%uint32(len(topicSets))]
			if _, _, err := c.Join("g", m, topics); err != nil {
				t.Fatalf("step %d: join %s: %v", step, m, err)
			}
			joined[m] = true
		case 2: // leave
			if joined[m] {
				if err := c.Leave("g", m); err != nil {
					t.Fatalf("step %d: leave %s: %v", step, m, err)
				}
				delete(joined, m)
			}
		}
		checkExclusivity(step)
	}
}

// TestZombieMemberFencedOut: a member that misses its session timeout
// is expelled and its partitions are reassigned. When it wakes up and
// tries to fetch or commit with its old generation, everything is
// rejected — it cannot keep processing "its" old partition. Re-joining
// is the only way back.
func TestZombieMemberFencedOut(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 120*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.Start(ctx)

	if _, _, err := c.Join("workers", "m1", []string{"jobs"}); err != nil {
		t.Fatal(err)
	}
	gen2, as2, err := c.Join("workers", "m2", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	// m2 owns partition 2 (range assignor: m1 gets 0,1).
	var m2Parts []int32
	for _, a := range as2 {
		if a.Topic == "jobs" {
			m2Parts = a.Partitions
		}
	}
	if len(m2Parts) != 1 || m2Parts[0] != 2 {
		t.Fatalf("m2 assignment: %+v", as2)
	}

	// m2 goes silent; m1 stays alive. The reaper must expel m2.
	deadline := time.Now().Add(3 * time.Second)
	var gen3 int32
	for time.Now().Before(deadline) {
		if err := c.Heartbeat("workers", "m1", generationOf(c, "workers")); err != nil {
			t.Fatalf("m1 heartbeat: %v", err)
		}
		c.mu.Lock()
		_, m2present := c.groups["workers"].members["m2"]
		gen3 = c.groups["workers"].generation
		m1Parts := c.groups["workers"].assignments["m1"]
		c.mu.Unlock()
		if !m2present {
			total := 0
			for _, a := range m1Parts {
				total += len(a.Partitions)
			}
			if total != 3 {
				t.Fatalf("m1 owns %d partitions after m2 expiry, want all 3", total)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.mu.Lock()
	_, m2still := c.groups["workers"].members["m2"]
	c.mu.Unlock()
	if m2still {
		t.Fatal("m2 never expelled")
	}
	if gen3 <= gen2 {
		t.Fatalf("generation did not bump on expulsion: %d → %d", gen2, gen3)
	}

	// The zombie wakes. Its old generation is refused everywhere: it
	// cannot fetch (no new records to process) and cannot commit.
	if err := c.CheckFetch("workers", "m2", gen2); !errors.Is(err, ErrRebalance) {
		t.Fatalf("zombie fetch: %v", err)
	}
	err = c.Commit("workers", "m2", "jobs", 2, 77, gen2)
	if !errors.Is(err, ErrRebalance) && !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("zombie commit: %v", err)
	}
	if got := c.Committed("workers", "jobs", 2); got != 0 {
		t.Fatalf("zombie commit landed: got %d want 0", got)
	}

	// Re-joining is the way back: m2 re-joins, generation bumps once,
	// partition 2 is its again, and commits work.
	gen4, as4, err := c.Join("workers", "m2", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if gen4 != gen3+1 {
		t.Fatalf("re-join after expulsion should bump exactly once: %d → %d", gen3, gen4)
	}
	found := false
	for _, a := range as4 {
		if a.Topic == "jobs" {
			for _, p := range a.Partitions {
				if p == 2 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("m2 did not get partition 2 back: %+v", as4)
	}
	if err := c.Commit("workers", "m2", "jobs", 2, 77, gen4); err != nil {
		t.Fatalf("commit after re-join: %v", err)
	}
	if got := c.Committed("workers", "jobs", 2); got != 77 {
		t.Fatalf("committed after re-join: got %d want 77", got)
	}
}

// TestReconnectBeforeTimeoutIsSeamless: a member that reconnects and
// re-joins BEFORE its session expires keeps its assignment and the
// generation does not move — nobody else is disturbed by the flap.
func TestReconnectBeforeTimeoutIsSeamless(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)

	if _, _, err := c.Join("workers", "m1", []string{"jobs"}); err != nil {
		t.Fatal(err)
	}
	gen2, _, err := c.Join("workers", "m2", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}

	// m1 "reconnects" (re-joins with the same subscription).
	gen3, as3, err := c.Join("workers", "m1", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if gen3 != gen2 {
		t.Fatalf("reconnect bumped generation: %d → %d", gen2, gen3)
	}
	if len(as3) != 1 || len(as3[0].Partitions) != 2 {
		t.Fatalf("m1 lost its assignment on reconnect: %+v", as3)
	}
	// m2's view is untouched: its heartbeat with gen2 still works.
	if err := c.Heartbeat("workers", "m2", gen2); err != nil {
		t.Fatalf("m2 heartbeat disturbed by m1 reconnect: %v", err)
	}
}
