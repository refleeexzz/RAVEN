package group

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

func partitions3(topic string) (int, bool) {
	switch topic {
	case "jobs":
		return 3, true
	case "jobs.retry":
		return 2, true
	}
	return 0, false
}

func TestRangeAssignor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		members    []memberInfo
		partitions int
		want       map[string][]int32 // member → partitions for the topic
	}{
		{
			name:    "one member gets everything",
			members: []memberInfo{{id: "m1", topics: []string{"jobs"}}},
			want:    map[string][]int32{"m1": {0, 1, 2}},
		},
		{
			name: "uneven split favors first member",
			members: []memberInfo{
				{id: "m1", topics: []string{"jobs"}},
				{id: "m2", topics: []string{"jobs"}},
			},
			want: map[string][]int32{"m1": {0, 1}, "m2": {2}},
		},
		{
			name: "more members than partitions",
			members: []memberInfo{
				{id: "m1", topics: []string{"jobs"}},
				{id: "m2", topics: []string{"jobs"}},
				{id: "m3", topics: []string{"jobs"}},
				{id: "m4", topics: []string{"jobs"}},
			},
			want: map[string][]int32{"m1": {0}, "m2": {1}, "m3": {2}, "m4": {}},
		},
		{
			name: "partial subscription",
			members: []memberInfo{
				{id: "m1", topics: []string{"jobs"}},
				{id: "m2", topics: []string{"jobs.retry"}},
			},
			want: map[string][]int32{"m1": {0, 1, 2}, "m2": {}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := rangeAssign(tc.members, partitions3)
			if len(got) != len(tc.members) {
				t.Fatalf("assignments for %d members, want %d", len(got), len(tc.members))
			}
			seen := map[int32]string{}
			for id, wantParts := range tc.want {
				var gotParts []int32
				for _, a := range got[id] {
					if a.Topic == "jobs" {
						gotParts = a.Partitions
					}
				}
				slices.Sort(gotParts)
				wp := append([]int32(nil), wantParts...)
				slices.Sort(wp)
				if len(wp) == 0 && len(gotParts) == 0 {
					continue
				}
				if !reflect.DeepEqual(gotParts, wp) {
					t.Errorf("member %s: got %v want %v", id, gotParts, wp)
				}
			}
			// No partition assigned twice.
			for id, as := range got {
				for _, a := range as {
					if a.Topic != "jobs" {
						continue
					}
					for _, p := range a.Partitions {
						if owner, dup := seen[p]; dup {
							t.Errorf("partition %d assigned to both %s and %s", p, owner, id)
						}
						seen[p] = id
					}
				}
			}
		})
	}
}

func newTestCoordinator(t *testing.T, timeout time.Duration) *Coordinator {
	t.Helper()
	c, err := NewCoordinator(t.TempDir(), partitions3, timeout, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestJoinAssignsAndBumpsGeneration(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)

	gen1, as1, err := c.Join("workers", "m1", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if gen1 != 1 || len(as1) != 1 || len(as1[0].Partitions) != 3 {
		t.Fatalf("first join: gen=%d assignments=%+v", gen1, as1)
	}

	gen2, as2, err := c.Join("workers", "m2", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if gen2 != 2 {
		t.Fatalf("second join should bump generation: gen=%d", gen2)
	}
	if len(as2) != 1 || len(as2[0].Partitions) != 1 {
		t.Fatalf("m2 should own 1 partition (range: m1 gets 2), got %+v", as2)
	}

	// m1's old generation is now stale.
	if err := c.Heartbeat("workers", "m1", gen1); !errors.Is(err, ErrRebalance) {
		t.Fatalf("stale heartbeat: %v", err)
	}
	if err := c.Heartbeat("workers", "m1", gen2); err != nil {
		t.Fatalf("current heartbeat: %v", err)
	}
}

// generationOf reads the current generation (test helper, same package).
func generationOf(c *Coordinator, groupID string) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return -1
	}
	return g.generation
}

// Regression test for the rebalance storm: a stale member re-joining
// with an unchanged subscription must NOT bump the generation. If it
// does, every other member's heartbeat goes stale, they re-join too,
// each re-join bumps again — the storm.
func TestRejoinUnchangedMemberKeepsGeneration(t *testing.T) {
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
		t.Fatalf("new member should bump: gen1=%d gen2=%d", gen1, gen2)
	}

	// m1's heartbeat with the old generation fails...
	if err := c.Heartbeat("workers", "m1", gen1); !errors.Is(err, ErrRebalance) {
		t.Fatalf("stale heartbeat: %v", err)
	}
	// ...but the failure itself must not move the generation.
	if got := generationOf(c, "workers"); got != gen2 {
		t.Fatalf("stale heartbeat bumped generation: %d → %d", gen2, got)
	}

	// m1 re-joins with the same subscription: current generation back,
	// no bump, and it gets its current assignment.
	gen3, as, err := c.Join("workers", "m1", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if gen3 != gen2 {
		t.Fatalf("re-join with unchanged membership bumped generation: %d → %d", gen2, gen3)
	}
	if len(as) != 1 || len(as[0].Partitions) != 2 {
		t.Fatalf("re-join should return current assignment (m1 owns 2 of 3): %+v", as)
	}

	// Topic order in the subscription is not a change.
	if gen, _, err := c.Join("workers", "m1", []string{"jobs"}); err != nil || gen != gen2 {
		t.Fatalf("idempotent re-join: gen=%d err=%v", gen, err)
	}

	// A changed subscription IS a membership change: bump once.
	gen4, _, err := c.Join("workers", "m1", []string{"jobs", "jobs.retry"})
	if err != nil {
		t.Fatal(err)
	}
	if gen4 != gen2+1 {
		t.Fatalf("subscription change should bump exactly once: %d → %d", gen2, gen4)
	}
}

func TestRebalanceOnMemberTimeout(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 60*time.Millisecond)
	c.Start(context.Background())

	if _, _, err := c.Join("workers", "m1", []string{"jobs"}); err != nil {
		t.Fatal(err)
	}
	gen2, _, err := c.Join("workers", "m2", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}

	// m1 keeps heartbeating; m2 goes silent and must be expelled.
	deadline := time.Now().Add(3 * time.Second)
	var gen3 int32
	for time.Now().Before(deadline) {
		if err := c.Heartbeat("workers", "m1", gen2); err != nil && !errors.Is(err, ErrRebalance) {
			t.Fatalf("m1 heartbeat: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
		c.mu.Lock()
		g := c.groups["workers"]
		members := len(g.members)
		gen3 = g.generation
		c.mu.Unlock()
		if members == 1 {
			break
		}
	}
	c.mu.Lock()
	g := c.groups["workers"]
	_, m2gone := g.members["m2"]
	finalMembers := len(g.members)
	c.mu.Unlock()
	if m2gone {
		t.Fatal("m2 still present after timeout")
	}
	if finalMembers != 1 {
		t.Fatalf("members after timeout: %d want 1", finalMembers)
	}
	if gen3 <= gen2 {
		t.Fatalf("generation did not bump on timeout: %d <= %d", gen3, gen2)
	}
	// After the rebalance, m1 owns all partitions again.
	c.mu.Lock()
	all := c.groups["workers"].assignments["m1"]
	c.mu.Unlock()
	total := 0
	for _, a := range all {
		total += len(a.Partitions)
	}
	if total != 3 {
		t.Fatalf("m1 owns %d partitions after rebalance, want 3", total)
	}
}

func TestLeaveRebalances(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)
	c.Join("g", "m1", []string{"jobs"})
	gen2, _, _ := c.Join("g", "m2", []string{"jobs"})
	if err := c.Leave("g", "m2"); err != nil {
		t.Fatal(err)
	}
	// m2's commits after leaving are rejected.
	err := c.Commit("g", "m2", "jobs", 0, 5, gen2)
	if !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("commit after leave: %v", err)
	}
	// Leaving twice is fine.
	if err := c.Leave("g", "m2"); err != nil {
		t.Fatal(err)
	}
}

func TestCommitAndRestoreOffsets(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, err := NewCoordinator(dir, partitions3, 30*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	gen, _, err := c.Join("workers", "m1", []string{"jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Commit("workers", "m1", "jobs", 0, 42, gen); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit("workers", "m1", "jobs", 1, 7, gen); err != nil {
		t.Fatal(err)
	}
	// Stale generation commit rejected.
	if err := c.Commit("workers", "m1", "jobs", 2, 1, gen-1); !errors.Is(err, ErrRebalance) {
		t.Fatalf("stale commit: %v", err)
	}
	if got := c.Committed("workers", "jobs", 0); got != 42 {
		t.Fatalf("committed: got %d want 42", got)
	}
	if got := c.Committed("workers", "jobs", 2); got != 0 {
		t.Fatalf("never committed: got %d want 0", got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: offsets survive without any members.
	c2, err := NewCoordinator(dir, partitions3, 30*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if got := c2.Committed("workers", "jobs", 0); got != 42 {
		t.Fatalf("restored offset: got %d want 42", got)
	}
	if got := c2.Committed("workers", "jobs", 1); got != 7 {
		t.Fatalf("restored offset p1: got %d want 7", got)
	}
}

func TestJoinUnknownTopic(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)
	_, _, err := c.Join("workers", "m1", []string{"nope"})
	if !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("got %v want ErrUnknownTopic", err)
	}
}

func TestOffsetFileSurvivesCorruptTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, err := NewCoordinator(dir, partitions3, 30*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	gen, _, _ := c.Join("g", "m1", []string{"jobs"})
	c.Commit("g", "m1", "jobs", 0, 100, gen)
	c.Close()

	// Simulate a crash mid-append: half a JSON line at the end.
	f, err := os.OpenFile(filepath.Join(dir, "offsets.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"group":"g","topic":"jobs","partition":0,"offse`)
	f.Close()

	c2, err := NewCoordinator(dir, partitions3, 30*time.Second, nil)
	if err != nil {
		t.Fatalf("reopen with corrupt offsets tail: %v", err)
	}
	defer c2.Close()
	if got := c2.Committed("g", "jobs", 0); got != 100 {
		t.Fatalf("offset after corrupt tail: got %d want 100", got)
	}
}

func TestActiveGroupsMetric(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator(t, 30*time.Second)
	if c.ActiveGroups() != 0 {
		t.Fatal("no groups expected")
	}
	c.Join("g1", "m1", []string{"jobs"})
	c.Join("g2", "m1", []string{"jobs"})
	if c.ActiveGroups() != 2 {
		t.Fatalf("active: got %d want 2", c.ActiveGroups())
	}
	c.Leave("g1", "m1")
	if c.ActiveGroups() != 1 {
		t.Fatalf("active after leave: got %d want 1", c.ActiveGroups())
	}
}
