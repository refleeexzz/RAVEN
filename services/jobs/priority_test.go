package jobs

import (
	"testing"
)

// The priority-topic contract is pinned with the worker agent: execution
// publishes go to jobs.p1..jobs.p9 (p1 = most urgent). These tables pin the
// jobs-service side of that contract down.
func TestTopicForPriority(t *testing.T) {
	cases := []struct {
		priority int
		want     string
	}{
		{1, "jobs.p1"},
		{2, "jobs.p2"},
		{5, "jobs.p5"},
		{9, "jobs.p9"},
		// Out-of-range input is clamped into the pinned family, never
		// allowed to mint an unexpected topic name.
		{0, "jobs.p1"},
		{-3, "jobs.p1"},
		{10, "jobs.p9"},
		{42, "jobs.p9"},
	}
	for _, c := range cases {
		if got := TopicForPriority(c.priority); got != c.want {
			t.Errorf("TopicForPriority(%d) = %q, want %q", c.priority, got, c.want)
		}
	}
}

func TestExecutionTopics(t *testing.T) {
	// Compatibility fanout on (the default while workers migrate): the
	// priority topic first, then the legacy "jobs" topic.
	got := executionTopics(3, true)
	if len(got) != 2 || got[0] != "jobs.p3" || got[1] != TopicJobs {
		t.Errorf("executionTopics(3, fanout) = %v, want [jobs.p3 jobs]", got)
	}

	// Fanout off (the target state): only the priority topic.
	got = executionTopics(3, false)
	if len(got) != 1 || got[0] != "jobs.p3" {
		t.Errorf("executionTopics(3, no fanout) = %v, want [jobs.p3]", got)
	}

	// Every valid priority lands inside the pinned family.
	for prio := 1; prio <= MaxPriority; prio++ {
		topics := executionTopics(prio, false)
		if len(topics) != 1 || topics[0] != TopicForPriority(prio) {
			t.Errorf("executionTopics(%d, false) = %v", prio, topics)
		}
	}
}

// EnsureTopics must create the whole pinned family plus the legacy topics.
// The topic list is derived, so this guards the derivation itself.
func TestPriorityTopicFamilySize(t *testing.T) {
	if MaxPriority != 9 {
		t.Errorf("MaxPriority = %d, want 9 (jobs.p1..jobs.p9 contract)", MaxPriority)
	}
	seen := map[string]bool{}
	for prio := 1; prio <= MaxPriority; prio++ {
		topic := TopicForPriority(prio)
		if seen[topic] {
			t.Errorf("duplicate topic %q", topic)
		}
		seen[topic] = true
	}
	if len(seen) != 9 {
		t.Errorf("priority family has %d topics, want 9", len(seen))
	}
}

// NewProducer defaults the legacy fanout ON: existing workers that only
// subscribe to "jobs" must keep receiving work until they migrate.
func TestProducerFanoutDefault(t *testing.T) {
	p := NewProducer("127.0.0.1:1", nil)
	if !p.LegacyFanout() {
		t.Error("LegacyFanout default = false, want true (compatibility default)")
	}
	p.SetLegacyFanout(false)
	if p.LegacyFanout() {
		t.Error("LegacyFanout after SetLegacyFanout(false) = true")
	}
}
