package worker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestParsePriorityTopics(t *testing.T) {
	cases := []struct {
		name string
		spec string
		want []string
	}{
		{"empty means default", "", []string{
			"jobs.p1", "jobs.p2", "jobs.p3", "jobs.p4", "jobs.p5",
			"jobs.p6", "jobs.p7", "jobs.p8", "jobs.p9", "jobs",
		}},
		{"default spec", "1..9+legacy", []string{
			"jobs.p1", "jobs.p2", "jobs.p3", "jobs.p4", "jobs.p5",
			"jobs.p6", "jobs.p7", "jobs.p8", "jobs.p9", "jobs",
		}},
		{"legacy only", "legacy", []string{"jobs"}},
		{"single level", "1", []string{"jobs.p1"}},
		{"range with legacy", "1..3+legacy", []string{"jobs.p1", "jobs.p2", "jobs.p3", "jobs"}},
		{"comma list", "1,3,legacy", []string{"jobs.p1", "jobs.p3", "jobs"}},
		{"duplicates collapse", "1..3,2..4", []string{"jobs.p1", "jobs.p2", "jobs.p3", "jobs.p4"}},
		{"unsorted input is normalized", "9,1,legacy", []string{"jobs.p1", "jobs.p9", "jobs"}},
		{"spaces tolerated", " 1..2 , legacy ", []string{"jobs.p1", "jobs.p2", "jobs"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParsePriorityTopics(tc.spec)
			if err != nil {
				t.Fatalf("ParsePriorityTopics(%q): %v", tc.spec, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ParsePriorityTopics(%q) = %v, want %v", tc.spec, got, tc.want)
			}
		})
	}
}

func TestParsePriorityTopicsInvalid(t *testing.T) {
	for _, spec := range []string{
		"0", "10", "p1", "abc", "1..", "..3", "3..1", "0..3", "1..10", ",",
	} {
		if _, err := ParsePriorityTopics(spec); err == nil {
			t.Errorf("ParsePriorityTopics(%q): want error", spec)
		}
	}
}

func TestTopicRank(t *testing.T) {
	cases := map[string]int{
		"jobs.p1":  1,
		"jobs.p5":  5,
		"jobs.p9":  9,
		"jobs":     10, // legacy is the lowest urgency
		"jobs.p10": 10, // outside the pinned family
		"jobs.px":  10,
		"jobs.dlq": 10,
		"other":    10,
	}
	for topic, want := range cases {
		if got := TopicRank(topic); got != want {
			t.Errorf("TopicRank(%q) = %d, want %d", topic, got, want)
		}
	}
}

// acquireN grabs n permits up front so every later Acquire queues.
func acquireN(s *prioSem, n int64) {
	for i := int64(0); i < n; i++ {
		if err := s.Acquire(context.Background(), legacyRank); err != nil {
			panic(err)
		}
	}
}

// waitGranted asserts which queued Acquire fires next.
func waitGranted(t *testing.T, ch <-chan error, name string) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not acquire in time", name)
	}
}

func mustQueue(t *testing.T, s *prioSem, rank int) <-chan error {
	t.Helper()
	ch := make(chan error, 1)
	go func() { ch <- s.Acquire(context.Background(), rank) }()
	return ch
}

// queueAll enqueues one Acquire per rank and blocks until every one of
// them is sitting in the waiter queue, so grant order is deterministic.
func queueAll(t *testing.T, s *prioSem, ranks ...int) []<-chan error {
	t.Helper()
	chs := make([]<-chan error, 0, len(ranks))
	for _, r := range ranks {
		chs = append(chs, mustQueue(t, s, r))
	}
	waitForWaiters(t, s, len(ranks))
	return chs
}

func waitForWaiters(t *testing.T, s *prioSem, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for waiterCount(s) < n {
		if time.Now().After(deadline) {
			t.Fatalf("waiters did not queue in time: %d < %d", waiterCount(s), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func waiterCount(s *prioSem) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters)
}

func TestPrioSemStrictOrder(t *testing.T) {
	s := newPrioSem(1, -1) // strict: aging disabled
	acquireN(s, 1)

	// Queue order is legacy, mid, high on purpose: grant order must follow
	// rank, not arrival.
	chs := queueAll(t, s, legacyRank, 5, 1)
	legacy, mid, high := chs[0], chs[1], chs[2]

	// One release: the p1 waiter wins although it queued last.
	s.Release()
	waitGranted(t, high, "p1 waiter")
	if waiterCount(s) != 2 {
		t.Fatalf("waiters left: %d, want 2", waiterCount(s))
	}

	s.Release()
	waitGranted(t, mid, "p5 waiter")
	s.Release()
	waitGranted(t, legacy, "legacy waiter")
}

func TestPrioSemFIFOWithinRank(t *testing.T) {
	s := newPrioSem(1, -1)
	acquireN(s, 1)

	// Queue one at a time: seq order (the FIFO tie-break) follows the
	// enqueue order, so each waiter must be in the queue before the next
	// goroutine starts.
	first := mustQueue(t, s, 3)
	waitForWaiters(t, s, 1)
	second := mustQueue(t, s, 3)
	waitForWaiters(t, s, 2)

	s.Release()
	waitGranted(t, first, "first p3 waiter")
	s.Release()
	waitGranted(t, second, "second p3 waiter")
}

func TestPrioSemAgingPromotes(t *testing.T) {
	now := time.Now()
	s := newPrioSem(1, time.Second)
	s.now = func() time.Time { return now } // frozen clock, moved by the test
	acquireN(s, 1)

	chs := queueAll(t, s, 1, legacyRank)
	high, legacy := chs[0], chs[1]

	// Freshly queued, p1 still beats legacy...
	s.Release()
	waitGranted(t, high, "fresh p1 waiter")

	// ...but after 10 aging intervals the legacy waiter reaches rank 0 and
	// beats even a brand-new p1: starvation is bounded by design.
	now = now.Add(11 * time.Second)
	fresh := queueAll(t, s, 1)
	s.Release()
	waitGranted(t, legacy, "aged legacy waiter")
	s.Release()
	waitGranted(t, fresh[0], "p1 waiter after legacy promotion")
}

func TestPrioSemCancelQueued(t *testing.T) {
	s := newPrioSem(1, -1)
	acquireN(s, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Acquire(ctx, 9) }()
	waitForWaiters(t, s, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire: got %v, want context.Canceled", err)
	}
	if n := waiterCount(s); n != 0 {
		t.Fatalf("cancelled waiter left behind: %d", n)
	}

	// The permit must still be whole: one release admits the next waiter.
	next := queueAll(t, s, 1)
	s.Release()
	waitGranted(t, next[0], "post-cancel waiter")
}

func TestPrioSemCancelRacingGrant(t *testing.T) {
	// A waiter whose grant races its cancellation must pass the permit on,
	// never leak it.
	for i := 0; i < 50; i++ {
		s := newPrioSem(1, -1)
		acquireN(s, 1)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Acquire(ctx, 1) }()
		for waiterCount(s) < 1 {
			time.Sleep(time.Millisecond)
		}
		cancel()    // may land before or after the grant below
		s.Release() // hands the permit to the (maybe cancelled) waiter
		if err := <-done; err == nil {
			s.Release() // the waiter won the race: return its permit
		}

		// Whatever happened, exactly one permit exists: acquiring it must
		// not block.
		acqCtx, acqCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer acqCancel()
		if err := s.Acquire(acqCtx, 1); err != nil {
			t.Fatalf("iteration %d: permit leaked: %v", i, err)
		}
	}
}

func TestPrioSemConcurrentStress(t *testing.T) {
	// Hammer the semaphore from many goroutines at mixed ranks: every
	// acquired permit is released, none is lost, none is double-granted.
	s := newPrioSem(4, time.Millisecond)
	var wg sync.WaitGroup
	var inFlight sync.Map // token -> bool, detects double grants
	token := 0
	var tokenMu sync.Mutex
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := s.Acquire(context.Background(), rank); err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				tokenMu.Lock()
				token++
				tok := token
				tokenMu.Unlock()
				if _, dup := inFlight.LoadOrStore(tok, true); dup {
					t.Errorf("token %d granted twice", tok)
				}
				inFlight.Delete(tok)
				s.Release()
			}
		}(g%10 + 1)
	}
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.free != 4 {
		t.Errorf("free permits at end: %d, want 4", s.free)
	}
	if len(s.waiters) != 0 {
		t.Errorf("waiters left: %d, want 0", len(s.waiters))
	}
}
