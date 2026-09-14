package worker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refleeexzz/RAVEN/services/jobs"
)

// Priority-aware consumption.
//
// Pinned contract with the jobs service: work is published to the topic
// family jobs.p1..jobs.p9 (p1 = most urgent) and, during the migration
// window, dual-published to the legacy "jobs" topic. The worker subscribes
// to the whole family plus legacy and executes higher-priority messages
// first.
//
// Why consumer-per-topic: the broker client fetch loop walks its assigned
// partitions in map order with no notion of priority, so a single
// multi-topic consumer cannot drain jobs.p1 before jobs.p2. Instead the
// worker runs one consumer per topic — all in the same "workers" group,
// all sharing the same lease/fencing/execution pipeline — and enforces the
// priority order where it actually matters: admission to an execution slot.
// The pinned "strict drain per fetch cycle" maps to this design as follows:
// while higher-priority work saturates the slots, lower-priority consumers
// block in Handle (backpressure: their offsets simply do not commit); when a
// slot frees, the highest-priority waiter gets it. Starvation budget: a
// waiter is promoted one rank every priorityAging interval, so legacy work
// is guaranteed a slot in bounded time even under a full p1 flood.

// DefaultPriorityTopicsSpec is the WORKER_PRIORITY_TOPICS default: the whole
// priority family plus the legacy topic.
//
// Spec format: items separated by ',' or '+'. An item is "legacy" (the
// legacy "jobs" topic), a single level "3" (jobs.p3) or a range "1..3"
// (jobs.p1..jobs.p3). Levels run 1..9; p1 is the most urgent. Output order
// is normalized: levels ascending, legacy last, duplicates removed.
const DefaultPriorityTopicsSpec = "1..9+legacy"

// maxPriorityLevel is the highest valid priority topic level (jobs.p9).
const maxPriorityLevel = 9

// legacyRank is the dispatch rank of the legacy topic (and of any topic
// outside the pinned family): the lowest urgency. Ranks run 1 (jobs.p1,
// most urgent) to legacyRank.
const legacyRank = maxPriorityLevel + 1

// defaultPriorityAging is how long a waiter must queue before its effective
// rank improves by one. A legacy waiter (rank 10) reaches the top rank
// after 10 intervals — 20s with the default — so the legacy topic can
// never starve. Params.PriorityAging overrides (tests).
const defaultPriorityAging = 2 * time.Second

// priorityTopic returns the topic name for a level: jobs.p<level>.
func priorityTopic(level int) string {
	return jobs.TopicJobs + ".p" + strconv.Itoa(level)
}

// ParsePriorityTopics turns a WORKER_PRIORITY_TOPICS spec into the ordered
// topic list to consume. Empty spec means the default. An unknown item or
// an out-of-range level is a boot-time config error, not a silent degrade.
func ParsePriorityTopics(spec string) ([]string, error) {
	if strings.TrimSpace(spec) == "" {
		spec = DefaultPriorityTopicsSpec
	}
	levels := map[int]bool{}
	legacy := false
	for _, item := range strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == '+' }) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if item == "legacy" {
			legacy = true
			continue
		}
		lo, hi, err := parseLevelItem(item)
		if err != nil {
			return nil, err
		}
		for l := lo; l <= hi; l++ {
			levels[l] = true
		}
	}
	if len(levels) == 0 && !legacy {
		return nil, fmt.Errorf("worker: WORKER_PRIORITY_TOPICS %q selects no topics", spec)
	}

	ordered := make([]int, 0, len(levels))
	for l := range levels {
		ordered = append(ordered, l)
	}
	sort.Ints(ordered)
	topics := make([]string, 0, len(ordered)+1)
	for _, l := range ordered {
		topics = append(topics, priorityTopic(l))
	}
	if legacy {
		topics = append(topics, jobs.TopicJobs)
	}
	return topics, nil
}

// parseLevelItem parses "3" or "1..3" into an inclusive level range.
func parseLevelItem(item string) (lo, hi int, err error) {
	bad := func() (int, int, error) {
		return 0, 0, fmt.Errorf("worker: WORKER_PRIORITY_TOPICS: invalid item %q "+
			`(want "legacy", a level 1-9 or a range like 1..3)`, item)
	}
	if before, after, found := strings.Cut(item, ".."); found {
		lo, err1 := strconv.Atoi(before)
		hi, err2 := strconv.Atoi(after)
		if err1 != nil || err2 != nil || lo < 1 || hi > maxPriorityLevel || lo > hi {
			return bad()
		}
		return lo, hi, nil
	}
	n, convErr := strconv.Atoi(item)
	if convErr != nil || n < 1 || n > maxPriorityLevel {
		return bad()
	}
	return n, n, nil
}

// TopicRank returns the dispatch rank of a consumed topic: jobs.p1 = 1
// (most urgent) ... jobs.p9 = 9, legacy "jobs" and anything else = 10.
func TopicRank(topic string) int {
	if rest, ok := strings.CutPrefix(topic, jobs.TopicJobs+".p"); ok {
		if n, err := strconv.Atoi(rest); err == nil && n >= 1 && n <= maxPriorityLevel {
			return n
		}
	}
	return legacyRank
}

// ---------------------------------------------------------------------------
// prioSem: a counting semaphore whose waiters are granted permits in
// (effective rank, arrival) order instead of FIFO.
// ---------------------------------------------------------------------------

// semWaiter is one blocked Acquire call.
type semWaiter struct {
	rank     int
	seq      uint64
	enqueued time.Time
	ch       chan struct{}
	granted  bool // a permit was handed off but not yet picked up
}

// prioSem admits at most cap holders. Waiters queue by effective rank:
// rank minus one per aging interval spent waiting (floor 0), FIFO inside
// the same effective rank. Aging is the anti-starvation budget.
type prioSem struct {
	mu      sync.Mutex
	free    int64
	waiters []*semWaiter
	seq     uint64
	aging   time.Duration
	now     func() time.Time // test hook
}

// newPrioSem builds a semaphore with cap permits. aging <= 0 disables
// promotion (strict priority).
func newPrioSem(cap int64, aging time.Duration) *prioSem {
	return &prioSem{free: cap, aging: aging, now: time.Now}
}

// Acquire takes one permit, blocking in rank order. A cancelled context
// returns the context error and takes nothing — the caller treats that as
// "not dispatched, do not commit".
func (s *prioSem) Acquire(ctx context.Context, rank int) error {
	s.mu.Lock()
	if s.free > 0 && len(s.waiters) == 0 {
		s.free--
		s.mu.Unlock()
		return nil
	}
	w := &semWaiter{rank: rank, seq: s.seq, enqueued: s.now(), ch: make(chan struct{}, 1)}
	s.seq++
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		if w.granted {
			// The permit was handed to us as ctx fired. The caller will not
			// Release (Acquire reports failure), so pass the permit on.
			s.grantNextLocked()
			return ctx.Err()
		}
		for i, x := range s.waiters {
			if x == w {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				break
			}
		}
		return ctx.Err()
	}
}

// Release returns one permit to the highest-rank waiter, or to the pool.
func (s *prioSem) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grantNextLocked()
}

// grantNextLocked hands one permit to the best waiter. Callers hold the
// lock and own a permit to give (a release, or a cancelled granted waiter).
func (s *prioSem) grantNextLocked() {
	if len(s.waiters) == 0 {
		s.free++
		return
	}
	best := 0
	bestEff := s.effRankLocked(s.waiters[0])
	for i := 1; i < len(s.waiters); i++ {
		eff := s.effRankLocked(s.waiters[i])
		if eff < bestEff || (eff == bestEff && s.waiters[i].seq < s.waiters[best].seq) {
			best, bestEff = i, eff
		}
	}
	w := s.waiters[best]
	s.waiters = append(s.waiters[:best], s.waiters[best+1:]...)
	w.granted = true
	w.ch <- struct{}{}
}

// effRankLocked is the waiter's rank after aging promotion.
func (s *prioSem) effRankLocked(w *semWaiter) int {
	if s.aging <= 0 {
		return w.rank
	}
	if r := w.rank - int(s.now().Sub(w.enqueued)/s.aging); r > 0 {
		return r
	}
	return 0
}
