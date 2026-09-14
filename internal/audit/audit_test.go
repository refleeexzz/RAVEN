package audit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeSender stands in for *pgxpool.Pool: it records every batch and can
// simulate a hung or a failing database.
type fakeSender struct {
	mu      sync.Mutex
	rows    int
	batches int
	err     error         // when set, every batch fails
	block   chan struct{} // when non-nil, SendBatch parks until it closes
}

func (f *fakeSender) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches++
	if f.err == nil {
		f.rows += b.Len()
	}
	return fakeResults{err: f.err}
}

func (f *fakeSender) stats() (rows, batches int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rows, f.batches
}

type fakeResults struct{ err error }

func (r fakeResults) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, r.err }
func (r fakeResults) Query() (pgx.Rows, error)         { return nil, r.err }
func (r fakeResults) QueryRow() pgx.Row                { return nil }
func (r fakeResults) Close() error                     { return r.err }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() Metrics {
	return Metrics{
		Dropped:      prometheus.NewCounter(prometheus.CounterOpts{Name: "test_dropped"}),
		Written:      prometheus.NewCounter(prometheus.CounterOpts{Name: "test_written"}),
		InsertErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_insert_errors"}),
	}
}

func fastOptions() Options {
	return Options{
		BufferSize:    8,
		BatchSize:     4,
		FlushInterval: 10 * time.Millisecond,
		InsertTimeout: 200 * time.Millisecond,
		DrainTimeout:  200 * time.Millisecond,
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestEmitNeverBlocksWithHungDatabase(t *testing.T) {
	t.Parallel()

	// No Run: the consumer is stuck behind the hung database. The channel
	// fills at exactly BufferSize; every event past that is dropped, and
	// Emit still returns immediately every single time.
	sender := &fakeSender{block: make(chan struct{})} // DB hangs forever
	met := testMetrics()
	w := NewWriter(sender, testLogger(), met, fastOptions()) // buffer 8

	start := time.Now()
	const total = 100
	for i := 0; i < total; i++ {
		w.Emit(Event{Action: "jobs.create", Outcome: OutcomeSuccess})
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Emit blocked: %d emits took %v", total, elapsed)
	}
	if got := testutil.ToFloat64(met.Dropped); got != total-8 {
		t.Fatalf("dropped = %v, want %d", got, total-8)
	}
}

func TestDrainBoundedWhenDatabaseHangs(t *testing.T) {
	t.Parallel()

	sender := &fakeSender{block: make(chan struct{})} // never unblocks
	opts := fastOptions()                             // drain timeout 200ms
	w := NewWriter(sender, testLogger(), testMetrics(), opts)

	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	w.Emit(Event{Action: "jobs.create", Outcome: OutcomeSuccess})
	waitFor(t, "event buffered", func() bool { return len(w.ch) == 0 })

	// The drain insert hits the hung DB and must give up at DrainTimeout
	// (the fake respects the insert context deadline), never hang shutdown.
	cancel()
	done := make(chan struct{})
	go func() { w.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after drain timeout")
	}
}

func TestBatchFlushBySize(t *testing.T) {
	t.Parallel()

	sender := &fakeSender{}
	met := testMetrics()
	opts := fastOptions()          // batch 4
	opts.BufferSize = 16           // all 9 emits must fit before Run wakes up
	opts.FlushInterval = time.Hour // the ticker must not interfere
	w := NewWriter(sender, testLogger(), met, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	// 9 events: exactly two full batches of 4 flush by size; the 9th waits
	// in memory (no ticker to save it) until the shutdown drain.
	for i := 0; i < 9; i++ {
		w.Emit(Event{Action: "users.update", Outcome: OutcomeSuccess})
	}

	waitFor(t, "8 rows in 2 batches", func() bool {
		rows, batches := sender.stats()
		return rows == 8 && batches == 2
	})

	cancel()
	w.Wait()

	// The drain flushed the leftover 9th event.
	if rows, batches := sender.stats(); rows != 9 || batches != 3 {
		t.Fatalf("after drain: rows=%d batches=%d, want 9/3", rows, batches)
	}
	if got := testutil.ToFloat64(met.Written); got != 9 {
		t.Fatalf("written = %v, want 9", got)
	}
	if got := testutil.ToFloat64(met.Dropped); got != 0 {
		t.Fatalf("dropped = %v, want 0", got)
	}
}

func TestBatchFlushByInterval(t *testing.T) {
	t.Parallel()

	sender := &fakeSender{}
	w := NewWriter(sender, testLogger(), testMetrics(), fastOptions()) // flush 10ms

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	// Below batch size: only the interval flush can move these rows.
	for i := 0; i < 3; i++ {
		w.Emit(Event{Action: "users.update", Outcome: OutcomeSuccess})
	}
	waitFor(t, "3 rows flushed by interval", func() bool {
		rows, _ := sender.stats()
		return rows == 3
	})

	cancel()
	w.Wait()
}

func TestInsertFailureDropsBatchAndKeepsGoing(t *testing.T) {
	t.Parallel()

	sender := &fakeSender{err: errors.New("connection refused")}
	met := testMetrics()
	w := NewWriter(sender, testLogger(), met, fastOptions())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	for i := 0; i < 4; i++ { // one full batch
		w.Emit(Event{Action: "jobs.cancel", Outcome: OutcomeSuccess})
	}
	waitFor(t, "insert error counted", func() bool {
		return testutil.ToFloat64(met.InsertErrors) == 1
	})

	// The database "recovers"; the writer must not be stuck or panicked.
	sender.mu.Lock()
	sender.err = nil
	sender.mu.Unlock()

	w.Emit(Event{Action: "jobs.cancel", Outcome: OutcomeSuccess})
	waitFor(t, "recovery batch written", func() bool {
		return testutil.ToFloat64(met.Written) == 1
	})

	cancel()
	w.Wait()
}

func TestDrainOnShutdownFlushesRemainder(t *testing.T) {
	t.Parallel()

	sender := &fakeSender{}
	met := testMetrics()
	opts := fastOptions()
	opts.FlushInterval = time.Hour // the ticker must NOT save us; drain must
	w := NewWriter(sender, testLogger(), met, opts)

	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	for i := 0; i < 3; i++ { // below batch size, interval far away
		w.Emit(Event{Action: "auth.logout", Outcome: OutcomeSuccess})
	}
	// Let Run pick the events up out of the channel before cancelling.
	waitFor(t, "writer consumed channel", func() bool { return len(w.ch) == 0 })

	cancel()
	w.Wait()

	if rows, _ := sender.stats(); rows != 3 {
		t.Fatalf("drained rows = %d, want 3", rows)
	}
}

func TestEmitDefaults(t *testing.T) {
	t.Parallel()

	sender := &fakeSender{}
	w := NewWriter(sender, testLogger(), Metrics{}, fastOptions())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.Emit(Event{Action: "jobs.requeue", Outcome: OutcomeFailure})
	waitFor(t, "one row", func() bool {
		rows, _ := sender.stats()
		return rows == 1
	})

	cancel()
	w.Wait()
}

func TestMarshalDetail(t *testing.T) {
	t.Parallel()

	if got := string(marshalDetail(nil)); got != `{}` {
		t.Fatalf("nil detail = %s, want {}", got)
	}
	raw := marshalDetail(map[string]any{"route": "POST /api/jobs", "status": 201})
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("detail is not valid JSON: %v", err)
	}
	if back["route"] != "POST /api/jobs" {
		t.Fatalf("round-trip lost route: %v", back)
	}
	// Unmarshalable content degrades to {} instead of failing the event.
	bad := map[string]any{"ch": make(chan int)}
	if got := string(marshalDetail(bad)); got != `{}` {
		t.Fatalf("bad detail = %s, want {}", got)
	}
}
