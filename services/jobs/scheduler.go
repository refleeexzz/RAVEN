package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Dispatcher is the delayed-job scheduler: every interval it releases due
// SCHEDULED jobs — flip to QUEUED plus a broker publish — so they reach
// workers at their scheduled_at time instead of at create time.
//
// Correctness notes:
//   - The claim (SELECT ... FOR UPDATE SKIP LOCKED), the status flip and the
//     broker publish commit in ONE transaction: a job only becomes QUEUED
//     when its message is already on the broker. If the publish fails the
//     whole batch rolls back and is retried on the next pass.
//   - SKIP LOCKED lets several replicas run the loop concurrently: each
//     pass grabs a disjoint set of rows.
//   - A crash between the publishes and the commit can republish a message
//     on the next pass; the worker claim fence ignores the duplicate
//     (at-least-once is the platform semantic).
//
// Lateness bound: a due job is released at most interval after its
// scheduled_at (plus one publish round-trip), assuming the broker is up.
type Dispatcher struct {
	pool     *pgxpool.Pool
	producer *Producer
	rdb      redis.UniversalClient // may be nil: events are best effort
	log      *slog.Logger
	metrics  *ServiceMetrics // may be nil in tests
	interval time.Duration
}

// NewDispatcher wires the delayed-job dispatcher. interval is how often a
// pass runs (JOBS_SCHEDULER_INTERVAL, default 1s).
func NewDispatcher(pool *pgxpool.Pool, producer *Producer, rdb redis.UniversalClient, log *slog.Logger, m *ServiceMetrics, interval time.Duration) *Dispatcher {
	if log == nil {
		log = slog.Default()
	}
	return &Dispatcher{
		pool:     pool,
		producer: producer,
		rdb:      rdb,
		log:      log,
		metrics:  m,
		interval: interval,
	}
}

// Run dispatches once at startup and then every interval until ctx is
// cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	d.log.Info("delayed-job dispatcher starting", slog.Duration("interval", d.interval))
	t := time.NewTicker(d.interval)
	defer t.Stop()
	d.dispatchQuiet(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.dispatchQuiet(ctx)
		}
	}
}

// dispatchQuiet runs one pass, logging instead of propagating failures.
func (d *Dispatcher) dispatchQuiet(ctx context.Context) {
	n, err := d.DispatchOnce(ctx)
	if err != nil {
		d.log.Warn("dispatcher pass failed", slog.Any("error", err))
		return
	}
	if n > 0 {
		d.log.Info("dispatcher released scheduled jobs", slog.Int("count", n))
	}
}

// DispatchOnce runs one pass: claim the due batch under SKIP LOCKED,
// publish each job inside the transaction and commit. Returns the number of
// jobs released.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	var released []*Job
	err := withTx(ctx, d.pool, func(tx pgx.Tx) error {
		due, err := claimDueScheduled(ctx, tx)
		if err != nil {
			return err
		}
		for _, j := range due {
			// Bounded publish: one slow broker must not hold the row locks
			// (and the transaction) open for long. A failure aborts the
			// batch; the rows stay SCHEDULED for the next pass.
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := d.producer.PublishExecution(pubCtx, j)
			cancel()
			if err != nil {
				return err
			}
			released = append(released, j)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Committed: side effects that may not roll the work back.
	for _, j := range released {
		PublishJobEvent(context.Background(), d.rdb, d.log, j)
		if d.metrics != nil {
			d.metrics.dispatched.Inc()
			d.metrics.observeTransition(j.Type, StatusQueued)
		}
		d.log.Info("scheduled job released",
			slog.String("job_id", j.ID), slog.String("type", j.Type),
			slog.Int("priority", j.Priority))
	}
	return len(released), nil
}
