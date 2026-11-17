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
// the status change and the pending dispatch commit together. the broker
// can then deliver the message to a worker that sees the committed row.
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

// release due jobs and drain pending dispatches, including work from earlier
// passes. the count includes jobs released even if a later publish fails.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	var released []*Job
	err := withTx(ctx, d.pool, func(tx pgx.Tx) error {
		var err error
		released, err = claimDueScheduled(ctx, tx)
		if err != nil {
			return err
		}
		return queueScheduledDispatch(ctx, tx, released)
	})
	if err != nil {
		return 0, err
	}

	// Committed: side effects that may not roll the work back.
	for _, j := range released {
		PublishJobEvent(ctx, d.rdb, d.log, j)
		if d.metrics != nil {
			d.metrics.dispatched.Inc()
			d.metrics.observeTransition(j.Type, StatusQueued)
		}
		d.log.Info("scheduled job released",
			slog.String("job_id", j.ID), slog.String("type", j.Type),
			slog.Int("priority", j.Priority))
	}
	return len(released), drainDispatch(ctx, d.pool, d.producer)
}
