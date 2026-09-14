package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// CronScheduler fires due cron_schedules: every interval it locks the due
// rows (FOR UPDATE SKIP LOCKED), spawns one QUEUED job per schedule,
// advances next_run_at and publishes — all in ONE transaction. That is the
// idempotency story:
//
//   - restart between fire and commit → the transaction rolls back, the
//     schedule is still due, the next pass fires it once;
//   - two replicas racing → SKIP LOCKED hands each row to exactly one
//     pass;
//   - broker down → the publish error rolls everything back, the run is
//     retried next pass instead of being lost.
//
// Missed runs do NOT catch up: after an outage the schedule fires once and
// next_run_at is set to the next future match, never a storm of backfills.
type CronScheduler struct {
	pool     *pgxpool.Pool
	producer *Producer
	rdb      redis.UniversalClient // may be nil: events are best effort
	log      *slog.Logger
	metrics  *ServiceMetrics // may be nil in tests
	interval time.Duration
}

// NewCronScheduler wires the cron loop. interval is how often a pass runs
// (JOBS_SCHEDULER_INTERVAL, default 1s).
func NewCronScheduler(pool *pgxpool.Pool, producer *Producer, rdb redis.UniversalClient, log *slog.Logger, m *ServiceMetrics, interval time.Duration) *CronScheduler {
	if log == nil {
		log = slog.Default()
	}
	return &CronScheduler{
		pool:     pool,
		producer: producer,
		rdb:      rdb,
		log:      log,
		metrics:  m,
		interval: interval,
	}
}

// Run fires once at startup and then every interval until ctx is cancelled.
func (s *CronScheduler) Run(ctx context.Context) {
	s.log.Info("cron scheduler starting", slog.Duration("interval", s.interval))
	t := time.NewTicker(s.interval)
	defer t.Stop()
	s.tickQuiet(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tickQuiet(ctx)
		}
	}
}

// tickQuiet runs one pass, logging instead of propagating failures.
func (s *CronScheduler) tickQuiet(ctx context.Context) {
	n, err := s.TickOnce(ctx)
	if err != nil {
		s.log.Warn("cron scheduler pass failed", slog.Any("error", err))
		return
	}
	if n > 0 {
		s.log.Info("cron scheduler spawned jobs", slog.Int("count", n))
	}
}

// TickOnce fires every due schedule once. Returns the number of jobs
// spawned.
func (s *CronScheduler) TickOnce(ctx context.Context) (int, error) {
	var spawned []*Job
	err := withTx(ctx, s.pool, func(tx pgx.Tx) error {
		due, err := dueCrons(ctx, tx)
		if err != nil {
			return err
		}
		for _, c := range due {
			job, err := s.fireCron(ctx, tx, c)
			if err != nil {
				return err
			}
			if job != nil {
				spawned = append(spawned, job)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Committed: best-effort side effects only.
	for _, j := range spawned {
		PublishJobEvent(context.Background(), s.rdb, s.log, j)
		if s.metrics != nil {
			s.metrics.cronSpawned.Inc()
			s.metrics.observeTransition(j.Type, StatusQueued)
		}
		s.log.Info("cron spawned job",
			slog.String("job_id", j.ID), slog.String("type", j.Type),
			slog.Int("priority", j.Priority))
	}
	return len(spawned), nil
}

// fireCron spawns the job instance for one locked, due schedule and
// advances it. Returns (nil, nil) when the schedule was disabled instead
// (corrupt row guard — see disableCron).
func (s *CronScheduler) fireCron(ctx context.Context, tx pgx.Tx, c *Cron) (*Job, error) {
	sched, err := ParseCron(c.Expr)
	if err != nil {
		s.log.Error("stored cron expression no longer parses, disabling schedule",
			slog.String("cron_id", c.ID), slog.String("expr", c.Expr), slog.Any("error", err))
		if derr := disableCron(ctx, tx, c.ID); derr != nil {
			return nil, derr
		}
		return nil, nil
	}

	now := time.Now().UTC()
	next, ok := sched.Next(now)
	if !ok {
		s.log.Error("stored cron expression can never fire, disabling schedule",
			slog.String("cron_id", c.ID), slog.String("expr", c.Expr))
		if derr := disableCron(ctx, tx, c.ID); derr != nil {
			return nil, derr
		}
		return nil, nil
	}

	j := &Job{
		ID:                  NewID(),
		Type:                c.Type,
		Payload:             c.Payload,
		Status:              StatusQueued,
		Priority:            c.Priority,
		MaxAttempts:         defaultMaxAttempts,
		OwnerID:             c.OwnerID,
		CreatedAt:           now,
		ExecutionGeneration: initialGeneration,
	}
	if err := insertJob(ctx, tx, j, true); err != nil {
		return nil, err
	}
	if err := advanceCron(ctx, tx, c.ID, now, next); err != nil {
		return nil, err
	}

	// Publish inside the transaction: a produce failure rolls back the job
	// insert AND the next_run_at advance, so the run is retried next pass.
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = s.producer.PublishExecution(pubCtx, j)
	cancel()
	if err != nil {
		return nil, err
	}
	return j, nil
}
