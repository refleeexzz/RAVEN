package jobs

import (
	"context"
	stderrors "errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

const (
	// sweeperLockKey is the Postgres advisory-lock id guarding the sweeper.
	// Any int64 works; it must simply be unique among advisory-lock users of
	// this database. Three jobs-service replicas run this loop — exactly one
	// holds the lock per pass, the others idle and retry next interval. A
	// holder that dies releases the lock with its session, so another
	// replica takes over at the next tick: no external leader election.
	sweeperLockKey int64 = 727_000_001

	// sweeperBatchSize bounds how many stranded jobs one pass recovers. A
	// bigger backlog is drained over the next intervals instead of holding
	// the advisory lock for a long time.
	sweeperBatchSize = 100

	// sweeperRecoverLease is the fresh lease a recovered job gets. It gives
	// a worker time to claim the republished message before the job becomes
	// a sweep candidate again — and if the republish itself failed, the next
	// pass retries it once this expires. ~2x the default sweep interval.
	sweeperRecoverLease = 30 * time.Second

	// leaseExpiredError is written into jobs.error and the job_attempts row
	// of the interrupted attempt.
	leaseExpiredError = "lease expired (worker lost)"
)

// errSweepRaceLost marks "the job stopped being a candidate between the
// candidate scan and the guarded update" — a worker legitimately claimed or
// finished it in between. Normal race, not a failure.
var errSweepRaceLost = stderrors.New("jobs: job no longer sweepable")

// Sweeper finds jobs stuck in PROCESSING/RETRYING past their lease (a worker
// died mid-flight or before its retry timer fired) and gives them a new
// generation, a RETRYING status and a fresh broker message. See ADR 009.
type Sweeper struct {
	pool     *pgxpool.Pool
	producer *Producer
	rdb      redis.UniversalClient // may be nil: events are best effort
	log      *slog.Logger
	metrics  *ServiceMetrics // may be nil in tests
	interval time.Duration
}

// NewSweeper wires the sweeper. interval is how often a pass runs
// (JOBS_SWEEP_INTERVAL, default 15s).
func NewSweeper(pool *pgxpool.Pool, producer *Producer, rdb redis.UniversalClient, log *slog.Logger, m *ServiceMetrics, interval time.Duration) *Sweeper {
	if log == nil {
		log = slog.Default()
	}
	return &Sweeper{
		pool:     pool,
		producer: producer,
		rdb:      rdb,
		log:      log,
		metrics:  m,
		interval: interval,
	}
}

// Run sweeps once at startup and then every interval until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	s.log.Info("job sweeper starting", slog.Duration("interval", s.interval))
	t := time.NewTicker(s.interval)
	defer t.Stop()
	s.sweepQuiet(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweepQuiet(ctx)
		}
	}
}

// sweepQuiet runs one pass, logging instead of propagating failures.
func (s *Sweeper) sweepQuiet(ctx context.Context) {
	n, err := s.SweepOnce(ctx)
	if err != nil {
		s.log.Warn("sweeper pass failed", slog.Any("error", err))
		return
	}
	if n > 0 {
		s.log.Info("sweeper recovered stranded jobs", slog.Int("count", n))
	}
}

// SweepOnce runs one pass: take the advisory lock (skip when another replica
// holds it), find expired leases, recover each in its own transaction and
// republish. Returns the number of jobs recovered.
func (s *Sweeper) SweepOnce(ctx context.Context) (int, error) {
	// Advisory locks are session-scoped, so the try-lock, the work and the
	// unlock must all happen on one pinned connection.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, errors.E(errors.KindUnavailable, "sweeper_conn_failed",
			"could not acquire a database connection", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, sweeperLockKey).Scan(&locked); err != nil {
		return 0, errors.E(errors.KindUnknown, "sweeper_lock_failed",
			"could not try the sweeper advisory lock", err)
	}
	if !locked {
		s.log.Debug("sweeper lock held by another replica, skipping pass")
		return 0, nil
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, sweeperLockKey); err != nil {
			// The lock dies with the session anyway; this is just hygiene.
			s.log.Warn("sweeper unlock failed", slog.Any("error", err))
		}
	}()

	if s.metrics != nil {
		s.metrics.sweeperRuns.Inc()
	}

	ids, err := s.candidates(ctx, conn)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, id := range ids {
		ok, err := s.recoverJob(ctx, id)
		if err != nil {
			s.log.Error("sweeper could not recover job",
				slog.String("job_id", id), slog.Any("error", err))
			continue
		}
		if ok {
			recovered++
		}
	}
	return recovered, nil
}

// candidates lists expired-lease job ids, oldest lease first.
func (s *Sweeper) candidates(ctx context.Context, q querier) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT id FROM jobs
		WHERE status IN ('PROCESSING', 'RETRYING') AND lease_until < now()
		ORDER BY lease_until
		LIMIT $1`, sweeperBatchSize)
	if err != nil {
		return nil, errors.E(errors.KindUnknown, "sweeper_scan_failed",
			"could not scan for expired leases", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, errors.E(errors.KindUnknown, "sweeper_scan_failed",
				"could not read an expired lease row", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.E(errors.KindUnknown, "sweeper_scan_failed",
			"could not scan for expired leases", err)
	}
	return ids, nil
}

// recoverJob gives one stranded job a new generation and a RETRYING (or DEAD,
// when attempts are already exhausted) status, writes the interrupted-attempt
// audit row, and republishes the work. The update re-checks the expiry
// predicate inside the transaction, so a worker that claimed or finished the
// job between the scan and here wins and nothing changes.
func (s *Sweeper) recoverJob(ctx context.Context, id string) (bool, error) {
	var j *Job
	err := withTx(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock the row and capture forensics for the interrupted attempt:
		// the update below clears worker_id, so grab it first.
		var (
			prevWorkerID string
			prevStarted  *time.Time
			interruptAtt int
		)
		err := tx.QueryRow(ctx, `
			SELECT worker_id, started_at, attempts + 1 FROM jobs WHERE id = $1
			FOR UPDATE`, id).
			Scan(&prevWorkerID, &prevStarted, &interruptAtt)
		if err != nil {
			if stderrors.Is(err, pgx.ErrNoRows) {
				return errSweepRaceLost
			}
			return errors.E(errors.KindUnknown, "sweeper_recover_failed",
				"could not lock the stranded job", err)
		}

		j, err = scanJob(tx.QueryRow(ctx, `
			UPDATE jobs
			SET execution_generation = execution_generation + 1,
			    status     = CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'RETRYING' END,
			    error      = $2,
			    worker_id  = '',
			    heartbeat_at = NULL,
			    lease_until = CASE WHEN attempts >= max_attempts THEN NULL
			                       ELSE now() + make_interval(secs => $3) END,
			    finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE finished_at END
			WHERE id = $1 AND status IN ('PROCESSING', 'RETRYING') AND lease_until < now()
			RETURNING `+jobColumns, id, leaseExpiredError, sweeperRecoverLease.Seconds()))
		if err != nil {
			if stderrors.Is(err, pgx.ErrNoRows) {
				return errSweepRaceLost
			}
			return errors.E(errors.KindUnknown, "sweeper_recover_failed",
				"could not take over the stranded job", err)
		}

		// Audit the interrupted attempt. started_at falls back to now: a
		// RETRYING job stranded before its retry timer fired never started
		// this attempt at all. finished_at is the sweep moment.
		startedAt := time.Now().UTC()
		if prevStarted != nil {
			startedAt = *prevStarted
		}
		finishedAt := time.Now().UTC()
		if err := insertAttempt(ctx, tx, id, interruptAtt, startedAt, &finishedAt,
			leaseExpiredError, prevWorkerID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if stderrors.Is(err, errSweepRaceLost) {
			return false, nil
		}
		return false, err
	}

	// The transaction committed: republish outside it. If the produce fails
	// the row keeps its sweeperRecoverLease, so a later pass retries the
	// publish instead of stranding the job a second time.
	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if j.Status == StatusDead {
		if err := s.producer.PublishJob(pubCtx, TopicDLQ, j); err != nil {
			s.log.Error("sweeper DLQ produce failed",
				slog.String("job_id", j.ID), slog.Any("error", err))
		}
	} else if err := s.producer.PublishJob(pubCtx, TopicJobs, j); err != nil {
		s.log.Error("sweeper republish failed; job stays RETRYING until the next pass",
			slog.String("job_id", j.ID), slog.Any("error", err))
	}

	PublishJobEvent(context.Background(), s.rdb, s.log, j)
	if s.metrics != nil {
		s.metrics.sweeperRecovered.WithLabelValues(sweeperOutcome(j.Status)).Inc()
	}
	s.log.Info("sweeper recovered job",
		slog.String("job_id", j.ID), slog.String("status", string(j.Status)),
		slog.Int("generation", j.ExecutionGeneration))
	return true, nil
}

// sweeperOutcome maps the recovery result to its metric label.
func sweeperOutcome(s Status) string {
	if s == StatusDead {
		return "dead"
	}
	return "retry"
}
