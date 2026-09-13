package jobs

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so store functions
// work standalone or inside a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

const jobColumns = `
	id, type, payload::text, status, priority, attempts, max_attempts,
	idempotency_key, owner_id::text, created_at, started_at, finished_at,
	error, worker_id, heartbeat_at, lease_until, execution_generation`

// scanJob reads one row of jobColumns.
func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Priority, &j.Attempts,
		&j.MaxAttempts, &j.IdempotencyKey, &j.OwnerID, &j.CreatedAt,
		&j.StartedAt, &j.FinishedAt, &j.Error, &j.WorkerID,
		&j.HeartbeatAt, &j.LeaseUntil, &j.ExecutionGeneration,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// insertJob stores a new job in QUEUED state. A duplicate idempotency key is
// reported as KindConflict so the caller can return the existing job.
func insertJob(ctx context.Context, q querier, j *Job) error {
	_, err := q.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, max_attempts,
		                  idempotency_key, owner_id)
		VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8::uuid)`,
		j.ID, j.Type, j.Payload, j.Status, j.Priority, j.MaxAttempts,
		j.IdempotencyKey, j.OwnerID,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if stderrors.As(err, &pgErr) && pgErr.Code == "23505" {
			return errors.E(errors.KindConflict, "idempotency_key_taken",
				"a job with this idempotency key already exists", err)
		}
		return errors.E(errors.KindUnknown, "job_insert_failed",
			"could not store the job", err)
	}
	return nil
}

// getJob loads one job by id.
func getJob(ctx context.Context, q querier, id string) (*Job, error) {
	j, err := scanJob(q.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return nil, errors.E(errors.KindNotFound, "job_not_found",
				"job does not exist", err)
		}
		return nil, errors.E(errors.KindUnknown, "job_lookup_failed",
			"could not load the job", err)
	}
	return j, nil
}

// jobByIdempotencyKey finds the job created for a key, or KindNotFound.
func jobByIdempotencyKey(ctx context.Context, q querier, key string) (*Job, error) {
	j, err := scanJob(q.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, key))
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return nil, errors.E(errors.KindNotFound, "job_not_found",
				"no job for this idempotency key", err)
		}
		return nil, errors.E(errors.KindUnknown, "job_lookup_failed",
			"could not load the job", err)
	}
	return j, nil
}

// listJobs returns one page plus the total matching row count. statusFilter /
// typeFilter empty means "no filter". Ordering: highest priority first, then
// oldest first — the order an operator wants to eyeball a queue in.
func listJobs(ctx context.Context, q querier, statusFilter, typeFilter string, limit, offset int) ([]*Job, int64, error) {
	var total int64
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM jobs
		WHERE ($1::text = '' OR status = $1) AND ($2::text = '' OR type = $2)`,
		statusFilter, typeFilter,
	).Scan(&total)
	if err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "job_count_failed",
			"could not count jobs", err)
	}

	rows, err := q.Query(ctx, `
		SELECT `+jobColumns+` FROM jobs
		WHERE ($1::text = '' OR status = $1) AND ($2::text = '' OR type = $2)
		ORDER BY priority DESC, created_at, id
		LIMIT $3 OFFSET $4`,
		statusFilter, typeFilter, limit, offset,
	)
	if err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "job_list_failed",
			"could not list jobs", err)
	}
	defer rows.Close()

	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, 0, errors.E(errors.KindUnknown, "job_list_failed",
				"could not read a job row", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "job_list_failed",
			"could not list jobs", err)
	}
	return out, total, nil
}

// cancelJob moves QUEUED/RETRYING -> CANCELLED atomically. The WHERE clause
// is the guard: a job that moved on (PROCESSING or beyond) is untouched.
// Returns the updated job; KindNotFound when the id does not exist;
// KindConflict when the job is in a non-cancellable state.
func cancelJob(ctx context.Context, q querier, id string) (*Job, error) {
	j, err := scanJob(q.QueryRow(ctx, `
		UPDATE jobs SET status = $2, finished_at = now()
		WHERE id = $1 AND status IN ('QUEUED', 'RETRYING')
		RETURNING `+jobColumns, id, StatusCancelled))
	if err == nil {
		return j, nil
	}
	if !stderrors.Is(err, pgx.ErrNoRows) {
		return nil, errors.E(errors.KindUnknown, "job_cancel_failed",
			"could not cancel the job", err)
	}
	// The optimistic update matched nothing. Say why.
	cur, getErr := getJob(ctx, q, id)
	if getErr != nil {
		return nil, getErr // KindNotFound or a real database error
	}
	return nil, errors.E(errors.KindConflict, "job_not_cancellable",
		"job is "+string(cur.Status)+" and cannot be cancelled", nil)
}

// requeueJob moves DEAD -> QUEUED atomically, resetting the execution fields
// so the job looks freshly created (job_attempts history is kept untouched).
// The generation is bumped: a zombie writer holding the pre-requeue token
// must not be able to finish the resurrected job.
func requeueJob(ctx context.Context, q querier, id string) (*Job, error) {
	j, err := scanJob(q.QueryRow(ctx, `
		UPDATE jobs
		SET status = $2, attempts = 0, error = '', worker_id = '',
		    started_at = NULL, finished_at = NULL,
		    heartbeat_at = NULL, lease_until = NULL,
		    execution_generation = execution_generation + 1
		WHERE id = $1 AND status = 'DEAD'
		RETURNING `+jobColumns, id, StatusQueued))
	if err == nil {
		return j, nil
	}
	if !stderrors.Is(err, pgx.ErrNoRows) {
		return nil, errors.E(errors.KindUnknown, "job_requeue_failed",
			"could not requeue the job", err)
	}
	cur, getErr := getJob(ctx, q, id)
	if getErr != nil {
		return nil, getErr
	}
	return nil, errors.E(errors.KindConflict, "job_not_dead",
		"only DEAD jobs can be requeued (job is "+string(cur.Status)+")", nil)
}

// markQueuedFailed flips a freshly created job to FAILED. Used when the
// broker produce fails right after insert: the row must not stay QUEUED or a
// worker would never see it and the user would wait forever.
func markQueuedFailed(ctx context.Context, q querier, id, errMsg string) error {
	_, err := q.Exec(ctx, `
		UPDATE jobs SET status = $2, error = $3, finished_at = now()
		WHERE id = $1 AND status = 'QUEUED'`,
		id, StatusFailed, errMsg,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "job_update_failed",
			"could not mark the job failed", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Worker-side writes. The worker service calls these; they live here because
// the jobs table has exactly one owner.
//
// Lease discipline (ADR 009): claiming a job writes heartbeat_at and
// lease_until = now() + lease; the worker renews both while executing; finish
// writes carry the execution_generation fencing token so a dead worker's late
// write matches zero rows and is rejected.
// ---------------------------------------------------------------------------

// StartJobFence is the at-least-once dedup fence and the lease claim: it moves
// the job to PROCESSING only when it is still QUEUED/RETRYING and the
// message's generation still matches the row (msgGeneration 0 = unknown, for
// pre-lease messages: accept whatever the row has). A cancelled, finished,
// already-running or stale-generation message matches nothing, and the worker
// skips it. Returns (nil, nil) in that case.
func StartJobFence(ctx context.Context, q querier, id, workerID string, msgGeneration int, lease time.Duration) (*Job, error) {
	j, err := scanJob(q.QueryRow(ctx, `
		UPDATE jobs
		SET status = $2, started_at = now(), worker_id = $3,
		    heartbeat_at = now(), lease_until = now() + make_interval(secs => $4)
		WHERE id = $1 AND status IN ('QUEUED', 'RETRYING')
		  AND ($5 = 0 OR execution_generation = $5)
		RETURNING `+jobColumns, id, StatusProcessing, workerID, lease.Seconds(), msgGeneration))
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.E(errors.KindUnknown, "job_start_failed",
			"could not mark the job processing", err)
	}
	return j, nil
}

// RenewJobLease extends the lease of a running job. Returns false when the
// lease is lost — the sweeper bumped the generation (worker declared dead) or
// the job left PROCESSING — in which case the worker must abandon the handler
// and must not write any result: the finish fence would reject it anyway.
func RenewJobLease(ctx context.Context, q querier, id string, generation int, lease time.Duration) (bool, error) {
	tag, err := q.Exec(ctx, `
		UPDATE jobs
		SET heartbeat_at = now(), lease_until = now() + make_interval(secs => $3)
		WHERE id = $1 AND execution_generation = $2 AND status = 'PROCESSING'`,
		id, generation, lease.Seconds())
	if err != nil {
		return false, errors.E(errors.KindUnknown, "job_renew_failed",
			"could not renew the job lease", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FinishJobSuccess records a successful attempt and moves the job to SUCCESS
// in one transaction. attempt is the 1-based number of the attempt that ran.
// generation is the fencing token the worker claimed the job with: when a
// newer generation owns the job (the sweeper took over) the update matches
// zero rows and the write is rejected. Returns the updated job, or (nil, nil)
// when the write was fenced — the caller treats that as a no-op success.
func FinishJobSuccess(ctx context.Context, q querier, id string, generation, attempt int, workerID string, startedAt, finishedAt time.Time) (*Job, error) {
	var j *Job
	err := withTx(ctx, q, func(tx pgx.Tx) error {
		if err := insertAttempt(ctx, tx, id, attempt, startedAt, &finishedAt, "", workerID); err != nil {
			return err
		}
		var err error
		j, err = scanJob(tx.QueryRow(ctx, `
			UPDATE jobs
			SET status = $2, attempts = $3, error = '', finished_at = $4,
			    lease_until = NULL
			WHERE id = $1 AND status = 'PROCESSING' AND execution_generation = $5
			RETURNING `+jobColumns, id, StatusSuccess, attempt, finishedAt, generation))
		if err != nil {
			if stderrors.Is(err, pgx.ErrNoRows) {
				// Fenced: a newer generation took over (or the job moved on
				// under us, e.g. cancel raced us). The attempt row above
				// stays: the attempt really ran.
				return nil
			}
			return errors.E(errors.KindUnknown, "job_finish_failed",
				"could not mark the job succeeded", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return j, nil // j may be nil when the write was fenced
}

// FinishJobFailure records a failed attempt and moves the job to RETRYING or
// DEAD in one transaction, fenced by generation exactly like
// FinishJobSuccess. For RETRYING, retryLease (backoff delay + lease) keeps
// the job sweepable: a worker that dies before its retry timer fires leaves
// an expired lease behind for the sweeper. Returns the updated job (nil when
// the write was fenced).
func FinishJobFailure(ctx context.Context, q querier, id string, generation, attempt int, workerID, errMsg string, dead bool, startedAt, finishedAt time.Time, retryLease time.Duration) (*Job, error) {
	next := StatusRetrying
	if dead {
		next = StatusDead
	}
	var j *Job
	err := withTx(ctx, q, func(tx pgx.Tx) error {
		if err := insertAttempt(ctx, tx, id, attempt, startedAt, &finishedAt, errMsg, workerID); err != nil {
			return err
		}
		var err error
		if dead {
			// Terminal: nothing left to recover, clear the lease.
			j, err = scanJob(tx.QueryRow(ctx, `
				UPDATE jobs
				SET status = $2, attempts = $3, error = $4, finished_at = $5,
				    lease_until = NULL
				WHERE id = $1 AND status = 'PROCESSING' AND execution_generation = $6
				RETURNING `+jobColumns, id, next, attempt, errMsg, finishedAt, generation))
		} else {
			// RETRYING keeps a lease so the sweeper recovers the job when the
			// worker dies before republishing. finished_at stays NULL: the
			// job is not done, it will run again.
			j, err = scanJob(tx.QueryRow(ctx, `
				UPDATE jobs
				SET status = $2, attempts = $3, error = $4, finished_at = NULL,
				    heartbeat_at = now(), lease_until = now() + make_interval(secs => $5)
				WHERE id = $1 AND status = 'PROCESSING' AND execution_generation = $6
				RETURNING `+jobColumns, id, next, attempt, errMsg,
				retryLease.Seconds(), generation))
		}
		if err != nil {
			if stderrors.Is(err, pgx.ErrNoRows) {
				return nil // fenced; attempt row stays
			}
			return errors.E(errors.KindUnknown, "job_finish_failed",
				"could not record the failed attempt", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return j, nil
}

// insertAttempt appends one row to job_attempts.
func insertAttempt(ctx context.Context, q querier, jobID string, attempt int, startedAt time.Time, finishedAt *time.Time, errMsg, workerID string) error {
	_, err := q.Exec(ctx, `
		INSERT INTO job_attempts (job_id, attempt, started_at, finished_at, error, worker_id)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		jobID, attempt, startedAt, finishedAt, errMsg, workerID,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "attempt_insert_failed",
			"could not record the attempt", err)
	}
	return nil
}

// countByStatus is a scrape-time helper for the raven_jobs_processing gauge.
func countByStatus(ctx context.Context, q querier, status Status) (int64, error) {
	var n int64
	err := q.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = $1`, status).Scan(&n)
	return n, err
}

// withTx runs fn in a transaction. q is normally the pool; this tiny local
// helper avoids exporting database.WithTx details through the store API.
func withTx(ctx context.Context, q querier, fn func(pgx.Tx) error) error {
	beginner, ok := q.(interface {
		Begin(ctx context.Context) (pgx.Tx, error)
	})
	if !ok {
		return errors.E(errors.KindUnknown, "tx_unsupported",
			"store handle cannot begin transactions", nil)
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return errors.E(errors.KindUnavailable, "tx_begin_failed",
			"could not begin a database transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.E(errors.KindUnavailable, "tx_commit_failed",
			"could not commit the database transaction", err)
	}
	return nil
}
