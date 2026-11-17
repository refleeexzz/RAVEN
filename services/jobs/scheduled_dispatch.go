package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	ravenerrors "github.com/refleeexzz/RAVEN/pkg/errors"
)

// record the whole batch in the transaction that makes its jobs claimable.
func queueScheduledDispatch(ctx context.Context, tx pgx.Tx, jobs []*Job) error {
	if len(jobs) == 0 {
		return nil
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"scheduled_job_outbox"},
		[]string{"job_id", "execution_generation"}, pgx.CopyFromSlice(len(jobs), func(i int) ([]any, error) {
			return []any{jobs[i].ID, jobs[i].ExecutionGeneration}, nil
		}))
	return err
}

// a manual requeue may overtake the previous generation's pending dispatch.
func queueImmediateDispatch(ctx context.Context, tx pgx.Tx, job *Job) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO scheduled_job_outbox (job_id, execution_generation) VALUES ($1, $2)
		ON CONFLICT (job_id) DO UPDATE SET execution_generation = EXCLUDED.execution_generation,
			queued_at = clock_timestamp()
		WHERE scheduled_job_outbox.execution_generation < EXCLUDED.execution_generation`,
		job.ID, job.ExecutionGeneration)
	if err != nil {
		return ravenerrors.E(ravenerrors.KindUnavailable, "job_dispatch_unavailable",
			"could not persist the pending job dispatch", err)
	}
	return nil
}

// lock the outbox entry so the worker can claim the job as soon as the broker
// delivers it. a request selects its generation, while recovery selects the
// oldest available entry.
func dispatchOne(ctx context.Context, q querier, producer *Producer, target *Job) (bool, error) {
	var jobID string
	var publishErr error
	err := withTx(ctx, q, func(tx pgx.Tx) error {
		const selectIntent = `SELECT job_id, execution_generation FROM scheduled_job_outbox `
		var row pgx.Row
		if target == nil {
			row = tx.QueryRow(ctx, selectIntent+`ORDER BY queued_at, job_id LIMIT 1 FOR UPDATE SKIP LOCKED`)
		} else {
			row = tx.QueryRow(ctx, selectIntent+`WHERE job_id=$1 AND execution_generation=$2 FOR UPDATE SKIP LOCKED`,
				target.ID, target.ExecutionGeneration)
		}
		var generation int
		if err := row.Scan(&jobID, &generation); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		job, err := getJob(ctx, tx, jobID, false)
		if err != nil {
			return err
		}
		if job.Status == StatusQueued && job.ExecutionGeneration == generation {
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			publishErr = producer.PublishExecution(pubCtx, job)
			cancel()
			if publishErr != nil {
				_, err = tx.Exec(ctx, `UPDATE scheduled_job_outbox SET queued_at = clock_timestamp() WHERE job_id = $1`, jobID)
				return err
			}
		}
		// a crash after acknowledgement and before commit can redeliver this generation.
		_, err = tx.Exec(ctx, `DELETE FROM scheduled_job_outbox WHERE job_id = $1`, jobID)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("dispatch outbox: %w", err)
	}
	if publishErr != nil {
		return false, fmt.Errorf("publish job %s: %w", jobID, publishErr)
	}
	return jobID != "", nil
}

type dispatchRecord func(context.Context, querier, *Producer, *Job) (bool, error)

func drainDispatch(ctx context.Context, pool *pgxpool.Pool, producer *Producer) error {
	return drainOutbox(ctx, pool, producer, dispatchOne)
}

func drainOutbox(ctx context.Context, pool *pgxpool.Pool, producer *Producer, dispatch dispatchRecord) error {
	for range dispatcherBatchSize {
		worked, err := dispatch(ctx, pool, producer, nil)
		if err != nil || !worked {
			return err
		}
	}
	return nil
}

// recover pending submissions even when future schedules are paused.
func runDispatchRecovery(ctx context.Context, pool *pgxpool.Pool, producer *Producer, log *slog.Logger, dispatch dispatchRecord) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		if err := drainOutbox(ctx, pool, producer, dispatch); err != nil && ctx.Err() == nil {
			log.Warn("dispatch recovery pass failed", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
