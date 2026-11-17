package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// both the worker and the jobs service need the trigger installed by this migration.
func CheckDeadLetterOutbox(ctx context.Context, q querier) error {
	var ready bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('job_dead_letter_outbox') IS NOT NULL`).Scan(&ready); err != nil {
		return fmt.Errorf("check dead letter outbox: %w", err)
	}
	if !ready {
		return fmt.Errorf("dead letter dispatch requires migration 000010 (dead_letter_outbox)")
	}
	return nil
}

// a target selects one terminal generation. recovery passes nil to take the oldest
// pending snapshot. a later manual requeue can change the live job independently.
func DispatchDeadLetter(ctx context.Context, q querier, producer *Producer, target *Job) (bool, error) {
	var job *Job
	var publishErr error
	err := withTx(ctx, q, func(tx pgx.Tx) error {
		const selectPending = `WITH pending AS (SELECT job_snapshot FROM job_dead_letter_outbox `
		const readSnapshot = ` FOR UPDATE SKIP LOCKED)
			SELECT ` + jobColumns + `
			FROM pending CROSS JOIN LATERAL jsonb_populate_record(NULL::jobs, pending.job_snapshot) AS snapshot`
		var row pgx.Row
		if target == nil {
			row = tx.QueryRow(ctx, selectPending+`ORDER BY queued_at, job_id, execution_generation LIMIT 1`+readSnapshot)
		} else {
			row = tx.QueryRow(ctx, selectPending+`WHERE job_id = $1 AND execution_generation = $2`+readSnapshot,
				target.ID, target.ExecutionGeneration)
		}

		var err error
		job, err = scanJob(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		publishErr = producer.PublishJob(pubCtx, TopicDLQ, job)
		cancel()
		if publishErr != nil {
			_, err = tx.Exec(ctx, `UPDATE job_dead_letter_outbox SET queued_at = clock_timestamp()
				WHERE job_id = $1 AND execution_generation = $2`, job.ID, job.ExecutionGeneration)
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM job_dead_letter_outbox WHERE job_id = $1 AND execution_generation = $2`,
			job.ID, job.ExecutionGeneration)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("dead letter outbox: %w", err)
	}
	if publishErr != nil {
		return false, fmt.Errorf("publish dead letter %s generation %d: %w", job.ID, job.ExecutionGeneration, publishErr)
	}
	return job != nil, nil
}
