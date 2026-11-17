//go:build integration

package integration

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestImmediateDispatchMigration(t *testing.T) {
	check(t)
	pool := schedulingDB(t, "raven_immediate_dispatch_migration")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	dsn := pool.Config().ConnString()
	const up = "000009_immediate_dispatch.up.sql"
	const down = "000009_immediate_dispatch.down.sql"
	if err := applyMigrationFile(ctx, dsn, down); err != nil {
		t.Fatal(err)
	}
	execSQL := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	checkPending := func(want map[string]int) {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT job_id, execution_generation FROM scheduled_job_outbox`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]int{}
		for rows.Next() {
			var id string
			var gen int
			if err := rows.Scan(&id, &gen); err != nil {
				t.Fatal(err)
			}
			got[id] = gen
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(got, want) {
			t.Fatalf("pending dispatches: got %v, want %v", got, want)
		}
	}
	execSQL(`
		INSERT INTO jobs (id, type, payload, status, execution_generation) VALUES
			('legacy', 'webhook', '{}', 'QUEUED', 3),
			('stale',  'webhook', '{}', 'QUEUED', 2),
			('later',  'webhook', '{}', 'SCHEDULED', 1),
			('done',   'webhook', '{}', 'SUCCESS', 1);
		INSERT INTO scheduled_job_outbox (job_id, execution_generation) VALUES ('stale', 1)`)

	// hold a legacy submission open until the migration is waiting for its
	// lock. the backfill must include the job when that transaction commits.
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback(ctx) }()
	if _, err := writer.Exec(ctx, `INSERT INTO jobs (id, type, payload) VALUES ('in-flight', 'webhook', '{}')`); err != nil {
		t.Fatal(err)
	}
	migrated := make(chan error, 1)
	go func() { migrated <- applyMigrationFile(ctx, dsn, up) }()
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
			  AND relation = 'jobs'::regclass AND mode = 'ShareRowExclusiveLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-migrated:
			t.Fatalf("migration returned before the existing writer committed: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-migrated; err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"legacy": 3, "stale": 2, "in-flight": 1}
	checkPending(want)

	// an older jobs replica inserts only the job. an older scheduler also
	// writes the outbox row before committing the queued occurrence.
	execSQL(`INSERT INTO jobs (id, type, payload) VALUES ('old-api', 'webhook', '{}')`)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO jobs (id, type, payload) VALUES ('old-cron', 'webhook', '{}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"scheduled_job_outbox"},
		[]string{"job_id", "execution_generation"}, pgx.CopyFromRows([][]any{{"old-cron", 1}})); err != nil {
		t.Fatalf("legacy scheduler COPY: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("legacy scheduler COMMIT: %v", err)
	}
	want["old-api"], want["old-cron"] = 1, 1
	checkPending(want)

	// a requeue can overtake an older pending generation. jobs cancelled
	// or deleted in the same transaction need no dispatch.
	execSQL(`
		UPDATE jobs SET status = 'DEAD' WHERE id = 'stale';
		UPDATE jobs SET status = 'QUEUED', execution_generation = 3 WHERE id = 'stale';
		INSERT INTO jobs (id, type, payload) VALUES ('cancelled', 'webhook', '{}');
		UPDATE jobs SET status = 'CANCELLED' WHERE id = 'cancelled';
		INSERT INTO jobs (id, type, payload) VALUES ('deleted', 'webhook', '{}');
		DELETE FROM jobs WHERE id = 'deleted'`)
	want["stale"] = 3
	checkPending(want)

	// the job and its pending dispatch roll back together even if the
	// deferred trigger has already fired.
	aborted, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aborted.Rollback(ctx) }()
	if _, err := aborted.Exec(ctx, `
		INSERT INTO jobs (id, type, payload) VALUES ('aborted', 'webhook', '{}');
		SET CONSTRAINTS jobs_queued_dispatch IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	if err := aborted.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	checkPending(want)

	if err := applyMigrationFile(ctx, dsn, down); err != nil {
		t.Fatalf("rollback with pending publications: %v", err)
	}
	execSQL(`INSERT INTO jobs (id, type, payload) VALUES ('after-downgrade', 'webhook', '{}')`)
	checkPending(want)
}
