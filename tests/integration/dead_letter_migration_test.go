//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDeadLetterOutboxMigration(t *testing.T) {
	check(t)
	ctx := t.Context()
	pool := recoveryDB(t, "raven_dead_letter_migration")
	dsn := pool.Config().ConnString()
	const up = "000010_dead_letter_outbox.up.sql"
	const down = "000010_dead_letter_outbox.down.sql"
	if err := applyMigrationFile(ctx, dsn, down); err != nil {
		t.Fatal(err)
	}
	seedJob(t, pool, "old", "DEAD", 1, 1, nil)
	seedJob(t, pool, "new", "PROCESSING", 0, 1, nil)
	if err := applyMigrationFile(ctx, dsn, up); err != nil {
		t.Fatal(err)
	}
	const failJob = "UPDATE jobs SET status='DEAD', attempts=1, error=$1, finished_at=now() WHERE id='new'"
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, failJob, "rolled back"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_dead_letter_outbox").Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("after upgrade and rolled-back transition: pending=%d err=%v", pending, err)
	}

	if _, err := pool.Exec(ctx, failJob, "first failure"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET status='PROCESSING', execution_generation=2 WHERE id='new'"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, failJob, "second failure"); err != nil {
		t.Fatal(err)
	}
	downSQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", down))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(downSQL)); err == nil || !strings.Contains(err.Error(), "drain job_dead_letter_outbox") {
		t.Fatalf("downgrade with outstanding dead letters: %v", err)
	}
	var generations []int32
	var reasons []string
	err = pool.QueryRow(ctx, `SELECT array_agg(execution_generation ORDER BY execution_generation),
		array_agg(job_snapshot->>'error' ORDER BY execution_generation)
		FROM job_dead_letter_outbox WHERE job_id='new'`).Scan(&generations, &reasons)
	if err != nil || !slices.Equal(generations, []int32{1, 2}) || !slices.Equal(reasons, []string{"first failure", "second failure"}) {
		t.Fatalf("retained terminal snapshots: generations=%v reasons=%v err=%v", generations, reasons, err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM job_dead_letter_outbox WHERE job_id='new'"); err != nil {
		t.Fatal(err)
	}
	if err := applyMigrationFile(ctx, dsn, down); err != nil {
		t.Fatalf("downgrade after draining: %v", err)
	}
}
