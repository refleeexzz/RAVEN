package jobs

import (
	"context"
	stderrors "errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// Cron mirrors a row of cron_schedules (migration 000004).
type Cron struct {
	ID        string
	OwnerID   *string
	Name      string
	Expr      string
	Type      string
	Payload   string // raw JSON, validated before insert
	Priority  int
	Enabled   bool
	NextRunAt time.Time
	LastRunAt *time.Time
	CreatedAt time.Time
}

// newCronID builds a cron id: "cron_" + uuid, greppable like job ids.
func newCronID() string { return "cron_" + uuid.NewString() }

// cronNameMaxLen keeps names one-line and index-friendly.
const cronNameMaxLen = 255

// validateCronRequest checks a create request and returns the parsed
// schedule plus the normalized values. Pure: unit tests drive it directly.
// Job fields reuse ValidateCreate, so a cron can only spawn jobs that
// CreateJob would accept (known type, 64 KiB payload cap, priority 1-9).
func validateCronRequest(name, expr, jobType, payloadJSON string, priority int32, now time.Time) (*cronSchedule, int, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, 0, errors.E(errors.KindInvalid, "cron_name_required",
			"name is required", nil)
	}
	if len(name) > cronNameMaxLen {
		return nil, 0, errors.E(errors.KindInvalid, "cron_name_too_long",
			"name must be at most 255 characters", nil)
	}
	sched, err := ParseCron(expr)
	if err != nil {
		return nil, 0, err
	}
	// The expression must have a fire time at all: "0 0 31 2 *" parses fine
	// but can never run — creating it would be a silent no-op forever.
	if _, ok := sched.Next(now); !ok {
		return nil, 0, errors.E(errors.KindInvalid, "cron_expr_never_fires",
			"this expression never matches any date", nil)
	}
	prio, _, err := ValidateCreate(jobType, payloadJSON, priority, 0)
	if err != nil {
		return nil, 0, err
	}
	return sched, prio, nil
}

const cronColumns = `
	id, owner_id::text, name, cron_expr, type, payload::text, priority,
	enabled, next_run_at, last_run_at, created_at`

// scanCron reads one row of cronColumns.
func scanCron(row pgx.Row) (*Cron, error) {
	var c Cron
	err := row.Scan(
		&c.ID, &c.OwnerID, &c.Name, &c.Expr, &c.Type, &c.Payload,
		&c.Priority, &c.Enabled, &c.NextRunAt, &c.LastRunAt, &c.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// insertCron stores a new schedule.
func insertCron(ctx context.Context, q querier, c *Cron) error {
	_, err := q.Exec(ctx, `
		INSERT INTO cron_schedules (id, owner_id, name, cron_expr, type, payload,
		                            priority, enabled, next_run_at, created_at)
		VALUES ($1, $2::uuid, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)`,
		c.ID, c.OwnerID, c.Name, c.Expr, c.Type, c.Payload,
		c.Priority, c.Enabled, c.NextRunAt, c.CreatedAt,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "cron_insert_failed",
			"could not store the cron schedule", err)
	}
	return nil
}

// getCron loads one schedule by id, KindNotFound when missing.
func getCron(ctx context.Context, q querier, id string) (*Cron, error) {
	c, err := scanCron(q.QueryRow(ctx,
		`SELECT `+cronColumns+` FROM cron_schedules WHERE id = $1`, id))
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return nil, errors.E(errors.KindNotFound, "cron_not_found",
				"cron schedule does not exist", err)
		}
		return nil, errors.E(errors.KindUnknown, "cron_lookup_failed",
			"could not load the cron schedule", err)
	}
	return c, nil
}

// listCrons returns one page plus the total, owner-scoped exactly like
// listJobs (JOBS-02): regular callers only ever see their own schedules.
func listCrons(ctx context.Context, q querier, ownerScope string, limit, offset int) ([]*Cron, int64, error) {
	var total int64
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM cron_schedules
		WHERE ($1::text = '' OR owner_id = $1::uuid)`, ownerScope).Scan(&total)
	if err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "cron_count_failed",
			"could not count cron schedules", err)
	}

	rows, err := q.Query(ctx, `
		SELECT `+cronColumns+` FROM cron_schedules
		WHERE ($1::text = '' OR owner_id = $1::uuid)
		ORDER BY created_at, id
		LIMIT $2 OFFSET $3`, ownerScope, limit, offset)
	if err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "cron_list_failed",
			"could not list cron schedules", err)
	}
	defer rows.Close()

	var out []*Cron
	for rows.Next() {
		c, err := scanCron(rows)
		if err != nil {
			return nil, 0, errors.E(errors.KindUnknown, "cron_list_failed",
				"could not read a cron row", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "cron_list_failed",
			"could not list cron schedules", err)
	}
	return out, total, nil
}

// deleteCron removes a schedule; KindNotFound when the id does not exist.
// Authorization (owner check) happens in the handler before the delete.
func deleteCron(ctx context.Context, q querier, id string) error {
	tag, err := q.Exec(ctx, `DELETE FROM cron_schedules WHERE id = $1`, id)
	if err != nil {
		return errors.E(errors.KindUnknown, "cron_delete_failed",
			"could not delete the cron schedule", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.E(errors.KindNotFound, "cron_not_found",
			"cron schedule does not exist", nil)
	}
	return nil
}

// cronDueBatchSize bounds how many due schedules one pass fires; a bigger
// backlog drains over the next intervals.
const cronDueBatchSize = 100

// dueCrons locks up to cronDueBatchSize enabled schedules whose time has
// come. FOR UPDATE SKIP LOCKED lets replicas share the loop; the caller
// advances next_run_at and commits inside the same transaction, so a due
// schedule fires exactly once even across restarts.
func dueCrons(ctx context.Context, tx pgx.Tx) ([]*Cron, error) {
	rows, err := tx.Query(ctx, `
		SELECT `+cronColumns+` FROM cron_schedules
		WHERE enabled AND next_run_at <= now()
		ORDER BY next_run_at, id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, cronDueBatchSize)
	if err != nil {
		return nil, errors.E(errors.KindUnknown, "cron_scan_failed",
			"could not scan for due cron schedules", err)
	}
	defer rows.Close()

	var out []*Cron
	for rows.Next() {
		c, err := scanCron(rows)
		if err != nil {
			return nil, errors.E(errors.KindUnknown, "cron_scan_failed",
				"could not read a due cron row", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.E(errors.KindUnknown, "cron_scan_failed",
			"could not scan for due cron schedules", err)
	}
	return out, nil
}

// advanceCron records one firing and sets the next one. The row is already
// locked by dueCrons inside the same transaction.
func advanceCron(ctx context.Context, tx pgx.Tx, id string, lastRun, nextRun time.Time) error {
	_, err := tx.Exec(ctx, `
		UPDATE cron_schedules SET last_run_at = $2, next_run_at = $3
		WHERE id = $1`, id, lastRun, nextRun)
	if err != nil {
		return errors.E(errors.KindUnknown, "cron_advance_failed",
			"could not advance the cron schedule", err)
	}
	return nil
}

// disableCron switches a schedule off. Used when a stored expression turns
// out to be unparseable or unfireable at run time (should not happen —
// create validates both — but a broken row must not hot-loop the
// scheduler).
func disableCron(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `
		UPDATE cron_schedules SET enabled = false WHERE id = $1`, id)
	if err != nil {
		return errors.E(errors.KindUnknown, "cron_disable_failed",
			"could not disable the cron schedule", err)
	}
	return nil
}
