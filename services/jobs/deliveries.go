package jobs

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// Webhook delivery observability (migration 000005).
//
// Every webhook delivery attempt the worker makes lands here: success, HTTP
// error, transport error and egress-guard refusal alike. The worker writes
// (it already holds a Postgres pool for the lease machinery, so a direct
// insert beats a gRPC hop that would couple execution to jobs-service
// availability); the jobs service reads the table back for ListDeliveries.
//
// WIRE-UP NOTE: Server.ListDeliveries is intentionally plain-Go. The rpc
// ListDeliveries proto messages, the registration on the gRPC server and
// the gateway REST route are a follow-up owned by the parent — the logic
// below is the whole owner-scoped read path, ready to be wrapped.

// MaxDeliverySnippetBytes caps response_snippet at 1 KiB. The worker
// truncates before insert; InsertWebhookDelivery clamps again so a buggy
// producer can never trip the database CHECK.
const MaxDeliverySnippetBytes = 1024

// WebhookDelivery mirrors one row of webhook_deliveries. Nullable columns
// (status_code, latency_ms) are pointers: NULL means "no response came
// back" / "no request left the worker".
type WebhookDelivery struct {
	ID              int64
	JobID           string
	Attempt         int
	URL             string
	StatusCode      *int32
	LatencyMS       *int32
	ResponseSnippet string
	Blocked         bool
	Error           string
	Ts              time.Time
}

const deliveryColumns = `
	id, job_id, attempt, url, status_code, latency_ms, response_snippet,
	blocked, error, ts`

// InsertWebhookDelivery appends one attempt row. Called by the worker after
// every webhook attempt, best effort: a failure is logged there and the job
// flow goes on untouched.
func InsertWebhookDelivery(ctx context.Context, q querier, d *WebhookDelivery) error {
	snippet := d.ResponseSnippet
	if len(snippet) > MaxDeliverySnippetBytes {
		snippet = snippet[:MaxDeliverySnippetBytes]
	}
	_, err := q.Exec(ctx, `
		INSERT INTO webhook_deliveries
			(job_id, attempt, url, status_code, latency_ms, response_snippet, blocked, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		d.JobID, d.Attempt, d.URL, d.StatusCode, d.LatencyMS, snippet, d.Blocked, d.Error,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "delivery_insert_failed",
			"could not record the webhook delivery", err)
	}
	return nil
}

// listWebhookDeliveries pages one job's deliveries oldest-first (the order
// the attempts happened in) plus the total row count for the job.
func listWebhookDeliveries(ctx context.Context, q querier, jobID string, limit, offset int) ([]*WebhookDelivery, int64, error) {
	var total int64
	if err := q.QueryRow(ctx,
		`SELECT count(*) FROM webhook_deliveries WHERE job_id = $1`, jobID,
	).Scan(&total); err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "delivery_count_failed",
			"could not count webhook deliveries", err)
	}

	rows, err := q.Query(ctx, `
		SELECT `+deliveryColumns+`
		FROM webhook_deliveries
		WHERE job_id = $1
		ORDER BY ts, id
		LIMIT $2 OFFSET $3`, jobID, limit, offset)
	if err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "delivery_list_failed",
			"could not list webhook deliveries", err)
	}
	defer rows.Close()

	out := make([]*WebhookDelivery, 0)
	for rows.Next() {
		var d WebhookDelivery
		if err := rows.Scan(
			&d.ID, &d.JobID, &d.Attempt, &d.URL, &d.StatusCode, &d.LatencyMS,
			&d.ResponseSnippet, &d.Blocked, &d.Error, &d.Ts,
		); err != nil {
			return nil, 0, errors.E(errors.KindUnknown, "delivery_list_failed",
				"could not read a webhook delivery row", err)
		}
		out = append(out, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "delivery_list_failed",
			"could not list webhook deliveries", err)
	}
	return out, total, nil
}

// ListDeliveries is the owner-scoped read path for webhook deliveries
// (JOBS-02, same rules as GetJob): the caller must own the job, admin:*
// bypasses, and a caller with no identity is internal traffic. Foreign jobs
// answer KindNotFound — a stranger's delivery history must be
// indistinguishable from a missing job.
//
// Pagination follows the platform rules (1-based page, size defaults to 20,
// caps at 100, page capped at MaxListPage — JOBS-04). Returns the page, the
// total delivery count for the job and a typed error.
func (s *Server) ListDeliveries(ctx context.Context, jobID string, page, pageSize int32) ([]*WebhookDelivery, int64, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	if jobID == "" {
		return nil, 0, errors.E(errors.KindInvalid, "job_id_required",
			"job_id is required", nil)
	}
	// Authorize against the job, not the delivery rows: ownership lives on
	// the jobs table. The owner lookup is local on purpose — it keeps this
	// read path decoupled from the jobs store internals.
	ownerID, err := deliveryJobOwner(ctx, s.pool, jobID)
	if err != nil {
		return nil, 0, err
	}
	if !caller.CanAccessJob(ownerID) {
		return nil, 0, errors.E(errors.KindNotFound, "job_not_found",
			"job does not exist", nil)
	}

	if page < 1 {
		page = 1
	}
	if page > MaxListPage {
		page = MaxListPage
	}
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return listWebhookDeliveries(ctx, s.pool, jobID, int(pageSize), pageOffset(page, pageSize))
}

// deliveryJobOwner reads just the owner of a job for the authorization
// check. KindNotFound when the job does not exist.
func deliveryJobOwner(ctx context.Context, q querier, jobID string) (*string, error) {
	var ownerID *string
	err := q.QueryRow(ctx,
		`SELECT owner_id::text FROM jobs WHERE id = $1`, jobID).Scan(&ownerID)
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return nil, errors.E(errors.KindNotFound, "job_not_found",
				"job does not exist", err)
		}
		return nil, errors.E(errors.KindUnknown, "job_lookup_failed",
			"could not load the job", err)
	}
	return ownerID, nil
}
