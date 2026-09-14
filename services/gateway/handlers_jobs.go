// handlers_jobs.go translates the jobs REST endpoints into jobs.JobService
// gRPC calls, and serves the live worker registry straight from Redis
// (SCAN worker:* + HGETALL, per docs/contracts/ports-and-env.md).
package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// jobsHandlers serves /api/jobs/* and /api/workers.
type jobsHandlers struct {
	jobs             *upstream
	client           genjobs.JobServiceClient
	deliveriesClient genjobs.JobDeliveriesServiceClient
	rdb              redis.UniversalClient
}

func newJobsHandlers(jobs *upstream, rdb redis.UniversalClient) *jobsHandlers {
	return &jobsHandlers{
		jobs:             jobs,
		client:           genjobs.NewJobServiceClient(jobs.conn),
		deliveriesClient: genjobs.NewJobDeliveriesServiceClient(jobs.conn),
		rdb:              rdb,
	}
}

// jobJSON is the public wire shape of a job (field list fixed by contract).
type jobJSON struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	Priority     int32           `json:"priority"`
	Attempts     int32           `json:"attempts"`
	MaxAttempts  int32           `json:"max_attempts"`
	CreatedAt    int64           `json:"created_at"`
	StartedAt    int64           `json:"started_at"`
	FinishedAt   int64           `json:"finished_at"`
	Error        string          `json:"error"`
	WorkerID     string          `json:"worker_id"`
	ScheduledAt  int64           `json:"scheduled_at"`
	ReplayedFrom string          `json:"replayed_from,omitempty"`
}

func jobToJSON(j *genjobs.Job) jobJSON {
	// payload_json should be a JSON document; if a buggy producer stored
	// something else, degrade to a JSON string instead of breaking the API.
	payload := json.RawMessage(j.GetPayloadJson())
	if len(payload) == 0 || !json.Valid(payload) {
		payload, _ = json.Marshal(j.GetPayloadJson())
	}
	return jobJSON{
		ID:           j.GetId(),
		Type:         j.GetType(),
		Payload:      payload,
		Status:       jobStatusName(j.GetStatus()),
		Priority:     j.GetPriority(),
		Attempts:     j.GetAttempts(),
		MaxAttempts:  j.GetMaxAttempts(),
		CreatedAt:    j.GetCreatedAt(),
		StartedAt:    j.GetStartedAt(),
		FinishedAt:   j.GetFinishedAt(),
		Error:        j.GetError(),
		WorkerID:     j.GetWorkerId(),
		ScheduledAt:  j.GetScheduledAt(),
		ReplayedFrom: j.GetReplayedFrom(),
	}
}

// jobStatusName renders JOB_STATUS_QUEUED as "QUEUED".
func jobStatusName(s genjobs.JobStatus) string {
	return strings.TrimPrefix(s.String(), "JOB_STATUS_")
}

// parseJobStatusFilter accepts ?status=queued, "QUEUED" or the full
// "JOB_STATUS_QUEUED" enum name. Empty means "no filter".
func parseJobStatusFilter(raw string) (genjobs.JobStatus, error) {
	if raw == "" {
		return genjobs.JobStatus_JOB_STATUS_UNSPECIFIED, nil
	}
	name := "JOB_STATUS_" + strings.ToUpper(strings.TrimPrefix(
		strings.TrimSpace(raw), "JOB_STATUS_"))
	value, ok := genjobs.JobStatus_value[name]
	status := genjobs.JobStatus(value)
	if !ok || status == genjobs.JobStatus_JOB_STATUS_UNSPECIFIED {
		return genjobs.JobStatus_JOB_STATUS_UNSPECIFIED, errors.E(errors.KindInvalid,
			"invalid_status_filter", "unknown job status "+raw, nil)
	}
	return status, nil
}

// createJobRequest is the body of POST /api/jobs.
type createJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int32           `json:"priority"`
	MaxAttempts int32           `json:"max_attempts"`
	ScheduledAt int64           `json:"scheduled_at"` // unix seconds; 0/absent = run now
}

// create handles POST /api/jobs. The Idempotency-Key header is forwarded
// into CreateJobRequest.idempotency_key — but never verbatim: it is first
// bound to a fingerprint of the whole request so a replayed key with a
// different payload cannot return the original job (see bindIdempotencyKey).
func (h *jobsHandlers) create(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	if strings.TrimSpace(req.Type) == "" {
		writeError(w, r, errors.E(errors.KindInvalid, "type_required",
			"job type is required", nil))
		return
	}

	var job *genjobs.Job
	err := h.jobs.call(r.Context(), "CreateJob", false, func(ctx context.Context) error {
		var err error
		job, err = h.client.CreateJob(ctx, &genjobs.CreateJobRequest{
			Type:           req.Type,
			PayloadJson:    string(req.Payload),
			Priority:       req.Priority,
			MaxAttempts:    req.MaxAttempts,
			ScheduledAt:    req.ScheduledAt,
			IdempotencyKey: bindIdempotencyKey(r.Header.Get("Idempotency-Key"), &req),
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, jobToJSON(job))
}

// bindIdempotencyKey derives the upstream idempotency key from the
// client-supplied key and a SHA-256 fingerprint of everything that defines
// the job (type, payload, priority, max_attempts, scheduled_at). Dedup
// semantics after binding: same key + same request → same upstream key →
// the jobs service returns the original job; same key + different request →
// a different upstream key → a NEW job instead of the wrong one. Without
// binding, the jobs service dedupes on the bare key and a replay with a
// swapped payload silently returns the original job (EDGE-03).
//
// The upstream key keeps the client key as a readable prefix and stays
// within the jobs service's 255-char contract; keys too long for
// "key:fingerprint" are fully hashed instead.
func bindIdempotencyKey(clientKey string, req *createJobRequest) string {
	key := strings.TrimSpace(clientKey)
	if key == "" {
		return ""
	}
	fp := sha256.Sum256([]byte(req.Type + "\x00" + string(req.Payload) + "\x00" +
		strconv.Itoa(int(req.Priority)) + "\x00" + strconv.Itoa(int(req.MaxAttempts)) +
		"\x00" + strconv.FormatInt(req.ScheduledAt, 10)))
	suffix := hex.EncodeToString(fp[:])[:16]
	if bound := key + ":" + suffix; len(bound) <= 255 {
		return bound
	}
	full := sha256.Sum256([]byte(key + "\x00" + suffix))
	return hex.EncodeToString(full[:])
}

// list handles GET /api/jobs?page=&page_size=&status=&type=.
func (h *jobsHandlers) list(w http.ResponseWriter, r *http.Request) {
	statusFilter, err := parseJobStatusFilter(r.URL.Query().Get("status"))
	if err != nil {
		writeError(w, r, err)
		return
	}

	var resp *genjobs.ListJobsResponse
	err = h.jobs.call(r.Context(), "ListJobs", true, func(ctx context.Context) error {
		var err error
		resp, err = h.client.ListJobs(ctx, &genjobs.ListJobsRequest{
			Page:         pageRequest(r),
			StatusFilter: statusFilter,
			TypeFilter:   r.URL.Query().Get("type"),
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	jobs := make([]jobJSON, 0, len(resp.GetJobs()))
	for _, j := range resp.GetJobs() {
		jobs = append(jobs, jobToJSON(j))
	}
	p := resp.GetPage()
	writeJSON(w, http.StatusOK, map[string]any{
		"jobs": jobs,
		"page": pageJSON{Page: p.GetPage(), PageSize: p.GetPageSize(), Total: p.GetTotal()},
	})
}

// get handles GET /api/jobs/{id}.
func (h *jobsHandlers) get(w http.ResponseWriter, r *http.Request) {
	var job *genjobs.Job
	err := h.jobs.call(r.Context(), "GetJob", true, func(ctx context.Context) error {
		var err error
		job, err = h.client.GetJob(ctx, &genjobs.GetJobRequest{Id: r.PathValue("id")})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, jobToJSON(job))
}

// cancel handles POST /api/jobs/{id}/cancel.
func (h *jobsHandlers) cancel(w http.ResponseWriter, r *http.Request) {
	var job *genjobs.Job
	err := h.jobs.call(r.Context(), "CancelJob", false, func(ctx context.Context) error {
		var err error
		job, err = h.client.CancelJob(ctx, &genjobs.CancelJobRequest{Id: r.PathValue("id")})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, jobToJSON(job))
}

// requeue handles POST /api/jobs/{id}/requeue (DLQ requeue, DEAD jobs only —
// the jobs service enforces the status rule).
func (h *jobsHandlers) requeue(w http.ResponseWriter, r *http.Request) {
	var job *genjobs.Job
	err := h.jobs.call(r.Context(), "RequeueJob", false, func(ctx context.Context) error {
		var err error
		job, err = h.client.RequeueJob(ctx, &genjobs.RequeueJobRequest{Id: r.PathValue("id")})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, jobToJSON(job))
}

// replay handles POST /api/jobs/{id}/replay: creates a NEW job copying
// type/payload/priority from the original. The jobs service enforces
// owner-scoping; the response carries replayed_from pointing at the source.
func (h *jobsHandlers) replay(w http.ResponseWriter, r *http.Request) {
	var job *genjobs.Job
	err := h.jobs.call(r.Context(), "ReplayJob", false, func(ctx context.Context) error {
		var err error
		job, err = h.client.ReplayJob(ctx, &genjobs.ReplayJobRequest{Id: r.PathValue("id")})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, jobToJSON(job))
}

// ---------------------------------------------------------------------------
// Webhook deliveries: GET /api/jobs/{id}/deliveries (migration 000005)
// ---------------------------------------------------------------------------

// deliveryJSON is the public wire shape of one webhook delivery attempt.
// StatusCode and LatencyMS are pointers: null means "no response came
// back" / "no request left the worker" (the database NULL), distinct from
// any real value.
type deliveryJSON struct {
	ID              int64  `json:"id"`
	JobID           string `json:"job_id"`
	Attempt         int32  `json:"attempt"`
	URL             string `json:"url"`
	StatusCode      *int32 `json:"status_code"`
	LatencyMS       *int32 `json:"latency_ms"`
	ResponseSnippet string `json:"response_snippet"`
	Blocked         bool   `json:"blocked"`
	Error           string `json:"error"`
	TS              int64  `json:"ts"`
}

func deliveryToJSON(d *genjobs.WebhookDelivery) deliveryJSON {
	return deliveryJSON{
		ID:              d.GetId(),
		JobID:           d.GetJobId(),
		Attempt:         d.GetAttempt(),
		URL:             d.GetUrl(),
		StatusCode:      d.StatusCode, // proto3-optional → null when absent
		LatencyMS:       d.LatencyMs,
		ResponseSnippet: d.GetResponseSnippet(),
		Blocked:         d.GetBlocked(),
		Error:           d.GetError(),
		TS:              d.GetTs(),
	}
}

// deliveries handles GET /api/jobs/{id}/deliveries?page=&page_size=. Owner
// scoping lives in the jobs service (foreign job = 404, no existence
// oracle), exactly like GET /api/jobs/{id}.
func (h *jobsHandlers) deliveries(w http.ResponseWriter, r *http.Request) {
	var resp *genjobs.ListDeliveriesResponse
	err := h.jobs.call(r.Context(), "ListDeliveries", true, func(ctx context.Context) error {
		var err error
		resp, err = h.deliveriesClient.ListDeliveries(ctx, &genjobs.ListDeliveriesRequest{
			JobId: r.PathValue("id"),
			Page:  pageRequest(r),
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	deliveries := make([]deliveryJSON, 0, len(resp.GetDeliveries()))
	for _, d := range resp.GetDeliveries() {
		deliveries = append(deliveries, deliveryToJSON(d))
	}
	p := resp.GetPage()
	writeJSON(w, http.StatusOK, map[string]any{
		"deliveries": deliveries,
		"page":       pageJSON{Page: p.GetPage(), PageSize: p.GetPageSize(), Total: p.GetTotal()},
	})
}

// ---------------------------------------------------------------------------
// Cron schedules: POST /api/crons, GET /api/crons, DELETE /api/crons/{id}
// ---------------------------------------------------------------------------

// cronJSON is the public wire shape of a cron schedule.
type cronJSON struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	CronExpr  string          `json:"cron_expr"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Priority  int32           `json:"priority"`
	Enabled   bool            `json:"enabled"`
	NextRunAt int64           `json:"next_run_at"`
	LastRunAt int64           `json:"last_run_at"`
	CreatedAt int64           `json:"created_at"`
}

func cronToJSON(c *genjobs.CronSchedule) cronJSON {
	payload := json.RawMessage(c.GetPayloadJson())
	if len(payload) == 0 || !json.Valid(payload) {
		payload, _ = json.Marshal(c.GetPayloadJson())
	}
	return cronJSON{
		ID:        c.GetId(),
		Name:      c.GetName(),
		CronExpr:  c.GetCronExpr(),
		Type:      c.GetType(),
		Payload:   payload,
		Priority:  c.GetPriority(),
		Enabled:   c.GetEnabled(),
		NextRunAt: c.GetNextRunAt(),
		LastRunAt: c.GetLastRunAt(),
		CreatedAt: c.GetCreatedAt(),
	}
}

// createCronRequest is the body of POST /api/crons. Enabled is a pointer so
// an absent field defaults to true while an explicit false stays false.
type createCronRequest struct {
	Name     string          `json:"name"`
	CronExpr string          `json:"cron_expr"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int32           `json:"priority"`
	Enabled  *bool           `json:"enabled"`
}

// createCron handles POST /api/crons.
func (h *jobsHandlers) createCron(w http.ResponseWriter, r *http.Request) {
	var req createCronRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeError(w, r, errors.E(errors.KindInvalid, "cron_name_required",
			"name is required", nil))
		return
	}
	if strings.TrimSpace(req.CronExpr) == "" {
		writeError(w, r, errors.E(errors.KindInvalid, "cron_expr_required",
			"cron_expr is required", nil))
		return
	}
	if strings.TrimSpace(req.Type) == "" {
		writeError(w, r, errors.E(errors.KindInvalid, "type_required",
			"job type is required", nil))
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	var cron *genjobs.CronSchedule
	err := h.jobs.call(r.Context(), "CreateCron", false, func(ctx context.Context) error {
		var err error
		cron, err = h.client.CreateCron(ctx, &genjobs.CreateCronRequest{
			Name:        req.Name,
			CronExpr:    req.CronExpr,
			Type:        req.Type,
			PayloadJson: string(req.Payload),
			Priority:    req.Priority,
			Enabled:     enabled,
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, cronToJSON(cron))
}

// listCrons handles GET /api/crons?page=&page_size=.
func (h *jobsHandlers) listCrons(w http.ResponseWriter, r *http.Request) {
	var resp *genjobs.ListCronsResponse
	err := h.jobs.call(r.Context(), "ListCrons", true, func(ctx context.Context) error {
		var err error
		resp, err = h.client.ListCrons(ctx, &genjobs.ListCronsRequest{Page: pageRequest(r)})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	crons := make([]cronJSON, 0, len(resp.GetCrons()))
	for _, c := range resp.GetCrons() {
		crons = append(crons, cronToJSON(c))
	}
	p := resp.GetPage()
	writeJSON(w, http.StatusOK, map[string]any{
		"crons": crons,
		"page":  pageJSON{Page: p.GetPage(), PageSize: p.GetPageSize(), Total: p.GetTotal()},
	})
}

// deleteCron handles DELETE /api/crons/{id}.
func (h *jobsHandlers) deleteCron(w http.ResponseWriter, r *http.Request) {
	err := h.jobs.call(r.Context(), "DeleteCron", false, func(ctx context.Context) error {
		_, err := h.client.DeleteCron(ctx, &genjobs.DeleteCronRequest{Id: r.PathValue("id")})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// workers handles GET /api/workers by scanning the worker registry in Redis:
// keys worker:<id> are hashes with a 15 s TTL, so whatever SCAN finds is a
// live worker (modulo the scan/fetch race, which we skip silently).
func (h *jobsHandlers) workers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	workers := make([]map[string]string, 0)
	var cursor uint64
	for {
		keys, next, err := h.rdb.Scan(ctx, cursor, "worker:*", 100).Result()
		if err != nil {
			writeError(w, r, errors.E(errors.KindUnavailable,
				"worker_registry_unavailable", "could not read the worker registry", err))
			return
		}
		for _, key := range keys {
			fields, err := h.rdb.HGetAll(ctx, key).Result()
			if err != nil || len(fields) == 0 {
				continue // key expired between SCAN and HGETALL
			}
			workers = append(workers, fields)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}
