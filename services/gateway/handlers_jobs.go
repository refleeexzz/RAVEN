// handlers_jobs.go translates the jobs REST endpoints into jobs.JobService
// gRPC calls, and serves the live worker registry straight from Redis
// (SCAN worker:* + HGETALL, per docs/contracts/ports-and-env.md).
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/redis/go-redis/v9"

	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// jobsHandlers serves /api/jobs/* and /api/workers.
type jobsHandlers struct {
	jobs   *upstream
	client genjobs.JobServiceClient
	rdb    redis.UniversalClient
}

func newJobsHandlers(jobs *upstream, rdb redis.UniversalClient) *jobsHandlers {
	return &jobsHandlers{
		jobs:   jobs,
		client: genjobs.NewJobServiceClient(jobs.conn),
		rdb:    rdb,
	}
}

// jobJSON is the public wire shape of a job (field list fixed by contract).
type jobJSON struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Priority    int32           `json:"priority"`
	Attempts    int32           `json:"attempts"`
	MaxAttempts int32           `json:"max_attempts"`
	CreatedAt   int64           `json:"created_at"`
	StartedAt   int64           `json:"started_at"`
	FinishedAt  int64           `json:"finished_at"`
	Error       string          `json:"error"`
	WorkerID    string          `json:"worker_id"`
}

func jobToJSON(j *genjobs.Job) jobJSON {
	// payload_json should be a JSON document; if a buggy producer stored
	// something else, degrade to a JSON string instead of breaking the API.
	payload := json.RawMessage(j.GetPayloadJson())
	if len(payload) == 0 || !json.Valid(payload) {
		payload, _ = json.Marshal(j.GetPayloadJson())
	}
	return jobJSON{
		ID:          j.GetId(),
		Type:        j.GetType(),
		Payload:     payload,
		Status:      jobStatusName(j.GetStatus()),
		Priority:    j.GetPriority(),
		Attempts:    j.GetAttempts(),
		MaxAttempts: j.GetMaxAttempts(),
		CreatedAt:   j.GetCreatedAt(),
		StartedAt:   j.GetStartedAt(),
		FinishedAt:  j.GetFinishedAt(),
		Error:       j.GetError(),
		WorkerID:    j.GetWorkerId(),
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
}

// create handles POST /api/jobs. The Idempotency-Key header is forwarded
// into CreateJobRequest.idempotency_key so retries collapse to one job.
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
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, jobToJSON(job))
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
