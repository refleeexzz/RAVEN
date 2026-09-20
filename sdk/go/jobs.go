package raven

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// CreateJobRequest is the body of POST /api/jobs. Type must be one of
// send_email, resize_image, webhook. Payload must marshal to a JSON object
// with the fields the type requires (webhook needs "url", send_email needs
// "to"). Priority 1–9 (1 = most urgent, default 5), MaxAttempts 1–25
// (default 4). ScheduledAt is an optional Unix-seconds time in the future;
// zero runs the job immediately.
type CreateJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority,omitempty"`
	MaxAttempts int             `json:"max_attempts,omitempty"`
	ScheduledAt int64           `json:"scheduled_at,omitempty"`
}

// NewJobPayload marshals v into the RawMessage CreateJobRequest wants.
// v must produce a JSON object.
func NewJobPayload(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("raven: marshal payload: %w", err)
	}
	return json.RawMessage(b), nil
}

// CreateJob queues a job. An Idempotency-Key is generated automatically;
// pass WithIdempotencyKey to pin it when retrying after a timeout.
func (c *Client) CreateJob(ctx context.Context, req CreateJobRequest, opts ...requestOption) (*Job, error) {
	var out Job
	if err := c.do(ctx, http.MethodPost, "/api/jobs", req, &out, true, opts...); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListJobsFilter narrows ListJobs. Status accepts "QUEUED", "queued" or
// "JOB_STATUS_QUEUED" (one of QUEUED PROCESSING SUCCESS FAILED RETRYING
// CANCELLED DEAD SCHEDULED). Type is an exact match.
type ListJobsFilter struct {
	Status   string
	Type     string
	Page     int
	PageSize int
}

// ListJobs pages through jobs, newest-priority first.
func (c *Client) ListJobs(ctx context.Context, f ListJobsFilter) (*JobList, error) {
	q := url.Values{}
	if f.Status != "" {
		q.Set("status", f.Status)
	}
	if f.Type != "" {
		q.Set("type", f.Type)
	}
	if f.Page > 0 {
		q.Set("page", strconv.Itoa(f.Page))
	}
	if f.PageSize > 0 {
		q.Set("page_size", strconv.Itoa(f.PageSize))
	}
	var out JobList
	if err := c.do(ctx, http.MethodGet, "/api/jobs", nil, &out, false,
		func(rc *requestConfig) { rc.query = q }); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetJob fetches one job by id.
func (c *Client) GetJob(ctx context.Context, id string) (*Job, error) {
	var out Job
	if err := c.do(ctx, http.MethodGet, "/api/jobs/"+url.PathEscape(id), nil, &out, false); err != nil {
		return nil, err
	}
	return &out, nil
}

// CancelJob moves a QUEUED/RETRYING/SCHEDULED job to CANCELLED. Jobs that
// already moved on answer 409 job_not_cancellable.
func (c *Client) CancelJob(ctx context.Context, id string, opts ...requestOption) (*Job, error) {
	var out Job
	if err := c.do(ctx, http.MethodPost, "/api/jobs/"+url.PathEscape(id)+"/cancel", nil, &out, true, opts...); err != nil {
		return nil, err
	}
	return &out, nil
}

// RequeueJob resurrects a DEAD job to QUEUED and republishes it. Any other
// status answers 409 job_not_dead.
func (c *Client) RequeueJob(ctx context.Context, id string, opts ...requestOption) (*Job, error) {
	var out Job
	if err := c.do(ctx, http.MethodPost, "/api/jobs/"+url.PathEscape(id)+"/requeue", nil, &out, true, opts...); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReplayJob clones a job into a brand-new one (fresh id, zero attempts,
// no schedule, ReplayedFrom pointing at the source). Works on any state.
func (c *Client) ReplayJob(ctx context.Context, id string, opts ...requestOption) (*Job, error) {
	var out Job
	if err := c.do(ctx, http.MethodPost, "/api/jobs/"+url.PathEscape(id)+"/replay", nil, &out, true, opts...); err != nil {
		return nil, err
	}
	return &out, nil
}

// JobDeliveries returns the webhook delivery history of a job, oldest
// first.
func (c *Client) JobDeliveries(ctx context.Context, id string, page, pageSize int) (*DeliveryList, error) {
	q := url.Values{}
	if page > 0 {
		q.Set("page", strconv.Itoa(page))
	}
	if pageSize > 0 {
		q.Set("page_size", strconv.Itoa(pageSize))
	}
	var out DeliveryList
	if err := c.do(ctx, http.MethodGet, "/api/jobs/"+url.PathEscape(id)+"/deliveries", nil, &out, false,
		func(rc *requestConfig) { rc.query = q }); err != nil {
		return nil, err
	}
	return &out, nil
}

// JobWatch streams status snapshots of one job until it reaches a terminal
// state (Job.Terminal) or ctx is cancelled. The channel is closed on
// termination; if the watch ended because of an error, that error is the
// return value of the goroutine — read it from the returned channel's
// companion. Prefer WatchJob for the simple blocking form.
//
// The channel is buffered by 1 so the poller never blocks between ticks.
func (c *Client) JobWatch(ctx context.Context, id string, interval time.Duration) (<-chan Job, <-chan error) {
	jobs := make(chan Job, 1)
	errs := make(chan error, 1)
	if interval <= 0 {
		interval = 2 * time.Second
	}
	go func() {
		defer close(jobs)
		defer close(errs)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		lastStatus := ""
		for {
			job, err := c.GetJob(ctx, id)
			if err != nil {
				errs <- err
				return
			}
			if job.Status != lastStatus || job.Terminal() {
				jobs <- *job
				lastStatus = job.Status
			}
			if job.Terminal() {
				return
			}
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			case <-ticker.C:
			}
		}
	}()
	return jobs, errs
}

// WatchJob polls the job every interval until it reaches a terminal state
// and returns the final snapshot. It is the blocking convenience wrapper
// around JobWatch. A zero interval defaults to 2 s.
func (c *Client) WatchJob(ctx context.Context, id string, interval time.Duration) (*Job, error) {
	jobs, errs := c.JobWatch(ctx, id, interval)
	var last *Job
	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				jobs = nil
			} else {
				j := job
				last = &j
			}
		case err, ok := <-errs:
			if ok && err != nil {
				return last, err
			}
			errs = nil
		}
		if jobs == nil && errs == nil {
			if last == nil {
				return nil, &Error{Code: "watch_ended", Message: "watch ended without a job snapshot"}
			}
			return last, nil
		}
	}
}
