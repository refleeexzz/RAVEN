package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// Server implements genjobs.JobServiceServer.
type Server struct {
	genjobs.UnimplementedJobServiceServer

	pool     *pgxpool.Pool
	rdb      redis.UniversalClient // may be nil in unit tests
	producer *Producer             // may be nil in unit tests
	log      *slog.Logger
	metrics  *ServiceMetrics // may be nil in unit tests

	// sched is the probed scheduling support (migration 000004). When off,
	// the service talks to the pre-000004 schema exactly like the old
	// binary did, and scheduling endpoints answer Unavailable. Probed once
	// at construction; unit tests may flip it directly.
	sched bool
}

// NewServer wires the gRPC service implementation. When pool is non-nil the
// scheduling schema support is probed once (3s budget); a failed probe is
// logged and treated as "unsupported", which keeps the pre-000004 behavior.
func NewServer(pool *pgxpool.Pool, rdb redis.UniversalClient, producer *Producer, log *slog.Logger, m *ServiceMetrics) *Server {
	s := &Server{pool: pool, rdb: rdb, producer: producer, log: log, metrics: m}
	if pool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		sched, err := probeScheduling(ctx, pool)
		cancel()
		if err != nil {
			log.Warn("scheduling schema probe failed, scheduling features disabled",
				slog.Any("error", err))
		} else {
			s.sched = sched
			if !sched {
				log.Info("migration 000004 not applied; delayed jobs, cron and replay are disabled")
			}
		}
	}
	return s
}

// ---------------------------------------------------------------------------
// CreateJob
// ---------------------------------------------------------------------------

func (s *Server) CreateJob(ctx context.Context, req *genjobs.CreateJobRequest) (*genjobs.Job, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	priority, maxAttempts, err := ValidateCreate(req.GetType(), req.GetPayloadJson(),
		req.GetPriority(), req.GetMaxAttempts())
	if err != nil {
		return nil, toStatus(err)
	}
	idemKey, err := normalizeIdempotencyKey(req.GetIdempotencyKey())
	if err != nil {
		return nil, toStatus(err)
	}
	scheduledAt, err := ValidateScheduledAt(req.GetScheduledAt(), time.Now().UTC())
	if err != nil {
		return nil, toStatus(err)
	}
	if scheduledAt != nil && !s.sched {
		return nil, toStatus(errSchedulingUnavailable())
	}

	j := &Job{
		ID:          NewID(),
		Type:        req.GetType(),
		Payload:     req.GetPayloadJson(),
		Status:      StatusQueued,
		Priority:    priority,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now().UTC(),
		// Matches the execution_generation column default; the broker message
		// must carry the real token, not the struct's zero value.
		ExecutionGeneration: initialGeneration,
	}
	if scheduledAt != nil {
		// Delayed job: park it in SCHEDULED. Nothing is published; the
		// dispatcher loop releases it when the time comes.
		j.Status = StatusScheduled
		j.ScheduledAt = scheduledAt
	}
	if idemKey != "" {
		j.IdempotencyKey = &idemKey
	}
	if !caller.System {
		// Owner-scoped by default (JOBS-02): the authenticated caller owns the
		// job. System callers (no identity) create ownerless jobs.
		j.OwnerID = &caller.UserID
	}

	// Idempotency fast path: claim the key in Redis first. If it is taken,
	// return the existing job — same key, same job, no error.
	if idemKey != "" && s.rdb != nil {
		existing, claimed, err := claimIdempotencyKey(ctx, s.rdb, idemKey, j.ID)
		if err != nil {
			// Redis down: fall through to the database unique index.
			s.log.WarnContext(ctx, "idempotency claim failed, relying on database",
				slog.Any("error", err))
		} else if !claimed {
			existingJob, getErr := getJob(ctx, s.pool, existing, s.sched)
			if getErr != nil {
				if errors.KindOf(getErr) != errors.KindNotFound {
					return nil, toStatus(getErr)
				}
				// Stale claim (previous create failed after claiming). Drop it
				// and create fresh.
				s.log.WarnContext(ctx, "idempotency key pointed at a missing job, reclaiming",
					slog.String("key", idemKey))
				releaseIdempotencyKey(ctx, s.rdb, s.log, idemKey)
			} else if !caller.CanAccessJob(existingJob.OwnerID) {
				// The key belongs to another owner's job (JOBS-02): say the
				// key is taken, never hand back a stranger's job.
				return nil, toStatus(errors.E(errors.KindConflict, "idempotency_key_taken",
					"a job with this idempotency key already exists", nil))
			} else {
				s.log.InfoContext(ctx, "idempotent create, returning existing job",
					slog.String("job_id", existingJob.ID))
				return existingJob.toProto(), nil
			}
		}
	}

	if err := insertJob(ctx, s.pool, j, s.sched); err != nil {
		if errors.KindOf(err) == errors.KindConflict && idemKey != "" {
			// Database backstop: the key is taken even though Redis let us
			// through (TTL expired, eviction, Redis was down). Return the
			// existing job instead of an error.
			existingJob, getErr := jobByIdempotencyKey(ctx, s.pool, idemKey, s.sched)
			if getErr == nil {
				if !caller.CanAccessJob(existingJob.OwnerID) {
					// Foreign key (JOBS-02): conflict, not the other user's job.
					return nil, toStatus(errors.E(errors.KindConflict, "idempotency_key_taken",
						"a job with this idempotency key already exists", nil))
				}
				return existingJob.toProto(), nil
			}
		}
		if idemKey != "" && s.rdb != nil {
			releaseIdempotencyKey(ctx, s.rdb, s.log, idemKey)
		}
		return nil, toStatus(err)
	}
	s.metricCreated(j)

	// A scheduled job is done here: it sits in SCHEDULED until the
	// dispatcher releases it at scheduled_at. No broker traffic yet.
	if j.Status == StatusScheduled {
		PublishJobEvent(ctx, s.rdb, s.log, j)
		s.metricTransition(j, StatusScheduled)
		s.log.InfoContext(ctx, "job scheduled",
			slog.String("job_id", j.ID), slog.String("type", j.Type),
			slog.Time("scheduled_at", *j.ScheduledAt))
		return j.toProto(), nil
	}

	// Publish the work. Failure here must not lose the job silently: mark the
	// row FAILED so the user sees what happened, and report Unavailable.
	if s.producer == nil {
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_unavailable",
			"the job broker is not configured", nil))
	}
	if err := s.producer.PublishExecution(ctx, j); err != nil {
		errMsg := "broker produce failed: " + err.Error()
		if uerr := markQueuedFailed(ctx, s.pool, j.ID, errMsg); uerr != nil {
			s.log.ErrorContext(ctx, "could not mark job failed after produce error",
				slog.String("job_id", j.ID), slog.Any("error", uerr))
		}
		if idemKey != "" && s.rdb != nil {
			releaseIdempotencyKey(ctx, s.rdb, s.log, idemKey)
		}
		j.Status = StatusFailed
		j.Error = errMsg
		PublishJobEvent(ctx, s.rdb, s.log, j)
		s.metricTransition(j, StatusFailed)
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_produce_failed",
			"could not queue the job, it was marked FAILED", err))
	}

	PublishJobEvent(ctx, s.rdb, s.log, j)
	s.metricTransition(j, StatusQueued)
	s.log.InfoContext(ctx, "job created",
		slog.String("job_id", j.ID), slog.String("type", j.Type))
	return j.toProto(), nil
}

// ---------------------------------------------------------------------------
// GetJob / ListJobs
// ---------------------------------------------------------------------------

func (s *Server) GetJob(ctx context.Context, req *genjobs.GetJobRequest) (*genjobs.Job, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "job_id_required",
			"id is required", nil))
	}
	j, err := getJob(ctx, s.pool, req.GetId(), s.sched)
	if err != nil {
		return nil, toStatus(err)
	}
	if err := caller.AuthorizeJob(j); err != nil {
		return nil, toStatus(err)
	}
	return j.toProto(), nil
}

func (s *Server) ListJobs(ctx context.Context, req *genjobs.ListJobsRequest) (*genjobs.ListJobsResponse, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	page, size := NormalizePage(req.GetPage())

	statusFilter := ""
	if req.GetStatusFilter() != genjobs.JobStatus_JOB_STATUS_UNSPECIFIED {
		st, ok := protoToStatus[req.GetStatusFilter()]
		if !ok {
			return nil, toStatus(errors.E(errors.KindInvalid, "status_filter_invalid",
				"unknown status filter", nil))
		}
		statusFilter = string(st)
	}

	jobs, total, err := listJobs(ctx, s.pool, statusFilter, req.GetTypeFilter(),
		caller.OwnerScope(), int(size), pageOffset(page, size), s.sched)
	if err != nil {
		return nil, toStatus(err)
	}

	out := &genjobs.ListJobsResponse{
		Jobs: make([]*genjobs.Job, 0, len(jobs)),
		Page: &gencommon.PageResponse{Page: page, PageSize: size, Total: total},
	}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, j.toProto())
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// CancelJob
// ---------------------------------------------------------------------------

func (s *Server) CancelJob(ctx context.Context, req *genjobs.CancelJobRequest) (*genjobs.Job, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "job_id_required",
			"id is required", nil))
	}
	// Authorize before the atomic transition. owner_id is immutable after
	// insert, so the read-then-update cannot be raced into another owner's
	// job.
	cur, err := getJob(ctx, s.pool, req.GetId(), s.sched)
	if err != nil {
		return nil, toStatus(err)
	}
	if err := caller.AuthorizeJob(cur); err != nil {
		return nil, toStatus(err)
	}
	j, err := cancelJob(ctx, s.pool, req.GetId(), s.sched)
	if err != nil {
		return nil, toStatus(err)
	}

	// Workers re-check status before executing, so a cancelled job that is
	// still sitting on the broker gets skipped by the fence. No broker
	// traffic needed here.
	PublishJobEvent(ctx, s.rdb, s.log, j)
	s.metricTransition(j, StatusCancelled)
	s.log.InfoContext(ctx, "job cancelled", slog.String("job_id", j.ID))
	return j.toProto(), nil
}

// ---------------------------------------------------------------------------
// RequeueJob (DEAD -> QUEUED)
// ---------------------------------------------------------------------------

func (s *Server) RequeueJob(ctx context.Context, req *genjobs.RequeueJobRequest) (*genjobs.Job, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "job_id_required",
			"id is required", nil))
	}
	cur, err := getJob(ctx, s.pool, req.GetId(), s.sched)
	if err != nil {
		return nil, toStatus(err)
	}
	if err := caller.AuthorizeJob(cur); err != nil {
		return nil, toStatus(err)
	}
	j, err := requeueJob(ctx, s.pool, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}

	if s.producer == nil {
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_unavailable",
			"the job broker is not configured", nil))
	}
	if err := s.producer.PublishExecution(ctx, j); err != nil {
		// Best-effort revert so the job does not sit QUEUED with no message
		// on the broker.
		if _, rerr := requeueRevert(ctx, s.pool, j.ID); rerr != nil {
			s.log.ErrorContext(ctx, "requeue revert failed after produce error",
				slog.String("job_id", j.ID), slog.Any("error", rerr))
		}
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_produce_failed",
			"could not requeue the job, it was left DEAD", err))
	}

	PublishJobEvent(ctx, s.rdb, s.log, j)
	s.metricTransition(j, StatusQueued)
	s.log.InfoContext(ctx, "job requeued", slog.String("job_id", j.ID))
	return j.toProto(), nil
}

// requeueRevert puts a job back to DEAD when the requeue produce failed.
func requeueRevert(ctx context.Context, q querier, id string) (*Job, error) {
	return scanJob(q.QueryRow(ctx, `
		UPDATE jobs SET status = $2
		WHERE id = $1 AND status = 'QUEUED'
		RETURNING `+jobColumns, id, StatusDead))
}

// ---------------------------------------------------------------------------
// JobDeliveriesService: gRPC adapter over the plain-Go delivery read path
// ---------------------------------------------------------------------------

// DeliveriesServer serves raven.jobs.v1.JobDeliveriesService. It is a thin
// adapter, not new logic: Server.ListDeliveries (deliveries.go) predates
// the rpc and intentionally stays plain-Go for direct internal callers —
// Go forbids two same-named methods with different signatures on one type,
// so the rpc lives on its own service and this wrapper forwards to the
// exact owner-scoped logic (authz included) unchanged.
type DeliveriesServer struct {
	genjobs.UnimplementedJobDeliveriesServiceServer
	srv *Server
}

// NewDeliveriesServer wraps srv in the gRPC adapter.
func NewDeliveriesServer(srv *Server) *DeliveriesServer {
	return &DeliveriesServer{srv: srv}
}

func (s *DeliveriesServer) ListDeliveries(ctx context.Context, req *genjobs.ListDeliveriesRequest) (*genjobs.ListDeliveriesResponse, error) {
	page, size := NormalizePage(req.GetPage())
	deliveries, total, err := s.srv.ListDeliveries(ctx, req.GetJobId(), page, size)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &genjobs.ListDeliveriesResponse{
		Deliveries: make([]*genjobs.WebhookDelivery, 0, len(deliveries)),
		Page:       &gencommon.PageResponse{Page: page, PageSize: size, Total: total},
	}
	for _, d := range deliveries {
		out.Deliveries = append(out.Deliveries, deliveryToProto(d))
	}
	return out, nil
}

// deliveryToProto maps the store row onto the wire shape. The nullable
// columns pass straight through as proto3-optional fields: absent means
// "no response came back" / "no request left the worker", preserving the
// database NULL exactly (a real 0 ms loopback round trip stays a real 0).
func deliveryToProto(d *WebhookDelivery) *genjobs.WebhookDelivery {
	return &genjobs.WebhookDelivery{
		Id:              d.ID,
		JobId:           d.JobID,
		Attempt:         int32(d.Attempt),
		Url:             d.URL,
		StatusCode:      d.StatusCode,
		LatencyMs:       d.LatencyMS,
		ResponseSnippet: d.ResponseSnippet,
		Blocked:         d.Blocked,
		Error:           d.Error,
		Ts:              d.Ts.Unix(),
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// NormalizePage applies the platform paging rules: 1-based page, page size
// defaults to 20 and caps at 100 (see internal/gen/common). The page number
// is additionally capped at MaxListPage: deep OFFSET scans are a database
// DoS vector, and (page-1)*size must never overflow int32 (JOBS-04).
func NormalizePage(p *gencommon.PageRequest) (page, size int32) {
	page, size = 1, defaultPageSize
	if p == nil {
		return page, size
	}
	if p.GetPage() > 0 {
		page = p.GetPage()
	}
	if page > MaxListPage {
		page = MaxListPage
	}
	if p.GetPageSize() > 0 {
		size = p.GetPageSize()
	}
	if size > maxPageSize {
		size = maxPageSize
	}
	return page, size
}

// MaxListPage is the deepest page ListJobs will serve (JOBS-04).
const MaxListPage = 1_000_000

// pageOffset computes the SQL OFFSET in 64-bit. With page <= MaxListPage and
// size <= maxPageSize the result always fits the int the store receives.
func pageOffset(page, size int32) int {
	return int((int64(page) - 1) * int64(size))
}

func (s *Server) metricCreated(j *Job) {
	if s.metrics != nil {
		s.metrics.created.Inc()
	}
}

func (s *Server) metricTransition(j *Job, to Status) {
	if s.metrics != nil {
		s.metrics.observeTransition(j.Type, to)
	}
}
