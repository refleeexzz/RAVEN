package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/metadata"

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
}

// NewServer wires the gRPC service implementation.
func NewServer(pool *pgxpool.Pool, rdb redis.UniversalClient, producer *Producer, log *slog.Logger, m *ServiceMetrics) *Server {
	return &Server{pool: pool, rdb: rdb, producer: producer, log: log, metrics: m}
}

// ---------------------------------------------------------------------------
// CreateJob
// ---------------------------------------------------------------------------

func (s *Server) CreateJob(ctx context.Context, req *genjobs.CreateJobRequest) (*genjobs.Job, error) {
	priority, maxAttempts, err := validateCreate(req.GetType(), req.GetPayloadJson(),
		req.GetPriority(), req.GetMaxAttempts())
	if err != nil {
		return nil, toStatus(err)
	}
	idemKey, err := normalizeIdempotencyKey(req.GetIdempotencyKey())
	if err != nil {
		return nil, toStatus(err)
	}

	j := &Job{
		ID:          NewID(),
		Type:        req.GetType(),
		Payload:     req.GetPayloadJson(),
		Status:      StatusQueued,
		Priority:    priority,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now().UTC(),
	}
	if idemKey != "" {
		j.IdempotencyKey = &idemKey
	}
	if owner := ownerFromMetadata(ctx); owner != "" {
		j.OwnerID = &owner
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
			existingJob, getErr := getJob(ctx, s.pool, existing)
			if getErr != nil {
				if errors.KindOf(getErr) != errors.KindNotFound {
					return nil, toStatus(getErr)
				}
				// Stale claim (previous create failed after claiming). Drop it
				// and create fresh.
				s.log.WarnContext(ctx, "idempotency key pointed at a missing job, reclaiming",
					slog.String("key", idemKey))
				releaseIdempotencyKey(ctx, s.rdb, s.log, idemKey)
			} else {
				s.log.InfoContext(ctx, "idempotent create, returning existing job",
					slog.String("job_id", existingJob.ID))
				return existingJob.toProto(), nil
			}
		}
	}

	if err := insertJob(ctx, s.pool, j); err != nil {
		if errors.KindOf(err) == errors.KindConflict && idemKey != "" {
			// Database backstop: the key is taken even though Redis let us
			// through (TTL expired, eviction, Redis was down). Return the
			// existing job instead of an error.
			existingJob, getErr := jobByIdempotencyKey(ctx, s.pool, idemKey)
			if getErr == nil {
				return existingJob.toProto(), nil
			}
		}
		if idemKey != "" && s.rdb != nil {
			releaseIdempotencyKey(ctx, s.rdb, s.log, idemKey)
		}
		return nil, toStatus(err)
	}
	s.metricCreated(j)

	// Publish the work. Failure here must not lose the job silently: mark the
	// row FAILED so the user sees what happened, and report Unavailable.
	if s.producer == nil {
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_unavailable",
			"the job broker is not configured", nil))
	}
	if err := s.producer.PublishJob(ctx, TopicJobs, j); err != nil {
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
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "job_id_required",
			"id is required", nil))
	}
	j, err := getJob(ctx, s.pool, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return j.toProto(), nil
}

func (s *Server) ListJobs(ctx context.Context, req *genjobs.ListJobsRequest) (*genjobs.ListJobsResponse, error) {
	page, size := normalizePage(req.GetPage())

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
		int(size), int((page-1)*size))
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
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "job_id_required",
			"id is required", nil))
	}
	j, err := cancelJob(ctx, s.pool, req.GetId())
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
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "job_id_required",
			"id is required", nil))
	}
	j, err := requeueJob(ctx, s.pool, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}

	if s.producer == nil {
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_unavailable",
			"the job broker is not configured", nil))
	}
	if err := s.producer.PublishJob(ctx, TopicJobs, j); err != nil {
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
// helpers
// ---------------------------------------------------------------------------

// normalizePage applies the platform paging rules: 1-based page, page size
// defaults to 20 and caps at 100 (see internal/gen/common).
func normalizePage(p *gencommon.PageRequest) (page, size int32) {
	page, size = 1, defaultPageSize
	if p == nil {
		return page, size
	}
	if p.GetPage() > 0 {
		page = p.GetPage()
	}
	if p.GetPageSize() > 0 {
		size = p.GetPageSize()
	}
	if size > maxPageSize {
		size = maxPageSize
	}
	return page, size
}

// ownerFromMetadata picks up the caller identity the gateway forwards as
// x-user-id. Unknown/invalid values are ignored (owner stays NULL): the jobs
// API trusts the gateway to have authenticated the caller already.
func ownerFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("x-user-id")
	if len(vals) == 0 {
		return ""
	}
	id, err := uuid.Parse(vals[0])
	if err != nil {
		return ""
	}
	return id.String()
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
