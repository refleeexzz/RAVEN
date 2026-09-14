package jobs

import (
	"context"
	"log/slog"
	"time"

	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// replay_job.go implements ReplayJob. It needs migration 000004
// (jobs.replayed_from) — on an older schema it answers Unavailable (see
// Server.sched).

// ReplayJob creates a NEW job copying type/payload/priority (and
// max_attempts) from an existing one: fresh id, zero attempts, no schedule,
// no idempotency key. replayed_from points back at the source for audit.
// Owner-scoped exactly like GetJob: replaying a foreign job is NotFound.
func (s *Server) ReplayJob(ctx context.Context, req *genjobs.ReplayJobRequest) (*genjobs.Job, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	if !s.sched {
		return nil, toStatus(errSchedulingUnavailable())
	}
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "job_id_required",
			"id is required", nil))
	}
	src, err := getJob(ctx, s.pool, req.GetId(), s.sched)
	if err != nil {
		return nil, toStatus(err)
	}
	if err := caller.AuthorizeJob(src); err != nil {
		return nil, toStatus(err)
	}

	now := time.Now().UTC()
	j := &Job{
		ID:                  NewID(),
		Type:                src.Type,
		Payload:             src.Payload,
		Status:              StatusQueued,
		Priority:            src.Priority,
		MaxAttempts:         src.MaxAttempts,
		OwnerID:             src.OwnerID,
		CreatedAt:           now,
		ExecutionGeneration: initialGeneration,
		ReplayedFrom:        &src.ID,
	}
	if err := insertJob(ctx, s.pool, j, s.sched); err != nil {
		return nil, toStatus(err)
	}
	s.metricCreated(j)

	if s.producer == nil {
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_unavailable",
			"the job broker is not configured", nil))
	}
	if err := s.producer.PublishExecution(ctx, j); err != nil {
		errMsg := "broker produce failed: " + err.Error()
		if uerr := markQueuedFailed(ctx, s.pool, j.ID, errMsg); uerr != nil {
			s.log.ErrorContext(ctx, "could not mark replayed job failed after produce error",
				slog.String("job_id", j.ID), slog.Any("error", uerr))
		}
		j.Status = StatusFailed
		j.Error = errMsg
		PublishJobEvent(ctx, s.rdb, s.log, j)
		s.metricTransition(j, StatusFailed)
		return nil, toStatus(errors.E(errors.KindUnavailable, "broker_produce_failed",
			"could not queue the replayed job, it was marked FAILED", err))
	}

	PublishJobEvent(ctx, s.rdb, s.log, j)
	s.metricTransition(j, StatusQueued)
	s.log.InfoContext(ctx, "job replayed",
		slog.String("job_id", j.ID), slog.String("replayed_from", src.ID),
		slog.String("type", j.Type))
	return j.toProto(), nil
}
