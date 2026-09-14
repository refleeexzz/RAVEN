package jobs

import (
	"context"
	"log/slog"
	"time"

	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// sched_server.go implements the cron RPCs. Everything here needs migration
// 000004 — on an older schema each RPC answers Unavailable (see
// Server.sched).

// ---------------------------------------------------------------------------
// Proto mapping
// ---------------------------------------------------------------------------

// toProto converts a cron schedule to the wire type.
func (c *Cron) toProto() *genjobs.CronSchedule {
	return &genjobs.CronSchedule{
		Id:          c.ID,
		Name:        c.Name,
		CronExpr:    c.Expr,
		Type:        c.Type,
		PayloadJson: c.Payload,
		Priority:    int32(c.Priority),
		Enabled:     c.Enabled,
		NextRunAt:   c.NextRunAt.Unix(),
		LastRunAt:   unixOrZero(c.LastRunAt),
		CreatedAt:   c.CreatedAt.Unix(),
	}
}

// ---------------------------------------------------------------------------
// CreateCron / ListCrons / DeleteCron
// ---------------------------------------------------------------------------

// CreateCron stores a recurring schedule. The first next_run_at is computed
// at create time from the caller's clock (UTC); the cron scheduler takes it
// from there.
func (s *Server) CreateCron(ctx context.Context, req *genjobs.CreateCronRequest) (*genjobs.CronSchedule, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	if !s.sched {
		return nil, toStatus(errSchedulingUnavailable())
	}
	now := time.Now().UTC()
	sched, priority, err := validateCronRequest(req.GetName(), req.GetCronExpr(),
		req.GetType(), req.GetPayloadJson(), req.GetPriority(), now)
	if err != nil {
		return nil, toStatus(err)
	}
	next, _ := sched.Next(now) // validateCronRequest proved it exists

	c := &Cron{
		ID:        newCronID(),
		Name:      req.GetName(),
		Expr:      req.GetCronExpr(),
		Type:      req.GetType(),
		Payload:   req.GetPayloadJson(),
		Priority:  priority,
		Enabled:   req.GetEnabled(),
		NextRunAt: next,
		CreatedAt: now,
	}
	if !caller.System {
		// Owner-scoped like jobs (JOBS-02): the schedule and every job it
		// spawns belong to the creator.
		c.OwnerID = &caller.UserID
	}
	if err := insertCron(ctx, s.pool, c); err != nil {
		return nil, toStatus(err)
	}
	if s.metrics != nil {
		s.metrics.cronCreated.Inc()
	}
	s.log.InfoContext(ctx, "cron created",
		slog.String("cron_id", c.ID), slog.String("expr", c.Expr),
		slog.Time("next_run_at", next))
	return c.toProto(), nil
}

// ListCrons returns the caller's schedules, paged like ListJobs.
func (s *Server) ListCrons(ctx context.Context, req *genjobs.ListCronsRequest) (*genjobs.ListCronsResponse, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	if !s.sched {
		return nil, toStatus(errSchedulingUnavailable())
	}
	page, size := NormalizePage(req.GetPage())
	crons, total, err := listCrons(ctx, s.pool, caller.OwnerScope(), int(size), pageOffset(page, size))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &genjobs.ListCronsResponse{
		Crons: make([]*genjobs.CronSchedule, 0, len(crons)),
		Page:  &gencommon.PageResponse{Page: page, PageSize: size, Total: total},
	}
	for _, c := range crons {
		out.Crons = append(out.Crons, c.toProto())
	}
	return out, nil
}

// DeleteCron removes a schedule. Owner-scoped: deleting a foreign schedule
// is NotFound, never an existence oracle.
func (s *Server) DeleteCron(ctx context.Context, req *genjobs.DeleteCronRequest) (*genjobs.DeleteCronResponse, error) {
	caller, err := CallerFromContext(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	if !s.sched {
		return nil, toStatus(errSchedulingUnavailable())
	}
	if req.GetId() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "cron_id_required",
			"id is required", nil))
	}
	cur, err := getCron(ctx, s.pool, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	if !caller.CanAccessJob(cur.OwnerID) {
		return nil, toStatus(errors.E(errors.KindNotFound, "cron_not_found",
			"cron schedule does not exist", nil))
	}
	if err := deleteCron(ctx, s.pool, req.GetId()); err != nil {
		return nil, toStatus(err)
	}
	s.log.InfoContext(ctx, "cron deleted", slog.String("cron_id", req.GetId()))
	return &genjobs.DeleteCronResponse{Ok: true}, nil
}
