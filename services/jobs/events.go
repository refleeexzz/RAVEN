package jobs

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// jobEvent is the JSON payload published on raven:events:jobs after every
// status change. The websocket service fans these out to subscribed clients
// (docs/contracts/ports-and-env.md §Live events).
type jobEvent struct {
	Type     string    `json:"type"` // always "job_status"
	JobID    string    `json:"job_id"`
	Status   Status    `json:"status"`
	WorkerID string    `json:"worker_id"`
	OwnerID  string    `json:"owner_id,omitempty"`
	At       time.Time `json:"at"`
}

// PublishJobEvent announces a status change. Publish failures are logged,
// never fatal: the event stream is a UI nicety, not the source of truth.
// A nil rdb (unit tests) makes this a no-op.
func PublishJobEvent(ctx context.Context, rdb redis.UniversalClient, log *slog.Logger, j *Job) {
	if rdb == nil {
		return
	}
	ev := jobEvent{
		Type:     "job_status",
		JobID:    j.ID,
		Status:   j.Status,
		WorkerID: j.WorkerID,
		At:       time.Now().UTC(),
	}
	if j.OwnerID != nil {
		ev.OwnerID = *j.OwnerID
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		log.Warn("job event marshal failed", slog.Any("error", err))
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := rdb.Publish(pubCtx, EventsChannel, raw).Err(); err != nil {
		log.Warn("job event publish failed",
			slog.String("job_id", j.ID), slog.Any("error", err))
	}
}
