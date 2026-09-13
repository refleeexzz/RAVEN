package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Worker registry (docs/contracts/ports-and-env.md §Worker registry): each
// worker keeps a Redis hash worker:<id> with a 15s TTL, refreshed every 5s.
// The gateway SCANs worker:* to answer GET /api/workers.
const (
	registryKeyPrefix = "worker:"
	registryTTL       = 15 * time.Second
	registryRefresh   = 5 * time.Second
)

// registry heartbeats this worker into Redis.
type registry struct {
	rdb redis.UniversalClient
	id  string
	log *slog.Logger

	startedAt time.Time
	stats     func() (processed, inFlight int64)
}

// newRegistry builds the heartbeat writer. stats reports the live counters
// written into the hash on every beat.
func newRegistry(rdb redis.UniversalClient, id string, log *slog.Logger, startedAt time.Time, stats func() (int64, int64)) *registry {
	return &registry{rdb: rdb, id: id, log: log, startedAt: startedAt, stats: stats}
}

// key is the Redis key of this worker's registry entry.
func (r *registry) key() string { return registryKeyPrefix + r.id }

// run beats every 5s until ctx is cancelled, then deletes the key so a dead
// worker disappears immediately instead of lingering until the TTL.
func (r *registry) run(ctx context.Context) {
	r.beat(ctx)
	t := time.NewTicker(registryRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.del()
			return
		case <-t.C:
			r.beat(ctx)
		}
	}
}

// beat writes the hash and refreshes its TTL. Errors are logged and retried
// on the next tick: the registry is discovery metadata, not correctness.
func (r *registry) beat(ctx context.Context) {
	if r.rdb == nil {
		return
	}
	processed, inFlight := r.stats()

	beatCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pipe := r.rdb.Pipeline()
	pipe.HSet(beatCtx, r.key(), map[string]any{
		"id":             r.id,
		"started_at":     r.startedAt.Format(time.RFC3339),
		"last_heartbeat": time.Now().UTC().Format(time.RFC3339),
		"jobs_processed": processed,
		"in_flight":      inFlight,
	})
	pipe.Expire(beatCtx, r.key(), registryTTL)
	if _, err := pipe.Exec(beatCtx); err != nil {
		r.log.Warn("registry heartbeat failed", slog.Any("error", err))
	}
}

// del removes the registry entry on shutdown. Uses a fresh context: the
// caller's ctx is already cancelled at this point.
func (r *registry) del() {
	if r.rdb == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.rdb.Del(ctx, r.key()).Err(); err != nil {
		r.log.Warn("registry delete failed", slog.Any("error", err))
	}
}
