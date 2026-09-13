package jobs

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// Idempotency-Key handling.
//
// Two layers, belt and suspenders:
//  1. Redis fast path: SET idem:<key> <job_id> NX PX 24h. A repeated request
//     finds the existing job id and returns it without touching the write
//     path.
//  2. Postgres backstop: the unique partial index on jobs.idempotency_key
//     catches the cases Redis cannot (eviction, TTL expiry while the row
//     lives, Redis down at request time). A 23505 conflict makes us load and
//     return the existing job.
//
// The TTL is a deliberate trade-off: 24h covers realistic client retry
// windows; after it the database still protects uniqueness for as long as
// the job row exists.
const (
	idempotencyKeyPrefix = "idem:"
	idempotencyTTL       = 24 * time.Hour
	maxIdempotencyKeyLen = 255
)

// normalizeIdempotencyKey trims and validates the caller-supplied key. An
// empty key means "no idempotency wanted" and is not an error.
func normalizeIdempotencyKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", nil
	}
	if len(key) > maxIdempotencyKeyLen {
		return "", errors.E(errors.KindInvalid, "idempotency_key_too_long",
			"idempotency key must be at most 255 characters", nil)
	}
	return key, nil
}

// claimIdempotencyKey tries to reserve key for jobID in Redis. It returns
// (existingJobID, claimed, err): existingJobID is set when the key was
// already taken. Redis failures are returned so the caller can fall back to
// the database unique index.
func claimIdempotencyKey(ctx context.Context, rdb redis.UniversalClient, key, jobID string) (existing string, claimed bool, err error) {
	ok, err := rdb.SetNX(ctx, idempotencyKeyPrefix+key, jobID, idempotencyTTL).Result()
	if err != nil {
		return "", false, err
	}
	if !ok {
		existing, err = rdb.Get(ctx, idempotencyKeyPrefix+key).Result()
		if err != nil {
			return "", false, err
		}
		return existing, false, nil
	}
	return "", true, nil
}

// releaseIdempotencyKey removes a claim. Used when the create flow fails
// after claiming (broker down): the key must not point at a job that never
// queued, or the client's retry would get a FAILED job back forever.
func releaseIdempotencyKey(ctx context.Context, rdb redis.UniversalClient, log *slog.Logger, key string) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := rdb.Del(ctx, idempotencyKeyPrefix+key).Err(); err != nil {
		log.Warn("could not release idempotency key", slog.Any("error", err))
	}
}
