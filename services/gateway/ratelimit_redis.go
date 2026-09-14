// ratelimit_redis.go is the distributed rate-limit backend: the same token
// bucket semantics as the in-process limiter (100 req/min, burst 20 by
// default), but with the bucket state in Redis so every gateway replica
// shares ONE budget per key.
//
// Algorithm choice — atomic token bucket via Lua (not INCR+EXPIRE):
// INCR+EXPIRE is a FIXED window, which lets a client fire 2× the limit at a
// window boundary and cannot say when to retry. A sliding window needs a
// sorted set per key (more memory, more commands). The Lua token bucket
// keeps the exact semantics the in-process limiter already had — smooth
// refill, precise Retry-After, one round trip — and runs atomically inside
// Redis, so concurrent replicas can never over-issue tokens. The clock is
// Redis TIME, so replicas with skewed clocks still share one timeline.
//
// Failure mode — fail-open onto the in-process limiter: if Redis errors,
// the request is still answered using per-process buckets (and a counter +
// throttled warn log make the degradation visible). We choose availability
// over exactness deliberately: rate limiting is a protective control, not a
// correctness one — a Redis blip must not take the whole public API down,
// and the in-process fallback still caps abuse at N_replicas × the limit.
// The alternative (fail-closed, 503 while Redis is down) turns a cache
// outage into a self-inflicted DoS on every route.
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisBucketKeyPrefix namespaces limiter keys inside the shared Redis.
const redisBucketKeyPrefix = "raven:ratelimit:"

// rateLimitFallbackWarnEvery throttles the fail-open warn log: one line per
// window per replica, no matter how many requests hit the degraded path.
const rateLimitFallbackWarnEvery = 30 * time.Second

// redisCallTimeout bounds one script call. A sick Redis must not add its
// own latency budget to every request before the fallback can answer —
// and a slow fail-open would let the memory bucket refill while the
// request waits, quietly widening the effective limit.
const redisCallTimeout = 500 * time.Millisecond

// rateLimitRedisCooldown is how long the circuit stays open after a Redis
// error: requests go straight to the in-process fallback (zero added
// latency), and the next request after the cooldown probes Redis again.
// Self-healing: a recovered Redis is picked up within one cooldown window.
const rateLimitRedisCooldown = 30 * time.Second

// redisTokenBucketScript is the whole bucket update, atomically.
// KEYS[1] = bucket key; ARGV = rate/sec (float), burst, idle TTL (ms).
// Returns {allowed 0/1, retry_after_ms}.
const redisTokenBucketScript = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local tokens = tonumber(data[1])
local updated = tonumber(data[2])
if tokens == nil or updated == nil then
  tokens = burst
  updated = now
end

local elapsed = math.max(0, now - updated) / 1000
tokens = math.min(burst, tokens + elapsed * rate)
updated = now

local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) / rate * 1000)
  if retry_ms < 1 then retry_ms = 1 end
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', updated)
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]))
return {allowed, retry_ms}
`

// evaler is the single go-redis call the limiter needs (one EVAL). An
// interface so tests can run the script contract against an in-process
// fake — no extra dependency (miniredis) just for one command.
type evaler interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd
}

// redisLimiter takes tokens through the Lua script. rpm/burst mirror the
// in-process limiter's constructor semantics.
type redisLimiter struct {
	rdb     evaler
	perSec  float64
	burst   int
	idleTTL time.Duration
}

func newRedisLimiter(rdb evaler, rpm, burst int) *redisLimiter {
	if rpm <= 0 {
		rpm = defaultRateLimitRPM
	}
	if burst <= 0 {
		burst = defaultRateLimitBurst
	}
	return &redisLimiter{
		rdb:     rdb,
		perSec:  float64(rpm) / 60,
		burst:   burst,
		idleTTL: bucketIdleTTL, // same idle eviction as the memory backend
	}
}

// allow takes one token for key via the script. Any Redis/script problem is
// an error — the fallbackLimiter owns what happens next. The call is
// bounded by redisCallTimeout so a sick Redis cannot stall the request.
func (l *redisLimiter) allow(ctx context.Context, key string) (bool, time.Duration, error) {
	callCtx, cancel := context.WithTimeout(ctx, redisCallTimeout)
	defer cancel()
	res, err := l.rdb.Eval(callCtx, redisTokenBucketScript,
		[]string{redisBucketKeyPrefix + key},
		l.perSec, l.burst, l.idleTTL.Milliseconds()).Result()
	if err != nil {
		return false, 0, err
	}
	pair, ok := res.([]any)
	if !ok || len(pair) != 2 {
		return false, 0, fmt.Errorf("token bucket script returned %T, want [allowed, retry_ms]", res)
	}
	allowed, err := scriptInt(pair[0])
	if err != nil {
		return false, 0, err
	}
	retryMS, err := scriptInt(pair[1])
	if err != nil {
		return false, 0, err
	}
	return allowed == 1, time.Duration(retryMS) * time.Millisecond, nil
}

// scriptInt reads an integer script reply field (Redis bulk integers come
// back as int64; be tolerant of the string form some proxies return).
func scriptInt(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case string:
		var parsed int64
		if _, err := fmt.Sscanf(n, "%d", &parsed); err != nil {
			return 0, fmt.Errorf("script reply %q is not an integer", n)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("script reply %v (%T) is not an integer", v, v)
	}
}

// fallbackLimiter is the fail-open composition: Redis first, the in-process
// limiter when Redis errors. It exists so "Redis down" degrades to the old
// per-replica behavior instead of an outage.
//
// The circuit breaker keeps the degraded mode cheap: the first Redis error
// opens the circuit for rateLimitRedisCooldown, and while it is open every
// request goes straight to the fallback (probing a dead Redis per request
// would add dial latency that refills the memory buckets mid-flight).
type fallbackLimiter struct {
	primary   *redisLimiter
	fallback  *rateLimiter
	log       *slog.Logger
	metrics   *serviceMetrics
	openUntil atomic.Int64 // unix nano: circuit open until this instant
	lastWarn  atomic.Int64 // unix nano of the last emitted warn (throttle)
}

func newFallbackLimiter(primary *redisLimiter, fallback *rateLimiter, log *slog.Logger, m *serviceMetrics) *fallbackLimiter {
	return &fallbackLimiter{primary: primary, fallback: fallback, log: log, metrics: m}
}

// allowRequest implements requestLimiter.
func (l *fallbackLimiter) allowRequest(ctx context.Context, key string) (bool, time.Duration) {
	if now := time.Now().UnixNano(); now < l.openUntil.Load() {
		// Circuit open: Redis was sick moments ago — fail fast.
		if l.metrics != nil {
			l.metrics.rateLimitFallbacks.Inc()
		}
		return l.fallback.allow(key)
	}

	allowed, retryAfter, err := l.primary.allow(ctx, key)
	if err == nil {
		return allowed, retryAfter
	}
	l.openUntil.Store(time.Now().Add(rateLimitRedisCooldown).UnixNano())
	if l.metrics != nil {
		l.metrics.rateLimitFallbacks.Inc()
	}
	now := time.Now().UnixNano()
	if last := l.lastWarn.Load(); now-last >= int64(rateLimitFallbackWarnEvery) &&
		l.lastWarn.CompareAndSwap(last, now) && l.log != nil {
		l.log.Warn("redis rate limiter unavailable, fail-open to in-process buckets",
			slog.Any("error", err))
	}
	return l.fallback.allow(key)
}
