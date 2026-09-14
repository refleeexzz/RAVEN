package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeRedis is the in-process Redis fake the limiter tests run against. It
// implements exactly one command — the EVAL of redisTokenBucketScript — by
// re-executing the same token-bucket semantics in Go over a map, with an
// injectable clock and an injectable failure. Anything else is an error, so
// a drift in the script/args contract fails loudly here.
type fakeRedis struct {
	mu      sync.Mutex
	buckets map[string]*fakeRedisBucket
	now     func() time.Time
	err     error // injected: every Eval fails (Redis down)
}

type fakeRedisBucket struct {
	tokens  float64
	updated time.Time
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{
		buckets: make(map[string]*fakeRedisBucket),
		now:     time.Now,
	}
}

func (f *fakeRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if f.err != nil {
		cmd.SetErr(f.err)
		return cmd
	}
	if script != redisTokenBucketScript {
		cmd.SetErr(errors.New("fake redis: unknown script"))
		return cmd
	}
	if len(keys) != 1 || len(args) != 3 {
		cmd.SetErr(errors.New("fake redis: wrong arity"))
		return cmd
	}
	rate, ok1 := args[0].(float64)
	burst, ok2 := args[1].(int)
	idleMS, ok3 := args[2].(int64)
	if !ok1 || !ok2 || !ok3 {
		cmd.SetErr(errors.New("fake redis: bad arg types"))
		return cmd
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	b, ok := f.buckets[keys[0]]
	if !ok {
		b = &fakeRedisBucket{tokens: float64(burst), updated: now}
		f.buckets[keys[0]] = b
	}
	if elapsed := now.Sub(b.updated).Seconds(); elapsed > 0 {
		b.tokens = math.Min(float64(burst), b.tokens+elapsed*rate)
		b.updated = now
	}
	allowed := int64(0)
	retryMS := int64(0)
	if b.tokens >= 1 {
		b.tokens--
		allowed = 1
	} else {
		retryMS = int64(math.Ceil((1 - b.tokens) / rate * 1000))
		if retryMS < 1 {
			retryMS = 1
		}
	}
	_ = idleMS // the fake never expires buckets; the TTL contract is redis-side
	cmd.SetVal([]any{allowed, retryMS})
	return cmd
}

// bucketCount reports how many buckets the fake holds (key-prefix checks).
func (f *fakeRedis) bucketCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.buckets)
}

// errRedisDown simulates a full Redis outage.
var errRedisDown = errors.New("fake redis: connection refused")

func TestRedisLimiterBurstThenDeny(t *testing.T) {
	t.Parallel()

	l := newRedisLimiter(newFakeRedis(), 60, 5) // 1 token/s, burst 5
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		ok, _, err := l.allow(ctx, "user:u1")
		if err != nil || !ok {
			t.Fatalf("request %d within burst: ok=%v err=%v, want allowed", i+1, ok, err)
		}
	}
	ok, retry, err := l.allow(ctx, "user:u1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("request beyond burst must be denied")
	}
	if retry <= 0 || retry > time.Second {
		t.Errorf("retry: got %v, want in (0, 1s]", retry)
	}
}

// TestRedisLimiterSharedAcrossInstances is the whole point of the
// distributed store: two limiter instances (two "replicas") against the
// same Redis share ONE budget per key.
func TestRedisLimiterSharedAcrossInstances(t *testing.T) {
	t.Parallel()

	rdb := newFakeRedis()
	replicaA := newRedisLimiter(rdb, 60, 4)
	replicaB := newRedisLimiter(rdb, 60, 4)
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 8; i++ {
		l := replicaA
		if i%2 == 1 {
			l = replicaB
		}
		if ok, _, err := l.allow(ctx, "user:u1"); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		} else if ok {
			allowed++
		}
	}
	if allowed != 4 {
		t.Fatalf("two replicas allowed %d of 8, want exactly 4 (one shared bucket)", allowed)
	}
	// ...and the state lives under one namespaced key.
	if rdb.bucketCount() != 1 {
		t.Errorf("buckets in redis: got %d, want 1", rdb.bucketCount())
	}
}

func TestRedisLimiterRefillsAndKeysAreNamespaced(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	rdb := newFakeRedis()
	rdb.now = clock.Now
	l := newRedisLimiter(rdb, 60, 1) // 1 token/s, burst 1
	ctx := context.Background()

	if ok, _, _ := l.allow(ctx, "ip:10.0.0.1"); !ok {
		t.Fatal("first request must pass")
	}
	if ok, _, _ := l.allow(ctx, "ip:10.0.0.1"); ok {
		t.Fatal("second request must be denied (burst 1)")
	}
	clock.Advance(time.Second)
	if ok, _, _ := l.allow(ctx, "ip:10.0.0.1"); !ok {
		t.Fatal("after 1s the token must have refilled")
	}
	// A different key gets its own bucket.
	if ok, _, _ := l.allow(ctx, "user:u9"); !ok {
		t.Fatal("a different key must have its own budget")
	}
	if rdb.bucketCount() != 2 {
		t.Fatalf("buckets: got %d, want 2", rdb.bucketCount())
	}
	rdb.mu.Lock()
	for key := range rdb.buckets {
		if len(key) == 0 || key[:len(redisBucketKeyPrefix)] != redisBucketKeyPrefix {
			t.Errorf("redis key %q must be namespaced with %q", key, redisBucketKeyPrefix)
		}
	}
	rdb.mu.Unlock()
}

func TestRedisLimiterDefaultsOnBadConfig(t *testing.T) {
	t.Parallel()

	l := newRedisLimiter(newFakeRedis(), 0, -1)
	if l.burst != defaultRateLimitBurst {
		t.Errorf("burst: got %d, want default %d", l.burst, defaultRateLimitBurst)
	}
	if l.perSec != float64(defaultRateLimitRPM)/60 {
		t.Errorf("perSec: got %v, want default", l.perSec)
	}
}

func TestRedisLimiterPropagatesRedisErrors(t *testing.T) {
	t.Parallel()

	rdb := newFakeRedis()
	rdb.err = errRedisDown
	l := newRedisLimiter(rdb, 60, 1)

	if _, _, err := l.allow(context.Background(), "k"); !errors.Is(err, errRedisDown) {
		t.Fatalf("got %v, want the redis error", err)
	}
}

// junkEvaler returns a malformed script reply (proxy/protocol drift).
type junkEvaler struct{}

func (junkEvaler) Eval(ctx context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal("not-a-pair")
	return cmd
}

func TestRedisLimiterRejectsMalformedReply(t *testing.T) {
	t.Parallel()

	l := newRedisLimiter(junkEvaler{}, 60, 1)
	if _, _, err := l.allow(context.Background(), "k"); err == nil {
		t.Fatal("a malformed script reply must surface as an error (→ fallback)")
	}
}

// TestFallbackLimiterFailOpen pins the documented failure mode: Redis down
// → the in-process limiter answers (availability), the fallback counter
// moves, and the service recovers by itself once Redis is back.
func TestFallbackLimiterFailOpen(t *testing.T) {
	t.Parallel()

	rdb := newFakeRedis()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fl := newFallbackLimiter(newRedisLimiter(rdb, 60, 2), newRateLimiter(60, 2), log, testMetrics(t))
	ctx := context.Background()

	// Redis down: the memory fallback serves the burst, then still limits.
	rdb.err = errRedisDown
	for i := 0; i < 2; i++ {
		if ok, _ := fl.allowRequest(ctx, "k"); !ok {
			t.Fatalf("fail-open request %d must be allowed by the fallback", i+1)
		}
	}
	if ok, _ := fl.allowRequest(ctx, "k"); ok {
		t.Fatal("the fallback still limits (fail-open, not fail-free)")
	}

	// Redis back: after the cooldown the primary takes over again with its
	// own state. (openUntil zeroed to simulate the cooldown having elapsed.)
	rdb.err = nil
	fl.openUntil.Store(0)
	if ok, _ := fl.allowRequest(ctx, "k"); !ok {
		t.Fatal("after recovery the redis backend must serve again (fresh bucket)")
	}
}

// TestFallbackLimiterConcurrent drills the atomic counter/throttle under
// -race: many goroutines failing over at once must not race.
func TestFallbackLimiterConcurrent(t *testing.T) {
	t.Parallel()

	rdb := newFakeRedis()
	rdb.err = errRedisDown
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fl := newFallbackLimiter(newRedisLimiter(rdb, 600, 100), newRateLimiter(600, 100), log, testMetrics(t))
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				fl.allowRequest(ctx, "shared-key")
			}
		}()
	}
	wg.Wait()
}

// countingEvaler wraps fakeRedis and counts Eval calls, so the circuit
// breaker can prove it skips Redis while open.
type countingEvaler struct {
	inner *fakeRedis
	calls atomic.Int64
}

func (c *countingEvaler) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	c.calls.Add(1)
	return c.inner.Eval(ctx, script, keys, args...)
}

// TestFallbackLimiterCircuitBreaker: after the first Redis error the
// circuit opens and requests fail fast to memory (no per-request Redis
// latency); after the cooldown the next request probes Redis again.
func TestFallbackLimiterCircuitBreaker(t *testing.T) {
	t.Parallel()

	rdb := newFakeRedis()
	rdb.err = errRedisDown
	counting := &countingEvaler{inner: rdb}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fl := newFallbackLimiter(newRedisLimiter(counting, 600, 100), newRateLimiter(600, 100), log, testMetrics(t))
	ctx := context.Background()

	// First request probes Redis, fails, opens the circuit.
	if ok, _ := fl.allowRequest(ctx, "k"); !ok {
		t.Fatal("first request must be served by the fallback")
	}
	if got := counting.calls.Load(); got != 1 {
		t.Fatalf("eval calls after first request: got %d, want 1", got)
	}

	// Circuit open: the next requests never touch Redis.
	for i := 0; i < 5; i++ {
		fl.allowRequest(ctx, "k")
	}
	if got := counting.calls.Load(); got != 1 {
		t.Fatalf("eval calls while circuit open: got %d, want 1 (fail fast)", got)
	}

	// Cooldown over: the next request probes again — and, Redis being
	// healthy now, the primary serves it.
	rdb.err = nil
	fl.openUntil.Store(0)
	if ok, _ := fl.allowRequest(ctx, "k"); !ok {
		t.Fatal("post-cooldown probe must be served by redis")
	}
	if got := counting.calls.Load(); got != 2 {
		t.Fatalf("eval calls after cooldown probe: got %d, want 2", got)
	}
}
