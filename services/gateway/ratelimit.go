// ratelimit.go is the per-key token bucket limiter. Keys are the user id for
// authenticated requests and the client IP for public ones. Buckets live in
// an in-memory map swept by a janitor goroutine so idle keys cannot grow the
// map forever. This is per-process limiting — good enough for the learning
// platform; a multi-replica deployment would move this to Redis.
package gateway

import (
	"context"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/raven/platform/pkg/errors"
)

// Limiter defaults, overridable via RATE_LIMIT_RPM / RATE_LIMIT_BURST.
const (
	defaultRateLimitRPM   = 100
	defaultRateLimitBurst = 20

	// bucketSweepInterval is how often the janitor runs; bucketIdleTTL is how
	// long a bucket may sit unused before the janitor drops it.
	bucketSweepInterval = 5 * time.Minute
	bucketIdleTTL       = 10 * time.Minute
)

// bucket is one key's token bucket state.
type bucket struct {
	tokens  float64   // available tokens, capped at burst
	updated time.Time // when tokens were last refilled
	seen    time.Time // last request, used by the janitor
}

// rateLimiter is safe for concurrent use. now is a clock injection point for
// tests.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	perSec  float64 // refill rate in tokens/second
	burst   int     // max tokens a bucket can hold
	now     func() time.Time
}

func newRateLimiter(rpm, burst int) *rateLimiter {
	if rpm <= 0 {
		rpm = defaultRateLimitRPM
	}
	if burst <= 0 {
		burst = defaultRateLimitBurst
	}
	return &rateLimiter{
		buckets: make(map[string]*bucket),
		perSec:  float64(rpm) / 60,
		burst:   burst,
		now:     time.Now,
	}
}

// allow takes one token from key's bucket. When the bucket is empty it
// reports how long the caller should wait before retrying.
func (l *rateLimiter) allow(key string) (allowed bool, retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(l.burst), updated: now}
		l.buckets[key] = b
	}

	// Refill for the elapsed time, capped at burst.
	if elapsed := now.Sub(b.updated).Seconds(); elapsed > 0 {
		b.tokens = math.Min(float64(l.burst), b.tokens+elapsed*l.perSec)
		b.updated = now
	}
	b.seen = now

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	// Seconds until the next full token exists; always strictly positive
	// here because tokens < 1 and perSec > 0.
	wait := time.Duration((1 - b.tokens) / l.perSec * float64(time.Second))
	return false, wait
}

// sweepOnce drops every bucket idle for longer than bucketIdleTTL.
func (l *rateLimiter) sweepOnce() {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := l.now().Add(-bucketIdleTTL)
	for key, b := range l.buckets {
		if b.seen.Before(cutoff) {
			delete(l.buckets, key)
		}
	}
}

// sweep runs the janitor until ctx is cancelled.
func (l *rateLimiter) sweep(ctx context.Context) {
	ticker := time.NewTicker(bucketSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.sweepOnce()
		}
	}
}

// rateLimitKey picks the limiting key: user id when the request is
// authenticated, otherwise the client IP.
func rateLimitKey(r *http.Request) string {
	if id, ok := IdentityFrom(r.Context()); ok && id.UserID != "" {
		return "user:" + id.UserID
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}

// rateLimit rejects requests over the limit with 429 and a Retry-After
// header. It runs after AuthN in the chain so authenticated users get their
// own bucket instead of sharing the IP bucket.
func (s *server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed, retryAfter := s.limiter.allow(rateLimitKey(r))
		if !allowed {
			s.metrics.rateLimited.Inc()
			// Round up: waiting exactly retryAfter may still land under 1 token.
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(retryAfter.Seconds())))))
			writeError(w, r, errors.E(errors.KindRateLimited, "rate_limited",
				"too many requests, slow down and try again", nil))
			return
		}
		next.ServeHTTP(w, r)
	})
}
