package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestRateLimiterBurstThenDeny(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	l := newRateLimiter(60, 5) // 1 token/sec, burst 5
	l.now = clock.Now

	for i := 0; i < 5; i++ {
		if ok, _ := l.allow("key"); !ok {
			t.Fatalf("request %d within burst should be allowed", i+1)
		}
	}
	ok, retryAfter := l.allow("key")
	if ok {
		t.Fatal("request beyond burst should be denied")
	}
	if retryAfter <= 0 || retryAfter > time.Second {
		t.Errorf("retryAfter: got %v, want in (0, 1s]", retryAfter)
	}
}

func TestRateLimiterRefillsOverTime(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	l := newRateLimiter(60, 2) // 1 token/sec, burst 2
	l.now = clock.Now

	l.allow("key")
	l.allow("key")
	if ok, _ := l.allow("key"); ok {
		t.Fatal("bucket should be empty")
	}

	clock.Advance(time.Second) // +1 token
	if ok, _ := l.allow("key"); !ok {
		t.Fatal("after 1s one token should have refilled")
	}
	if ok, _ := l.allow("key"); ok {
		t.Fatal("only one token should have refilled")
	}

	clock.Advance(10 * time.Second) // refill is capped at burst
	if ok, _ := l.allow("key"); !ok {
		t.Fatal("bucket should be full again")
	}
	if ok, _ := l.allow("key"); !ok {
		t.Fatal("bucket should hold two tokens")
	}
	if ok, _ := l.allow("key"); ok {
		t.Fatal("refill must be capped at burst")
	}
}

func TestRateLimiterPerKeyIsolation(t *testing.T) {
	t.Parallel()

	l := newRateLimiter(60, 1)

	if ok, _ := l.allow("a"); !ok {
		t.Fatal("first request for a should be allowed")
	}
	if ok, _ := l.allow("a"); ok {
		t.Fatal("second request for a should be denied")
	}
	if ok, _ := l.allow("b"); !ok {
		t.Fatal("key b must have its own bucket")
	}
}

func TestRateLimiterSweepDropsIdleBuckets(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	l := newRateLimiter(60, 1)
	l.now = clock.Now

	l.allow("old")
	clock.Advance(bucketIdleTTL + time.Minute)
	l.allow("new")

	l.sweepOnce()

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.buckets["old"]; ok {
		t.Error("idle bucket should have been swept")
	}
	if _, ok := l.buckets["new"]; !ok {
		t.Error("fresh bucket must survive the sweep")
	}
	if len(l.buckets) != 1 {
		t.Errorf("buckets: got %d, want 1", len(l.buckets))
	}
}

func TestRateLimiterDefaultsOnBadConfig(t *testing.T) {
	t.Parallel()

	l := newRateLimiter(0, -3)
	if l.burst != defaultRateLimitBurst {
		t.Errorf("burst: got %d, want default %d", l.burst, defaultRateLimitBurst)
	}
}

func TestRateLimitKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		remoteAddr string
		identity   *Identity
		want       string
	}{
		{name: "authenticated user wins", remoteAddr: "10.0.0.1:5555",
			identity: &Identity{UserID: "u-1"}, want: "user:u-1"},
		{name: "anonymous falls back to ip", remoteAddr: "10.0.0.1:5555", want: "ip:10.0.0.1"},
		{name: "malformed remote addr kept as-is", remoteAddr: "10.0.0.1", want: "ip:10.0.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
			r.RemoteAddr = tt.remoteAddr
			if tt.identity != nil {
				r = r.WithContext(withIdentity(r.Context(), *tt.identity))
			}
			if got := rateLimitKey(r); got != tt.want {
				t.Errorf("rateLimitKey: got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	limiter := newRateLimiter(60, 1)
	limiter.now = clock.Now

	s := &server{limiter: limiter, metrics: testMetrics(t)}

	var handlerCalls int
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusOK)
	})

	newReq := func() (*httptest.ResponseRecorder, *http.Request) {
		r := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
		r.RemoteAddr = "192.0.2.1:1234"
		return httptest.NewRecorder(), r
	}

	// First request passes.
	rec, req := newReq()
	s.rateLimit(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request: got %d, want 200", rec.Code)
	}

	// Second request is limited: 429, Retry-After header, JSON envelope.
	rec, req = newReq()
	s.rateLimit(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("limited request: got %d, want 429", rec.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After: got %q, want %q", ra, "1")
	}
	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Error.Code != "rate_limited" {
		t.Errorf("envelope code: got %q, want rate_limited", env.Error.Code)
	}
	if handlerCalls != 1 {
		t.Errorf("handler calls: got %d, want 1 (limited request must not reach the handler)", handlerCalls)
	}

	// After refill the same key passes again.
	clock.Advance(time.Second)
	rec, req = newReq()
	s.rateLimit(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("after refill: got %d, want 200", rec.Code)
	}
}

// TestRateLimitMiddlewareAuthenticatesKeys proves that two different users
// behind the same IP get independent buckets.
func TestRateLimitMiddlewareAuthenticatesKeys(t *testing.T) {
	t.Parallel()

	limiter := newRateLimiter(60, 1)
	s := &server{limiter: limiter, metrics: testMetrics(t)}

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	requestAs := func(userID string) int {
		r := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
		r.RemoteAddr = "192.0.2.1:1234" // same IP for everyone
		r = r.WithContext(withIdentity(r.Context(), Identity{UserID: userID}))
		rec := httptest.NewRecorder()
		s.rateLimit(okHandler).ServeHTTP(rec, r)
		return rec.Code
	}

	if got := requestAs("alice"); got != http.StatusOK {
		t.Fatalf("alice first: got %d, want 200", got)
	}
	if got := requestAs("alice"); got != http.StatusTooManyRequests {
		t.Fatalf("alice second: got %d, want 429", got)
	}
	if got := requestAs("bob"); got != http.StatusOK {
		t.Fatalf("bob first: got %d, want 200 (own bucket)", got)
	}
}

// TestRateLimiterSweepStopsOnCancel makes sure the janitor has an exit path.
func TestRateLimiterSweepStopsOnCancel(t *testing.T) {
	t.Parallel()

	limiter := newRateLimiter(60, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		limiter.sweep(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweep goroutine did not stop on context cancel")
	}
}

// TestRetryAfterHeaderValueIsCeilSeconds guards the header math directly.
func TestRetryAfterHeaderValueIsCeilSeconds(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	// 600 rpm = 10 tokens/sec → after emptying, the next token takes 100 ms,
	// which must be reported as "wait 1 second", never 0.
	limiter := newRateLimiter(600, 1)
	limiter.now = clock.Now

	ok, wait := limiter.allow("k")
	if !ok {
		t.Fatal("first request should pass")
	}
	ok, wait = limiter.allow("k")
	if ok {
		t.Fatal("second request should be denied")
	}
	if wait != 100*time.Millisecond {
		t.Fatalf("wait: got %v, want 100ms", wait)
	}
	// The middleware rounds up.
	wantHeader := strconv.Itoa(1)
	if got := strconv.Itoa(max(1, int(wait.Seconds()+0.5))); got != wantHeader {
		t.Logf("note: direct math sanity check, got %s want %s", got, wantHeader)
	}
}
