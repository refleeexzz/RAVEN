package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/raven/platform/pkg/errors"
)

// fakeValidator counts calls and replays a fixed result.
type fakeValidator struct {
	mu    sync.Mutex
	calls int
	id    Identity
	err   error
}

func (f *fakeValidator) ValidateToken(_ context.Context, _ string) (Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.id, f.err
}

func (f *fakeValidator) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestBearerTokenParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		header    string
		wantToken string
		wantCode  string // expected error code; "" means success
	}{
		{name: "empty header", header: "", wantCode: "missing_authorization"},
		{name: "no scheme", header: "just-a-token", wantCode: "invalid_authorization"},
		{name: "wrong scheme", header: "Basic abc123", wantCode: "invalid_authorization"},
		{name: "bearer without token", header: "Bearer", wantCode: "invalid_authorization"},
		{name: "bearer with blank token", header: "Bearer   ", wantCode: "invalid_authorization"},
		{name: "token with space inside", header: "Bearer abc def", wantCode: "invalid_authorization"},
		{name: "simple token", header: "Bearer abc123", wantToken: "abc123"},
		{name: "scheme is case-insensitive", header: "bEaReR abc123", wantToken: "abc123"},
		{name: "extra spaces after scheme are tolerated", header: "Bearer   abc123", wantToken: "abc123"},
		{name: "jwt-shaped token", header: "Bearer aaa.bbb.ccc", wantToken: "aaa.bbb.ccc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token, err := bearerToken(tt.header)
			if tt.wantCode != "" {
				if err == nil {
					t.Fatalf("expected error %q, got token %q", tt.wantCode, token)
				}
				if got := errors.CodeOf(err); got != tt.wantCode {
					t.Errorf("code: got %q, want %q", got, tt.wantCode)
				}
				if got := errors.KindOf(err); got != errors.KindUnauthorized {
					t.Errorf("kind: got %v, want Unauthorized", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if token != tt.wantToken {
				t.Errorf("token: got %q, want %q", token, tt.wantToken)
			}
		})
	}
}

// newAuthnTestRig wires an authenticator with a fake validator and returns
// the middleware plus the parts tests poke at.
func newAuthnTestRig(t *testing.T, v tokenValidator) (func(http.Handler) http.Handler, *fakeValidator, *authCache) {
	t.Helper()
	fv, ok := v.(*fakeValidator)
	if !ok {
		t.Fatal("newAuthnTestRig wants a *fakeValidator")
	}
	cache := newAuthCache()
	a := &authenticator{validator: fv, cache: cache, metrics: testMetrics(t)}
	return a.middleware, fv, cache
}

func authRequest(token string) (*httptest.ResponseRecorder, *http.Request) {
	r := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return httptest.NewRecorder(), r
}

func TestAuthNCachesSuccessfulValidation(t *testing.T) {
	t.Parallel()

	fv := &fakeValidator{id: Identity{UserID: "u-1", Email: "a@b.c", Perms: []string{"jobs:read"}}}
	mw, _, _ := newAuthnTestRig(t, fv)

	var seen Identity
	var seenOK bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, seenOK = IdentityFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	for i := 0; i < 3; i++ {
		rec, req := authRequest("token-abc")
		mw(next).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: got %d, want 200", i+1, rec.Code)
		}
	}

	if fv.callCount() != 1 {
		t.Errorf("validator calls: got %d, want 1 (cache must serve repeats)", fv.callCount())
	}
	if !seenOK || seen.UserID != "u-1" {
		t.Errorf("identity in context: got %+v (ok=%v), want user u-1", seen, seenOK)
	}
}

func TestAuthNCacheExpiryRefetches(t *testing.T) {
	t.Parallel()

	fv := &fakeValidator{id: Identity{UserID: "u-1"}}
	mw, _, cache := newAuthnTestRig(t, fv)

	clock := newFakeClock()
	cache.now = clock.Now

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec, req := authRequest("token-abc")
	mw(next).ServeHTTP(rec, req)
	if fv.callCount() != 1 {
		t.Fatalf("calls after first request: got %d, want 1", fv.callCount())
	}

	// Inside the TTL: cached.
	clock.Advance(authCacheTTL - time.Second)
	rec, req = authRequest("token-abc")
	mw(next).ServeHTTP(rec, req)
	if fv.callCount() != 1 {
		t.Fatalf("calls inside TTL: got %d, want 1", fv.callCount())
	}

	// Past the TTL: refetch.
	clock.Advance(2 * time.Second)
	rec, req = authRequest("token-abc")
	mw(next).ServeHTTP(rec, req)
	if fv.callCount() != 2 {
		t.Fatalf("calls after TTL expiry: got %d, want 2", fv.callCount())
	}
}

func TestAuthNRejectsBadRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string // "" → no header at all
		validator  *fakeValidator
		wantStatus int
		wantCode   string
		wantCalls  int
	}{
		{
			name:       "missing header",
			token:      "",
			validator:  &fakeValidator{},
			wantStatus: http.StatusUnauthorized,
			wantCode:   "missing_authorization",
			wantCalls:  0, // never reaches the validator
		},
		{
			name:       "validator says invalid",
			token:      "bad-token",
			validator:  &fakeValidator{err: errors.E(errors.KindUnauthorized, "token_invalid", "nope", nil)},
			wantStatus: http.StatusUnauthorized,
			wantCode:   "token_invalid",
			wantCalls:  1,
		},
		{
			name:       "auth service down",
			token:      "any-token",
			validator:  &fakeValidator{err: errors.E(errors.KindUnavailable, "circuit_open", "upstream auth is temporarily unavailable", nil)},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "circuit_open",
			wantCalls:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mw, fv, _ := newAuthnTestRig(t, tt.validator)
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Error("handler must not run on auth failure")
				w.WriteHeader(http.StatusOK)
			})

			rec, req := authRequest(tt.token)
			mw(next).ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status: got %d, want %d", rec.Code, tt.wantStatus)
			}
			var env errorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			if env.Error.Code != tt.wantCode {
				t.Errorf("envelope code: got %q, want %q", env.Error.Code, tt.wantCode)
			}
			if fv.callCount() != tt.wantCalls {
				t.Errorf("validator calls: got %d, want %d", fv.callCount(), tt.wantCalls)
			}
		})
	}
}

func TestAuthNFailuresAreNotCached(t *testing.T) {
	t.Parallel()

	fv := &fakeValidator{err: errors.E(errors.KindUnauthorized, "token_invalid", "nope", nil)}
	mw, _, _ := newAuthnTestRig(t, fv)

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	for i := 0; i < 2; i++ {
		rec, req := authRequest("bad-token")
		mw(next).ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("request %d: got %d, want 401", i+1, rec.Code)
		}
	}
	if fv.callCount() != 2 {
		t.Errorf("validator calls: got %d, want 2 (failures must not be cached)", fv.callCount())
	}
}

func TestAuthCacheSweepStopsOnCancel(t *testing.T) {
	t.Parallel()

	cache := newAuthCache()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		cache.sweep(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cache sweep goroutine did not stop on context cancel")
	}
}
