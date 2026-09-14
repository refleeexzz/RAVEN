package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/refleeexzz/RAVEN/internal/audit"
	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// captureEmitter records every emitted event.
type captureEmitter struct {
	mu     sync.Mutex
	events []audit.Event
}

func (c *captureEmitter) Emit(e audit.Event) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func (c *captureEmitter) all() []audit.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]audit.Event(nil), c.events...)
}

func (c *captureEmitter) one(t *testing.T) audit.Event {
	t.Helper()
	ev := c.all()
	if len(ev) != 1 {
		t.Fatalf("expected exactly 1 audit event, got %d: %+v", len(ev), ev)
	}
	return ev[0]
}

// auditTestRig builds a gateway server wired for audit middleware tests:
// real middleware chain, fake token validator, captured audit events.
type auditTestRig struct {
	srv    *server
	sink   *captureEmitter
	mux    *http.ServeMux
	tokens *fakeValidator
}

func newAuditTestRig(t *testing.T, v tokenValidator) *auditTestRig {
	t.Helper()
	sink := &captureEmitter{}
	metr := metrics.New("gateway-audit-test-" + t.Name())
	fv, _ := v.(*fakeValidator)
	s := &server{
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		metr:    metr,
		metrics: testMetrics(t),
		limiter: newRateLimiter(100_000, 100_000),
		authn: &authenticator{
			validator: v,
			cache:     newAuthCache(),
			metrics:   testMetrics(t),
		},
		audit:     sink,
		jwtSecret: "audit-test-secret",
		otelMW:    func(next http.Handler) http.Handler { return next },
	}
	return &auditTestRig{srv: s, sink: sink, mux: http.NewServeMux(), tokens: fv}
}

// route registers one route through the REAL wrapAPI chain.
func (rig *auditTestRig) route(method, path, perm string, h http.HandlerFunc) {
	rig.mux.Handle(method+" "+path, rig.srv.wrapAPI(route{method: method, path: path, perm: perm, handler: h}))
}

func okStub(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func TestAuditMutationSuccess(t *testing.T) {
	t.Parallel()

	rig := newAuditTestRig(t, &fakeValidator{
		id: Identity{UserID: "u-1", Email: "a@b.c", Perms: []string{ravenauth.PermJobsCancel}},
	})
	rig.route("POST", "/api/jobs/{id}/cancel", ravenauth.PermJobsCancel, okStub)

	req := httptest.NewRequest("POST", "/api/jobs/job_9/cancel", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer good")
	req.Header.Set("X-Request-ID", "req-123")
	req.Header.Set("User-Agent", "raven-cli/1.0")
	req.RemoteAddr = "203.0.113.7:5555"
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Action != "jobs.cancel" {
		t.Errorf("action = %q, want jobs.cancel", e.Action)
	}
	if e.ActorID != "u-1" {
		t.Errorf("actor = %q, want u-1", e.ActorID)
	}
	if e.ResourceType != "job" || e.ResourceID != "job_9" {
		t.Errorf("resource = %q/%q, want job/job_9", e.ResourceType, e.ResourceID)
	}
	if e.Outcome != audit.OutcomeSuccess {
		t.Errorf("outcome = %q, want success", e.Outcome)
	}
	if e.IP != "203.0.113.7" {
		t.Errorf("ip = %q, want 203.0.113.7", e.IP)
	}
	if e.UserAgent != "raven-cli/1.0" {
		t.Errorf("user agent = %q", e.UserAgent)
	}
	if e.TraceID != "req-123" {
		t.Errorf("trace id = %q, want req-123", e.TraceID)
	}
	if e.TS.IsZero() {
		t.Error("ts must be set")
	}
	if e.Detail["route"] != "POST /api/jobs/{id}/cancel" {
		t.Errorf("detail.route = %v", e.Detail["route"])
	}
	if e.Detail["status"] != http.StatusOK {
		t.Errorf("detail.status = %v", e.Detail["status"])
	}
	params, _ := e.Detail["params"].(map[string]string)
	if params["id"] != "job_9" {
		t.Errorf("detail.params.id = %v", e.Detail["params"])
	}
}

func TestAuditSkipsReads(t *testing.T) {
	t.Parallel()

	rig := newAuditTestRig(t, &fakeValidator{
		id: Identity{UserID: "u-1", Perms: []string{ravenauth.PermJobsRead}},
	})
	rig.route("GET", "/api/jobs/{id}", ravenauth.PermJobsRead, okStub)

	req := httptest.NewRequest("GET", "/api/jobs/job_9", nil)
	req.Header.Set("Authorization", "Bearer good")
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rig.sink.all(); len(got) != 0 {
		t.Fatalf("GET must not be audited, got %+v", got)
	}
}

func TestAuditDeniedIsAttributed(t *testing.T) {
	t.Parallel()

	// Valid token, missing permission -> 403, still attributed to the user.
	rig := newAuditTestRig(t, &fakeValidator{
		id: Identity{UserID: "u-2", Perms: []string{ravenauth.PermJobsRead}},
	})
	rig.route("POST", "/api/jobs/{id}/cancel", ravenauth.PermJobsCancel, okStub)

	req := httptest.NewRequest("POST", "/api/jobs/job_9/cancel", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer good")
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Outcome != audit.OutcomeDenied {
		t.Errorf("outcome = %q, want denied", e.Outcome)
	}
	if e.ActorID != "u-2" {
		t.Errorf("actor = %q, want u-2 (denied attempts must be attributed)", e.ActorID)
	}
}

func TestAuditUnauthorizedStaysAnonymous(t *testing.T) {
	t.Parallel()

	rig := newAuditTestRig(t, &fakeValidator{
		err: errors.E(errors.KindUnauthorized, "token_invalid", "nope", nil),
	})
	rig.route("DELETE", "/api/users/{id}", ravenauth.PermUsersDelete, okStub)

	req := httptest.NewRequest("DELETE", "/api/users/u-9", nil)
	req.Header.Set("Authorization", "Bearer bad")
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Outcome != audit.OutcomeDenied {
		t.Errorf("outcome = %q, want denied", e.Outcome)
	}
	if e.ActorID != audit.ActorAnonymous {
		t.Errorf("actor = %q, want anonymous", e.ActorID)
	}
	if e.Action != "users.delete" {
		t.Errorf("action = %q, want users.delete", e.Action)
	}
}

func TestAuditLoginFailure(t *testing.T) {
	t.Parallel()

	rig := newAuditTestRig(t, &fakeValidator{})
	rig.route("POST", "/api/auth/login", "", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, errors.E(errors.KindUnauthorized, "invalid_credentials",
			"invalid email or password", nil))
	})

	req := httptest.NewRequest("POST", "/api/auth/login",
		strings.NewReader(`{"email":"mallory@x.io","password":"wrong-password"}`))
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Action != "auth.login.failure" {
		t.Errorf("action = %q, want auth.login.failure", e.Action)
	}
	if e.Outcome != audit.OutcomeFailure {
		t.Errorf("outcome = %q, want failure (bad credentials are not denied)", e.Outcome)
	}
	if e.ActorID != "mallory@x.io" {
		t.Errorf("actor = %q, want the attempted email", e.ActorID)
	}
}

func TestAuditLoginSuccessActorFromToken(t *testing.T) {
	t.Parallel()

	rig := newAuditTestRig(t, &fakeValidator{})
	rig.route("POST", "/api/auth/login", "", func(w http.ResponseWriter, _ *http.Request) {
		tok, err := ravenauth.MintAccessToken("audit-test-secret", ravenauth.Claims{
			RegisteredClaims: jwt.RegisteredClaims{Subject: "user-42"},
		})
		if err != nil {
			t.Fatalf("mint token: %v", err)
		}
		writeJSON(w, http.StatusOK, tokenPairJSON{AccessToken: tok, RefreshToken: "rt"})
	})

	req := httptest.NewRequest("POST", "/api/auth/login",
		strings.NewReader(`{"email":"ada@x.io","password":"correct-horse"}`))
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Action != "auth.login.success" {
		t.Errorf("action = %q, want auth.login.success", e.Action)
	}
	if e.ActorID != "user-42" {
		t.Errorf("actor = %q, want user-42 from the minted token sub", e.ActorID)
	}
}

func TestAuditRedactsSecretsAndRestoresBody(t *testing.T) {
	t.Parallel()

	rig := newAuditTestRig(t, &fakeValidator{
		id: Identity{UserID: "admin-1", Perms: []string{ravenauth.PermUsersWrite}},
	})

	const rawBody = `{"email":"new@x.io","password":"sup3r-secret","profile":{"refresh_token":"tok-abc","bio":"hello"}}`
	rig.route("POST", "/api/users", ravenauth.PermUsersWrite, func(w http.ResponseWriter, r *http.Request) {
		// The handler must still see the ORIGINAL body, secrets included.
		body, _ := io.ReadAll(r.Body)
		if string(body) != rawBody {
			t.Errorf("handler saw altered body: %s", body)
		}
		writeJSON(w, http.StatusCreated, map[string]string{"user_id": "u-new"})
	})

	req := httptest.NewRequest("POST", "/api/users", strings.NewReader(rawBody))
	req.Header.Set("Authorization", "Bearer good")
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Action != "users.create" {
		t.Errorf("action = %q, want users.create", e.Action)
	}
	if e.ResourceID != "u-new" {
		t.Errorf("resource id = %q, want u-new from the response", e.ResourceID)
	}

	// The audit detail must never carry secrets, at any depth.
	detail, _ := json.Marshal(e.Detail)
	if strings.Contains(string(detail), "sup3r-secret") || strings.Contains(string(detail), "tok-abc") {
		t.Fatalf("detail leaked a secret: %s", detail)
	}
	if !strings.Contains(string(detail), "[REDACTED]") {
		t.Errorf("detail must mark redacted keys: %s", detail)
	}
	if !strings.Contains(string(detail), "hello") || !strings.Contains(string(detail), "new@x.io") {
		t.Errorf("non-sensitive fields must survive: %s", detail)
	}
}

func TestAuditPanicStillEmitted(t *testing.T) {
	t.Parallel()

	rig := newAuditTestRig(t, &fakeValidator{})
	rig.route("POST", "/api/auth/register", "", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})

	req := httptest.NewRequest("POST", "/api/auth/register",
		strings.NewReader(`{"email":"x@y.z","password":"pw-12345678"}`))
	rec := httptest.NewRecorder()
	rig.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Outcome != audit.OutcomeFailure {
		t.Errorf("outcome = %q, want failure", e.Outcome)
	}
	if e.Detail["status"] != http.StatusInternalServerError {
		t.Errorf("detail.status = %v, want 500", e.Detail["status"])
	}
}

func TestAuditDisabledIsPassthrough(t *testing.T) {
	t.Parallel()

	// s.audit == nil: the middleware must be invisible.
	s := &server{}
	h := s.auditRecord(http.HandlerFunc(okStub))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/api/jobs", strings.NewReader(`{}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestAuditActionForMapping(t *testing.T) {
	t.Parallel()

	cases := map[string][2]string{
		"POST /api/auth/register":     {"auth.register", "auth"},
		"POST /api/auth/login":        {"auth.login", "auth"},
		"POST /api/auth/refresh":      {"auth.refresh", "auth"},
		"POST /api/auth/logout":       {"auth.logout", "auth"},
		"POST /api/users":             {"users.create", "user"},
		"PUT /api/users/{id}":         {"users.update", "user"},
		"DELETE /api/users/{id}":      {"users.delete", "user"},
		"POST /api/jobs":              {"jobs.create", "job"},
		"POST /api/jobs/{id}/cancel":  {"jobs.cancel", "job"},
		"POST /api/jobs/{id}/requeue": {"jobs.requeue", "job"},
		// Fallback: unknown mutations still audit, derived from the URL.
		"POST /api/widgets/{id}/poke": {"widgets.post", "widget"},
	}
	for pattern, want := range cases {
		action, rt := auditActionFor(pattern)
		if action != want[0] || rt != want[1] {
			t.Errorf("auditActionFor(%q) = (%q, %q), want (%q, %q)",
				pattern, action, rt, want[0], want[1])
		}
	}
}

func TestAuditOutcomeMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status int
		public bool
		want   audit.Outcome
	}{
		{200, false, audit.OutcomeSuccess},
		{201, false, audit.OutcomeSuccess},
		{401, false, audit.OutcomeDenied},
		{403, false, audit.OutcomeDenied},
		{401, true, audit.OutcomeFailure}, // bad login credentials
		{400, false, audit.OutcomeFailure},
		{404, false, audit.OutcomeFailure},
		{429, false, audit.OutcomeFailure},
		{500, false, audit.OutcomeFailure},
		{503, false, audit.OutcomeFailure},
	}
	for _, c := range cases {
		if got := auditOutcome(c.status, c.public); got != c.want {
			t.Errorf("auditOutcome(%d, public=%v) = %q, want %q", c.status, c.public, got, c.want)
		}
	}
}

// TestAuditMiddlewareSitsOutsideTimeout pins the chain order the design
// relies on: auditRecord must wrap Timeout, auditActor must sit between
// AuthN and AuthZ.
func TestAuditChainOrder(t *testing.T) {
	t.Parallel()

	// Fire a request that times out: the client gets 503 from the timeout
	// handler and the audit event must record that same 503.
	rig := newAuditTestRig(t, &fakeValidator{})
	s := rig.srv

	// Shrink the timeout by rebuilding the chain the way wrapAPI does, but
	// with a tiny timeout (the constant is 10s — too slow for a test).
	stub := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.Chain(stub,
		middleware.RequestID,
		s.auditRecord,
		middleware.Timeout(10*time.Millisecond),
	)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/api/jobs", strings.NewReader(`{}`)))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	e := rig.sink.one(t)
	if e.Outcome != audit.OutcomeFailure {
		t.Errorf("outcome = %q, want failure", e.Outcome)
	}
	if e.Detail["status"] != http.StatusServiceUnavailable {
		t.Errorf("detail.status = %v, want the 503 the client got", e.Detail["status"])
	}
}
