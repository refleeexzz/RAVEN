// Package security is the black-box pre-release security regression suite
// for RAVEN's edge layer: the API gateway (services/gateway) and the
// websocket service (services/websocket).
//
// The tests attack the real public surface: the gateway runs through its
// production entrypoint (gateway.Run) against in-process fake gRPC
// upstreams, and the websocket service runs through ws.NewHandler on an
// httptest server. No external services are needed.
//
// Every test is tied to a finding in docs/security/gateway.md.
package security

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	gws "github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/pkg/metrics"

	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"

	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	"github.com/refleeexzz/RAVEN/services/gateway"
	ws "github.com/refleeexzz/RAVEN/services/websocket"
)

// ---------------------------------------------------------------------------
// Fake upstreams. They speak the real generated gRPC contracts so the
// gateway under test cannot tell them apart from the real services.
// ---------------------------------------------------------------------------

// tokenIdentities is the fake auth service's token table. Anything not
// listed here is an invalid token.
var tokenIdentities = map[string]*genauth.ValidateTokenResponse{
	"token-alice":  {Valid: true, UserId: "user-alice", Email: "alice@example.com", Permissions: []string{"jobs:create", "jobs:read"}},
	"token-reader": {Valid: true, UserId: "user-reader", Email: "reader@example.com", Permissions: []string{"users:read"}},
	"token-admin":  {Valid: true, UserId: "user-admin", Email: "admin@example.com", Permissions: []string{"admin:*"}},
	"token-iso-a":  {Valid: true, UserId: "user-iso-a", Email: "a@example.com", Permissions: []string{"jobs:read"}},
	"token-iso-b":  {Valid: true, UserId: "user-iso-b", Email: "b@example.com", Permissions: []string{"jobs:read"}},
}

type fakeAuth struct {
	genauth.UnimplementedAuthServiceServer
}

func (fakeAuth) ValidateToken(_ context.Context, req *genauth.ValidateTokenRequest) (*genauth.ValidateTokenResponse, error) {
	if id, ok := tokenIdentities[req.GetAccessToken()]; ok {
		return id, nil
	}
	return &genauth.ValidateTokenResponse{Valid: false}, nil
}

func (fakeAuth) Login(context.Context, *genauth.LoginRequest) (*genauth.TokenPair, error) {
	return &genauth.TokenPair{AccessToken: "token-alice", RefreshToken: "refresh-1"}, nil
}

func (fakeAuth) Register(context.Context, *genauth.RegisterRequest) (*genauth.RegisterResponse, error) {
	return &genauth.RegisterResponse{UserId: "user-new", Email: "new@example.com"}, nil
}

func (fakeAuth) Refresh(context.Context, *genauth.RefreshRequest) (*genauth.TokenPair, error) {
	return &genauth.TokenPair{AccessToken: "token-alice", RefreshToken: "refresh-2"}, nil
}

func (fakeAuth) Logout(context.Context, *genauth.LogoutRequest) (*genauth.LogoutResponse, error) {
	return &genauth.LogoutResponse{Ok: true}, nil
}

// capturedCreate records what the gateway forwarded to the jobs service.
type capturedCreate struct {
	idempotencyKey string
	userMetadata   string // x-user-id gRPC metadata the gateway attached
	jobType        string
	payload        string
}

type fakeJobs struct {
	genjobs.UnimplementedJobServiceServer
	mu       sync.Mutex
	creates  []capturedCreate
	getError error // returned by GetJob when set
}

func (f *fakeJobs) CreateJob(ctx context.Context, req *genjobs.CreateJobRequest) (*genjobs.Job, error) {
	var userID string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		userID = strings.Join(md.Get("x-user-id"), ",")
	}
	f.mu.Lock()
	f.creates = append(f.creates, capturedCreate{
		idempotencyKey: req.GetIdempotencyKey(),
		userMetadata:   userID,
		jobType:        req.GetType(),
		payload:        req.GetPayloadJson(),
	})
	f.mu.Unlock()
	return &genjobs.Job{
		Id:          "job-00000000-0000-0000-0000-000000000001",
		Type:        req.GetType(),
		PayloadJson: req.GetPayloadJson(),
		Status:      genjobs.JobStatus_JOB_STATUS_QUEUED,
	}, nil
}

func (f *fakeJobs) GetJob(context.Context, *genjobs.GetJobRequest) (*genjobs.Job, error) {
	f.mu.Lock()
	err := f.getError
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &genjobs.Job{Id: "job-1", Type: "email.send", PayloadJson: `{}`, Status: genjobs.JobStatus_JOB_STATUS_QUEUED}, nil
}

func (f *fakeJobs) ListJobs(context.Context, *genjobs.ListJobsRequest) (*genjobs.ListJobsResponse, error) {
	return &genjobs.ListJobsResponse{
		Jobs: nil,
		Page: &gencommon.PageResponse{Page: 1, PageSize: 20, Total: 0},
	}, nil
}

func (f *fakeJobs) CancelJob(context.Context, *genjobs.CancelJobRequest) (*genjobs.Job, error) {
	return &genjobs.Job{Id: "job-1", Status: genjobs.JobStatus_JOB_STATUS_CANCELLED}, nil
}

func (f *fakeJobs) RequeueJob(context.Context, *genjobs.RequeueJobRequest) (*genjobs.Job, error) {
	return &genjobs.Job{Id: "job-1", Status: genjobs.JobStatus_JOB_STATUS_QUEUED}, nil
}

func (f *fakeJobs) captured() []capturedCreate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capturedCreate(nil), f.creates...)
}

type fakeUsers struct {
	genusers.UnimplementedUserServiceServer
}

func (fakeUsers) GetUser(_ context.Context, req *genusers.GetUserRequest) (*genusers.User, error) {
	return &genusers.User{Id: req.GetId(), Email: "someone@example.com"}, nil
}

func (fakeUsers) ListUsers(context.Context, *genusers.ListUsersRequest) (*genusers.ListUsersResponse, error) {
	return &genusers.ListUsersResponse{
		Users: nil,
		Page:  &gencommon.PageResponse{Page: 1, PageSize: 20, Total: 0},
	}, nil
}

func (fakeUsers) CreateUser(_ context.Context, req *genusers.CreateUserRequest) (*genusers.User, error) {
	return &genusers.User{Id: "user-new", Email: req.GetEmail()}, nil
}

func (fakeUsers) UpdateUser(_ context.Context, req *genusers.UpdateUserRequest) (*genusers.User, error) {
	return &genusers.User{Id: req.GetId(), DisplayName: req.GetDisplayName()}, nil
}

func (fakeUsers) DeleteUser(context.Context, *genusers.DeleteUserRequest) (*genusers.DeleteUserResponse, error) {
	return &genusers.DeleteUserResponse{Ok: true}, nil
}

// ---------------------------------------------------------------------------
// Harness: fake upstreams on ephemeral ports, gateway booted via its real
// Run entrypoint.
// ---------------------------------------------------------------------------

var (
	sharedJobs    *fakeJobs
	sharedAuthURL string
	usersURL      string
	jobsURL       string

	// sharedGateway is the default instance under test, with rate limits set
	// so high they never interfere with functional tests.
	sharedGateway string // base URL, e.g. http://127.0.0.1:PORT
)

func serveGRPC(t *testing.T, register func(s *grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	register(s)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(func() { s.Stop() })
	return lis.Addr().String()
}

func TestMain(m *testing.M) {
	// TestMain cannot use t, so failures here panic — a broken harness is
	// never silent.
	lis := func() net.Listener {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		return l
	}

	boot := func(svc string, register func(s *grpc.Server)) string {
		l := lis()
		s := grpc.NewServer()
		register(s)
		go func() { _ = s.Serve(l) }()
		_ = svc
		return l.Addr().String()
	}

	sharedJobs = &fakeJobs{}
	sharedAuthURL = boot("auth", func(s *grpc.Server) { genauth.RegisterAuthServiceServer(s, fakeAuth{}) })
	usersURL = boot("users", func(s *grpc.Server) { genusers.RegisterUserServiceServer(s, fakeUsers{}) })
	jobsURL = boot("jobs", func(s *grpc.Server) { genjobs.RegisterJobServiceServer(s, sharedJobs) })

	sharedGateway, _ = bootGateway(sharedAuthURL, usersURL, jobsURL, 60000, 100000)

	os.Exit(m.Run())
}

// bootGateway starts a real gateway (production Run path) with fake
// upstreams and returns its base URL and a stop function.
func bootGateway(authAddr, usersAddr, jobsAddr string, rpm, burst int) (string, func()) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	httpAddr := l.Addr().String()
	_ = l.Close() // gateway re-binds it; tiny race is acceptable in tests

	ctx, cancel := context.WithCancel(context.Background())
	cfg := gateway.Config{
		HTTPAddr:       httpAddr,
		AuthGRPCAddr:   authAddr,
		UsersGRPCAddr:  usersAddr,
		JobsGRPCAddr:   jobsAddr,
		WSAddr:         "http://127.0.0.1:1", // dead on purpose: proxy must never be hit by traversal tests
		BrokerOpsAddr:  "http://127.0.0.1:1",
		RedisAddr:      "127.0.0.1:1", // down: gateway runs degraded, still serves
		JWTSecret:      "test-secret",
		LogLevel:       "error",
		RateLimitRPM:   rpm,
		RateLimitBurst: burst,
	}
	go func() { _ = gateway.Run(ctx, cfg) }()

	base := "http://" + httpAddr
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Get(base + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			cancel()
			panic("gateway did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return base, cancel
}

// bootDedicatedGateway starts a fresh gateway for one test (rate-limit tests
// need isolated buckets).
func bootDedicatedGateway(t *testing.T, rpm, burst int) string {
	t.Helper()
	base, cancel := bootGateway(sharedAuthURL, usersURL, jobsURL, rpm, burst)
	t.Cleanup(cancel)
	return base
}

// doReq issues one request against the shared gateway with an optional
// bearer token and body.
func doReq(t *testing.T, method, url, token, body string, headers map[string]string) *http.Response {
	t.Helper()
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	buf := make([]byte, 0, 512)
	tmp := make([]byte, 512)
	for {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
		if len(buf) > 1<<20 {
			t.Fatal("response body unexpectedly large")
		}
	}
	return string(buf)
}

// ---------------------------------------------------------------------------
// Smoke: the harness works and the contract's protected routes reject
// anonymous traffic while public routes answer. (Register: clean bill
// "route table matches contract", docs/security/gateway.md.)
// ---------------------------------------------------------------------------

func TestProtectedRoutesRejectAnonymous(t *testing.T) {
	protected := []struct{ method, path string }{
		{"POST", "/api/auth/logout"},
		{"GET", "/api/users"},
		{"GET", "/api/users/00000000-0000-0000-0000-000000000001"},
		{"POST", "/api/users"},
		{"PUT", "/api/users/00000000-0000-0000-0000-000000000001"},
		{"DELETE", "/api/users/00000000-0000-0000-0000-000000000001"},
		{"POST", "/api/jobs"},
		{"GET", "/api/jobs"},
		{"GET", "/api/jobs/job-1"},
		{"POST", "/api/jobs/job-1/cancel"},
		{"POST", "/api/jobs/job-1/requeue"},
		{"GET", "/api/workers"},
	}
	for _, rt := range protected {
		body := ""
		if rt.method == "POST" || rt.method == "PUT" {
			body = `{}`
		}
		resp := doReq(t, rt.method, sharedGateway+rt.path, "", body, nil)
		b := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s %s without token: got %d (%s), want 401", rt.method, rt.path, resp.StatusCode, b)
		}
		if !strings.Contains(b, "missing_authorization") {
			t.Errorf("%s %s: want missing_authorization envelope, got %s", rt.method, rt.path, b)
		}
	}

	// Public routes must not 401.
	resp := doReq(t, "POST", sharedGateway+"/api/auth/login", "", `{"email":"a@b.c","password":"pw"}`, nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("public login: got %d, want 200", resp.StatusCode)
	}
	readBody(t, resp)

	resp = doReq(t, "GET", sharedGateway+"/api/health/services", "", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("public health aggregate: got %d, want 200", resp.StatusCode)
	}
	readBody(t, resp)
}

// ---------------------------------------------------------------------------
// Websocket harness (black-box via the exported NewHandler, like the
// service's own e2e tests).
// ---------------------------------------------------------------------------

func newWSServer(t *testing.T, allowAnon bool, allowedOrigins []string) (*httptest.Server, *ws.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := metrics.New("websocket-sec-" + strings.ReplaceAll(t.Name(), "/", "-"))
	wm := ws.NewMetrics()
	reg.Register(wm.Collectors()...)
	hub := ws.NewHub(log, wm)
	handler := ws.NewHandler(ws.HandlerConfig{
		Hub:            hub,
		Fanout:         ws.NewFanout(nil, hub, log), // nil Redis: single-node mode
		Presence:       nil,                         // nil-safe: presence disabled
		JWTSecret:      "test-secret",
		AllowAnonymous: allowAnon,
		AllowedOrigins: allowedOrigins,
		Logger:         log,
		Metrics:        reg,
		Health:         health.NewRegistry(time.Second),
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv, hub
}

func makeWSJWT(t *testing.T, sub string) string {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   sub,
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}).SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// dialWSOrigin attempts a handshake with an explicit Origin header
// (origin == "" sends none) and reports whether the upgrade succeeded,
// plus the HTTP status on rejection.
func dialWSOrigin(t *testing.T, srv *httptest.Server, token, origin string) (bool, int) {
	t.Helper()
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?token=" + url.QueryEscape(token)
	hdr := http.Header{}
	if origin != "" {
		hdr.Set("Origin", origin)
	}
	c, resp, err := gws.DefaultDialer.Dial(u, hdr)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
			_ = resp.Body.Close()
		}
		return false, status
	}
	_ = c.Close()
	return true, http.StatusSwitchingProtocols
}

// sameOrigin builds an Origin header value whose host matches the test
// server (the same-origin case a same-site console produces).
func sameOrigin(srv *httptest.Server) string {
	return "http://" + srv.Listener.Addr().String()
}

// ---------------------------------------------------------------------------
// EDGE-01: cross-site WebSocket hijacking. Before the fix the upgrader's
// CheckOrigin allowed every origin, so any web page could open an
// authenticated socket through a victim's browser once it knew a token
// (tokens live in the URL query, hence in browser history and proxy logs).
// The fix denies cross-origin handshakes that present a real JWT unless the
// origin is allowlisted via WS_ALLOWED_ORIGINS; anonymous dev tokens stay
// permissive so local demos keep working.
// ---------------------------------------------------------------------------

func TestWSCrossOriginJWTDenied(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)

	tok := makeWSJWT(t, "user-victim")
	ok, status := dialWSOrigin(t, srv, tok, "http://evil.example")
	if ok {
		t.Fatal("cross-origin handshake with a real JWT must be rejected (EDGE-01)")
	}
	if status != http.StatusForbidden {
		t.Fatalf("rejection status: got %d, want 403", status)
	}
}

func TestWSSameOriginAndNonBrowserAllowed(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)
	tok := makeWSJWT(t, "user-legit")

	if ok, status := dialWSOrigin(t, srv, tok, sameOrigin(srv)); !ok {
		t.Errorf("same-origin JWT handshake must keep working (status %d)", status)
	}
	if ok, status := dialWSOrigin(t, srv, tok, ""); !ok {
		t.Errorf("non-browser client (no Origin header) must keep working (status %d)", status)
	}
}

func TestWSCrossOriginAnonAllowedInDevMode(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)
	// Anonymous tokens are the documented local-demo path: they stay
	// connectable cross-origin when no allowlist is configured.
	if ok, status := dialWSOrigin(t, srv, "anon-demo", "http://evil.example"); !ok {
		t.Errorf("anon dev token cross-origin must stay allowed (status %d)", status)
	}
}

func TestWSAllowedOriginsAllowlist(t *testing.T) {
	srv, _ := newWSServer(t, true, []string{"http://console.example"})

	tok := makeWSJWT(t, "user-victim")
	if ok, status := dialWSOrigin(t, srv, tok, "http://console.example"); !ok {
		t.Errorf("allowlisted origin must connect (status %d)", status)
	}
	// With an allowlist configured the policy is strict for everyone,
	// including anonymous tokens.
	if ok, _ := dialWSOrigin(t, srv, "anon-demo", "http://evil.example"); ok {
		t.Error("non-allowlisted origin must be denied once an allowlist exists")
	}
	if ok, status := dialWSOrigin(t, srv, tok, "https://console.example"); ok {
		t.Errorf("scheme downgrade must not match the allowlist (status %d)", status)
	}
}

// ---------------------------------------------------------------------------
// EDGE-02: the edge served no browser-security headers at all. API responses
// are JSON, but without X-Content-Type-Options a browser can be tricked into
// sniffing a response as HTML on some paths, and without frame/Referrer
// policy the error envelope leaks the full URL (including ?token=) via the
// Referer header. The fix sets a strict baseline on every response.
// ---------------------------------------------------------------------------

var requiredSecurityHeaders = map[string]string{
	"X-Content-Type-Options":  "nosniff",
	"X-Frame-Options":         "DENY",
	"Referrer-Policy":         "no-referrer",
	"Content-Security-Policy": "default-src 'none'",
}

func assertSecurityHeaders(t *testing.T, where string, h http.Header) {
	t.Helper()
	for k, want := range requiredSecurityHeaders {
		if got := h.Get(k); got != want {
			t.Errorf("%s: header %s = %q, want %q", where, k, got, want)
		}
	}
}

func TestGatewaySecurityHeaders(t *testing.T) {
	// Ops endpoint.
	resp := doReq(t, "GET", sharedGateway+"/health", "", "", nil)
	assertSecurityHeaders(t, "GET /health", resp.Header)
	readBody(t, resp)

	// Error path (401 from AuthN middleware).
	resp = doReq(t, "GET", sharedGateway+"/api/jobs", "", "", nil)
	assertSecurityHeaders(t, "401 /api/jobs", resp.Header)
	readBody(t, resp)

	// CORS preflight (answered by the outermost middleware).
	resp = doReq(t, "OPTIONS", sharedGateway+"/api/jobs", "", "", map[string]string{
		"Origin": "http://console.example",
	})
	assertSecurityHeaders(t, "OPTIONS preflight", resp.Header)
	readBody(t, resp)
}

func TestWebsocketSecurityHeaders(t *testing.T) {
	srv, _ := newWSServer(t, false, nil)

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	assertSecurityHeaders(t, "ws GET /health", resp.Header)
	readBody(t, resp)

	resp, err = http.Get(srv.URL + "/debug/stats")
	if err != nil {
		t.Fatal(err)
	}
	assertSecurityHeaders(t, "ws GET /debug/stats", resp.Header)
	readBody(t, resp)
}

// ---------------------------------------------------------------------------
// EDGE-03: the gateway forwarded the Idempotency-Key verbatim, so the jobs
// service deduped on the bare key: replaying a key with a DIFFERENT payload
// returned the original job — the caller believes it enqueued B while the
// platform runs A. The gateway now binds the key to a SHA-256 fingerprint
// of the full create semantics (type, payload, priority, max_attempts)
// before forwarding, so same-key+same-payload still collapses to one job
// while same-key+different-payload can never return the wrong job.
// ---------------------------------------------------------------------------

func TestIdempotencyKeyBoundToPayload(t *testing.T) {
	sharedJobs.mu.Lock()
	sharedJobs.creates = nil
	sharedJobs.mu.Unlock()

	create := func(key, payload string) {
		t.Helper()
		resp := doReq(t, "POST", sharedGateway+"/api/jobs", "token-alice",
			`{"type":"email.send","payload":`+payload+`}`,
			map[string]string{"Idempotency-Key": key})
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create with key %q payload %s: got %d", key, payload, resp.StatusCode)
		}
		readBody(t, resp)
	}

	create("order-42", `{"to":"a@b.c"}`)
	create("order-42", `{"to":"mallory@evil.example"}`) // same key, new payload
	create("order-42", `{"to":"a@b.c"}`)                // exact retry of the first

	got := sharedJobs.captured()
	if len(got) != 3 {
		t.Fatalf("fake jobs saw %d creates, want 3", len(got))
	}
	first, second, third := got[0], got[1], got[2]

	if first.idempotencyKey == "" || first.idempotencyKey == "order-42" {
		t.Errorf("forwarded key must be bound to the payload, got verbatim %q", first.idempotencyKey)
	}
	if first.idempotencyKey == second.idempotencyKey {
		t.Errorf("same key with a different payload forwarded the same upstream key %q — "+
			"the jobs service would wrongly return the original job", first.idempotencyKey)
	}
	if first.idempotencyKey != third.idempotencyKey {
		t.Errorf("exact retry must stay idempotent: keys %q vs %q", first.idempotencyKey, third.idempotencyKey)
	}
	// The upstream key stays within the jobs service's 255-char limit and
	// keeps the client key readable for debugging.
	if len(first.idempotencyKey) > 255 {
		t.Errorf("forwarded key exceeds 255 chars: %d", len(first.idempotencyKey))
	}
	if !strings.HasPrefix(first.idempotencyKey, "order-42") {
		t.Errorf("forwarded key should keep the client key as prefix, got %q", first.idempotencyKey)
	}
}

func TestIdempotencyKeyAbsentStaysEmpty(t *testing.T) {
	sharedJobs.mu.Lock()
	sharedJobs.creates = nil
	sharedJobs.mu.Unlock()

	resp := doReq(t, "POST", sharedGateway+"/api/jobs", "token-alice",
		`{"type":"email.send","payload":{}}`, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create without key: got %d", resp.StatusCode)
	}
	readBody(t, resp)

	got := sharedJobs.captured()
	if len(got) != 1 || got[0].idempotencyKey != "" {
		t.Fatalf("no Idempotency-Key header must forward an empty key, got %+v", got)
	}
}

func TestIdempotencyLongKeyStillBounded(t *testing.T) {
	sharedJobs.mu.Lock()
	sharedJobs.creates = nil
	sharedJobs.mu.Unlock()

	longKey := strings.Repeat("k", 300) // over the jobs service's 255-char cap
	resp := doReq(t, "POST", sharedGateway+"/api/jobs", "token-alice",
		`{"type":"email.send","payload":{}}`,
		map[string]string{"Idempotency-Key": longKey})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create with 300-char key: got %d (gateway must not push keys over the upstream cap)", resp.StatusCode)
	}
	readBody(t, resp)
	got := sharedJobs.captured()
	if len(got) != 1 || len(got[0].idempotencyKey) > 255 {
		t.Fatalf("forwarded key must stay within the 255-char contract, got %d chars", len(got[0].idempotencyKey))
	}
}

// ---------------------------------------------------------------------------
// Clean-bill verifications. These assert security properties that already
// hold; they exist so a future refactor cannot quietly break them. Each maps
// to an "info / clean" entry in docs/security/gateway.md.
// ---------------------------------------------------------------------------

// Authorization: the route table's per-route permission is enforced, the
// admin wildcard works, and "authenticated-only" (permAuthenticated) needs
// any valid token but no specific permission.
func TestPermissionEnforcedPerRoute(t *testing.T) {
	cases := []struct {
		name, method, path, token, body string
		want                            int
	}{
		{"reader can list users", "GET", "/api/users", "token-reader", "", http.StatusOK},
		{"reader cannot delete users", "DELETE", "/api/users/u-1", "token-reader", "", http.StatusForbidden},
		{"reader cannot create jobs", "POST", "/api/jobs", "token-reader", `{"type":"t","payload":{}}`, http.StatusForbidden},
		{"admin wildcard deletes users", "DELETE", "/api/users/u-1", "token-admin", "", http.StatusOK},
		{"jobs:create can create jobs", "POST", "/api/jobs", "token-alice", `{"type":"t","payload":{}}`, http.StatusCreated},
		{"jobs:read can list jobs", "GET", "/api/jobs", "token-alice", "", http.StatusOK},
		{"logout needs only a valid token", "POST", "/api/auth/logout", "token-reader", `{"refresh_token":"r"}`, http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := doReq(t, c.method, sharedGateway+c.path, c.token, c.body, nil)
			b := readBody(t, resp)
			if resp.StatusCode != c.want {
				t.Fatalf("%s %s: got %d (%s), want %d", c.method, c.path, resp.StatusCode, b, c.want)
			}
			if c.want == http.StatusForbidden && !strings.Contains(b, "forbidden") {
				t.Fatalf("403 must carry the forbidden envelope, got %s", b)
			}
		})
	}
}

// BOLA at the edge: the gateway builds upstream identity (x-user-id gRPC
// metadata) from the validated token only. A client-supplied X-User-ID
// header must be ignored. Owner-scoping of job reads lives in the jobs
// service (jobs auditor); this pins the gateway's side of the contract.
func TestClientSuppliedUserIDHeaderIgnored(t *testing.T) {
	sharedJobs.mu.Lock()
	sharedJobs.creates = nil
	sharedJobs.mu.Unlock()

	resp := doReq(t, "POST", sharedGateway+"/api/jobs", "token-alice",
		`{"type":"t","payload":{}}`, map[string]string{"X-User-ID": "user-mallory"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: got %d", resp.StatusCode)
	}
	readBody(t, resp)

	got := sharedJobs.captured()
	if len(got) != 1 {
		t.Fatalf("fake jobs saw %d creates, want 1", len(got))
	}
	if got[0].userMetadata != "user-alice" {
		t.Fatalf("upstream x-user-id = %q, want %q — client header must not override the token identity",
			got[0].userMetadata, "user-alice")
	}
}

// CORS: Allow-Origin "*" is only acceptable because credentials are never
// attached (Bearer header auth, no cookies). Pin the invariant: the
// Access-Control-Allow-Credentials header must never appear — "*" +
// credentials is the critical misconfiguration.
func TestCORSPreflightNeverAllowsCredentials(t *testing.T) {
	resp := doReq(t, "OPTIONS", sharedGateway+"/api/jobs", "", "", map[string]string{
		"Origin":                         "http://evil.example",
		"Access-Control-Request-Method":  "POST",
		"Access-Control-Request-Headers": "Authorization",
	})
	readBody(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("preflight: got %d, want 204", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Allow-Origin = %q, want *", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("Allow-Credentials must never be emitted with Allow-Origin *, got %q", got)
	}
}

// Payload size: every JSON body endpoint is capped at 1 MiB.
func TestBodySizeCap(t *testing.T) {
	big := `{"email":"` + strings.Repeat("a", 2<<20) + `","password":"x"}`
	resp := doReq(t, "POST", sharedGateway+"/api/auth/login", "", big, nil)
	b := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(b, "invalid_json") {
		t.Fatalf("2 MiB body: got %d (%s...), want 400 invalid_json", resp.StatusCode, b[:min(80, len(b))])
	}
}

// JSON bomb: nesting depth is capped by encoding/json (max depth 10000), so
// a deeply nested job payload is rejected at decode time, before reaching
// the upstream.
func TestDeeplyNestedPayloadRejected(t *testing.T) {
	sharedJobs.mu.Lock()
	sharedJobs.creates = nil
	sharedJobs.mu.Unlock()

	deep := strings.Repeat("[", 50000) + strings.Repeat("]", 50000) // ~100 KB, way over depth cap
	resp := doReq(t, "POST", sharedGateway+"/api/jobs", "token-alice",
		`{"type":"t","payload":`+deep+`}`, nil)
	b := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("deeply nested payload: got %d, want 400", resp.StatusCode)
	}
	if !strings.Contains(b, "invalid_json") {
		t.Fatalf("want invalid_json, got %s", b[:min(120, len(b))])
	}
	if got := sharedJobs.captured(); len(got) != 0 {
		t.Fatalf("upstream must never see the bomb, saw %d creates", len(got))
	}
}

// ---------------------------------------------------------------------------
// EDGE-04: path confusion at the /ws proxy mount. Raw ".." segments are
// cleaned by ServeMux with a 307 before routing — but ENCODED dot-segments
// (%2e%2e) bypass the cleaning and were proxied verbatim to the websocket
// service. Nothing sensitive answered today (the ws mux 404s them), yet the
// "everything under /ws is forwarded as-is" invariant made any future ws
// subtree route silently reachable through the gateway. The gateway now
// rejects /ws requests whose decoded path contains a ".." segment with 400.
// ---------------------------------------------------------------------------

// Path confusion: ../ sequences (raw or encoded) aimed at the /ws proxy
// mount are neutralized by the gateway — cleaned (307) or rejected (400) —
// and never proxied. The ws upstream in this suite points at a dead port,
// so any proxy hit would surface as 503 websocket_upstream_unavailable.
func TestProxyPathTraversalIsContained(t *testing.T) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	// Encoded traversal must be rejected outright with 400 invalid_path.
	for _, path := range []string{
		"/ws/%2e%2e/api/jobs",
		"/ws/..%2fapi/jobs",
		"/ws/%2E%2E/debug/stats",
		"/ws/x/%2e%2e/%2e%2e/metrics",
	} {
		resp, err := client.Get(sharedGateway + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		b := readBody(t, resp)
		if strings.Contains(b, "websocket_upstream_unavailable") {
			t.Errorf("GET %s reached the websocket proxy — encoded traversal escaped the mount", path)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s: got %d, want 400 invalid_path", path, resp.StatusCode)
		}
	}

	// Raw traversal is cleaned by ServeMux: a 307 to the cleaned path, which
	// the gateway itself answers (401 for /api/jobs, 404 for unknown).
	for _, path := range []string{
		"/ws/../api/jobs",
		"/api/../debug/stats",
		"/ws//../metrics",
	} {
		resp, err := client.Get(sharedGateway + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		b := readBody(t, resp)
		if strings.Contains(b, "websocket_upstream_unavailable") {
			t.Errorf("GET %s reached the websocket proxy", path)
		}
		if resp.StatusCode != http.StatusTemporaryRedirect && resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: got %d, want 307 or 404", path, resp.StatusCode)
		}
	}
}

// rawRequest sends one literal HTTP/1.1 request over a fresh TCP connection
// and parses the response. Used to attack the HTTP parser itself.
func rawRequest(t *testing.T, baseURL, raw string) *http.Response {
	t.Helper()
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := fmt.Fprint(conn, raw); err != nil {
		t.Fatalf("write: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp
}

// CRLF / response splitting: the request ID is echoed into a response
// header. Attack the echo with CR/LF/NUL in X-Request-ID — Go's HTTP parser
// must reject or neutralize them, and no attacker header may appear in the
// response.
func TestRequestIDEchoCannotSplitResponse(t *testing.T) {
	attacks := []struct{ name, raw string }{
		{"obs-fold", "GET /health HTTP/1.1\r\nHost: x\r\nX-Request-ID: a\r\n b\r\nConnection: close\r\n\r\n"},
		{"bare CR in value", "GET /health HTTP/1.1\r\nHost: x\r\nX-Request-ID: a\rb\r\nConnection: close\r\n\r\n"},
		{"NUL in value", "GET /health HTTP/1.1\r\nHost: x\r\nX-Request-ID: a\x00b\r\nConnection: close\r\n\r\n"},
	}
	for _, at := range attacks {
		t.Run(at.name, func(t *testing.T) {
			resp := rawRequest(t, sharedGateway, at.raw)
			b := readBody(t, resp)
			if resp.StatusCode == http.StatusBadRequest {
				return // parser rejected it outright — ideal
			}
			// If the server answered, the echo must be clean.
			echo := resp.Header.Get("X-Request-ID")
			if strings.ContainsAny(echo, "\r\n\x00") {
				t.Fatalf("echoed X-Request-ID contains control bytes: %q", echo)
			}
			if resp.Header.Get("X-Injected") != "" || strings.Contains(b, "X-Injected") {
				t.Fatal("attacker header appeared in the response")
			}
		})
	}

	// Sanity: a normal request ID round-trips.
	resp := rawRequest(t, sharedGateway,
		"GET /health HTTP/1.1\r\nHost: x\r\nX-Request-ID: req-123\r\nConnection: close\r\n\r\n")
	readBody(t, resp)
	if got := resp.Header.Get("X-Request-ID"); got != "req-123" {
		t.Fatalf("legit request ID echo: got %q", got)
	}
}

// Request smuggling: Go's net/http rejects requests that carry both
// Content-Length and Transfer-Encoding, closing the classic TE.CL desync
// at the edge itself.
func TestTransferEncodingPlusContentLengthRejected(t *testing.T) {
	resp := rawRequest(t, sharedGateway,
		"POST /api/auth/login HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n")
	readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("TE+CL request: got %d, want 400", resp.StatusCode)
	}
}

// XSS surface: reflected user input in the error envelope is JSON-escaped
// (encoding/json escapes <, >, & by default) and always served as
// application/json.
func TestReflectedInputIsJSONEscaped(t *testing.T) {
	resp := doReq(t, "GET", sharedGateway+"/api/jobs?status=<script>alert(1)</script>", "token-alice", "", nil)
	b := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad status filter: got %d, want 400", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("error Content-Type = %q, want application/json", ct)
	}
	if strings.Contains(b, "<script>") {
		t.Fatalf("reflected input not escaped: %s", b)
	}
	if !strings.Contains(b, "unknown job status") {
		t.Fatalf("envelope should carry the message, got %s", b)
	}
}

// Stack trace / internal detail disclosure: raw gRPC errors (which can
// contain dial errors and internal addresses) must never reach the client;
// the envelope carries the generic text for the mapped kind.
func TestUpstreamInternalsNeverLeak(t *testing.T) {
	sharedJobs.mu.Lock()
	sharedJobs.getError = status.Error(codes.Internal, "dial tcp 10.99.0.5:5432: connection refused")
	sharedJobs.mu.Unlock()
	defer func() {
		sharedJobs.mu.Lock()
		sharedJobs.getError = nil
		sharedJobs.mu.Unlock()
	}()

	resp := doReq(t, "GET", sharedGateway+"/api/jobs/job-1", "token-alice", "", nil)
	b := readBody(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("internal upstream error: got %d, want 500", resp.StatusCode)
	}
	if strings.Contains(b, "10.99.0.5") || strings.Contains(b, "connection refused") {
		t.Fatalf("internal detail leaked to client: %s", b)
	}
	if !strings.Contains(b, "internal error") {
		t.Fatalf("client should see the generic message, got %s", b)
	}
}

// Rate limiting: the public-route limiter keys on the real peer address
// (RemoteAddr). A spoofed X-Forwarded-For per request must NOT mint fresh
// buckets — there is no trusted-proxy mode, so XFF is ignored by design.
func TestRateLimitIgnoresXForwardedFor(t *testing.T) {
	gw := bootDedicatedGateway(t, 60, 4) // 1 token/s, burst 4

	body := `{"email":"a@b.c","password":"pw"}`
	for i := 0; i < 4; i++ {
		resp := doReq(t, "POST", gw+"/api/auth/login", "", body, map[string]string{
			"X-Forwarded-For": fmt.Sprintf("10.%d.%d.%d", i, i, i),
		})
		readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: got %d, want 200 (burst)", i+1, resp.StatusCode)
		}
	}
	resp := doReq(t, "POST", gw+"/api/auth/login", "", body, map[string]string{
		"X-Forwarded-For": "10.255.255.255",
	})
	b := readBody(t, resp)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("XFF spoofing minted a fresh bucket: got %d, want 429", resp.StatusCode)
	}
	if !strings.Contains(b, "rate_limited") {
		t.Fatalf("want rate_limited envelope, got %s", b)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}
}

// Rate-limit race: a concurrent burst at the bucket boundary must never
// over-issue tokens. Run under -race with the rest of the suite.
func TestRateLimitConcurrentBurstExact(t *testing.T) {
	const burst = 4
	const total = 30
	gw := bootDedicatedGateway(t, 60, burst) // 1 token/s refill: negligible within the burst

	var okCount, limitedCount atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(gw+"/api/auth/login", "application/json",
				strings.NewReader(`{"email":"a@b.c","password":"pw"}`))
			if err != nil {
				return // a transport error is not a pass
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				okCount.Add(1)
			} else if resp.StatusCode == http.StatusTooManyRequests {
				limitedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	// Exact allowance is burst + refill during the burst window; anything
	// meaningfully above it means tokens were issued twice.
	if got := okCount.Load(); got > burst+2 {
		t.Fatalf("bucket issued %d tokens for a burst of %d — non-atomic take", got, burst)
	}
	if okCount.Load()+limitedCount.Load() < total/2 {
		t.Fatalf("too many transport errors: ok=%d limited=%d of %d", okCount.Load(), limitedCount.Load(), total)
	}
}

// Rate limiting: authenticated users get their own bucket — exhausting one
// user's budget must not affect another user on the same IP.
func TestRateLimitPerUserIsolation(t *testing.T) {
	gw := bootDedicatedGateway(t, 60, 4)

	for i := 0; i < 4; i++ {
		resp := doReq(t, "GET", gw+"/api/jobs", "token-iso-a", "", nil)
		readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("iso-a request %d: got %d, want 200", i+1, resp.StatusCode)
		}
	}
	resp := doReq(t, "GET", gw+"/api/jobs", "token-iso-a", "", nil)
	readBody(t, resp)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("iso-a over budget: got %d, want 429", resp.StatusCode)
	}
	resp = doReq(t, "GET", gw+"/api/jobs", "token-iso-b", "", nil)
	readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("iso-b must be unaffected by iso-a's budget: got %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Websocket clean bills: protocol-level authorization and DoS guards.
// ---------------------------------------------------------------------------

func dialWS(t *testing.T, srv *httptest.Server, token string) *gws.Conn {
	t.Helper()
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?token=" + url.QueryEscape(token)
	c, resp, err := gws.DefaultDialer.Dial(u, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial %q: %v (status %d)", token, err, status)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func sendWS(t *testing.T, c *gws.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := c.WriteMessage(gws.TextMessage, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readWS(t *testing.T, c *gws.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("frame not JSON: %v (%s)", err, raw)
	}
	return m
}

// Authorization: the reserved user:<id> rooms (where owner-targeted job
// events land) reject client join/leave/publish. Case variants are not the
// reserved prefix but are also disconnected from owner delivery (rooms are
// exact strings), so they grant nothing.
func TestWSReservedRoomsRejectClients(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)
	alice := dialWS(t, srv, "anon-alice")

	for _, frame := range []map[string]any{
		{"op": "join", "room": "user:bob"},
		{"op": "leave", "room": "user:bob"},
		{"op": "msg", "room": "user:bob", "data": map[string]any{"x": 1}},
	} {
		sendWS(t, alice, frame)
		got := readWS(t, alice)
		if got["op"] != "error" {
			t.Fatalf("%v: got %v, want an error frame", frame, got)
		}
	}
}

// Message injection: the server overwrites sender identity. A client-supplied
// "from" field is ignored, and the delivered frame carries the real
// authenticated user.
func TestWSCannotForgeSenderIdentity(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)
	alice := dialWS(t, srv, "anon-alice")
	bob := dialWS(t, srv, "anon-bob")

	sendWS(t, alice, map[string]any{
		"op": "dm", "to": "bob", "from": "mallory",
		"data": map[string]any{"hi": true},
	})
	got := readWS(t, bob)
	if got["op"] != "msg" || got["from"] != "alice" {
		t.Fatalf("dm must carry the real sender: %v", got)
	}
}

// Message injection: a client cannot emit "event" frames (the op reserved
// for platform job events). The op is rejected; and a room message whose
// payload imitates a job event is still delivered as op=msg with a sender,
// never as op=event.
func TestWSClientCannotForgeEvents(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)
	alice := dialWS(t, srv, "anon-alice")
	bob := dialWS(t, srv, "anon-bob")

	sendWS(t, bob, map[string]any{"op": "join", "room": "jobs"})
	if got := readWS(t, bob); got["op"] != "joined" {
		t.Fatalf("bob join: %v", got)
	}
	sendWS(t, alice, map[string]any{"op": "join", "room": "jobs"})
	if got := readWS(t, alice); got["op"] != "joined" {
		t.Fatalf("alice join: %v", got)
	}

	sendWS(t, alice, map[string]any{"op": "event", "room": "jobs", "data": map[string]any{"type": "job_status"}})
	if got := readWS(t, alice); got["op"] != "error" {
		t.Fatalf("client event op must be rejected: %v", got)
	}

	sendWS(t, alice, map[string]any{
		"op": "msg", "room": "jobs",
		"data": map[string]any{"type": "job_status", "status": "DEAD", "job_id": "job-fake"},
	})
	got := readWS(t, bob)
	if got["op"] != "msg" {
		t.Fatalf("imitation event must arrive as op=msg, got %v", got)
	}
	if got["from"] != "alice" {
		t.Fatalf("imitation event must carry a sender: %v", got)
	}
}

// DoS: inbound frames are capped at 32 KiB; the server closes the
// connection (close code 1009) on an oversized frame.
func TestWSFrameSizeCapEnforced(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)
	alice := dialWS(t, srv, "anon-alice")

	big := make([]byte, 64*1024)
	for i := range big {
		big[i] = 'a'
	}
	if err := alice.WriteMessage(gws.TextMessage, big); err != nil {
		// The server hit the read limit mid-frame and reset the connection
		// before the client finished writing — the cap fired even earlier.
		t.Logf("oversized write cut short by the server (cap enforced): %v", err)
		return
	}
	_ = alice.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, _, err := alice.ReadMessage()
		if err == nil {
			continue
		}
		var ce *gws.CloseError
		if errors.As(err, &ce) {
			if ce.Code != gws.CloseMessageTooBig {
				t.Fatalf("close code = %d, want 1009 (message too big)", ce.Code)
			}
			return
		}
		// A bare read error (connection dropped) is also an acceptable
		// enforcement signal.
		return
	}
}

// DoS: the per-connection message budget (20/s sustained, burst 40) yields
// error frames, not silent acceptance.
func TestWSMessageRateLimitEnforced(t *testing.T) {
	srv, _ := newWSServer(t, true, nil)
	alice := dialWS(t, srv, "anon-alice")

	const sent = 45 // burst is 40, refill 20/s — the last frames must be refused
	for i := 0; i < sent; i++ {
		sendWS(t, alice, map[string]any{"op": "ping"})
	}
	pongs, errors := 0, 0
	for i := 0; i < sent; i++ {
		got := readWS(t, alice)
		switch got["op"] {
		case "pong":
			pongs++
		case "error":
			errors++
			if !strings.Contains(fmt.Sprint(got["message"]), "rate limit") {
				t.Fatalf("unexpected error frame: %v", got)
			}
		default:
			t.Fatalf("unexpected frame: %v", got)
		}
	}
	if errors == 0 {
		t.Fatal("45 instant pings against a burst of 40 produced no rate-limit error")
	}
	if pongs > 40 {
		t.Fatalf("bucket over-issued: %d pongs for a burst of 40", pongs)
	}
}
