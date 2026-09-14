// Package security holds the offensive security regression suite. These
// tests run with the default build tag and need no external services:
// targets are httptest servers, fake resolvers and pure functions.
//
// Every test here pins a finding from docs/security/jobs.md.
package security

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	apperrors "github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/services/jobs"
	"github.com/refleeexzz/RAVEN/services/worker"
)

const kindNotFound = apperrors.KindNotFound

func kindOf(err error) apperrors.Kind { return apperrors.KindOf(err) }

func codeOf(err error) string { return apperrors.CodeOf(err) }

func pageReq(page, size int32) *gencommon.PageRequest {
	return &gencommon.PageRequest{Page: page, PageSize: size}
}

// Fixed UUIDs: ownerFromMetadata-style parsing requires real UUIDs.
const (
	userA = "11111111-1111-1111-1111-111111111111"
	userB = "22222222-2222-2222-2222-222222222222"
)

func permanentErr(t *testing.T, err error) *worker.PermanentError {
	t.Helper()
	var pe *worker.PermanentError
	if !errors.As(err, &pe) {
		t.Fatalf("want PermanentError, got %T: %v", err, err)
	}
	return pe
}

// ---------------------------------------------------------------------------
// JOBS-01: SSRF — the webhook handler must not reach internal targets.
// ---------------------------------------------------------------------------

// Literal-IP targets need no DNS, so this table is deterministic offline.
func TestWebhookBlocksPrivateAndReservedTargets(t *testing.T) {
	guard := worker.NewEgressGuard(false)
	h := worker.WebhookHandler(guard.HTTPClient(2*time.Second), guard)

	targets := []string{
		"http://127.0.0.1:9101/topics",             // broker ops
		"http://127.0.0.1/",                        // loopback
		"http://10.0.0.5/",                         // private A
		"http://172.16.0.1/",                       // private B
		"http://172.31.255.255/",                   // private B edge
		"http://192.168.1.1/",                      // private C
		"http://169.254.169.254/latest/meta-data/", // cloud metadata (link-local)
		"http://[::1]/",                            // v6 loopback
		"http://[fe80::1]/",                        // v6 link-local
		"http://[fc00::1]/",                        // v6 unique-local
		"http://0.0.0.0/",                          // unspecified
		"http://100.64.0.1/",                       // CGNAT
		"http://198.18.0.1/",                       // benchmarking
		"http://224.0.0.1/",                        // multicast
		"http://240.0.0.1/",                        // reserved
		"http://[::ffff:127.0.0.1]/",               // v4-mapped v6 loopback
		"http://[::ffff:a9fe:a9fe]/",               // v4-mapped metadata
		"https://user:pw@127.0.0.1/",               // userinfo must be rejected
	}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			job := &jobs.Job{ID: "job_ssrf", Payload: `{"url":"` + target + `"}`}
			err := h(context.Background(), job)
			permanentErr(t, err)
		})
	}
}

func TestWebhookBlocksNonHTTPSchemes(t *testing.T) {
	guard := worker.NewEgressGuard(false)
	h := worker.WebhookHandler(guard.HTTPClient(2*time.Second), guard)

	for _, target := range []string{
		"file:///etc/passwd",
		"ftp://example.com/x",
		"gopher://127.0.0.1:6379/_INFO", // classic redis smuggling shape
		"127.0.0.1:9101",                // scheme-less
	} {
		t.Run(target, func(t *testing.T) {
			job := &jobs.Job{ID: "job_scheme", Payload: `{"url":"` + target + `"}`}
			permanentErr(t, h(context.Background(), job))
		})
	}
}

// fakeResolver maps hostnames to IPs deterministically, no DNS involved.
func fakeResolver(table map[string][]net.IP) func(context.Context, string) ([]net.IP, error) {
	return func(_ context.Context, host string) ([]net.IP, error) {
		if ips, ok := table[host]; ok {
			return ips, nil
		}
		return nil, &net.DNSError{Err: "no such host", Name: host}
	}
}

func TestEgressGuardResolution(t *testing.T) {
	public := net.ParseIP("93.184.216.34")
	guard := worker.NewEgressGuard(false).WithResolver(fakeResolver(map[string][]net.IP{
		"public.example":     {public},
		"internal.example":   {net.ParseIP("10.1.2.3")},
		"metadata.example":   {net.ParseIP("169.254.169.254")},
		"mixed.example":      {public, net.ParseIP("192.168.0.1")}, // one bad IP = blocked
		"v6internal.example": {net.ParseIP("fd00::1")},
		"decimal.example":    {net.ParseIP("127.0.0.1")}, // "2130706433" style trick
		"2130706433.example": {net.ParseIP("127.0.0.1")},
	}))

	allowed := []string{"http://public.example/hook", "https://public.example:8443/hook"}
	for _, u := range allowed {
		if err := guard.CheckURL(context.Background(), u); err != nil {
			t.Errorf("Check(%s): want allowed, got %v", u, err)
		}
	}

	blocked := []string{
		"http://internal.example/",
		"http://metadata.example/latest/meta-data",
		"http://mixed.example/",
		"http://v6internal.example/",
		"http://2130706433.example/", // resolves to loopback
		"http://decimal.example/",
	}
	for _, u := range blocked {
		err := guard.CheckURL(context.Background(), u)
		if err == nil {
			t.Errorf("Check(%s): want blocked, got nil", u)
			continue
		}
		if !worker.IsBlockedTarget(err) {
			t.Errorf("Check(%s): want blocked-target error, got %v", u, err)
		}
	}

	// DNS failure is retryable (transient), NOT a permanent block.
	err := guard.CheckURL(context.Background(), "http://unknown.invalid/")
	if err == nil || worker.IsBlockedTarget(err) {
		t.Errorf("DNS failure must be a retryable non-blocked error, got %v", err)
	}
}

func TestEgressGuardRedirectPolicy(t *testing.T) {
	guard := worker.NewEgressGuard(false).WithResolver(fakeResolver(map[string][]net.IP{
		"public.example": {net.ParseIP("93.184.216.34")},
	}))
	mkReq := func(raw string) *http.Request {
		req, err := http.NewRequest(http.MethodPost, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		return req
	}

	// Redirect to a public host: fine.
	if err := guard.CheckRedirect(mkReq("https://public.example/next"),
		[]*http.Request{mkReq("https://public.example/start")}); err != nil {
		t.Errorf("public redirect: want allowed, got %v", err)
	}

	// Redirect to a private/link-local target: blocked (the classic bypass).
	err := guard.CheckRedirect(mkReq("http://169.254.169.254/latest/meta-data"),
		[]*http.Request{mkReq("https://public.example/start")})
	if err == nil || !worker.IsBlockedTarget(err) {
		t.Errorf("redirect to metadata: want blocked-target, got %v", err)
	}

	// Redirect budget: at most MaxWebhookRedirects hops.
	via := make([]*http.Request, worker.MaxWebhookRedirects)
	for i := range via {
		via[i] = mkReq("https://public.example/hop")
	}
	err = guard.CheckRedirect(mkReq("https://public.example/too-far"), via)
	if !errors.Is(err, worker.ErrRedirectLimit) {
		t.Errorf("redirect #%d: want ErrRedirectLimit, got %v", worker.MaxWebhookRedirects+1, err)
	}
}

// A real httptest server IS on loopback: with the default guard the handler
// must refuse before a single byte is sent.
func TestWebhookHandlerBlocksLoopbackServerByDefault(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	guard := worker.NewEgressGuard(false)
	h := worker.WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	job := &jobs.Job{ID: "job_loop", Payload: `{"url":"` + srv.URL + `"}`}

	permanentErr(t, h(context.Background(), job))
	if got := hits.Load(); got != 0 {
		t.Errorf("server was hit %d times; blocked requests must never leave the worker", got)
	}
}

// The dev/test escape hatch: WORKER_WEBHOOK_ALLOW_PRIVATE=true semantics,
// here flipped explicitly on the guard instead of the process env.
func TestWebhookHandlerAllowsPrivateWhenEnabled(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	guard := worker.NewEgressGuard(true)
	h := worker.WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	job := &jobs.Job{ID: "job_ok", Payload: `{"url":"` + srv.URL + `"}`}

	if err := h(context.Background(), job); err != nil {
		t.Fatalf("private target with allow-private guard: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("hits: got %d, want 1", got)
	}
}

// The redirect cap holds even when private targets are allowed.
func TestWebhookRedirectsCapped(t *testing.T) {
	var hits atomic.Int64
	var last *httptest.Server
	mux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/final" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, last.URL+"/next-"+r.URL.Path, http.StatusFound)
	})
	last = httptest.NewServer(mux)
	defer last.Close()

	guard := worker.NewEgressGuard(true)
	h := worker.WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	job := &jobs.Job{ID: "job_redir", Payload: `{"url":"` + last.URL + `/start"}`}

	// Infinite redirect chain: the cap turns it into a permanent failure
	// instead of burning every retry on a loop.
	permanentErr(t, h(context.Background(), job))
	if got := hits.Load(); got > int64(worker.MaxWebhookRedirects)+1 {
		t.Errorf("redirect chain made %d requests, cap is %d", got, worker.MaxWebhookRedirects)
	}
}

// A huge response body must not be slurped: the handler drains at most
// MaxWebhookResponseBody.
func TestWebhookResponseBodyCapped(t *testing.T) {
	body := strings.Repeat("x", 4*worker.MaxWebhookResponseBody)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	guard := worker.NewEgressGuard(true)
	h := worker.WebhookHandler(guard.HTTPClient(5*time.Second), guard)
	job := &jobs.Job{ID: "job_big", Payload: `{"url":"` + srv.URL + `"}`}

	start := time.Now()
	if err := h(context.Background(), job); err != nil {
		t.Fatalf("200 with huge body must succeed: %v", err)
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Errorf("huge body took %v; the capped drain should be fast", d)
	}
}

// ---------------------------------------------------------------------------
// JOBS-06: the webhook handler must not leak goroutines or connections.
// (goleak-style: goroutine count settles back after the run.)
// ---------------------------------------------------------------------------

func TestWebhookHandlerNoGoroutineLeak(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	guard := worker.NewEgressGuard(true)
	h := worker.WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	strict := worker.WebhookHandler(worker.NewEgressGuard(false).HTTPClient(2*time.Second),
		worker.NewEgressGuard(false))
	jobOK := &jobs.Job{ID: "job_leak_ok", Payload: `{"url":"` + srv.URL + `"}`}
	jobBlocked := &jobs.Job{ID: "job_leak_block", Payload: `{"url":"http://169.254.169.254/"}`}

	before := numGoroutines()
	for i := 0; i < 50; i++ {
		if err := h(context.Background(), jobOK); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		permanentErr(t, strict(context.Background(), jobBlocked))
	}
	// Idle connections may linger briefly; the guard must not spawn anything
	// permanent. Allow a small slack for the shared transport's keep-alives.
	deadline := time.Now().Add(3 * time.Second)
	for {
		leaked := numGoroutines() - before
		if leaked <= 4 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines leaked: before=%d after=%d", before, numGoroutines())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func numGoroutines() int {
	// Let the scheduler settle so transient goroutines finish.
	time.Sleep(20 * time.Millisecond)
	return runtime.NumGoroutine()
}

// ---------------------------------------------------------------------------
// JOBS-02: BOLA / IDOR — owner scoping on the jobs API.
// ---------------------------------------------------------------------------

func TestCallerFromMetadata(t *testing.T) {
	t.Run("no identity means internal system caller", func(t *testing.T) {
		c, err := jobs.CallerFromMetadata(metadata.MD{})
		if err != nil {
			t.Fatal(err)
		}
		if !c.System || c.UserID != "" || c.Admin {
			t.Errorf("got %+v, want system caller", c)
		}
	})

	t.Run("plain user", func(t *testing.T) {
		c, err := jobs.CallerFromMetadata(metadata.Pairs("x-user-id", userA))
		if err != nil {
			t.Fatal(err)
		}
		if c.System || c.Admin || c.UserID != userA {
			t.Errorf("got %+v", c)
		}
	})

	t.Run("admin via x-user-perms", func(t *testing.T) {
		c, err := jobs.CallerFromMetadata(metadata.Pairs(
			"x-user-id", userA, "x-user-perms", "jobs:read, admin:*"))
		if err != nil {
			t.Fatal(err)
		}
		if !c.Admin || c.System {
			t.Errorf("got %+v, want admin", c)
		}
	})

	t.Run("malformed user id is rejected, not silently trusted", func(t *testing.T) {
		_, err := jobs.CallerFromMetadata(metadata.Pairs("x-user-id", "not-a-uuid"))
		if err == nil {
			t.Fatal("want error for malformed x-user-id")
		}
	})

	t.Run("perms without a user id are rejected (privilege injection)", func(t *testing.T) {
		_, err := jobs.CallerFromMetadata(metadata.Pairs("x-user-perms", "admin:*"))
		if err == nil {
			t.Fatal("want error: x-user-perms without x-user-id must not grant admin")
		}
	})
}

func TestCanAccessJob(t *testing.T) {
	ownerA := userA

	cases := []struct {
		name  string
		md    metadata.MD
		owner *string
		want  bool
	}{
		{"owner reads own job", metadata.Pairs("x-user-id", userA), &ownerA, true},
		{"stranger cannot read", metadata.Pairs("x-user-id", userB), &ownerA, false},
		{"stranger cannot read ownerless job", metadata.Pairs("x-user-id", userB), nil, false},
		{"admin reads anyone", metadata.Pairs("x-user-id", userB, "x-user-perms", "admin:*"), &ownerA, true},
		{"admin reads ownerless", metadata.Pairs("x-user-id", userB, "x-user-perms", "admin:*"), nil, true},
		{"system caller reads anyone", metadata.MD{}, &ownerA, true},
		{"system caller reads ownerless", metadata.MD{}, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			caller, err := jobs.CallerFromMetadata(c.md)
			if err != nil {
				t.Fatal(err)
			}
			if got := caller.CanAccessJob(c.owner); got != c.want {
				t.Errorf("CanAccessJob = %v, want %v (caller %+v)", got, c.want, caller)
			}
		})
	}
}

// Foreign jobs must be indistinguishable from missing ones (no existence
// oracle): the authorization failure is KindNotFound, not KindForbidden.
func TestAuthorizeJobHidesExistence(t *testing.T) {
	caller, err := jobs.CallerFromMetadata(metadata.Pairs("x-user-id", userB))
	if err != nil {
		t.Fatal(err)
	}
	err = caller.AuthorizeJob(&jobs.Job{ID: "job_x", OwnerID: &[]string{userA}[0]})
	if err == nil {
		t.Fatal("want error for foreign job")
	}
	if got := kindOf(err); got != kindNotFound {
		t.Errorf("kind: got %v, want NotFound (existence must not leak)", got)
	}
}

// ---------------------------------------------------------------------------
// JOBS-03: payload size cap on CreateJob.
// ---------------------------------------------------------------------------

func TestValidateCreatePayloadCap(t *testing.T) {
	pad := func(n int) string { return `{"pad":"` + strings.Repeat("a", n) + `"}` }

	// Exactly at the cap: accepted.
	exact := pad(jobs.MaxPayloadBytes - len(`{"pad":""}`))
	if _, _, err := jobs.ValidateCreate("webhook", exact, 0, 0); err != nil {
		t.Errorf("payload at the cap must be accepted: %v", err)
	}

	// One byte over: rejected with a client error, not a crash or a 500.
	over := pad(jobs.MaxPayloadBytes - len(`{"pad":""}`) + 1)
	_, _, err := jobs.ValidateCreate("webhook", over, 0, 0)
	if err == nil {
		t.Fatal("payload over the cap must be rejected")
	}
	if codeOf(err) != "payload_too_large" {
		t.Errorf("error code: got %q, want payload_too_large", codeOf(err))
	}
}

// ---------------------------------------------------------------------------
// JOBS-04: pagination must not overflow the SQL OFFSET.
// ---------------------------------------------------------------------------

func TestNormalizePageClamps(t *testing.T) {
	if page, size := jobs.NormalizePage(nil); page != 1 || size != 20 {
		t.Errorf("nil page: got (%d, %d), want (1, 20)", page, size)
	}

	// Max int32 page used to overflow (page-1)*size in 32-bit arithmetic and
	// hand Postgres a negative OFFSET -> 500 on a valid-looking request.
	page, size := jobs.NormalizePage(pageReq(math.MaxInt32, 100))
	if page < 1 || page > jobs.MaxListPage {
		t.Errorf("page %d out of clamp range [1, %d]", page, jobs.MaxListPage)
	}
	// The offset derived from the clamped values must stay non-negative and
	// fit the int the store layer receives.
	off := (int64(page) - 1) * int64(size)
	if off < 0 || off > math.MaxInt32 {
		t.Errorf("offset overflow: (page %d, size %d) -> %d", page, size, off)
	}

	if _, size := jobs.NormalizePage(pageReq(1, -5)); size != 20 {
		t.Errorf("negative size: got %d, want default 20", size)
	}
	if _, size := jobs.NormalizePage(pageReq(1, 1<<30)); size != 100 {
		t.Errorf("huge size: got %d, want cap 100", size)
	}
}
