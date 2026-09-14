// jobs_test.go covers the jobs family, watch polling, workers and health
// against the fake gateway defined in cli_test.go.
package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSubmitSendsIdempotencyKey(t *testing.T) {
	srv, fg := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, _ := runCLI(t, "jobs", "submit",
		"--type", "send_email",
		"--payload", `{"to":"a@b.c","subject":"hi"}`,
		"--priority", "7",
		"--idempotency-key", "order-42",
	)
	if code != ExitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	if fg.submitKey != "order-42" {
		t.Fatalf("Idempotency-Key header = %q, want order-42", fg.submitKey)
	}
	if fg.authHeader != "Bearer access-tok" {
		t.Fatalf("Authorization header = %q", fg.authHeader)
	}
	if fg.submitBody["type"] != "send_email" {
		t.Fatalf("body type = %v", fg.submitBody["type"])
	}
	if fg.submitBody["priority"] != float64(7) {
		t.Fatalf("body priority = %v", fg.submitBody["priority"])
	}
	payload, _ := json.Marshal(fg.submitBody["payload"])
	if string(payload) != `{"subject":"hi","to":"a@b.c"}` {
		t.Fatalf("body payload = %s", payload)
	}
	if !strings.Contains(out, "job job-1 submitted") {
		t.Fatalf("stdout: %q", out)
	}
}

func TestSubmitGeneratesIdempotencyKey(t *testing.T) {
	srv, fg := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, _, _ := runCLI(t, "jobs", "submit", "--type", "webhook", "--payload", `{}`)
	if code != ExitOK {
		t.Fatalf("exit = %d", code)
	}
	if !strings.HasPrefix(fg.submitKey, "cli-") {
		t.Fatalf("auto key = %q, want cli- prefix", fg.submitKey)
	}
}

func TestSubmitJSONOutput(t *testing.T) {
	srv, _ := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, _ := runCLI(t, "jobs", "submit", "--type", "send_email", "--json")
	if code != ExitOK {
		t.Fatalf("exit = %d", code)
	}
	var job jobJSON
	if err := json.Unmarshal([]byte(out), &job); err != nil {
		t.Fatalf("--json output is not a job: %q (%v)", out, err)
	}
	if job.ID != "job-1" || job.Status != "QUEUED" {
		t.Fatalf("job = %+v", job)
	}
}

func TestSubmitErrorEnvelope(t *testing.T) {
	srv, _ := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, _, stderr := runCLI(t, "jobs", "submit", "--type", "explode", "--payload", `{}`)
	if code != ExitError {
		t.Fatalf("exit = %d, want 1 (429 is not an auth error)", code)
	}
	want := "rate_limited: too many requests (request id: req-429)"
	if !strings.Contains(stderr, want) {
		t.Fatalf("stderr %q missing %q", stderr, want)
	}
}

func TestJobsListPaginated(t *testing.T) {
	srv, fg := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, _ := runCLI(t, "jobs", "list",
		"--status", "queued", "--page", "2", "--page-size", "5")
	if code != ExitOK {
		t.Fatalf("exit = %d", code)
	}
	q := fg.listQuery
	if q.Get("page") != "2" || q.Get("page_size") != "5" || q.Get("status") != "queued" {
		t.Fatalf("query = %v", q)
	}
	for _, want := range []string{"job-a", "job-b", "SUCCESS", "page 2 · 5/page · 7 total"} {
		if !strings.Contains(out, want) {
			t.Fatalf("list output %q missing %q", out, want)
		}
	}

	code, out, _ = runCLI(t, "jobs", "list", "--json")
	if code != ExitOK {
		t.Fatalf("--json exit = %d", code)
	}
	var list jobListJSON
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("--json output invalid: %v", err)
	}
	if len(list.Jobs) != 2 || list.Page.Total != 7 {
		t.Fatalf("list = %+v", list)
	}
}

func TestJobsGetCancelRequeue(t *testing.T) {
	srv, fg := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, _ := runCLI(t, "jobs", "get", "job-9")
	if code != ExitOK {
		t.Fatalf("get exit = %d", code)
	}
	for _, want := range []string{"id:", "job-9", "type:", "send_email", `"to": "a@b.c"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("get output %q missing %q", out, want)
		}
	}

	code, out, _ = runCLI(t, "jobs", "cancel", "job-9")
	if code != ExitOK || !strings.Contains(out, "job job-9 cancelled (status CANCELLED)") {
		t.Fatalf("cancel: exit = %d, out = %q", code, out)
	}
	code, out, _ = runCLI(t, "jobs", "requeue", "job-9")
	if code != ExitOK || !strings.Contains(out, "job job-9 requeued (status QUEUED)") {
		t.Fatalf("requeue: exit = %d, out = %q", code, out)
	}
	if len(fg.actionCalls) != 2 || fg.actionCalls[0] != "cancel:job-9" || fg.actionCalls[1] != "requeue:job-9" {
		t.Fatalf("action calls = %v", fg.actionCalls)
	}
}

// newWatchServer serves GET /api/jobs/{id} with a scripted status
// progression driven by a call counter.
func newWatchServer(t *testing.T, statuses []string, final jobJSON) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		idx := int(n) - 1
		status := final.Status
		if idx < len(statuses) {
			status = statuses[idx]
		}
		body := map[string]any{
			"id": r.PathValue("id"), "type": "send_email",
			"payload": map[string]any{}, "status": status,
			"priority": 0, "attempts": 1, "max_attempts": 3,
			"error": final.Error, "worker_id": final.WorkerID,
		}
		writeJSONT(w, http.StatusOK, body)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestJobsWatchUntilSuccess(t *testing.T) {
	srv, calls := newWatchServer(t,
		[]string{"QUEUED", "QUEUED", "PROCESSING"},
		jobJSON{Status: "SUCCESS", WorkerID: "worker-1"},
	)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, _ := runCLI(t, "jobs", "watch", "job-1", "--interval", "50ms", "--timeout", "10s")
	if code != ExitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(out, "QUEUED") || !strings.Contains(out, "PROCESSING") || !strings.Contains(out, "SUCCESS") {
		t.Fatalf("watch output should show the transitions: %q", out)
	}
	// QUEUED appears twice from the server but must print once (changes only).
	if strings.Count(out, "QUEUED") != 1 {
		t.Fatalf("duplicate status lines: %q", out)
	}
	if got := atomic.LoadInt32(calls); got < 4 {
		t.Fatalf("server saw %d polls, want >= 4", got)
	}
}

func TestJobsWatchFailureExitsOne(t *testing.T) {
	srv, _ := newWatchServer(t, []string{"PROCESSING"}, jobJSON{Status: "FAILED", Error: "smtp timeout"})
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, stderr := runCLI(t, "jobs", "watch", "job-1", "--interval", "50ms", "--timeout", "10s")
	if code != ExitError {
		t.Fatalf("exit = %d, want 1 for a FAILED job", code)
	}
	if !strings.Contains(out, "smtp timeout") {
		t.Fatalf("watch should surface the job error: %q", out)
	}
	if !strings.Contains(stderr, "FAILED") {
		t.Fatalf("stderr should name the final status: %q", stderr)
	}
}

func TestJobsWatchDeadIsTerminal(t *testing.T) {
	srv, _ := newWatchServer(t, nil, jobJSON{Status: "DEAD", Error: "max attempts"})
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, _, _ := runCLI(t, "jobs", "watch", "job-1", "--interval", "50ms")
	if code != ExitError {
		t.Fatalf("exit = %d, want 1 for DEAD", code)
	}
}

func TestJobsWatchTimeout(t *testing.T) {
	srv, _ := newWatchServer(t, []string{"QUEUED", "QUEUED", "QUEUED", "QUEUED", "QUEUED", "QUEUED", "QUEUED"}, jobJSON{Status: "QUEUED"})
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, _, stderr := runCLI(t, "jobs", "watch", "job-1", "--interval", "50ms", "--timeout", "200ms")
	if code != ExitError {
		t.Fatalf("exit = %d, want 1 on timeout", code)
	}
	if !strings.Contains(stderr, "timed out") {
		t.Fatalf("stderr should report the timeout: %q", stderr)
	}
}

func TestWorkersTable(t *testing.T) {
	srv, _ := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, _ := runCLI(t, "workers")
	if code != ExitOK {
		t.Fatalf("exit = %d", code)
	}
	for _, want := range []string{"worker-1", "42", "IN FLIGHT"} {
		if !strings.Contains(out, want) {
			t.Fatalf("workers output %q missing %q", out, want)
		}
	}
}

func TestHealthDegradedExitsOne(t *testing.T) {
	srv, fg := newFakeGateway(t)
	newTestEnv(t, srv.URL)
	fg.degraded = true

	code, out, stderr := runCLI(t, "health")
	if code != ExitError {
		t.Fatalf("exit = %d, want 1 when a service is degraded", code)
	}
	if !strings.Contains(out, "overall: degraded") {
		t.Fatalf("output should say overall degraded: %q", out)
	}
	if !strings.Contains(stderr, "not healthy") {
		t.Fatalf("stderr should explain the failure: %q", stderr)
	}
}

func TestHealthJSON(t *testing.T) {
	srv, _ := newFakeGateway(t)
	newTestEnv(t, srv.URL)

	code, out, _ := runCLI(t, "health", "--json")
	if code != ExitOK {
		t.Fatalf("exit = %d", code)
	}
	var payload healthPayload
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("--json output invalid: %v", err)
	}
	if len(payload.Services) != 2 || payload.Services[0].Name != "gateway" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestAPIURLFlagBeatsEnv(t *testing.T) {
	srv, fg := newFakeGateway(t)
	newTestEnv(t, srv.URL)
	t.Setenv("RAVEN_API_URL", "http://127.0.0.1:1/api") // dead on purpose

	// health needs no token, so this isolates URL resolution.
	code, _, _ := runCLI(t, "health", "--api-url", srv.URL+"/api")
	if code != ExitOK {
		t.Fatalf("--api-url should override the env var, exit = %d", code)
	}

	// And the env beats the default when no flag/config exists.
	t.Setenv("RAVEN_API_URL", srv.URL+"/api")
	code, _, _ = runCLI(t, "health")
	if code != ExitOK {
		t.Fatalf("RAVEN_API_URL should be honored, exit = %d", code)
	}
	_ = fg
}
