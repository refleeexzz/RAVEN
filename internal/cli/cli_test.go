// cli_test.go covers the cross-command behavior of the CLI against a fake
// gateway (httptest): login/logout, config file permissions, the error
// envelope, auth failures and usage errors.
package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// fakeGateway records what the CLI sent and serves canned answers.
type fakeGateway struct {
	t *testing.T

	mu          sync.Mutex
	authHeader  string
	loginBody   map[string]string
	logoutBody  map[string]string
	submitBody  map[string]any
	submitKey   string
	listQuery   url.Values
	actionCalls []string
	degraded    bool
}

// newFakeGateway starts an httptest server rooted at /api and returns it
// plus the recorder. Handlers mimic services/gateway's wire shapes.
func newFakeGateway(t *testing.T) (*httptest.Server, *fakeGateway) {
	t.Helper()
	fg := &fakeGateway{t: t}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		fg.mu.Lock()
		fg.loginBody = body
		fg.mu.Unlock()
		if body["email"] == "bad@raven.dev" {
			writeEnvelope(w, http.StatusUnauthorized, "invalid_credentials", "invalid email or password", "req-login-1")
			return
		}
		writeJSONT(w, http.StatusOK, map[string]any{
			"access_token":       "access-tok",
			"refresh_token":      "refresh-tok",
			"access_expires_at":  1893456000,
			"refresh_expires_at": 1893456000,
		})
	})

	mux.HandleFunc("POST /api/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		fg.mu.Lock()
		fg.authHeader = r.Header.Get("Authorization")
		fg.logoutBody = body
		fg.mu.Unlock()
		writeJSONT(w, http.StatusOK, map[string]bool{"ok": true})
	})

	mux.HandleFunc("POST /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		fg.mu.Lock()
		fg.authHeader = r.Header.Get("Authorization")
		fg.submitBody = body
		fg.submitKey = r.Header.Get("Idempotency-Key")
		fg.mu.Unlock()
		if body["type"] == "explode" {
			writeEnvelope(w, http.StatusTooManyRequests, "rate_limited", "too many requests", "req-429")
			return
		}
		writeJSONT(w, http.StatusCreated, map[string]any{
			"id": "job-1", "type": body["type"], "payload": body["payload"],
			"status": "QUEUED", "priority": body["priority"],
			"attempts": 0, "max_attempts": body["max_attempts"],
			"created_at": 1767225600,
		})
	})

	mux.HandleFunc("GET /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		fg.mu.Lock()
		fg.listQuery = r.URL.Query()
		fg.mu.Unlock()
		writeJSONT(w, http.StatusOK, map[string]any{
			"jobs": []map[string]any{
				{"id": "job-a", "type": "send_email", "payload": map[string]any{}, "status": "SUCCESS", "priority": 0, "attempts": 1, "max_attempts": 3, "created_at": 1767225600},
				{"id": "job-b", "type": "webhook", "payload": map[string]any{}, "status": "QUEUED", "priority": 5, "attempts": 0, "max_attempts": 3, "created_at": 1767225601},
			},
			"page": map[string]any{"page": 2, "page_size": 5, "total": 7},
		})
	})

	mux.HandleFunc("GET /api/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		writeJSONT(w, http.StatusOK, map[string]any{
			"id": r.PathValue("id"), "type": "send_email",
			"payload": map[string]any{"to": "a@b.c"}, "status": "QUEUED",
			"priority": 1, "attempts": 0, "max_attempts": 3, "created_at": 1767225600,
		})
	})

	mux.HandleFunc("POST /api/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		fg.mu.Lock()
		fg.actionCalls = append(fg.actionCalls, "cancel:"+r.PathValue("id"))
		fg.mu.Unlock()
		writeJSONT(w, http.StatusOK, map[string]any{
			"id": r.PathValue("id"), "type": "send_email", "payload": map[string]any{},
			"status": "CANCELLED", "priority": 1, "attempts": 0, "max_attempts": 3,
		})
	})

	mux.HandleFunc("POST /api/jobs/{id}/requeue", func(w http.ResponseWriter, r *http.Request) {
		fg.mu.Lock()
		fg.actionCalls = append(fg.actionCalls, "requeue:"+r.PathValue("id"))
		fg.mu.Unlock()
		writeJSONT(w, http.StatusOK, map[string]any{
			"id": r.PathValue("id"), "type": "send_email", "payload": map[string]any{},
			"status": "QUEUED", "priority": 1, "attempts": 0, "max_attempts": 3,
		})
	})

	mux.HandleFunc("GET /api/workers", func(w http.ResponseWriter, r *http.Request) {
		writeJSONT(w, http.StatusOK, map[string]any{
			"workers": []map[string]string{
				{"id": "worker-1", "started_at": "2026-01-01T00:00:00Z", "last_heartbeat": "2026-01-01T00:05:00Z", "jobs_processed": "42", "in_flight": "1"},
			},
		})
	})

	mux.HandleFunc("GET /api/health/services", func(w http.ResponseWriter, r *http.Request) {
		fg.mu.Lock()
		degraded := fg.degraded
		fg.mu.Unlock()
		status := "ok"
		if degraded {
			status = "degraded"
		}
		writeJSONT(w, http.StatusOK, map[string]any{
			"checked_at": "2026-01-01T00:00:00Z",
			"services": []map[string]any{
				{"name": "gateway", "status": "ok", "latency_ms": 1, "detail": "self"},
				{"name": "auth", "status": status, "latency_ms": 3, "detail": "reachable"},
			},
		})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, fg
}

func writeJSONT(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeEnvelope(w http.ResponseWriter, status int, code, message, reqID string) {
	writeJSONT(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message, "request_id": reqID},
	})
}

// newTestEnv isolates the CLI config in a temp dir and points the API at
// the fake gateway. It returns the config path.
func newTestEnv(t *testing.T, baseURL string) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("RAVEN_CONFIG", cfgPath)
	t.Setenv("RAVEN_API_URL", baseURL+"/api")
	return cfgPath
}

// runCLI executes the CLI with empty stdin and captures the streams.
func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	code := Run(args, strings.NewReader(""), &out, &errBuf)
	return code, out.String(), errBuf.String()
}

// seedLoggedIn writes a config as if login had already happened.
func seedLoggedIn(t *testing.T, cfgPath string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{
		"api_url":       os.Getenv("RAVEN_API_URL"),
		"email":         "e2e@raven.dev",
		"access_token":  "access-tok",
		"refresh_token": "refresh-tok",
	})
	if err := os.WriteFile(cfgPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoginSavesConfig(t *testing.T) {
	srv, fg := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)

	code, out, _ := runCLI(t, "login", "--email", "e2e@raven.dev", "--password", "supersecret123")
	if code != ExitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(out, "logged in as e2e@raven.dev") {
		t.Fatalf("stdout missing login confirmation: %q", out)
	}
	if fg.loginBody["email"] != "e2e@raven.dev" || fg.loginBody["password"] != "supersecret123" {
		t.Fatalf("gateway got wrong login body: %v", fg.loginBody)
	}

	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("config not written: %v", err)
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("config is not JSON: %v", err)
	}
	if cfg.AccessToken != "access-tok" || cfg.RefreshToken != "refresh-tok" {
		t.Fatalf("tokens not saved: %+v", cfg)
	}
	if bytes.Contains(raw, []byte("supersecret123")) {
		t.Fatal("password must never be written to the config file")
	}

	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	// 0600 is enforceable on POSIX; on Windows the runtime maps chmod to the
	// read-only attribute and the profile ACL does the real protection, so
	// there we only assert the file exists (done above).
	if runtime.GOOS != "windows" {
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("config perm = %o, want 600", got)
		}
	}
}

func TestLoginBadCredentials(t *testing.T) {
	srv, _ := newFakeGateway(t)
	newTestEnv(t, srv.URL)

	code, _, stderr := runCLI(t, "login", "--email", "bad@raven.dev", "--password", "nope")
	if code != ExitAuth {
		t.Fatalf("exit = %d, want 3", code)
	}
	want := "invalid_credentials: invalid email or password (request id: req-login-1)"
	if !strings.Contains(stderr, want) {
		t.Fatalf("stderr %q missing %q", stderr, want)
	}
}

func TestLoginPasswordFromEnv(t *testing.T) {
	srv, fg := newFakeGateway(t)
	newTestEnv(t, srv.URL)
	t.Setenv("RAVEN_PASSWORD", "env-secret")

	code, _, stderr := runCLI(t, "login", "--email", "e2e@raven.dev")
	if code != ExitOK {
		t.Fatalf("exit = %d, want 0 (stderr: %s)", code, stderr)
	}
	if fg.loginBody["password"] != "env-secret" {
		t.Fatalf("password not taken from RAVEN_PASSWORD: %v", fg.loginBody)
	}
}

func TestLogoutClearsTokens(t *testing.T) {
	srv, fg := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	seedLoggedIn(t, cfgPath)

	code, out, _ := runCLI(t, "logout")
	if code != ExitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(out, "logged out") {
		t.Fatalf("stdout: %q", out)
	}
	if fg.logoutBody["refresh_token"] != "refresh-tok" {
		t.Fatalf("logout body = %v, want the saved refresh token", fg.logoutBody)
	}
	if fg.authHeader != "Bearer access-tok" {
		t.Fatalf("logout auth header = %q", fg.authHeader)
	}

	raw, _ := os.ReadFile(cfgPath)
	var cfg config
	_ = json.Unmarshal(raw, &cfg)
	if cfg.AccessToken != "" || cfg.RefreshToken != "" {
		t.Fatalf("tokens not cleared: %+v", cfg)
	}
	if cfg.Email != "e2e@raven.dev" {
		t.Fatalf("email should survive logout: %+v", cfg)
	}
}

func TestAuthRequiredWithoutLogin(t *testing.T) {
	srv, _ := newFakeGateway(t)
	newTestEnv(t, srv.URL) // no seedLoggedIn: empty config

	for _, args := range [][]string{
		{"jobs", "list"}, {"jobs", "get", "x"}, {"workers"}, {"logout"},
	} {
		code, _, stderr := runCLI(t, args...)
		if code != ExitAuth {
			t.Fatalf("%v: exit = %d, want 3", args, code)
		}
		if !strings.Contains(stderr, "not logged in") {
			t.Fatalf("%v: stderr %q should explain the fix", args, stderr)
		}
	}
}

func TestHealthNeedsNoToken(t *testing.T) {
	srv, _ := newFakeGateway(t)
	newTestEnv(t, srv.URL)

	code, out, _ := runCLI(t, "health")
	if code != ExitOK {
		t.Fatalf("exit = %d, want 0", code)
	}
	for _, want := range []string{"SERVICE", "gateway", "auth", "overall: ok"} {
		if !strings.Contains(out, want) {
			t.Fatalf("health output %q missing %q", out, want)
		}
	}
}

func TestErrorEnvelopeWithoutLoginDoesNotHideMessage(t *testing.T) {
	// Even unauthenticated flows must render the envelope cleanly (the
	// console once printed "[object Object]" here — the CLI shows
	// code + message + request id).
	srv, _ := newFakeGateway(t)
	newTestEnv(t, srv.URL)

	code, _, stderr := runCLI(t, "login", "--email", "bad@raven.dev", "--password", "x")
	if code != ExitAuth {
		t.Fatalf("exit = %d", code)
	}
	for _, want := range []string{"invalid_credentials", "invalid email or password", "req-login-1"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr %q missing %q", stderr, want)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	srv, _ := newFakeGateway(t)
	newTestEnv(t, srv.URL)

	cases := [][]string{
		{"bogus"},
		{"jobs", "bogus"},
		{"jobs", "get"},                   // missing id
		{"jobs", "get", "a", "b"},         // too many args
		{"jobs", "submit"},                // missing --type
		{"jobs", "list", "--page", "abc"}, // bad flag value
		{"jobs", "submit", "--type", "send_email", "--payload", "{nope"},
	}
	for _, args := range cases {
		if code, _, _ := runCLI(t, args...); code != ExitUsage {
			t.Fatalf("%v: exit = %d, want 2", args, code)
		}
	}
}

func TestHelpAndVersion(t *testing.T) {
	srv, _ := newFakeGateway(t)
	newTestEnv(t, srv.URL)

	code, _, stderr := runCLI(t)
	if code != ExitUsage || !strings.Contains(stderr, "commands:") {
		t.Fatalf("bare run: exit = %d, stderr = %q", code, stderr)
	}
	code, out, _ := runCLI(t, "help")
	if code != ExitOK || !strings.Contains(out, "jobs watch") {
		t.Fatalf("help: exit = %d, out = %q", code, out)
	}
	code, out, _ = runCLI(t, "version")
	if code != ExitOK || !strings.Contains(out, "raven dev") {
		t.Fatalf("version: exit = %d, out = %q", code, out)
	}
	code, out, _ = runCLI(t, "version", "--json")
	if code != ExitOK {
		t.Fatalf("version --json: exit = %d", code)
	}
	var v map[string]string
	if err := json.Unmarshal([]byte(out), &v); err != nil || v["version"] != "dev" {
		t.Fatalf("version --json: %q, err %v", out, err)
	}
}

func TestConfigCorruptFile(t *testing.T) {
	srv, _ := newFakeGateway(t)
	cfgPath := newTestEnv(t, srv.URL)
	if err := os.WriteFile(cfgPath, []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runCLI(t, "jobs", "list")
	if code != ExitError {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr, "corrupt") {
		t.Fatalf("stderr should name the corrupt config: %q", stderr)
	}
}
