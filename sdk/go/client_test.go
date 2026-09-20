package raven_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	raven "github.com/refleeexzz/RAVEN/sdk/go"
)

// newServer builds a mock gateway with the given handler and a client
// pointed at it.
func newServer(t *testing.T, handler http.Handler, opts ...raven.Option) (*httptest.Server, *raven.Client) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c := raven.New(append([]raven.Option{raven.WithBaseURL(srv.URL)}, opts...)...)
	return srv, c
}

func jobJSON(id, status string) string {
	return fmt.Sprintf(`{"id":%q,"type":"webhook","payload":{"url":"https://x.example"},"status":%q,"priority":5,"attempts":0,"max_attempts":4,"created_at":1759998000,"started_at":0,"finished_at":0,"error":"","worker_id":"","scheduled_at":0}`, id, status)
}

// TestRoutes exercises the happy path of every endpoint method, checking
// the HTTP method, path and decoding the response model. Table-driven: one
// row per capability.
func TestRoutes(t *testing.T) {
	type check func(t *testing.T, c *raven.Client)

	tests := []struct {
		name       string
		wantMethod string
		wantPath   string
		wantQuery  string
		body       string
		run        check
	}{
		{
			name:       "register",
			wantMethod: "POST",
			wantPath:   "/api/auth/register",
			body:       `{"user_id":"u-1","email":"me@example.com"}`,
			run: func(t *testing.T, c *raven.Client) {
				resp, err := c.Register(context.Background(), raven.RegisterRequest{
					Email: "me@example.com", Password: "12345678", DisplayName: "Me",
				})
				if err != nil {
					t.Fatal(err)
				}
				if resp.UserID != "u-1" || resp.Email != "me@example.com" {
					t.Fatalf("unexpected response: %+v", resp)
				}
			},
		},
		{
			name:       "login stores tokens",
			wantMethod: "POST",
			wantPath:   "/api/auth/login",
			body:       `{"access_token":"at-1","refresh_token":"rt-1","access_expires_at":9999999999,"refresh_expires_at":9999999999}`,
			run: func(t *testing.T, c *raven.Client) {
				pair, err := c.Login(context.Background(), "me@example.com", "12345678")
				if err != nil {
					t.Fatal(err)
				}
				if pair.AccessToken != "at-1" || pair.RefreshToken != "rt-1" {
					t.Fatalf("unexpected pair: %+v", pair)
				}
				if c.Tokens() == nil || c.Tokens().AccessToken != "at-1" {
					t.Fatalf("client did not store the pair: %+v", c.Tokens())
				}
			},
		},
		{
			name:       "create job sends auto idempotency key",
			wantMethod: "POST",
			wantPath:   "/api/jobs",
			body:       jobJSON("job_1", "QUEUED"),
			run: func(t *testing.T, c *raven.Client) {
				payload, _ := raven.NewJobPayload(map[string]any{"url": "https://x.example"})
				job, err := c.CreateJob(context.Background(), raven.CreateJobRequest{
					Type: "webhook", Payload: payload,
				})
				if err != nil {
					t.Fatal(err)
				}
				if job.ID != "job_1" || job.Status != "QUEUED" {
					t.Fatalf("unexpected job: %+v", job)
				}
				if job.Terminal() {
					t.Fatal("QUEUED must not be terminal")
				}
			},
		},
		{
			name:       "list jobs with filter",
			wantMethod: "GET",
			wantPath:   "/api/jobs",
			wantQuery:  "page=2&page_size=50&status=failed",
			body:       `{"jobs":[` + jobJSON("job_9", "FAILED") + `],"page":{"page":2,"page_size":50,"total":1}}`,
			run: func(t *testing.T, c *raven.Client) {
				list, err := c.ListJobs(context.Background(), raven.ListJobsFilter{
					Status: "failed", Page: 2, PageSize: 50,
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(list.Jobs) != 1 || list.Page.Total != 1 || list.Page.Page != 2 {
					t.Fatalf("unexpected list: %+v", list)
				}
				if !list.Jobs[0].Terminal() {
					t.Fatal("FAILED must be terminal")
				}
			},
		},
		{
			name:       "get job",
			wantMethod: "GET",
			wantPath:   "/api/jobs/job_1",
			body:       jobJSON("job_1", "SUCCESS"),
			run: func(t *testing.T, c *raven.Client) {
				job, err := c.GetJob(context.Background(), "job_1")
				if err != nil {
					t.Fatal(err)
				}
				if job.ID != "job_1" || !job.Terminal() {
					t.Fatalf("unexpected job: %+v", job)
				}
			},
		},
		{
			name:       "cancel job",
			wantMethod: "POST",
			wantPath:   "/api/jobs/job_1/cancel",
			body:       jobJSON("job_1", "CANCELLED"),
			run: func(t *testing.T, c *raven.Client) {
				job, err := c.CancelJob(context.Background(), "job_1")
				if err != nil {
					t.Fatal(err)
				}
				if job.Status != "CANCELLED" {
					t.Fatalf("unexpected job: %+v", job)
				}
			},
		},
		{
			name:       "requeue job",
			wantMethod: "POST",
			wantPath:   "/api/jobs/job_1/requeue",
			body:       jobJSON("job_1", "QUEUED"),
			run: func(t *testing.T, c *raven.Client) {
				if _, err := c.RequeueJob(context.Background(), "job_1"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:       "replay job",
			wantMethod: "POST",
			wantPath:   "/api/jobs/job_1/replay",
			body:       strings.Replace(jobJSON("job_2", "QUEUED"), `,"scheduled_at":0}`, `,"scheduled_at":0,"replayed_from":"job_1"}`, 1),
			run: func(t *testing.T, c *raven.Client) {
				job, err := c.ReplayJob(context.Background(), "job_1")
				if err != nil {
					t.Fatal(err)
				}
				if job.ReplayedFrom != "job_1" {
					t.Fatalf("unexpected replay: %+v", job)
				}
			},
		},
		{
			name:       "deliveries",
			wantMethod: "GET",
			wantPath:   "/api/jobs/job_1/deliveries",
			body:       `{"deliveries":[{"id":41,"job_id":"job_1","attempt":1,"url":"https://x.example","status_code":200,"latency_ms":83,"response_snippet":"{}","blocked":false,"error":"","ts":1767225600}],"page":{"page":1,"page_size":20,"total":1}}`,
			run: func(t *testing.T, c *raven.Client) {
				list, err := c.JobDeliveries(context.Background(), "job_1", 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				if len(list.Deliveries) != 1 {
					t.Fatalf("unexpected deliveries: %+v", list)
				}
				d := list.Deliveries[0]
				if d.StatusCode == nil || *d.StatusCode != 200 || d.LatencyMs == nil {
					t.Fatalf("nullable fields broken: %+v", d)
				}
			},
		},
		{
			name:       "create cron",
			wantMethod: "POST",
			wantPath:   "/api/crons",
			body:       `{"id":"cron_1","name":"nightly","cron_expr":"0 3 * * *","type":"webhook","payload":{"url":"https://x.example"},"priority":5,"enabled":true,"next_run_at":1767225600,"last_run_at":0,"created_at":1767220000}`,
			run: func(t *testing.T, c *raven.Client) {
				payload, _ := raven.NewJobPayload(map[string]any{"url": "https://x.example"})
				cron, err := c.CreateCron(context.Background(), raven.CreateCronRequest{
					Name: "nightly", CronExpr: "0 3 * * *", Type: "webhook", Payload: payload,
				})
				if err != nil {
					t.Fatal(err)
				}
				if cron.ID != "cron_1" || cron.NextRunAt != 1767225600 || !cron.Enabled {
					t.Fatalf("unexpected cron: %+v", cron)
				}
			},
		},
		{
			name:       "list crons",
			wantMethod: "GET",
			wantPath:   "/api/crons",
			body:       `{"crons":[{"id":"cron_1","name":"n","cron_expr":"0 3 * * *","type":"webhook","payload":{},"priority":5,"enabled":true,"next_run_at":1,"last_run_at":0,"created_at":1}],"page":{"page":1,"page_size":20,"total":1}}`,
			run: func(t *testing.T, c *raven.Client) {
				list, err := c.ListCrons(context.Background(), 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				if len(list.Crons) != 1 || list.Page.Total != 1 {
					t.Fatalf("unexpected crons: %+v", list)
				}
			},
		},
		{
			name:       "delete cron",
			wantMethod: "DELETE",
			wantPath:   "/api/crons/cron_1",
			body:       `{"ok":true}`,
			run: func(t *testing.T, c *raven.Client) {
				if err := c.DeleteCron(context.Background(), "cron_1"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:       "create api key",
			wantMethod: "POST",
			wantPath:   "/api/keys",
			body:       `{"key":"rav_live_secret","api_key":{"id":"k1","name":"ci","prefix":"rav_live_9f2k","scopes":["jobs:read"],"created_at":"2026-01-01T12:00:00Z","last_used_at":null}}`,
			run: func(t *testing.T, c *raven.Client) {
				resp, err := c.CreateAPIKey(context.Background(), raven.CreateAPIKeyRequest{
					Name: "ci", Scopes: []string{"jobs:read"},
				})
				if err != nil {
					t.Fatal(err)
				}
				if resp.Key != "rav_live_secret" || resp.APIKey.ID != "k1" {
					t.Fatalf("unexpected key response: %+v", resp)
				}
			},
		},
		{
			name:       "list api keys",
			wantMethod: "GET",
			wantPath:   "/api/keys",
			body:       `{"api_keys":[{"id":"k1","name":"ci","prefix":"rav_live_9f2k","scopes":["jobs:read"],"created_at":"2026-01-01T12:00:00Z","last_used_at":"2026-01-02T08:30:00Z"}]}`,
			run: func(t *testing.T, c *raven.Client) {
				keys, err := c.ListAPIKeys(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(keys) != 1 || keys[0].LastUsedAt == nil {
					t.Fatalf("unexpected keys: %+v", keys)
				}
			},
		},
		{
			name:       "revoke api key",
			wantMethod: "DELETE",
			wantPath:   "/api/keys/k1",
			body:       `{"ok":true}`,
			run: func(t *testing.T, c *raven.Client) {
				if err := c.RevokeAPIKey(context.Background(), "k1"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:       "workers",
			wantMethod: "GET",
			wantPath:   "/api/workers",
			body:       `{"workers":[{"id":"w-1","started_at":"2025-10-09T12:00:00Z","last_heartbeat":"2025-10-09T12:04:35Z","jobs_processed":"138","in_flight":"2"}]}`,
			run: func(t *testing.T, c *raven.Client) {
				workers, err := c.ListWorkers(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(workers) != 1 || workers[0].JobsProcessed != "138" {
					t.Fatalf("unexpected workers: %+v", workers)
				}
			},
		},
		{
			name:       "health services",
			wantMethod: "GET",
			wantPath:   "/api/health/services",
			body:       `{"checked_at":"2026-01-01T12:00:00Z","services":[{"name":"gateway","status":"ok","latency_ms":0,"detail":"self"},{"name":"worker_pool","status":"degraded","latency_ms":5,"detail":"no workers registered"}]}`,
			run: func(t *testing.T, c *raven.Client) {
				report, err := c.HealthServices(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(report.Services) != 2 || report.Services[1].Status != "degraded" {
					t.Fatalf("unexpected report: %+v", report)
				}
			},
		},
		{
			name:       "audit list",
			wantMethod: "GET",
			wantPath:   "/api/audit",
			wantQuery:  "action=job.cancel&limit=10",
			body:       `{"events":[{"id":7,"ts":"2026-01-01T12:00:00Z","actor_id":"u-1","action":"job.cancel","resource_type":"job","resource_id":"job_1","outcome":"allowed"}],"next_before_id":7}`,
			run: func(t *testing.T, c *raven.Client) {
				list, err := c.ListAuditEvents(context.Background(), raven.AuditFilter{
					Action: "job.cancel", Limit: 10,
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(list.Events) != 1 || list.NextBeforeID != 7 || list.Events[0].Outcome != "allowed" {
					t.Fatalf("unexpected audit list: %+v", list)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var idemSeen string
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tc.wantMethod {
					t.Errorf("method = %s, want %s", r.Method, tc.wantMethod)
				}
				if r.URL.Path != tc.wantPath {
					t.Errorf("path = %s, want %s", r.URL.Path, tc.wantPath)
				}
				if tc.wantQuery != "" && r.URL.RawQuery != tc.wantQuery {
					t.Errorf("query = %s, want %s", r.URL.RawQuery, tc.wantQuery)
				}
				idemSeen = r.Header.Get("Idempotency-Key")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.body))
			})
			_, c := newServer(t, handler)
			tc.run(t, c)

			// Mutating routes must carry an auto-generated Idempotency-Key.
			mutating := tc.wantMethod == "POST" || tc.wantMethod == "DELETE"
			isAuthRoute := strings.HasPrefix(tc.wantPath, "/api/auth/")
			if mutating && !isAuthRoute && idemSeen == "" {
				t.Error("mutating request went out without an Idempotency-Key")
			}
		})
	}
}

func TestErrorEnvelope(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":"job_not_found","message":"job does not exist","request_id":"req-123"}}`))
	})
	_, c := newServer(t, handler)

	_, err := c.GetJob(context.Background(), "job_nope")
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*raven.Error)
	if !ok {
		t.Fatalf("error is %T, want *raven.Error", err)
	}
	if re.Code != "job_not_found" || re.Message != "job does not exist" || re.RequestID != "req-123" || re.StatusCode != 404 {
		t.Fatalf("unexpected error: %+v", re)
	}
	if !raven.IsCode(err, "job_not_found") {
		t.Error("IsCode returned false for matching code")
	}
	if raven.IsCode(err, "other") {
		t.Error("IsCode returned true for foreign code")
	}
	if !strings.Contains(err.Error(), "req-123") {
		t.Errorf("Error() should quote the request id: %s", err.Error())
	}
}

func TestErrorNonEnvelopeBody(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>proxy exploded</html>"))
	})
	_, c := newServer(t, handler)

	_, err := c.GetJob(context.Background(), "job_1")
	re, ok := err.(*raven.Error)
	if !ok {
		t.Fatalf("error is %T, want *raven.Error", err)
	}
	if re.Code != "http_502" || re.StatusCode != 502 {
		t.Fatalf("unexpected fallback error: %+v", re)
	}
}

func TestIdempotencyKeyOverride(t *testing.T) {
	var seen string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jobJSON("job_1", "QUEUED")))
	})
	_, c := newServer(t, handler)

	payload, _ := raven.NewJobPayload(map[string]any{"url": "https://x.example"})
	_, err := c.CreateJob(context.Background(), raven.CreateJobRequest{
		Type: "webhook", Payload: payload,
	}, raven.WithIdempotencyKey("retry-attempt-42"))
	if err != nil {
		t.Fatal(err)
	}
	if seen != "retry-attempt-42" {
		t.Fatalf("Idempotency-Key = %q, want override", seen)
	}
}

func TestAPIKeyAuthHeader(t *testing.T) {
	var auth string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"workers":[]}`))
	})
	_, c := newServer(t, handler, raven.WithAPIKey("rav_live_abc"))

	if _, err := c.ListWorkers(context.Background()); err != nil {
		t.Fatal(err)
	}
	if auth != "ApiKey rav_live_abc" {
		t.Fatalf("Authorization = %q, want ApiKey scheme", auth)
	}
}

// TestAutoRefresh verifies that an expired access token triggers a refresh
// round-trip before the real request, and that a fresh pair is used.
func TestAutoRefresh(t *testing.T) {
	var refreshCalls, jobCalls int32
	var bearerSeen string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&refreshCalls, 1)
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["refresh_token"] != "rt-old" {
			t.Errorf("refresh presented %q, want rt-old", body["refresh_token"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at-new","refresh_token":"rt-new","access_expires_at":9999999999,"refresh_expires_at":9999999999}`))
	})
	mux.HandleFunc("/api/jobs/job_1", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&jobCalls, 1)
		bearerSeen = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jobJSON("job_1", "SUCCESS")))
	})
	_, c := newServer(t, mux, raven.WithTokenPair(raven.TokenPair{
		AccessToken:      "at-old",
		RefreshToken:     "rt-old",
		AccessExpiresAt:  time.Now().Add(-time.Minute).Unix(), // already stale
		RefreshExpiresAt: time.Now().Add(time.Hour).Unix(),
	}))

	if _, err := c.GetJob(context.Background(), "job_1"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&refreshCalls) != 1 {
		t.Fatalf("refresh calls = %d, want 1", refreshCalls)
	}
	if bearerSeen != "Bearer at-new" {
		t.Fatalf("request used %q, want the refreshed token", bearerSeen)
	}
	if c.Tokens().RefreshToken != "rt-new" {
		t.Fatalf("stored pair not rotated: %+v", c.Tokens())
	}
}

// TestRetryOn401 covers the clock-skew path: the server rejects a token the
// client thought was fresh, the client refreshes once and retries.
func TestRetryOn401(t *testing.T) {
	var jobCalls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at-new","refresh_token":"rt-new","access_expires_at":9999999999,"refresh_expires_at":9999999999}`))
	})
	mux.HandleFunc("/api/jobs/job_1", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&jobCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"token_expired","message":"expired","request_id":"r1"}}`))
			return
		}
		_, _ = w.Write([]byte(jobJSON("job_1", "SUCCESS")))
	})
	_, c := newServer(t, mux, raven.WithTokenPair(raven.TokenPair{
		AccessToken:      "at-old",
		RefreshToken:     "rt-old",
		AccessExpiresAt:  time.Now().Add(time.Hour).Unix(), // client thinks it is fresh
		RefreshExpiresAt: time.Now().Add(time.Hour).Unix(),
	}))

	job, err := c.GetJob(context.Background(), "job_1")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "SUCCESS" {
		t.Fatalf("unexpected job: %+v", job)
	}
	if atomic.LoadInt32(&jobCalls) != 2 {
		t.Fatalf("job calls = %d, want 2 (one 401 + one retry)", jobCalls)
	}
}

func TestLogoutClearsTokens(t *testing.T) {
	var gotBody map[string]string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	_, c := newServer(t, handler, raven.WithTokenPair(raven.TokenPair{
		AccessToken: "at", RefreshToken: "rt-gone",
	}))

	if err := c.Logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotBody["refresh_token"] != "rt-gone" {
		t.Fatalf("logout body = %+v, want the stored refresh token", gotBody)
	}
	if c.Tokens() != nil {
		t.Fatal("tokens should be cleared after logout")
	}
}

// TestWatchJob polls a job that flips from PROCESSING to SUCCESS and
// expects the terminal snapshot back.
func TestWatchJob(t *testing.T) {
	var calls int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if atomic.AddInt32(&calls, 1) < 3 {
			_, _ = w.Write([]byte(jobJSON("job_1", "PROCESSING")))
			return
		}
		_, _ = w.Write([]byte(jobJSON("job_1", "SUCCESS")))
	})
	_, c := newServer(t, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	job, err := c.WatchJob(ctx, "job_1", 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "SUCCESS" || !job.Terminal() {
		t.Fatalf("watch returned %+v, want terminal SUCCESS", job)
	}
	if atomic.LoadInt32(&calls) < 3 {
		t.Fatalf("watch polled %d times, want >= 3", calls)
	}
}

func TestWatchJobContextCancel(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jobJSON("job_1", "PROCESSING")))
	})
	_, c := newServer(t, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, err := c.WatchJob(ctx, "job_1", 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected context error from a watch that never terminates")
	}
}

func TestCustomHTTPClientAndTimeout(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jobJSON("job_1", "QUEUED")))
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	// A 50 ms client timeout must surface as an error wrapping a deadline.
	c := raven.New(raven.WithBaseURL(srv.URL), raven.WithTimeout(50*time.Millisecond))
	if _, err := c.GetJob(context.Background(), "job_1"); err == nil {
		t.Fatal("expected a timeout error")
	}

	// WithHTTPClient swaps the transport entirely.
	c2 := raven.New(raven.WithBaseURL(srv.URL), raven.WithHTTPClient(&http.Client{Timeout: 2 * time.Second}))
	if _, err := c2.GetJob(context.Background(), "job_1"); err != nil {
		t.Fatalf("custom http client should have waited: %v", err)
	}
}
