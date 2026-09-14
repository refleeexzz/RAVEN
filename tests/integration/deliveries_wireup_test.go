//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

// Deliveries wire-up end to end: REST → gateway → gRPC
// (raven.jobs.v1.JobDeliveriesService) → the owner-scoped read path, on a
// real per-test Postgres (000002 + 000003 + 000005) with seeded delivery
// rows. Uses the gateway's production Run entrypoint.

// deliveriesFakeAuth knows three tokens: owner, stranger, admin.
type deliveriesFakeAuth struct {
	genauth.UnimplementedAuthServiceServer
	ownerID string
}

func (f deliveriesFakeAuth) ValidateToken(_ context.Context, req *genauth.ValidateTokenRequest) (*genauth.ValidateTokenResponse, error) {
	switch req.GetAccessToken() {
	case "token-owner":
		return &genauth.ValidateTokenResponse{
			Valid: true, UserId: f.ownerID, Permissions: []string{"jobs:read"},
		}, nil
	case "token-stranger":
		return &genauth.ValidateTokenResponse{
			Valid: true, UserId: uuid.NewString(), Permissions: []string{"jobs:read"},
		}, nil
	case "token-admin":
		return &genauth.ValidateTokenResponse{
			Valid: true, UserId: uuid.NewString(), Permissions: []string{"jobs:read", "admin:*"},
		}, nil
	}
	return &genauth.ValidateTokenResponse{Valid: false}, nil
}

func TestDeliveriesWireUpEndToEnd(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool := deliveriesDB(t, "raven_deliveries_wire")

	ownerID := uuid.NewString()
	jobID := "job-" + uuid.NewString()

	// Seed: one owned job with three delivery attempts (success, HTTP 500,
	// blocked) — the full shape matrix in one page.
	if _, err := pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, attempts, max_attempts, owner_id)
		VALUES ($1, 'webhook', '{"url":"https://hook.example"}', 'SUCCESS', 5, 1, 4, $2)`,
		jobID, ownerID); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	for _, d := range []struct {
		attempt int
		status  *int
		latency *int
		blocked bool
		errMsg  string
	}{
		{attempt: 1, status: ptr(200), latency: ptr(12), blocked: false, errMsg: ""},
		{attempt: 2, status: ptr(500), latency: ptr(31), blocked: false, errMsg: "webhook returned 500"},
		{attempt: 3, status: nil, latency: nil, blocked: true, errMsg: "scheme not allowed"},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO webhook_deliveries (job_id, attempt, url, status_code, latency_ms, response_snippet, blocked, error)
			VALUES ($1, $2, 'https://hook.example', $3, $4, 'snippet', $5, $6)`,
			jobID, d.attempt, d.status, d.latency, d.blocked, d.errMsg); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
	}
	// A stranger's job with its own delivery — the owner must never see it.
	foreignJobID := "job-" + uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, attempts, max_attempts, owner_id)
		VALUES ($1, 'webhook', '{}', 'SUCCESS', 5, 1, 4, $2)`,
		foreignJobID, uuid.NewString()); err != nil {
		t.Fatalf("seed foreign job: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	jobsSrv := jobs.NewServer(pool, nil, nil, log, nil)
	jobsAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genjobs.RegisterJobServiceServer(s, jobsSrv)
		genjobs.RegisterJobDeliveriesServiceServer(s, jobs.NewDeliveriesServer(jobsSrv))
	})
	authAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genauth.RegisterAuthServiceServer(s, deliveriesFakeAuth{ownerID: ownerID})
	})
	base := bootKeysGateway(t, authAddr, jobsAddr, "")

	// Owner reads the full history, oldest first, NULLs preserved.
	status, body := keysReq(t, "GET", base+"/api/jobs/"+jobID+"/deliveries", "Bearer token-owner", "")
	if status != http.StatusOK {
		t.Fatalf("owner list: got %d (%v), want 200", status, body)
	}
	page := body["page"].(map[string]any)
	if page["total"].(float64) != 3 {
		t.Fatalf("total: got %v, want 3", page["total"])
	}
	ds := body["deliveries"].([]any)
	if len(ds) != 3 {
		t.Fatalf("deliveries: got %d, want 3", len(ds))
	}
	first := ds[0].(map[string]any)
	if first["status_code"].(float64) != 200 || first["latency_ms"].(float64) != 12 {
		t.Errorf("row 1: got %v", first)
	}
	third := ds[2].(map[string]any)
	if _, present := third["status_code"]; !present {
		t.Error("row 3 must carry the status_code key...")
	}
	if third["status_code"] != nil || third["latency_ms"] != nil {
		t.Errorf("row 3 (blocked): status/latency must be JSON null, got %v", third)
	}
	if third["blocked"] != true {
		t.Errorf("row 3 blocked: got %v", third["blocked"])
	}

	// Pagination flows through (?page_size=2&page=2 → the third row).
	status, body = keysReq(t, "GET", base+"/api/jobs/"+jobID+"/deliveries?page_size=2&page=2", "Bearer token-owner", "")
	if status != http.StatusOK {
		t.Fatalf("page 2: got %d", status)
	}
	if got := len(body["deliveries"].([]any)); got != 1 {
		t.Errorf("page 2 size: got %d, want 1", got)
	}

	// A stranger gets 404 — foreign delivery history is indistinguishable
	// from a missing job (JOBS-02, enforced by the jobs service).
	status, _ = keysReq(t, "GET", base+"/api/jobs/"+jobID+"/deliveries", "Bearer token-stranger", "")
	if status != http.StatusNotFound {
		t.Errorf("stranger: got %d, want 404", status)
	}
	status, _ = keysReq(t, "GET", base+"/api/jobs/"+foreignJobID+"/deliveries", "Bearer token-owner", "")
	if status != http.StatusNotFound {
		t.Errorf("owner on foreign job: got %d, want 404", status)
	}

	// Admin bypass works THROUGH the gateway now: the edge forwards
	// x-user-perms and the jobs service honors admin:*.
	status, body = keysReq(t, "GET", base+"/api/jobs/"+foreignJobID+"/deliveries", "Bearer token-admin", "")
	if status != http.StatusOK {
		t.Errorf("admin on foreign job: got %d (%v), want 200", status, body)
	}

	// The route requires jobs:read — anonymous is 401.
	status, _ = keysReq(t, "GET", base+"/api/jobs/"+jobID+"/deliveries", "", "")
	if status != http.StatusUnauthorized {
		t.Errorf("anonymous: got %d, want 401", status)
	}
}

func ptr[T any](v T) *T { return &v }
