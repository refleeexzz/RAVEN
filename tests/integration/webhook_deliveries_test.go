//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/metadata"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/database"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
	workersvc "github.com/refleeexzz/RAVEN/services/worker"
)

// Webhook delivery observability: every attempt (success, HTTP error,
// transport error, egress-guard refusal) must leave one row in
// webhook_deliveries, and ListDeliveries must serve it owner-scoped.
//
// Every test gets its own database with migrations 000002 + 000003 + 000005
// applied verbatim, following the recoveryDB pattern: shared-schema
// isolation by database, never by table.

// deliveriesDB is recoveryDB plus migration 000005 (webhook_deliveries).
func deliveriesDB(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ident := pgx.Identifier{name}.Sanitize()
	if _, err := env.pool.Exec(ctx, `DROP DATABASE IF EXISTS `+ident+` WITH (FORCE)`); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
	if _, err := env.pool.Exec(ctx, `CREATE DATABASE `+ident); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}

	base, err := env.pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()

	for _, m := range []string{
		"000002_jobs.up.sql",
		"000003_job_leases.up.sql",
		"000005_webhook_deliveries.up.sql",
	} {
		if err := applyMigrationFile(ctx, dsn, m); err != nil {
			t.Fatalf("apply %s: %v", m, err)
		}
	}

	pool, err := database.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// startDeliveryWorker boots one worker consuming the legacy topic with
// private webhook targets allowed (httptest lives on loopback).
func startDeliveryWorker(t *testing.T, pool *pgxpool.Pool, brokerAddr, id string) (*workersvc.Worker, func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	producer := jobs.NewProducer(brokerAddr, log)
	t.Cleanup(func() { _ = producer.Close() })

	w := workersvc.New(workersvc.Params{
		Pool:                 pool,
		Producer:             producer,
		Log:                  log,
		Concurrency:          2,
		JobTimeout:           10 * time.Second,
		JobLease:             30 * time.Second,
		WorkerID:             id,
		AllowPrivateWebhooks: true,
	})

	consumer := client.NewConsumer(brokerAddr, "workers", []string{jobs.TopicJobs},
		w.Handle, client.WithConsumerLogger(log))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = consumer.Run(ctx) }()

	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("consumer did not stop in time")
		}
	}
	return w, stop
}

// deliveryRow is one webhook_deliveries row read back for assertions.
type deliveryRow struct {
	Attempt    int
	URL        string
	StatusCode *int
	LatencyMS  *int
	Snippet    string
	Blocked    bool
	Err        string
}

func readDeliveries(t *testing.T, pool *pgxpool.Pool, jobID string) []deliveryRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT attempt, url, status_code, latency_ms, response_snippet, blocked, error
		FROM webhook_deliveries WHERE job_id = $1 ORDER BY ts, id`, jobID)
	if err != nil {
		t.Fatalf("read deliveries of %s: %v", jobID, err)
	}
	defer rows.Close()
	var out []deliveryRow
	for rows.Next() {
		var d deliveryRow
		if err := rows.Scan(&d.Attempt, &d.URL, &d.StatusCode, &d.LatencyMS,
			&d.Snippet, &d.Blocked, &d.Err); err != nil {
			t.Fatalf("scan delivery: %v", err)
		}
		out = append(out, d)
	}
	return out
}

// waitDeliveries polls until the job has at least n delivery rows.
func waitDeliveries(t *testing.T, pool *pgxpool.Pool, jobID string, n int, timeout time.Duration) []deliveryRow {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := readDeliveries(t, pool, jobID); len(got) >= n {
			return got
		}
		time.Sleep(100 * time.Millisecond)
	}
	got := readDeliveries(t, pool, jobID)
	t.Fatalf("job %s has %d deliveries after %s, want %d", jobID, len(got), timeout, n)
	return nil
}

func TestWebhookDeliverySuccessRecorded(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool := deliveriesDB(t, "raven_deliveries_ok")
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer hook.Close()

	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	jobsClient := startJobsAPI(t, pool, producer, log)

	_, stopWorker := startDeliveryWorker(t, pool, brokerAddr, "worker-deliveries-ok")
	defer stopWorker()

	ownerID := uuid.NewString()
	ownerCtx := metadata.AppendToOutgoingContext(ctx, "x-user-id", ownerID)
	job, err := jobsClient.CreateJob(ownerCtx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: `{"url":"` + hook.URL + `","event":"ping"}`, Priority: 5})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	waitJobStatusFor(t, pool, job.GetId(), "SUCCESS", 30*time.Second)

	rows := waitDeliveries(t, pool, job.GetId(), 1, 10*time.Second)
	d := rows[0]
	if d.Attempt != 1 {
		t.Errorf("attempt: got %d, want 1", d.Attempt)
	}
	if d.URL != hook.URL {
		t.Errorf("url: got %q, want %q", d.URL, hook.URL)
	}
	if d.StatusCode == nil || *d.StatusCode != 200 {
		t.Errorf("status_code: got %v, want 200", d.StatusCode)
	}
	if d.LatencyMS == nil {
		t.Error("latency_ms must be set on a successful delivery")
	}
	if d.Snippet == "" {
		t.Error("snippet must carry the response body prefix")
	}
	if d.Blocked {
		t.Error("blocked must be false on a successful delivery")
	}
	if d.Err != "" {
		t.Errorf("error: got %q, want empty", d.Err)
	}

	// Owner-scoped read: the owner sees the history...
	srv := jobs.NewServer(pool, nil, nil, log, nil)
	ownerRead := metadata.NewIncomingContext(ctx, metadata.Pairs("x-user-id", ownerID))
	page, total, err := srv.ListDeliveries(ownerRead, job.GetId(), 1, 20)
	if err != nil {
		t.Fatalf("ListDeliveries (owner): %v", err)
	}
	if total != 1 || len(page) != 1 {
		t.Fatalf("ListDeliveries (owner): got %d rows total %d, want 1", len(page), total)
	}
	if page[0].StatusCode == nil || *page[0].StatusCode != 200 {
		t.Errorf("ListDeliveries status: got %v, want 200", page[0].StatusCode)
	}

	// ...a stranger gets NotFound (no existence oracle, JOBS-02)...
	strangerRead := metadata.NewIncomingContext(ctx, metadata.Pairs("x-user-id", uuid.NewString()))
	if _, _, err := srv.ListDeliveries(strangerRead, job.GetId(), 1, 20); err == nil {
		t.Fatal("ListDeliveries (stranger): want NotFound error")
	}

	// ...admin:* bypasses...
	adminRead := metadata.NewIncomingContext(ctx, metadata.Pairs(
		"x-user-id", uuid.NewString(), "x-user-perms", "admin:*"))
	if _, _, err := srv.ListDeliveries(adminRead, job.GetId(), 1, 20); err != nil {
		t.Fatalf("ListDeliveries (admin): %v", err)
	}

	// ...and no identity at all is internal traffic with full access.
	if _, _, err := srv.ListDeliveries(ctx, job.GetId(), 1, 20); err != nil {
		t.Fatalf("ListDeliveries (system): %v", err)
	}
}

func TestWebhookDeliveryFailuresRecorded(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool := deliveriesDB(t, "raven_deliveries_fail")
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	// Always-500 target: the job dies after the default 4 attempts and every
	// one of them must leave a row.
	var calls atomic.Int64
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("kaput"))
	}))
	defer fail.Close()

	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	jobsClient := startJobsAPI(t, pool, producer, log)

	_, stopWorker := startDeliveryWorker(t, pool, brokerAddr, "worker-deliveries-fail")
	defer stopWorker()

	httpJob, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: `{"url":"` + fail.URL + `"}`, Priority: 5})
	if err != nil {
		t.Fatalf("CreateJob (500 target): %v", err)
	}

	// Egress refusal: the scheme check refuses ftp:// no matter the
	// allow-private escape hatch, so this works even with
	// WORKER_WEBHOOK_ALLOW_PRIVATE=true in the suite environment.
	blockedJob, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: `{"url":"ftp://example.com/hook"}`, Priority: 5})
	if err != nil {
		t.Fatalf("CreateJob (blocked target): %v", err)
	}

	// Transport error: connection refused on loopback port 1.
	transportJob, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: `{"url":"http://127.0.0.1:1/hook"}`, Priority: 5})
	if err != nil {
		t.Fatalf("CreateJob (transport): %v", err)
	}

	waitJobStatusFor(t, pool, httpJob.GetId(), "DEAD", 60*time.Second)
	waitJobStatusFor(t, pool, blockedJob.GetId(), "DEAD", 30*time.Second)
	waitJobStatusFor(t, pool, transportJob.GetId(), "DEAD", 60*time.Second)

	// HTTP error: 4 attempts, 4 rows, all with status 500 and the error.
	httpRows := waitDeliveries(t, pool, httpJob.GetId(), 4, 10*time.Second)
	for i, d := range httpRows {
		if d.Attempt != i+1 {
			t.Errorf("row %d attempt: got %d, want %d", i, d.Attempt, i+1)
		}
		if d.StatusCode == nil || *d.StatusCode != 500 {
			t.Errorf("row %d status: got %v, want 500", i, d.StatusCode)
		}
		if d.Blocked {
			t.Errorf("row %d blocked: got true, want false", i)
		}
		if d.Err == "" {
			t.Errorf("row %d error must describe the 500", i)
		}
		if d.Snippet != "kaput" {
			t.Errorf("row %d snippet: got %q, want %q", i, d.Snippet, "kaput")
		}
	}

	// Blocked: exactly 1 row (permanent failure, no retries), blocked=true,
	// no status code — the request never reached the wire.
	blockedRows := waitDeliveries(t, pool, blockedJob.GetId(), 1, 10*time.Second)
	if len(blockedRows) != 1 {
		t.Fatalf("blocked job rows: got %d, want exactly 1", len(blockedRows))
	}
	if d := blockedRows[0]; !d.Blocked || d.StatusCode != nil || d.Err == "" {
		t.Errorf("blocked row: blocked=%v status=%v err=%q", d.Blocked, d.StatusCode, d.Err)
	}

	// Transport: 4 attempts, 4 rows, status NULL, error set, not blocked.
	transportRows := waitDeliveries(t, pool, transportJob.GetId(), 4, 10*time.Second)
	for i, d := range transportRows {
		if d.StatusCode != nil {
			t.Errorf("row %d status: got %v, want NULL (no response)", i, d.StatusCode)
		}
		if d.Blocked {
			t.Errorf("row %d blocked: got true, want false (transport, not policy)", i)
		}
		if d.Err == "" {
			t.Errorf("row %d error must describe the transport failure", i)
		}
	}
}

// TestWebhookDeliveryInsertFailureDoesNotBreakJobs proves the best-effort
// contract: a worker pointing at a schema WITHOUT webhook_deliveries still
// executes webhook jobs to completion; the failed insert only shows up in
// the logs.
func TestWebhookDeliveryInsertFailureDoesNotBreakJobs(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool := recoveryDB(t, "raven_deliveries_notable") // 000002 + 000003 only
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer hook.Close()

	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	jobsClient := startJobsAPI(t, pool, producer, log)

	_, stopWorker := startDeliveryWorker(t, pool, brokerAddr, "worker-deliveries-notable")
	defer stopWorker()

	job, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: `{"url":"` + hook.URL + `"}`, Priority: 5})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// The table does not exist in this schema; the job must still succeed.
	waitJobStatusFor(t, pool, job.GetId(), "SUCCESS", 30*time.Second)
}

// waitJobStatusFor is waitJobStatus against an explicit pool (the shared one
// only knows the default database).
func waitJobStatusFor(t *testing.T, pool *pgxpool.Pool, id, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, id).Scan(&last); err != nil {
			t.Fatalf("read status of %s: %v", id, err)
		}
		if last == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s in %s (last: %s)", id, want, timeout, last)
}
