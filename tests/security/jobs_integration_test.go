//go:build integration

// Integration security tests for the job system (JOBS-01 SSRF end-to-end,
// JOBS-02 BOLA over the wire, JOBS-03 payload cap, JOBS-04 pagination
// overflow). They reuse the auth suite's lazy container harness
// (authSecCheck) and apply the jobs migrations on top. A real worker runs
// against an in-process broker so the webhook egress guard is attacked over
// the full path: gRPC create -> broker -> worker -> HTTP target.
//
// Findings are registered in docs/security/jobs.md.
package security

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/refleeexzz/RAVEN/internal/broker"
	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
	workersvc "github.com/refleeexzz/RAVEN/services/worker"
)

var (
	jobsSecOnce sync.Once
	jobsSecErr  error

	jobsSecClient     genjobs.JobServiceClient
	jobsSecBrokerAddr string
	jobsSecDSN        string
	jobsSecRedisAddr  string
	jobsSecLog        *slog.Logger
)

// jobsSecCheck layers the jobs fixtures on the shared containers: migrations
// 000002 + 000003, an in-process broker and the jobs gRPC service.
func jobsSecCheck(t *testing.T) {
	t.Helper()
	authSecCheck(t) // shared postgres + redis, or Skip
	jobsSecOnce.Do(func() {
		jobsSecErr = jobsSecSetup()
	})
	if jobsSecErr != nil {
		t.Skipf("skipping jobs security integration test: %v", jobsSecErr)
	}
}

func jobsSecSetup() error {
	ctx := context.Background()
	jobsSecLog = slog.New(slog.NewTextHandler(io.Discard, nil))

	dsn, err := authSecE.pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("postgres dsn: %w", err)
	}
	jobsSecDSN = dsn
	for _, mig := range []string{"000002_jobs.up.sql", "000003_job_leases.up.sql"} {
		if err := jobsSecApplyMigration(ctx, dsn, mig); err != nil {
			return err
		}
	}

	redisURL, err := authSecE.redisContainer.ConnectionString(ctx)
	if err != nil {
		return fmt.Errorf("redis url: %w", err)
	}
	redisOpt, err := goredis.ParseURL(redisURL)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	jobsSecRedisAddr = redisOpt.Addr

	addr, err := jobsSecStartBroker()
	if err != nil {
		return err
	}
	jobsSecBrokerAddr = addr

	if err := jobs.EnsureTopics(ctx, addr, jobsSecLog); err != nil {
		return fmt.Errorf("ensure topics: %w", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	genjobs.RegisterJobServiceServer(srv,
		jobs.NewServer(authSecE.pool, authSecE.rdb, jobs.NewProducer(addr, jobsSecLog), jobsSecLog, nil))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial jobs: %w", err)
	}
	jobsSecClient = genjobs.NewJobServiceClient(conn)
	return nil
}

// jobsSecApplyMigration runs one migration file verbatim (simple protocol),
// same pattern as tests/integration.
func jobsSecApplyMigration(ctx context.Context, dsn, file string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", file))
	if err != nil {
		return fmt.Errorf("read %s: %w", file, err)
	}
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		func() {
			conn, cerr := pgx.ConnectConfig(ctx, cfg)
			if cerr != nil {
				lastErr = cerr
				return
			}
			defer conn.Close(ctx)
			_, lastErr = conn.Exec(ctx, string(raw))
		}()
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("apply %s: %w", file, lastErr)
}

// jobsSecStartBroker boots an in-process broker on a random port. Cleanup is
// left to process exit, like the suite's containers.
func jobsSecStartBroker() (string, error) {
	b, err := broker.New(broker.Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        os.TempDir() + string(os.PathSeparator) + fmt.Sprintf("raven-sec-broker-%d", time.Now().UnixNano()),
		FsyncEvery:     20 * time.Millisecond,
		FsyncRecords:   1000,
		SessionTimeout: 10 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		return "", fmt.Errorf("broker.New: %w", err)
	}
	go func() { _ = b.Run(context.Background()) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if a := b.Addr(); a != "" {
			if c, err := net.DialTimeout("tcp", a, 200*time.Millisecond); err == nil {
				_ = c.Close()
				return a, nil
			}
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("broker did not start listening in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- helpers ---------------------------------------------------------------

// asUser returns a context carrying the identity the gateway would forward.
func asUser(ctx context.Context, userID string, perms ...string) context.Context {
	md := metadata.Pairs("x-user-id", userID)
	if len(perms) > 0 {
		md.Set("x-user-perms", strings.Join(perms, ","))
	}
	return metadata.NewOutgoingContext(ctx, md)
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want gRPC %s, got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("want gRPC %s, got %s (%v)", want, got, err)
	}
}

func jobsSecWaitStatus(t *testing.T, id, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var st string
		if err := authSecE.pool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, id).Scan(&st); err == nil && st == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	var st string
	_ = authSecE.pool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, id).Scan(&st)
	t.Fatalf("job %s did not reach %s in %s (last: %s)", id, want, timeout, st)
}

// startJobsSecWorker runs a worker until the returned cancel is called.
func startJobsSecWorker(t *testing.T, allowPrivate bool) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workersvc.Run(ctx, workersvc.Config{
			HTTPAddr:            "127.0.0.1:0",
			DatabaseURL:         jobsSecDSN,
			RedisAddr:           jobsSecRedisAddr,
			BrokerAddr:          jobsSecBrokerAddr,
			LogLevel:            "warn",
			Concurrency:         4,
			JobTimeout:          10 * time.Second,
			WebhookAllowPrivate: allowPrivate,
		})
	}()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("worker run: %v", err)
			}
		case <-time.After(45 * time.Second):
			t.Error("worker did not shut down in time")
		}
	}
}

// --- JOBS-02: BOLA / IDOR over the wire ------------------------------------

func TestIntegrationJobOwnerIsolation(t *testing.T) {
	jobsSecCheck(t)
	ctx := context.Background()

	// User A creates a job. No worker runs in this test: it stays QUEUED.
	jobA, err := jobsSecClient.CreateJob(asUser(ctx, userA), &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"a@example.com"}`})
	if err != nil {
		t.Fatalf("CreateJob as A: %v", err)
	}

	// User B: GetJob / CancelJob on A's job must look exactly like "no such
	// job" — no existence oracle, no foreign state changes.
	_, err = jobsSecClient.GetJob(asUser(ctx, userB), &genjobs.GetJobRequest{Id: jobA.GetId()})
	requireCode(t, err, codes.NotFound)
	_, err = jobsSecClient.CancelJob(asUser(ctx, userB), &genjobs.CancelJobRequest{Id: jobA.GetId()})
	requireCode(t, err, codes.NotFound)
	_, err = jobsSecClient.RequeueJob(asUser(ctx, userB), &genjobs.RequeueJobRequest{Id: jobA.GetId()})
	requireCode(t, err, codes.NotFound)

	// B's listing must not contain A's job, and the total must be honest
	// about the scope.
	listB, err := jobsSecClient.ListJobs(asUser(ctx, userB), &genjobs.ListJobsRequest{
		Page: &gencommon.PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("ListJobs as B: %v", err)
	}
	for _, j := range listB.GetJobs() {
		if j.GetId() == jobA.GetId() {
			t.Fatal("ListJobs leaked another user's job")
		}
	}

	// A sees and cancels their own job.
	got, err := jobsSecClient.GetJob(asUser(ctx, userA), &genjobs.GetJobRequest{Id: jobA.GetId()})
	if err != nil || got.GetId() != jobA.GetId() {
		t.Fatalf("GetJob as A: %+v, %v", got, err)
	}
	cancelled, err := jobsSecClient.CancelJob(asUser(ctx, userA), &genjobs.CancelJobRequest{Id: jobA.GetId()})
	if err != nil || cancelled.GetStatus() != genjobs.JobStatus_JOB_STATUS_CANCELLED {
		t.Fatalf("CancelJob as A: %+v, %v", cancelled, err)
	}

	// Admin bypass (admin:* in x-user-perms) works for any job.
	adminCtx := asUser(ctx, userB, "jobs:read", "admin:*")
	if _, err := jobsSecClient.GetJob(adminCtx, &genjobs.GetJobRequest{Id: jobA.GetId()}); err != nil {
		t.Fatalf("admin GetJob on foreign job: %v", err)
	}

	// System callers (no identity — internal traffic on the gRPC port) keep
	// full access; the existing jobs/worker suite depends on this.
	if _, err := jobsSecClient.GetJob(ctx, &genjobs.GetJobRequest{Id: jobA.GetId()}); err != nil {
		t.Fatalf("system GetJob: %v", err)
	}

	// Ownerless job (created by a system caller): invisible to users,
	// visible to admins and system.
	sysJob, err := jobsSecClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"sys@example.com"}`})
	if err != nil {
		t.Fatalf("CreateJob as system: %v", err)
	}
	_, err = jobsSecClient.GetJob(asUser(ctx, userA), &genjobs.GetJobRequest{Id: sysJob.GetId()})
	requireCode(t, err, codes.NotFound)
	if _, err := jobsSecClient.GetJob(adminCtx, &genjobs.GetJobRequest{Id: sysJob.GetId()}); err != nil {
		t.Fatalf("admin GetJob on ownerless job: %v", err)
	}

	// A malformed identity must be rejected, never silently trusted.
	badIDCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-user-id", "not-a-uuid"))
	_, err = jobsSecClient.GetJob(badIDCtx, &genjobs.GetJobRequest{Id: jobA.GetId()})
	requireCode(t, err, codes.Unauthenticated)

	// Permissions without an identity are a privilege-injection attempt.
	forgeCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-user-perms", "admin:*"))
	_, err = jobsSecClient.GetJob(forgeCtx, &genjobs.GetJobRequest{Id: jobA.GetId()})
	requireCode(t, err, codes.Unauthenticated)
}

func TestIntegrationIdempotencyAcrossOwners(t *testing.T) {
	jobsSecCheck(t)
	ctx := context.Background()

	key := fmt.Sprintf("sec-idem-%d", time.Now().UnixNano())
	first, err := jobsSecClient.CreateJob(asUser(ctx, userA), &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"owner-a@example.com"}`, IdempotencyKey: key})
	if err != nil {
		t.Fatalf("CreateJob A: %v", err)
	}

	// Same key, same owner: idempotent replay returns the same job.
	replay, err := jobsSecClient.CreateJob(asUser(ctx, userA), &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"owner-a@example.com"}`, IdempotencyKey: key})
	if err != nil || replay.GetId() != first.GetId() {
		t.Fatalf("same-owner replay: got %v, %v", replay.GetId(), err)
	}

	// Same key, different owner: the key is taken — but the other user's job
	// must NOT leak back.
	_, err = jobsSecClient.CreateJob(asUser(ctx, userB), &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"thief@example.com"}`, IdempotencyKey: key})
	requireCode(t, err, codes.AlreadyExists)
}

// --- JOBS-03 / JOBS-04: input hardening over the wire ----------------------

func TestIntegrationPayloadCapOverGRPC(t *testing.T) {
	jobsSecCheck(t)
	ctx := context.Background()

	over := `{"pad":"` + strings.Repeat("a", jobs.MaxPayloadBytes) + `"}`
	_, err := jobsSecClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: over})
	requireCode(t, err, codes.InvalidArgument)

	atCap := `{"pad":"` + strings.Repeat("a", jobs.MaxPayloadBytes-len(`{"pad":""}`)) + `"}`
	if _, err := jobsSecClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: atCap}); err != nil {
		t.Fatalf("payload at the cap must be accepted: %v", err)
	}
}

func TestIntegrationPaginationNoOverflow(t *testing.T) {
	jobsSecCheck(t)
	ctx := context.Background()

	// (MaxInt32-1)*100 used to wrap the int32 multiplication and hand
	// Postgres a negative OFFSET -> Internal on a syntactically valid list.
	resp, err := jobsSecClient.ListJobs(ctx, &genjobs.ListJobsRequest{
		Page: &gencommon.PageRequest{Page: math.MaxInt32, PageSize: 100}})
	if err != nil {
		t.Fatalf("ListJobs with huge page: %v", err)
	}
	if resp.GetPage().GetPage() < 1 || resp.GetPage().GetPage() > jobs.MaxListPage {
		t.Errorf("page not clamped: got %d", resp.GetPage().GetPage())
	}

	// Negative/zero page params fall back to defaults instead of erroring.
	resp, err = jobsSecClient.ListJobs(ctx, &genjobs.ListJobsRequest{
		Page: &gencommon.PageRequest{Page: -3, PageSize: -5}})
	if err != nil {
		t.Fatalf("ListJobs with negative params: %v", err)
	}
	if resp.GetPage().GetPage() != 1 || resp.GetPage().GetPageSize() != 20 {
		t.Errorf("negative params: got page %d size %d, want (1, 20)",
			resp.GetPage().GetPage(), resp.GetPage().GetPageSize())
	}
}

// --- JOBS-01: SSRF end to end ----------------------------------------------

// A webhook job pointing at a loopback HTTP server must be refused by the
// egress guard and land in the DLQ path (DEAD, permanent) without a single
// request leaving the worker. With the dev escape hatch the same job runs.
func TestIntegrationWebhookSSRFEndToEnd(t *testing.T) {
	jobsSecCheck(t)
	ctx := context.Background()

	var hits atomic.Int64
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer hook.Close()

	// Phase 1: secure-by-default worker. The env escape hatch is explicitly
	// off so the test cannot inherit it from the outside.
	t.Setenv("WORKER_WEBHOOK_ALLOW_PRIVATE", "false")
	stop1 := startJobsSecWorker(t, false)

	victim, err := jobsSecClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: `{"url":"` + hook.URL + `"}`})
	if err != nil {
		t.Fatalf("CreateJob webhook: %v", err)
	}
	jobsSecWaitStatus(t, victim.GetId(), "DEAD", 30*time.Second)
	stop1()

	if got := hits.Load(); got != 0 {
		t.Fatalf("SSRF guard failed: the loopback target was hit %d times", got)
	}
	var errMsg string
	if err := authSecE.pool.QueryRow(ctx,
		`SELECT error FROM jobs WHERE id = $1`, victim.GetId()).Scan(&errMsg); err != nil {
		t.Fatalf("read job error: %v", err)
	}
	if !strings.Contains(errMsg, "not allowed") {
		t.Errorf("DEAD error must name the egress refusal, got %q", errMsg)
	}

	// Phase 2: the env escape hatch (what the existing jobs/worker
	// integration suite relies on). Requeue the dead job; it must succeed.
	t.Setenv("WORKER_WEBHOOK_ALLOW_PRIVATE", "true")
	stop2 := startJobsSecWorker(t, false) // Config false; env true wins
	if _, err := jobsSecClient.RequeueJob(ctx, &genjobs.RequeueJobRequest{Id: victim.GetId()}); err != nil {
		t.Fatalf("RequeueJob: %v", err)
	}
	jobsSecWaitStatus(t, victim.GetId(), "SUCCESS", 30*time.Second)
	stop2()
	if got := hits.Load(); got == 0 {
		t.Error("with the escape hatch the webhook should have been delivered")
	}
}
