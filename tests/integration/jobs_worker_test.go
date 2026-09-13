//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
	workersvc "github.com/refleeexzz/RAVEN/services/worker"
)

// TestJobsWorkerLifecycle runs the whole job system end to end: in-process
// broker, real Postgres + Redis (from the shared TestMain containers), the
// jobs gRPC service and one worker process wired through services/worker.Run.
func TestJobsWorkerLifecycle(t *testing.T) {
	check(t) // shared env: postgres + redis in testcontainers
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Apply migration 000002 on top of the 000001 schema the shared env
	// already applied.
	dsn, err := env.pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	if err := applyMigrationFile(ctx, dsn, "000002_jobs.up.sql"); err != nil {
		t.Fatalf("apply migration 000002: %v", err)
	}

	redisURL, err := env.redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}
	redisOpt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}

	// In-process broker on a random port (same pattern as internal/broker's
	// e2e tests).
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()

	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	// Jobs gRPC service on a random loopback port.
	jobsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	jobsGRPC := grpc.NewServer()
	genjobs.RegisterJobServiceServer(jobsGRPC,
		jobs.NewServer(env.pool, env.rdb, producer, log, nil))
	go func() { _ = jobsGRPC.Serve(jobsLis) }()
	defer jobsGRPC.Stop()

	conn, err := grpc.NewClient(jobsLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial jobs: %v", err)
	}
	defer conn.Close()
	jobsClient := genjobs.NewJobServiceClient(conn)

	// Subscribe to the live event stream before anything runs.
	events, stopEvents := collectJobEvents(ctx, t)
	defer stopEvents()

	// -----------------------------------------------------------------------
	// Phase A: no worker yet. Validation, idempotency and cancel are
	// deterministic because nothing consumes the queue.
	// -----------------------------------------------------------------------

	// Validation errors.
	_, err = jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "mine_crypto", PayloadJson: `{}`, Priority: 5})
	requireGRPCCode(t, err, codes.InvalidArgument)
	_, err = jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{broken`, Priority: 5})
	requireGRPCCode(t, err, codes.InvalidArgument)
	_, err = jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{}`, Priority: 42})
	requireGRPCCode(t, err, codes.InvalidArgument)

	// Cancel a QUEUED job. The broker message stays behind; the worker fence
	// must skip it later.
	cancelled, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type:        "send_email",
		PayloadJson: `{"to":"cancelled@example.com","subject":"never sent"}`,
		Priority:    5,
	})
	if err != nil {
		t.Fatalf("CreateJob (to cancel): %v", err)
	}
	if cancelled.GetStatus() != genjobs.JobStatus_JOB_STATUS_QUEUED {
		t.Fatalf("new job status: got %v, want QUEUED", cancelled.GetStatus())
	}
	got, err := jobsClient.CancelJob(ctx, &genjobs.CancelJobRequest{Id: cancelled.GetId()})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if got.GetStatus() != genjobs.JobStatus_JOB_STATUS_CANCELLED {
		t.Fatalf("cancelled job status: got %v, want CANCELLED", got.GetStatus())
	}
	// Cancelling again is a conflict (maps to AlreadyExists).
	_, err = jobsClient.CancelJob(ctx, &genjobs.CancelJobRequest{Id: cancelled.GetId()})
	requireGRPCCode(t, err, codes.AlreadyExists)
	// Cancelling an unknown job is NotFound.
	_, err = jobsClient.CancelJob(ctx, &genjobs.CancelJobRequest{Id: "job_missing"})
	requireGRPCCode(t, err, codes.NotFound)

	// Idempotency: same key twice -> same job id, and only one broker
	// message (asserted later by a single SUCCESS cycle).
	idem1, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"idem@example.com"}`,
		Priority: 5, IdempotencyKey: "order-42-confirmation"})
	if err != nil {
		t.Fatalf("CreateJob idem 1: %v", err)
	}
	idem2, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"idem@example.com"}`,
		Priority: 5, IdempotencyKey: "order-42-confirmation"})
	if err != nil {
		t.Fatalf("CreateJob idem 2: %v", err)
	}
	if idem1.GetId() != idem2.GetId() {
		t.Fatalf("idempotent create: got %s then %s, want the same id",
			idem1.GetId(), idem2.GetId())
	}

	// A job to run to success in phase B.
	okJob, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"ok@example.com","subject":"hi"}`,
		Priority: 7})
	if err != nil {
		t.Fatalf("CreateJob ok: %v", err)
	}

	// GetJob + ListJobs sanity.
	gotJob, err := jobsClient.GetJob(ctx, &genjobs.GetJobRequest{Id: okJob.GetId()})
	if err != nil || gotJob.GetType() != "send_email" || gotJob.GetPriority() != 7 {
		t.Fatalf("GetJob: %+v, err %v", gotJob, err)
	}
	_, err = jobsClient.GetJob(ctx, &genjobs.GetJobRequest{Id: "job_missing"})
	requireGRPCCode(t, err, codes.NotFound)
	list, err := jobsClient.ListJobs(ctx, &genjobs.ListJobsRequest{
		Page:         &gencommon.PageRequest{Page: 1, PageSize: 10},
		StatusFilter: genjobs.JobStatus_JOB_STATUS_CANCELLED,
	})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	foundCancelled := false
	for _, j := range list.GetJobs() {
		if j.GetId() == cancelled.GetId() {
			foundCancelled = true
		}
		if j.GetStatus() != genjobs.JobStatus_JOB_STATUS_CANCELLED {
			t.Errorf("status filter leak: %+v", j)
		}
	}
	if !foundCancelled {
		t.Error("ListJobs(status=CANCELLED) must contain the cancelled job")
	}

	// Webhook target: fail the first 4 hits, succeed after. Exactly the
	// default max_attempts, so the job dies, then requeue succeeds.
	var webhookCalls atomic.Int64
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := webhookCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		if n <= 4 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer hook.Close()

	hookJob, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: `{"url":"` + hook.URL + `","event":"ping"}`,
		Priority: 5})
	if err != nil {
		t.Fatalf("CreateJob webhook: %v", err)
	}

	// -----------------------------------------------------------------------
	// Phase B: start one worker through the real service wiring.
	// -----------------------------------------------------------------------

	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workersvc.Run(workerCtx, workersvc.Config{
			HTTPAddr:    "127.0.0.1:0",
			DatabaseURL: dsn,
			RedisAddr:   redisOpt.Addr,
			BrokerAddr:  brokerAddr,
			LogLevel:    "warn",
			Concurrency: 4,
			JobTimeout:  10 * time.Second,
		})
	}()

	// The happy-path job reaches SUCCESS with exactly one attempt row.
	waitJobStatus(t, okJob.GetId(), "SUCCESS", 30*time.Second)
	var attempts int
	var workerID string
	if err := env.pool.QueryRow(ctx,
		`SELECT attempts, worker_id FROM jobs WHERE id = $1`, okJob.GetId(),
	).Scan(&attempts, &workerID); err != nil {
		t.Fatalf("read job row: %v", err)
	}
	if attempts != 1 {
		t.Errorf("attempts: got %d, want 1", attempts)
	}
	if !strings.HasPrefix(workerID, "worker-") {
		t.Errorf("worker_id: got %q", workerID)
	}
	var attemptRows int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, okJob.GetId(),
	).Scan(&attemptRows); err != nil {
		t.Fatalf("count attempt rows: %v", err)
	}
	if attemptRows != 1 {
		t.Errorf("job_attempts rows: got %d, want 1", attemptRows)
	}

	// The cancelled job was consumed and skipped by the fence: still
	// CANCELLED, no attempt rows, after a generous grace period.
	time.Sleep(2 * time.Second)
	st := jobStatus(t, cancelled.GetId())
	if st != "CANCELLED" {
		t.Errorf("cancelled job moved: got %s, want CANCELLED", st)
	}
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, cancelled.GetId(),
	).Scan(&attemptRows); err != nil {
		t.Fatalf("count attempt rows: %v", err)
	}
	if attemptRows != 0 {
		t.Errorf("cancelled job has %d attempt rows, want 0", attemptRows)
	}

	// The idempotent pair produced exactly one execution.
	waitJobStatus(t, idem1.GetId(), "SUCCESS", 30*time.Second)
	if err := env.pool.QueryRow(ctx,
		`SELECT attempts FROM jobs WHERE id = $1`, idem1.GetId()).Scan(&attempts); err != nil {
		t.Fatalf("read idem job: %v", err)
	}
	if attempts != 1 {
		t.Errorf("idempotent job attempts: got %d, want 1 (duplicate create must not re-run)", attempts)
	}

	// The webhook job fails 4x and dies. Retries are visible in between.
	waitJobStatus(t, hookJob.GetId(), "DEAD", 60*time.Second)
	var errMsg string
	if err := env.pool.QueryRow(ctx,
		`SELECT attempts, error FROM jobs WHERE id = $1`, hookJob.GetId(),
	).Scan(&attempts, &errMsg); err != nil {
		t.Fatalf("read dead job: %v", err)
	}
	if attempts != 4 {
		t.Errorf("dead job attempts: got %d, want 4", attempts)
	}
	if errMsg == "" {
		t.Error("dead job must carry the last error")
	}
	if c := webhookCalls.Load(); c != 4 {
		t.Errorf("webhook calls: got %d, want 4", c)
	}
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE job_id = $1 AND error <> ''`, hookJob.GetId(),
	).Scan(&attemptRows); err != nil {
		t.Fatalf("count failed attempt rows: %v", err)
	}
	if attemptRows != 4 {
		t.Errorf("failed attempt rows: got %d, want 4", attemptRows)
	}

	// A DLQ message exists for the dead job.
	waitForDLQ(t, brokerAddr, 1, 15*time.Second)

	// Requeueing a non-DEAD job is a conflict.
	_, err = jobsClient.RequeueJob(ctx, &genjobs.RequeueJobRequest{Id: okJob.GetId()})
	requireGRPCCode(t, err, codes.AlreadyExists)

	// Requeue the dead job: attempts reset, error cleared, runs again and
	// succeeds (the webhook answers 200 from the 5th call on).
	requeued, err := jobsClient.RequeueJob(ctx, &genjobs.RequeueJobRequest{Id: hookJob.GetId()})
	if err != nil {
		t.Fatalf("RequeueJob: %v", err)
	}
	if requeued.GetStatus() != genjobs.JobStatus_JOB_STATUS_QUEUED ||
		requeued.GetAttempts() != 0 || requeued.GetError() != "" {
		t.Fatalf("requeued job: status %v attempts %d error %q",
			requeued.GetStatus(), requeued.GetAttempts(), requeued.GetError())
	}
	waitJobStatus(t, hookJob.GetId(), "SUCCESS", 30*time.Second)
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, hookJob.GetId(),
	).Scan(&attemptRows); err != nil {
		t.Fatalf("count attempt rows: %v", err)
	}
	if attemptRows != 5 {
		t.Errorf("attempt rows after requeue: got %d, want 5 (history is kept)", attemptRows)
	}

	// Live events: we must have seen the success and the death announced.
	waitForEvent(t, events, okJob.GetId(), "SUCCESS", 10*time.Second)
	waitForEvent(t, events, hookJob.GetId(), "DEAD", 10*time.Second)
	waitForEvent(t, events, hookJob.GetId(), "SUCCESS", 10*time.Second)

	// Worker registry: the heartbeat hash exists while the worker runs.
	waitForRegistry(t, 10*time.Second)

	// -----------------------------------------------------------------------
	// Phase C: graceful worker shutdown removes the registry entry.
	// -----------------------------------------------------------------------

	stopWorker()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("worker run: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("worker did not shut down in time")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		keys, _ := env.rdb.Keys(ctx, "worker:*").Result()
		if len(keys) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry keys still present after shutdown: %v", keys)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// startTestBroker boots an in-process broker on a random port.
func startTestBroker(t *testing.T) (addr string, stop func()) {
	t.Helper()
	b, err := broker.New(broker.Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        t.TempDir(),
		FsyncEvery:     20 * time.Millisecond,
		FsyncRecords:   1000,
		SessionTimeout: 10 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if a := b.Addr(); a != "" {
			c, err := net.DialTimeout("tcp", a, 200*time.Millisecond)
			if err == nil {
				c.Close()
				return a, func() {
					cancel()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("broker did not shut down in time")
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("broker did not start listening in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// applyMigrationFile runs one migration file verbatim (simple protocol, like
// cmd/migrate).
func applyMigrationFile(ctx context.Context, dsn, file string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	path := filepath.Join("..", "..", "migrations", file)
	sql, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		lastErr = func() error {
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				return err
			}
			defer conn.Close(ctx)
			_, err = conn.Exec(ctx, string(sql))
			return err
		}()
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("apply %s: %w", file, lastErr)
}

// jobStatus reads the current status straight from Postgres.
func jobStatus(t *testing.T, id string) string {
	t.Helper()
	var st string
	if err := env.pool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, id).Scan(&st); err != nil {
		t.Fatalf("read status of %s: %v", id, err)
	}
	return st
}

// waitJobStatus polls Postgres until the job reaches the wanted status.
func waitJobStatus(t *testing.T, id, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st := jobStatus(t, id); st == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s in %s (last: %s)", id, want, timeout, jobStatus(t, id))
}

// waitForDLQ polls the broker until jobs.dlq holds at least n records.
func waitForDLQ(t *testing.T, brokerAddr string, n uint64, timeout time.Duration) {
	t.Helper()
	admin := client.NewAdmin(brokerAddr)
	defer admin.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := admin.ListTopics(context.Background())
		if err == nil {
			for _, topic := range resp.Topics {
				if topic.Name != jobs.TopicDLQ {
					continue
				}
				var total uint64
				for _, p := range topic.Partitions {
					total += p.HighWatermark
				}
				if total >= n {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("jobs.dlq did not reach %d records in %s", n, timeout)
}

// jobEventRecord mirrors the event JSON for assertions.
type jobEventRecord struct {
	Type     string `json:"type"`
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	WorkerID string `json:"worker_id"`
}

// collectJobEvents subscribes to raven:events:jobs and collects events.
func collectJobEvents(ctx context.Context, t *testing.T) (*eventCollector, func()) {
	t.Helper()
	sub := env.rdb.Subscribe(ctx, jobs.EventsChannel)
	c := &eventCollector{}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				var ev jobEventRecord
				if err := json.Unmarshal([]byte(m.Payload), &ev); err != nil {
					continue
				}
				c.mu.Lock()
				c.events = append(c.events, ev)
				c.mu.Unlock()
			}
		}
	}()
	return c, func() {
		cancel()
		_ = sub.Close()
	}
}

type eventCollector struct {
	mu     sync.Mutex
	events []jobEventRecord
}

func (c *eventCollector) seen(jobID, status string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range c.events {
		if ev.JobID == jobID && ev.Status == status && ev.Type == "job_status" {
			return true
		}
	}
	return false
}

// waitForEvent polls the collector until the event shows up.
func waitForEvent(t *testing.T, c *eventCollector, jobID, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.seen(jobID, status) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no %s event for job %s in %s", status, jobID, timeout)
}

// waitForRegistry polls Redis until a worker heartbeat hash with the contract
// fields exists.
func waitForRegistry(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		keys, _ := env.rdb.Keys(context.Background(), "worker:*").Result()
		for _, k := range keys {
			fields, err := env.rdb.HGetAll(context.Background(), k).Result()
			if err != nil || len(fields) == 0 {
				continue
			}
			_, hasID := fields["id"]
			_, hasStarted := fields["started_at"]
			_, hasBeat := fields["last_heartbeat"]
			_, hasProcessed := fields["jobs_processed"]
			_, hasInflight := fields["in_flight"]
			ttl, _ := env.rdb.TTL(context.Background(), k).Result()
			if hasID && hasStarted && hasBeat && hasProcessed && hasInflight && ttl > 0 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("no live worker registry entry with the contract fields")
}
