//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/database"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
	workersvc "github.com/refleeexzz/RAVEN/services/worker"
)

// Stranded job recovery (ADR 009): leases + generation fencing + sweeper.
//
// Every test here gets its own database on the shared Postgres container.
// That keeps this file independent of jobs_worker_test.go's migration
// application order: this file runs first alphabetically, and touching the
// shared "raven" database's jobs schema from two files would make one of
// them fail on CREATE TABLE.
//
// The SQL-level fence/renew/sweeper tests run against the real database on
// purpose: the fencing guarantees live in the UPDATE ... WHERE clauses, so
// mocking the store would test nothing.

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

// recoveryDB creates a fresh database, applies migrations 000002 + 000003
// verbatim and returns a pool on it.
func recoveryDB(t *testing.T, name string) *pgxpool.Pool {
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
	// Rewrite the database in the URL itself. (pgx.ConnConfig.ConnString
	// re-emits the original string and would silently drop a Database
	// override.)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()

	if err := applyMigrationFile(ctx, dsn, "000002_jobs.up.sql"); err != nil {
		t.Fatalf("apply migration 000002: %v", err)
	}
	if err := applyMigrationFile(ctx, dsn, "000003_job_leases.up.sql"); err != nil {
		t.Fatalf("apply migration 000003: %v", err)
	}

	pool, err := database.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// startJobsAPI serves the jobs gRPC API on a random loopback port, backed by
// the given pool. Redis and metrics are nil: idempotency keys and event
// fanout are not what these tests exercise.
func startJobsAPI(t *testing.T, pool *pgxpool.Pool, producer *jobs.Producer, log *slog.Logger) genjobs.JobServiceClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	genjobs.RegisterJobServiceServer(srv, jobs.NewServer(pool, nil, producer, log, nil))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial jobs: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return genjobs.NewJobServiceClient(conn)
}

// startRecoveryWorker boots one worker with its own producer and consumer in
// the shared "workers" group. stopConsumer cancels the consumer (the member
// leaves the group immediately, like a process vanishing) without draining:
// pair it with w.HardStop() for a kill -9, or with w.Shutdown for a graceful
// stop. The worker's producer is closed via t.Cleanup.
func startRecoveryWorker(t *testing.T, pool *pgxpool.Pool, brokerAddr, id string, lease, jobTimeout, httpTimeout time.Duration) (*workersvc.Worker, func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	producer := jobs.NewProducer(brokerAddr, log)
	t.Cleanup(func() { _ = producer.Close() })

	w := workersvc.New(workersvc.Params{
		Pool:        pool,
		Producer:    producer,
		Log:         log,
		Concurrency: 4,
		JobTimeout:  jobTimeout,
		JobLease:    lease,
		WorkerID:    id,
		HTTPClient:  &http.Client{Timeout: httpTimeout},
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

// newGateServer serves POSTs that block until the named gate is released.
// The stock webhook handler calls it, so a "stuck worker" needs no test-only
// job type: the handler simply waits on HTTP. When the client goes away
// (lease lost, timeout) the handler returns early, like any real server.
func newGateServer(t *testing.T) (baseURL string, release func(name string)) {
	t.Helper()
	var mu sync.Mutex
	gates := map[string]chan struct{}{}
	get := func(name string) chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		ch, ok := gates[name]
		if !ok {
			ch = make(chan struct{})
			gates[name] = ch
		}
		return ch
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch := get(r.URL.Query().Get("gate"))
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-ch:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			// caller hung up while we were "working"
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func(name string) { close(get(name)) }
}

// jobRow reads the execution columns straight from Postgres.
func jobRow(t *testing.T, pool *pgxpool.Pool, id string) (status, workerID string, generation, attempts int) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT status, worker_id, execution_generation, attempts FROM jobs WHERE id = $1`, id).
		Scan(&status, &workerID, &generation, &attempts)
	if err != nil {
		t.Fatalf("read job row %s: %v", id, err)
	}
	return status, workerID, generation, attempts
}

// waitJobRow polls until the job reaches the wanted status and generation.
func waitJobRow(t *testing.T, pool *pgxpool.Pool, id, wantStatus string, wantGen int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, _, gen, _ := jobRow(t, pool, id)
		if st == wantStatus && gen == wantGen {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	st, wid, gen, att := jobRow(t, pool, id)
	t.Fatalf("job %s did not reach (%s, gen %d) in %s (last: %s, gen %d, worker %q, attempts %d)",
		id, wantStatus, wantGen, timeout, st, gen, wid, att)
}

// waitWorkerIdle polls until the worker has nothing in flight.
func waitWorkerIdle(t *testing.T, w *workersvc.Worker, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if w.InFlight() == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("worker %s still has %d jobs in flight after %s", w.ID(), w.InFlight(), timeout)
}

// seedJob inserts a job row with the given execution state, bypassing the
// API — the sweeper and fence tests hand-craft states the API never
// produces on purpose.
func seedJob(t *testing.T, pool *pgxpool.Pool, id, status string, attempts, maxAttempts int, leaseUntil *time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO jobs (id, type, payload, status, priority, attempts, max_attempts,
		                  worker_id, started_at, heartbeat_at, lease_until)
		VALUES ($1, 'webhook', '{}', $2, 5, $3, $4, 'seed-w',
		        now() - interval '10 seconds', now() - interval '5 seconds', $5)`,
		id, status, attempts, maxAttempts, leaseUntil)
	if err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

// topicRecords sums the high watermarks of one topic across partitions.
func topicRecords(t *testing.T, brokerAddr, topic string) uint64 {
	t.Helper()
	admin := client.NewAdmin(brokerAddr)
	defer admin.Close()
	resp, err := admin.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	var total uint64
	for _, tp := range resp.Topics {
		if tp.Name != topic {
			continue
		}
		for _, p := range tp.Partitions {
			total += p.HighWatermark
		}
	}
	return total
}

func quietRecoveryLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// The chaos test: worker A is SIGKILLed mid-job; the sweeper requeues it and
// worker B finishes it. A's late write is rejected by the generation fence.
// ---------------------------------------------------------------------------

func TestJobRecoveryAfterWorkerKill(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := quietRecoveryLog()

	const (
		lease         = 1 * time.Second
		sweepInterval = 500 * time.Millisecond
	)

	pool := recoveryDB(t, "raven_rec_chaos")
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}
	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	jobsClient := startJobsAPI(t, pool, producer, log)

	sweeper := jobs.NewSweeper(pool, producer, nil, log, nil, sweepInterval)
	sweepCtx, stopSweep := context.WithCancel(ctx)
	defer stopSweep()
	go sweeper.Run(sweepCtx)

	gateURL, release := newGateServer(t)

	// Worker A runs alone first: it claims every partition, so it
	// deterministically picks the job up. B joins only after A "dies" — the
	// replacement pod, not a racing peer.
	wA, stopA := startRecoveryWorker(t, pool, brokerAddr, "worker-A",
		lease, 60*time.Second, 30*time.Second)

	job, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type:        "webhook",
		PayloadJson: fmt.Sprintf(`{"url":"%s?gate=a"}`, gateURL),
		Priority:    5,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// A claims it (generation 1, lease ticking) and blocks inside the gate.
	waitJobRow(t, pool, job.GetId(), "PROCESSING", 1, 15*time.Second)
	if _, wid, _, _ := jobRow(t, pool, job.GetId()); wid != "worker-A" {
		t.Fatalf("job claimed by %q, want worker-A", wid)
	}

	// SIGKILL: the consumer leaves the group, then the worker hard-stops —
	// no drain, no retry flush, lease renewals stop. The handler stays
	// frozen inside the gate, exactly like a killed process that never got
	// to write its result.
	killTime := time.Now()
	stopA()
	wA.HardStop()

	// The sweeper must requeue within lease + ~2 intervals: the lease
	// (renewed up to the kill) expires at kill+1s at the latest, and a pass
	// runs every 500ms.
	waitJobRow(t, pool, job.GetId(), "RETRYING", 2, lease+2*sweepInterval+3*time.Second)
	recoveredIn := time.Since(killTime)
	t.Logf("sweeper recovered the job %v after the kill", recoveredIn)
	if _, wid, _, attempts := jobRow(t, pool, job.GetId()); wid != "" || attempts != 0 {
		t.Errorf("recovered row: worker_id %q (want cleared), attempts %d (want preserved 0)", wid, attempts)
	}

	// The replacement worker joins the (empty) group, gets the partitions
	// and picks up the sweeper's generation-2 message.
	wB, stopB := startRecoveryWorker(t, pool, brokerAddr, "worker-B",
		lease, 10*time.Second, 30*time.Second)
	waitJobRow(t, pool, job.GetId(), "PROCESSING", 2, 15*time.Second)
	if _, wid, _, _ := jobRow(t, pool, job.GetId()); wid != "worker-B" {
		t.Fatalf("recovered job claimed by %q, want worker-B", wid)
	}

	// Release the gate: B finishes for real; A's frozen handler unblocks too
	// and tries a late finish with generation 1 — the fence must reject it.
	release("a")
	waitJobRow(t, pool, job.GetId(), "SUCCESS", 2, 15*time.Second)

	// Wait until A's late write definitely landed (and bounced off).
	waitWorkerIdle(t, wA, 10*time.Second)

	st, wid, gen, attempts := jobRow(t, pool, job.GetId())
	if st != "SUCCESS" || wid != "worker-B" || gen != 2 || attempts != 1 {
		t.Errorf("final row: got (%s, %s, gen %d, attempts %d), want (SUCCESS, worker-B, gen 2, attempts 1)",
			st, wid, gen, attempts)
	}
	if got := wA.Processed(); got != 0 {
		t.Errorf("dead worker processed count: got %d, want 0 (its write was fenced)", got)
	}

	// Attempts preserved + audited. Three rows: the sweeper's
	// interrupted-attempt row (attempt 1, worker A, "lease expired"), B's
	// winning success (attempt 1), and A's late fenced finish — the attempt
	// row stays because the attempt really ran; only its job-row write was
	// rejected.
	var attemptRows, expiredRows, successRows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, job.GetId()).Scan(&attemptRows); err != nil {
		t.Fatalf("count attempt rows: %v", err)
	}
	if attemptRows != 3 {
		t.Errorf("job_attempts rows: got %d, want 3 (interrupted + B's success + A's fenced audit)", attemptRows)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM job_attempts
		WHERE job_id = $1 AND error = 'lease expired (worker lost)' AND worker_id = 'worker-A'`,
		job.GetId()).Scan(&expiredRows); err != nil {
		t.Fatalf("count expired rows: %v", err)
	}
	if expiredRows != 1 {
		t.Errorf("lease-expired audit rows: got %d, want 1", expiredRows)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM job_attempts
		WHERE job_id = $1 AND error = '' AND worker_id = 'worker-B'`,
		job.GetId()).Scan(&successRows); err != nil {
		t.Fatalf("count success rows: %v", err)
	}
	if successRows != 1 {
		t.Errorf("worker-B success rows: got %d, want 1", successRows)
	}

	// Belt and braces: a direct stale-generation finish against the store
	// must also be fenced and change nothing.
	late, err := jobs.FinishJobSuccess(ctx, pool, job.GetId(), 1, 1, "worker-A",
		killTime, time.Now())
	if err != nil {
		t.Fatalf("late finish: %v", err)
	}
	if late != nil {
		t.Error("stale-generation FinishJobSuccess must return nil (fenced)")
	}
	st, wid, gen, attempts = jobRow(t, pool, job.GetId())
	if st != "SUCCESS" || wid != "worker-B" || gen != 2 || attempts != 1 {
		t.Errorf("after late write: got (%s, %s, gen %d, attempts %d) — the fence let it through",
			st, wid, gen, attempts)
	}

	// Graceful stop for B (it is idle by now).
	stopB()
	wB.Shutdown(5 * time.Second)

	// -----------------------------------------------------------------------
	// Scenario 2: a LIVE worker that loses its lease mid-execution (the
	// sweeper took over) must abandon the handler and write nothing.
	// -----------------------------------------------------------------------

	wC, stopC := startRecoveryWorker(t, pool, brokerAddr, "worker-C",
		lease, 30*time.Second, 30*time.Second)
	job2, err := jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type:        "webhook",
		PayloadJson: fmt.Sprintf(`{"url":"%s?gate=c"}`, gateURL),
		Priority:    5,
	})
	if err != nil {
		t.Fatalf("CreateJob 2: %v", err)
	}
	waitJobRow(t, pool, job2.GetId(), "PROCESSING", 1, 15*time.Second)

	// Simulate the sweeper taking over — this UPDATE is exactly what
	// recoverJob does (bump generation, back to RETRYING).
	if _, err := pool.Exec(ctx, `
		UPDATE jobs
		SET execution_generation = execution_generation + 1,
		    status = 'RETRYING', worker_id = '', heartbeat_at = NULL, lease_until = NULL
		WHERE id = $1`, job2.GetId()); err != nil {
		t.Fatalf("simulate takeover: %v", err)
	}

	// C's next renewal (lease/3 = 333ms) observes the loss, cancels the
	// handler and the execute path writes nothing.
	waitWorkerIdle(t, wC, 10*time.Second)
	st, _, gen, attempts = jobRow(t, pool, job2.GetId())
	if st != "RETRYING" || gen != 2 || attempts != 0 {
		t.Errorf("abandoned job row: got (%s, gen %d, attempts %d), want (RETRYING, gen 2, attempts 0)",
			st, gen, attempts)
	}
	var cAttempts int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, job2.GetId()).Scan(&cAttempts); err != nil {
		t.Fatalf("count C attempt rows: %v", err)
	}
	if cAttempts != 0 {
		t.Errorf("worker C wrote %d attempt rows after losing the lease, want 0", cAttempts)
	}

	stopC()
	wC.Shutdown(5 * time.Second)
}

// ---------------------------------------------------------------------------
// Sweeper candidate scan: only expired leases on live executions qualify.
// ---------------------------------------------------------------------------

func TestSweeperSelectsOnlyExpiredLeases(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := quietRecoveryLog()

	pool := recoveryDB(t, "raven_rec_sweep")
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}
	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	sweeper := jobs.NewSweeper(pool, producer, nil, log, nil, time.Minute)

	past := time.Now().Add(-2 * time.Second)
	future := time.Now().Add(time.Hour)

	seedJob(t, pool, "job_s1", "PROCESSING", 0, 4, &past)   // expired -> recovered
	seedJob(t, pool, "job_s2", "PROCESSING", 0, 4, &future) // live lease -> untouched
	seedJob(t, pool, "job_s3", "PROCESSING", 0, 4, nil)     // no lease -> untouched
	seedJob(t, pool, "job_s4", "RETRYING", 1, 4, &past)     // expired retry -> recovered
	seedJob(t, pool, "job_s5", "QUEUED", 0, 4, &past)       // not executing -> untouched
	seedJob(t, pool, "job_s6", "SUCCESS", 1, 4, &past)      // terminal -> untouched
	seedJob(t, pool, "job_s7", "PROCESSING", 4, 4, &past)   // attempts gone -> DEAD

	n, err := sweeper.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if n != 3 {
		t.Fatalf("recovered %d jobs, want 3 (job_s1, job_s4, job_s7)", n)
	}

	// Recovered jobs: generation bumped, attempts preserved, worker cleared,
	// error recorded, fresh lease set so the republish has time to land.
	st, wid, gen, attempts := jobRow(t, pool, "job_s1")
	if st != "RETRYING" || gen != 2 || attempts != 0 || wid != "" {
		t.Errorf("job_s1: got (%s, gen %d, attempts %d, worker %q)", st, gen, attempts, wid)
	}
	var errMsg string
	var leaseUntil time.Time
	if err := pool.QueryRow(ctx,
		`SELECT error, lease_until FROM jobs WHERE id = 'job_s1'`).Scan(&errMsg, &leaseUntil); err != nil {
		t.Fatalf("read job_s1: %v", err)
	}
	if errMsg != "lease expired (worker lost)" {
		t.Errorf("job_s1 error: got %q", errMsg)
	}
	if !leaseUntil.After(time.Now()) {
		t.Errorf("job_s1 must carry the sweeper's fresh lease, got %v", leaseUntil)
	}

	st, _, gen, attempts = jobRow(t, pool, "job_s4")
	if st != "RETRYING" || gen != 2 || attempts != 1 {
		t.Errorf("job_s4: got (%s, gen %d, attempts %d), want (RETRYING, 2, 1)", st, gen, attempts)
	}

	// Attempts exhausted -> DEAD, terminal, lease cleared, DLQ copy.
	st, _, gen, _ = jobRow(t, pool, "job_s7")
	if st != "DEAD" || gen != 2 {
		t.Errorf("job_s7: got (%s, gen %d), want (DEAD, 2)", st, gen)
	}
	var finishedAt *time.Time
	var deadLease *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT finished_at, lease_until FROM jobs WHERE id = 'job_s7'`).Scan(&finishedAt, &deadLease); err != nil {
		t.Fatalf("read job_s7: %v", err)
	}
	if finishedAt == nil || deadLease != nil {
		t.Errorf("job_s7: finished_at %v (want set), lease_until %v (want NULL)", finishedAt, deadLease)
	}

	// Untouched rows, generation and all.
	for id, want := range map[string]string{
		"job_s2": "PROCESSING", "job_s3": "PROCESSING",
		"job_s5": "QUEUED", "job_s6": "SUCCESS",
	} {
		st, _, gen, _ = jobRow(t, pool, id)
		if st != want || gen != 1 {
			t.Errorf("%s: got (%s, gen %d), want (%s, 1) — sweeper must not touch it", id, st, gen, want)
		}
	}

	// One interrupted-attempt audit row per recovered job, none elsewhere.
	for _, id := range []string{"job_s1", "job_s4", "job_s7"} {
		var c int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM job_attempts
			WHERE job_id = $1 AND error = 'lease expired (worker lost)'`, id).Scan(&c); err != nil {
			t.Fatalf("count attempts %s: %v", id, err)
		}
		if c != 1 {
			t.Errorf("%s: got %d lease-expired attempt rows, want 1", id, c)
		}
	}

	// The republishes landed: 2 work messages + 1 DLQ copy.
	if got := topicRecords(t, brokerAddr, jobs.TopicJobs); got != 2 {
		t.Errorf("jobs topic records: got %d, want 2", got)
	}
	if got := topicRecords(t, brokerAddr, jobs.TopicDLQ); got != 1 {
		t.Errorf("dlq topic records: got %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Advisory lock: exactly one sweeper replica works per pass.
// ---------------------------------------------------------------------------

func TestSweeperAdvisoryLockSingleWinner(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := quietRecoveryLog()

	pool := recoveryDB(t, "raven_rec_lock")
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}
	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	sweeper := jobs.NewSweeper(pool, producer, nil, log, nil, time.Minute)

	expired := time.Now().Add(-2 * time.Second)
	seedJob(t, pool, "job_l1", "PROCESSING", 0, 4, &expired)

	// A rival replica holds the lock (the key must match sweeperLockKey in
	// services/jobs/sweeper.go).
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	var gotLock bool
	if err := conn.QueryRow(ctx,
		`SELECT pg_try_advisory_lock($1)`, int64(727_000_001)).Scan(&gotLock); err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !gotLock {
		t.Fatal("could not take the sweeper advisory lock from a clean database")
	}

	// The sweeper must skip its pass: 0 recovered, and the expired job
	// proves it — nobody swept while the rival held the lock.
	n, err := sweeper.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce under a held lock: %v", err)
	}
	if n != 0 {
		t.Fatalf("sweeper recovered %d jobs while another replica held the lock", n)
	}
	if st, _, gen, _ := jobRow(t, pool, "job_l1"); st != "PROCESSING" || gen != 1 {
		t.Errorf("job_l1 moved while the lock was held: got (%s, gen %d)", st, gen)
	}

	// Release: the next pass takes over.
	if _, err := conn.Exec(ctx,
		`SELECT pg_advisory_unlock($1)`, int64(727_000_001)); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	n, err = sweeper.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce after unlock: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered %d jobs after unlock, want 1", n)
	}
	waitJobRow(t, pool, "job_l1", "RETRYING", 2, 5*time.Second)
}

// ---------------------------------------------------------------------------
// Store-level fencing against the real database: the guarantees live in the
// SQL, so that is where they are tested.
// ---------------------------------------------------------------------------

func TestJobGenerationFenceAndRenew(t *testing.T) {
	check(t)
	ctx := context.Background()

	pool := recoveryDB(t, "raven_rec_fence")
	seedJob(t, pool, "job_f1", "QUEUED", 0, 4, nil)

	const lease = 30 * time.Second

	// Claim: the fence stamps worker, heartbeat and lease.
	job, err := jobs.StartJobFence(ctx, pool, "job_f1", "worker-A", 1, lease)
	if err != nil {
		t.Fatalf("fence: %v", err)
	}
	if job == nil || job.Status != jobs.StatusProcessing || job.WorkerID != "worker-A" {
		t.Fatalf("claim: got %+v", job)
	}
	if job.HeartbeatAt == nil || job.LeaseUntil == nil {
		t.Fatal("claim must stamp heartbeat_at and lease_until")
	}
	if job.LeaseUntil.Before(time.Now().Add(lease - 5*time.Second)) {
		t.Errorf("lease_until %v is not ~%v in the future", job.LeaseUntil, lease)
	}

	// Duplicate claim: the status fence still dedups.
	dup, err := jobs.StartJobFence(ctx, pool, "job_f1", "worker-B", 1, lease)
	if err != nil || dup != nil {
		t.Errorf("duplicate claim: got (%+v, %v), want (nil, nil)", dup, err)
	}

	// Renew while ours: ok.
	ok, err := jobs.RenewJobLease(ctx, pool, "job_f1", 1, lease)
	if err != nil || !ok {
		t.Errorf("renew own lease: got (%v, %v), want (true, nil)", ok, err)
	}

	// The sweeper takes over (what recoverJob does).
	if _, err := pool.Exec(ctx, `
		UPDATE jobs
		SET execution_generation = execution_generation + 1,
		    status = 'RETRYING', worker_id = '', heartbeat_at = NULL, lease_until = NULL
		WHERE id = 'job_f1'`); err != nil {
		t.Fatalf("simulate takeover: %v", err)
	}

	// Renew fails after losing the lease.
	ok, err = jobs.RenewJobLease(ctx, pool, "job_f1", 1, lease)
	if err != nil || ok {
		t.Errorf("renew after takeover: got (%v, %v), want (false, nil)", ok, err)
	}

	// The fence rejects the stale-generation finish; the row is untouched.
	now := time.Now()
	late, err := jobs.FinishJobSuccess(ctx, pool, "job_f1", 1, 1, "worker-A",
		now.Add(-time.Second), now)
	if err != nil || late != nil {
		t.Errorf("stale finish: got (%+v, %v), want (nil, nil)", late, err)
	}
	if st, _, gen, att := jobRow(t, pool, "job_f1"); st != "RETRYING" || gen != 2 || att != 0 {
		t.Errorf("after stale finish: got (%s, gen %d, attempts %d), want (RETRYING, 2, 0)",
			st, gen, att)
	}

	// A stale-generation broker message is skipped at claim time...
	stale, err := jobs.StartJobFence(ctx, pool, "job_f1", "worker-B", 1, lease)
	if err != nil || stale != nil {
		t.Errorf("stale-generation claim: got (%+v, %v), want (nil, nil)", stale, err)
	}

	// ...while a pre-lease message (generation 0 = unknown) is accepted:
	// the row's current generation wins, which keeps the upgrade path safe.
	claimed, err := jobs.StartJobFence(ctx, pool, "job_f1", "worker-B", 0, lease)
	if err != nil {
		t.Fatalf("legacy claim: %v", err)
	}
	if claimed == nil || claimed.ExecutionGeneration != 2 || claimed.WorkerID != "worker-B" {
		t.Fatalf("legacy claim: got %+v", claimed)
	}

	// The new owner finishes with the current generation.
	done, err := jobs.FinishJobSuccess(ctx, pool, "job_f1", 2, 1, "worker-B", now, time.Now())
	if err != nil || done == nil {
		t.Fatalf("new owner finish: got (%+v, %v)", done, err)
	}
	if done.Status != jobs.StatusSuccess {
		t.Errorf("new owner finish: status %s, want SUCCESS", done.Status)
	}

	// Renew on a terminal job is refused by the status guard.
	ok, err = jobs.RenewJobLease(ctx, pool, "job_f1", 2, lease)
	if err != nil || ok {
		t.Errorf("renew terminal job: got (%v, %v), want (false, nil)", ok, err)
	}
}
