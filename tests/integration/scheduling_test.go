//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/database"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
	workersvc "github.com/refleeexzz/RAVEN/services/worker"
)

// each scheduling test uses a fresh database with jobs, leases, schedules and the outbox.
func schedulingDB(t *testing.T, name string) *pgxpool.Pool {
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

	for _, mig := range []string{
		"000002_jobs.up.sql",
		"000003_job_leases.up.sql",
		"000004_scheduling.up.sql",
		"000008_scheduled_dispatch.up.sql",
		"000009_immediate_dispatch.up.sql",
	} {
		if err := applyMigrationFile(ctx, dsn, mig); err != nil {
			t.Fatalf("apply %s: %v", mig, err)
		}
	}

	pool, err := database.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// asUser returns a context carrying the gateway identity headers the jobs
// service resolves (x-user-id). It must be OUTGOING metadata: this is the
// client side of the call, and only outgoing metadata crosses the wire.
func asUser(ctx context.Context, userID string, perms ...string) context.Context {
	md := metadata.Pairs("x-user-id", userID)
	if len(perms) > 0 {
		md.Set("x-user-perms", perms[0])
	}
	return metadata.NewOutgoingContext(ctx, md)
}

func poolJobStatus(t *testing.T, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var st string
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, id).Scan(&st); err != nil {
		t.Fatalf("read status of %s: %v", id, err)
	}
	return st
}

func waitPoolJobStatus(t *testing.T, pool *pgxpool.Pool, id, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st := poolJobStatus(t, pool, id); st == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s in %s (last: %s)",
		id, want, timeout, poolJobStatus(t, pool, id))
}

// topicHighWatermark sums the high watermark of every partition of a topic.
func topicHighWatermark(t *testing.T, brokerAddr, topic string) uint64 {
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

// TestSchedulingLifecycle exercises migration 000004 end to end: delayed
// jobs, replay, priority-topic routing and cron — real Postgres, real
// broker, real worker.
func TestSchedulingLifecycle(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	pool := schedulingDB(t, "raven_sched_test")

	redisURL, err := env.redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}
	redisOpt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}

	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()
	jobsClient := startJobsAPI(t, pool, producer, log)

	// Scheduling loops on a fast tick so the test stays quick.
	schedCtx, stopSched := context.WithCancel(ctx)
	defer stopSched()
	go jobs.NewDispatcher(pool, producer, env.rdb, log, nil, 200*time.Millisecond).Run(schedCtx)
	go jobs.NewCronScheduler(pool, producer, env.rdb, log, nil, 200*time.Millisecond).Run(schedCtx)

	// Two tenant identities for the owner-scoping checks.
	userA := "11111111-1111-1111-1111-111111111111"
	userB := "22222222-2222-2222-2222-222222222222"

	// -----------------------------------------------------------------------
	// Delayed jobs.
	// -----------------------------------------------------------------------

	// Distant past is rejected.
	_, err = jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"a@b.c"}`,
		ScheduledAt: time.Now().Add(-2 * time.Minute).Unix()})
	requireGRPCCode(t, err, codes.InvalidArgument)

	// Negative is rejected.
	_, err = jobsClient.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"a@b.c"}`, ScheduledAt: -10})
	requireGRPCCode(t, err, codes.InvalidArgument)

	// Future: SCHEDULED, not published, scheduled_at exposed on reads.
	fireAt := time.Now().Add(2 * time.Second)
	delayed, err := jobsClient.CreateJob(asUser(ctx, userA), &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"delayed@example.com"}`,
		Priority: 5, ScheduledAt: fireAt.Unix()})
	if err != nil {
		t.Fatalf("CreateJob delayed: %v", err)
	}
	if delayed.GetStatus() != genjobs.JobStatus_JOB_STATUS_SCHEDULED {
		t.Fatalf("delayed job status: got %v, want SCHEDULED", delayed.GetStatus())
	}
	if delayed.GetScheduledAt() != fireAt.Truncate(time.Second).Unix() &&
		delayed.GetScheduledAt() != fireAt.Unix() {
		t.Errorf("scheduled_at: got %d, want %d", delayed.GetScheduledAt(), fireAt.Unix())
	}
	gotDelayed, err := jobsClient.GetJob(asUser(ctx, userA), &genjobs.GetJobRequest{Id: delayed.GetId()})
	if err != nil || gotDelayed.GetScheduledAt() == 0 {
		t.Fatalf("GetJob must expose scheduled_at: %+v, err %v", gotDelayed, err)
	}
	// Still SCHEDULED before its time: nothing published it early.
	time.Sleep(700 * time.Millisecond)
	if st := poolJobStatus(t, pool, delayed.GetId()); st != "SCHEDULED" {
		t.Fatalf("delayed job left SCHEDULED early: %s", st)
	}

	// A scheduled job can be cancelled before it ever runs; the dispatcher
	// must then leave it alone forever.
	cancelMe, err := jobsClient.CreateJob(asUser(ctx, userA), &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"never@example.com"}`,
		ScheduledAt: time.Now().Add(1500 * time.Millisecond).Unix()})
	if err != nil {
		t.Fatalf("CreateJob to cancel: %v", err)
	}
	cancelled, err := jobsClient.CancelJob(asUser(ctx, userA), &genjobs.CancelJobRequest{Id: cancelMe.GetId()})
	if err != nil {
		t.Fatalf("CancelJob scheduled: %v", err)
	}
	if cancelled.GetStatus() != genjobs.JobStatus_JOB_STATUS_CANCELLED {
		t.Fatalf("cancel scheduled: got %v, want CANCELLED", cancelled.GetStatus())
	}
	time.Sleep(2 * time.Second) // well past its due time
	if st := poolJobStatus(t, pool, cancelMe.GetId()); st != "CANCELLED" {
		t.Errorf("cancelled scheduled job moved: got %s, want CANCELLED", st)
	}

	// the queued state is visible before publication. wait for the broker
	// acknowledgement before taking the initial watermark readings.
	dispatchDeadline := time.Now().Add(10 * time.Second)
	for {
		var published bool
		if err := pool.QueryRow(ctx, `
			SELECT status = 'QUEUED' AND NOT EXISTS
				(SELECT 1 FROM scheduled_job_outbox WHERE job_id = jobs.id)
			FROM jobs WHERE id = $1`, delayed.GetId()).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if published {
			break
		}
		if time.Now().After(dispatchDeadline) {
			t.Fatal("delayed job was not published")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// -----------------------------------------------------------------------
	// Priority routing (contract: jobs.p<priority>, fanout to legacy "jobs").
	// -----------------------------------------------------------------------
	baseP2 := topicHighWatermark(t, brokerAddr, "jobs.p2")
	baseLegacy := topicHighWatermark(t, brokerAddr, jobs.TopicJobs)
	prioJob, err := jobsClient.CreateJob(asUser(ctx, userA), &genjobs.CreateJobRequest{
		Type: "send_email", PayloadJson: `{"to":"prio@example.com"}`, Priority: 2})
	if err != nil {
		t.Fatalf("CreateJob priority: %v", err)
	}
	if got := topicHighWatermark(t, brokerAddr, "jobs.p2"); got != baseP2+1 {
		t.Errorf("jobs.p2 watermark: got %d, want %d (priority topic must receive the job)", got, baseP2+1)
	}
	if got := topicHighWatermark(t, brokerAddr, jobs.TopicJobs); got != baseLegacy+1 {
		t.Errorf("legacy jobs watermark: got %d, want %d (fanout compatibility)", got, baseLegacy+1)
	}

	// -----------------------------------------------------------------------
	// Replay.
	// -----------------------------------------------------------------------

	// Owner B cannot replay owner A's job: NotFound, not an oracle.
	_, err = jobsClient.ReplayJob(asUser(ctx, userB), &genjobs.ReplayJobRequest{Id: prioJob.GetId()})
	requireGRPCCode(t, err, codes.NotFound)
	// Unknown id is NotFound too.
	_, err = jobsClient.ReplayJob(asUser(ctx, userA), &genjobs.ReplayJobRequest{Id: "job_missing"})
	requireGRPCCode(t, err, codes.NotFound)

	replayed, err := jobsClient.ReplayJob(asUser(ctx, userA), &genjobs.ReplayJobRequest{Id: prioJob.GetId()})
	if err != nil {
		t.Fatalf("ReplayJob: %v", err)
	}
	if replayed.GetId() == prioJob.GetId() {
		t.Fatal("replay must mint a new job id")
	}
	if replayed.GetStatus() != genjobs.JobStatus_JOB_STATUS_QUEUED ||
		replayed.GetAttempts() != 0 ||
		replayed.GetReplayedFrom() != prioJob.GetId() ||
		replayed.GetScheduledAt() != 0 ||
		replayed.GetPriority() != 2 ||
		replayed.GetType() != prioJob.GetType() {
		t.Fatalf("replayed job: %+v", replayed)
	}

	// -----------------------------------------------------------------------
	// Cron.
	// -----------------------------------------------------------------------

	// Invalid expressions are rejected at create time.
	for _, bad := range []string{"0 0 31 2 *", "not a cron", "* * * *", "0 0 * * * *", "0 0 * * MON"} {
		_, err = jobsClient.CreateCron(asUser(ctx, userA), &genjobs.CreateCronRequest{
			Name: "bad", CronExpr: bad, Type: "send_email", PayloadJson: `{}`, Enabled: true})
		requireGRPCCode(t, err, codes.InvalidArgument)
	}
	// Unknown job type is rejected (reuses CreateJob validation).
	_, err = jobsClient.CreateCron(asUser(ctx, userA), &genjobs.CreateCronRequest{
		Name: "bad-type", CronExpr: "* * * * *", Type: "mine_crypto", PayloadJson: `{}`, Enabled: true})
	requireGRPCCode(t, err, codes.InvalidArgument)

	// keep the next scheduled run well beyond the observation window.
	// the test makes exactly one occurrence due by updating its deadline below.
	dailyHour := time.Now().UTC().Add(12 * time.Hour).Hour()
	cron, err := jobsClient.CreateCron(asUser(ctx, userA), &genjobs.CreateCronRequest{
		Name: "daily-ping", CronExpr: fmt.Sprintf("0 %d * * *", dailyHour), Type: "send_email",
		PayloadJson: `{"to":"cron@example.com"}`, Priority: 7, Enabled: true})
	if err != nil {
		t.Fatalf("CreateCron: %v", err)
	}
	if cron.GetId() == "" || cron.GetNextRunAt() == 0 || !cron.GetEnabled() || cron.GetPriority() != 7 {
		t.Fatalf("created cron: %+v", cron)
	}

	// Owner B sees none of owner A's schedules; owner A sees theirs.
	listB, err := jobsClient.ListCrons(asUser(ctx, userB), &genjobs.ListCronsRequest{})
	if err != nil {
		t.Fatalf("ListCrons B: %v", err)
	}
	if len(listB.GetCrons()) != 0 {
		t.Errorf("owner B sees %d foreign crons, want 0", len(listB.GetCrons()))
	}
	listA, err := jobsClient.ListCrons(asUser(ctx, userA), &genjobs.ListCronsRequest{})
	if err != nil {
		t.Fatalf("ListCrons A: %v", err)
	}
	foundCron := false
	for _, c := range listA.GetCrons() {
		if c.GetId() == cron.GetId() {
			foundCron = true
		}
	}
	if !foundCron {
		t.Error("owner A's cron missing from their listing")
	}

	// Force the schedule due and watch exactly one job instance appear,
	// with next_run_at advanced past the previous value.
	if _, err := pool.Exec(ctx,
		`UPDATE cron_schedules SET next_run_at = now() - interval '1 second' WHERE id = $1`,
		cron.GetId()); err != nil {
		t.Fatalf("force cron due: %v", err)
	}
	var spawned int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM jobs WHERE type = 'send_email'
			 AND payload->>'to' = 'cron@example.com'`).Scan(&spawned); err != nil {
			t.Fatalf("count spawned: %v", err)
		}
		if spawned >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if spawned != 1 {
		t.Fatalf("cron spawned %d jobs, want exactly 1", spawned)
	}
	// Idempotency: give the loop several more passes — no duplicate may
	// appear, because next_run_at advanced transactionally with the insert.
	time.Sleep(time.Second)
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE type = 'send_email'
		 AND payload->>'to' = 'cron@example.com'`).Scan(&spawned); err != nil {
		t.Fatalf("recount spawned: %v", err)
	}
	if spawned != 1 {
		t.Errorf("cron duplicated a run: %d jobs, want 1", spawned)
	}
	var lastRunSet bool
	var nextRun time.Time
	if err := pool.QueryRow(ctx,
		`SELECT last_run_at IS NOT NULL, next_run_at FROM cron_schedules WHERE id = $1`,
		cron.GetId()).Scan(&lastRunSet, &nextRun); err != nil {
		t.Fatalf("read cron row: %v", err)
	}
	if !lastRunSet {
		t.Error("last_run_at not recorded after firing")
	}
	// next_run_at must point at a current-or-future minute; the "exactly 1
	// spawn" check above already proves it advanced transactionally (a
	// stale next_run_at would re-fire every pass).
	if nextRun.Before(time.Now().Add(-time.Minute)) {
		t.Errorf("next_run_at stuck in the past after firing: %v", nextRun)
	}

	// Foreign delete is NotFound; the owner can delete.
	_, err = jobsClient.DeleteCron(asUser(ctx, userB), &genjobs.DeleteCronRequest{Id: cron.GetId()})
	requireGRPCCode(t, err, codes.NotFound)
	if _, err := jobsClient.DeleteCron(asUser(ctx, userA), &genjobs.DeleteCronRequest{Id: cron.GetId()}); err != nil {
		t.Fatalf("DeleteCron: %v", err)
	}
	_, err = jobsClient.DeleteCron(asUser(ctx, userA), &genjobs.DeleteCronRequest{Id: cron.GetId()})
	requireGRPCCode(t, err, codes.NotFound)

	// -----------------------------------------------------------------------
	// Phase B: a worker consumes everything released so far.
	// -----------------------------------------------------------------------

	dsn, err := env.pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/raven_sched_test"

	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workersvc.Run(workerCtx, workersvc.Config{
			HTTPAddr:    "127.0.0.1:0",
			DatabaseURL: u.String(),
			RedisAddr:   redisOpt.Addr,
			BrokerAddr:  brokerAddr,
			LogLevel:    "warn",
			Concurrency: 4,
			JobTimeout:  10 * time.Second,
		})
	}()

	// The released delayed job and the replayed copy run to SUCCESS; the
	// cron-spawned instance too (send_email succeeds by construction).
	waitPoolJobStatus(t, pool, delayed.GetId(), "SUCCESS", 30*time.Second)
	waitPoolJobStatus(t, pool, replayed.GetId(), "SUCCESS", 30*time.Second)

	stopWorker()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("worker run: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("worker did not shut down in time")
	}
}

// TestCronSchedulerNoCatchUp pins the missed-runs policy: a schedule that
// was due long ago fires exactly once when the loop comes back, and
// next_run_at lands on the NEXT future match — never a backfill storm.
func TestCronSchedulerNoCatchUp(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	pool := schedulingDB(t, "raven_cron_catchup")
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, brokerAddr, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}
	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()

	// A schedule due many "runs" ago (every minute, last_run hours past).
	if _, err := pool.Exec(ctx, `
		INSERT INTO cron_schedules (id, owner_id, name, cron_expr, type, payload,
		                            priority, enabled, next_run_at, created_at)
		VALUES ('cron_catchup', NULL, 'catchup', '* * * * *', 'send_email',
		        '{"to":"catchup@example.com"}'::jsonb, 5, true,
		        now() - interval '3 hours', now())`); err != nil {
		t.Fatalf("seed cron: %v", err)
	}

	scheduler := jobs.NewCronScheduler(pool, producer, nil, log, nil, time.Hour)
	n, err := scheduler.TickOnce(ctx)
	if err != nil {
		t.Fatalf("TickOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("TickOnce spawned %d jobs, want exactly 1 (no catch-up)", n)
	}

	// Second pass: next_run_at is now in the future, nothing fires.
	n, err = scheduler.TickOnce(ctx)
	if err != nil {
		t.Fatalf("TickOnce 2: %v", err)
	}
	if n != 0 {
		t.Fatalf("second TickOnce spawned %d jobs, want 0", n)
	}

	var nextRun time.Time
	if err := pool.QueryRow(ctx,
		`SELECT next_run_at FROM cron_schedules WHERE id = 'cron_catchup'`).Scan(&nextRun); err != nil {
		t.Fatalf("read next_run_at: %v", err)
	}
	if nextRun.Before(time.Now()) {
		t.Errorf("next_run_at %v is in the past, want the next future match", nextRun)
	}

	// The spawned job carries the schedule's payload, priority and owner.
	var payload string
	var priority int
	if err := pool.QueryRow(ctx, `
		SELECT payload::text, priority FROM jobs
		WHERE payload->>'to' = 'catchup@example.com'`).Scan(&payload, &priority); err != nil {
		t.Fatalf("read spawned job: %v", err)
	}
	if priority != 5 {
		t.Errorf("spawned job priority: got %d, want 5", priority)
	}
}

// the broker is offline when jobs become due. replacement schedulers
// must find pending publications even when no new jobs are due.
func TestScheduledDispatchRecoversAfterBrokerFailure(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	pool := schedulingDB(t, "raven_dispatch_recovery")
	if _, err := pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, scheduled_at)
		VALUES ('job_due', 'send_email', '{}', 'SCHEDULED', 3, now()),
		       ('job_cancel', 'send_email', '{}', 'SCHEDULED', 6, now());
		INSERT INTO cron_schedules (id, name, cron_expr, type, payload, priority, next_run_at)
		VALUES ('cron_due', 'yearly', '0 0 1 1 *', 'send_email', '{}', 8, now())`); err != nil {
		t.Fatal(err)
	}
	deadProducer := jobs.NewProducer("127.0.0.1:1", log)
	defer deadProducer.Close()
	n, err := jobs.NewDispatcher(pool, deadProducer, nil, log, nil, time.Hour).DispatchOnce(ctx)
	if n != 2 || err == nil {
		t.Fatalf("dispatch with broker down: staged=%d, err=%v", n, err)
	}
	n, err = jobs.NewCronScheduler(pool, deadProducer, nil, log, nil, time.Hour).TickOnce(ctx)
	if n != 1 || err == nil {
		t.Fatalf("cron with broker down: spawned=%d, err=%v", n, err)
	}
	var pending int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM scheduled_job_outbox o JOIN jobs j ON j.id = o.job_id
		WHERE j.status = 'QUEUED' AND j.execution_generation = o.execution_generation`).Scan(&pending); err != nil || pending != 3 {
		t.Fatalf("expected all three jobs queued with pending publications: count=%d, err=%v", pending, err)
	}
	api := startJobsAPI(t, pool, deadProducer, log)
	if _, err := api.CancelJob(ctx, &genjobs.CancelJobRequest{Id: "job_cancel"}); err != nil {
		t.Fatalf("cancel pending job: %v", err)
	}

	addr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, addr, log); err != nil {
		t.Fatal(err)
	}
	producer := jobs.NewProducer(addr, log)
	defer producer.Close()
	dispatch := jobs.NewDispatcher(pool, producer, nil, log, nil, time.Hour).DispatchOnce
	tick := jobs.NewCronScheduler(pool, producer, nil, log, nil, time.Hour).TickOnce
	// start two replicas of each scheduler to exercise concurrent recovery.
	type result struct {
		count int
		err   error
	}
	passes := []func(context.Context) (int, error){dispatch, tick, dispatch, tick}
	done := make(chan result, len(passes))
	for _, pass := range passes {
		go func() {
			n, err := pass(ctx)
			done <- result{n, err}
		}()
	}
	for range passes {
		r := <-done
		if r.count != 0 || r.err != nil {
			t.Errorf("recovery should have no new work to stage: count=%d, err=%v", r.count, r.err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM scheduled_job_outbox`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("outbox after recovery: count=%d, err=%v", pending, err)
	}
	for topic, want := range map[string]uint64{"jobs.p3": 1, "jobs.p6": 0, "jobs.p8": 1, jobs.TopicJobs: 2} {
		if got := topicHighWatermark(t, addr, topic); got != want {
			t.Errorf("records on %s after recovery: got %d, want %d", topic, got, want)
		}
	}
}

func TestScheduledDispatchDoesNotPublishBeforeCommit(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool := schedulingDB(t, "raven_dispatch_commit")
	addr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, addr, log); err != nil {
		t.Fatal(err)
	}
	producer := jobs.NewProducer(addr, log)
	defer producer.Close()

	// fail the commit after the writes have succeeded. a message sent
	// inside the transaction would already be visible on the broker.
	if _, err := pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, scheduled_at)
		VALUES ('job_due', 'send_email', '{}', 'SCHEDULED', 4, now());
		INSERT INTO cron_schedules (id, name, cron_expr, type, payload, priority, next_run_at)
		VALUES ('cron_due', 'yearly', '0 0 1 1 *', 'send_email', '{}', 7, now());
		CREATE FUNCTION reject_job_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'test: job commit failed';
		END $$;
		CREATE CONSTRAINT TRIGGER reject_job_at_commit AFTER INSERT OR UPDATE ON jobs
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_job_commit()`); err != nil {
		t.Fatal(err)
	}
	if _, err := jobs.NewDispatcher(pool, producer, nil, log, nil, time.Hour).DispatchOnce(ctx); err == nil {
		t.Fatal("expected delayed job commit to fail")
	}
	if _, err := jobs.NewCronScheduler(pool, producer, nil, log, nil, time.Hour).TickOnce(ctx); err == nil {
		t.Fatal("expected cron job commit to fail")
	}
	for _, topic := range []string{"jobs.p4", "jobs.p7", jobs.TopicJobs} {
		if n := topicHighWatermark(t, addr, topic); n != 0 {
			t.Errorf("published %d records on %s before commit", n, topic)
		}
	}
	var dueJob, dueCron, emptyOutbox bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM jobs WHERE id = 'job_due' AND status = 'SCHEDULED'),
		       EXISTS (SELECT 1 FROM cron_schedules WHERE id = 'cron_due' AND last_run_at IS NULL AND next_run_at <= now()),
		       NOT EXISTS (SELECT 1 FROM scheduled_job_outbox)`).Scan(&dueJob, &dueCron, &emptyOutbox); err != nil {
		t.Fatal(err)
	}
	if !dueJob || !dueCron || !emptyOutbox {
		t.Fatalf("rollback left inconsistent state: due job=%t due cron=%t empty outbox=%t", dueJob, dueCron, emptyOutbox)
	}
}

func TestScheduledDispatchRetriesAfterAck(t *testing.T) {
	check(t)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool := schedulingDB(t, "raven_dispatch_after_ack")
	addr, stopBroker := startTestBroker(t)
	defer stopBroker()
	if err := jobs.EnsureTopics(ctx, addr, log); err != nil {
		t.Fatal(err)
	}
	producer := jobs.NewProducer(addr, log)
	defer producer.Close()
	if _, err := pool.Exec(ctx, `
		INSERT INTO cron_schedules (id, name, cron_expr, type, payload, priority, next_run_at)
		VALUES ('cron_due', 'yearly', '0 0 1 1 *', 'send_email', '{}', 3, now());
		CREATE FUNCTION reject_outbox_delete() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'test: ACK could not be recorded';
		END $$;
		CREATE TRIGGER reject_ack BEFORE DELETE ON scheduled_job_outbox
		FOR EACH ROW EXECUTE FUNCTION reject_outbox_delete()`); err != nil {
		t.Fatal(err)
	}
	n, err := jobs.NewCronScheduler(pool, producer, nil, log, nil, time.Hour).TickOnce(ctx)
	if n != 1 || err == nil {
		t.Fatalf("expected one committed occurrence and cleanup failure: count=%d, err=%v", n, err)
	}
	if got := topicHighWatermark(t, addr, "jobs.p3"); got != 1 {
		t.Fatalf("expected first message before outbox cleanup: got %d", got)
	}
	var jobID string
	var generation int
	if err := pool.QueryRow(ctx, `SELECT job_id, execution_generation FROM scheduled_job_outbox`).Scan(&jobID, &generation); err != nil {
		t.Fatal(err)
	}
	// a migration rollback must preserve this unconfirmed delivery.
	if err := applyMigrationFile(ctx, pool.Config().ConnString(), "000009_immediate_dispatch.down.sql"); err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000008_scheduled_dispatch.down.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(down)); err == nil {
		t.Fatal("down migration discarded a pending delivery")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER reject_ack ON scheduled_job_outbox`); err != nil {
		t.Fatal(err)
	}
	n, err = jobs.NewCronScheduler(pool, producer, nil, log, nil, time.Hour).TickOnce(ctx)
	if n != 0 || err != nil {
		t.Fatalf("retry should only publish the existing occurrence: count=%d, err=%v", n, err)
	}

	readCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	var copies []client.Message
	consumer := client.NewConsumer(addr, "check-redelivery", []string{"jobs.p3"}, func(_ context.Context, msg client.Message) error {
		copies = append(copies, msg)
		if len(copies) == 2 {
			stop()
		}
		return nil
	}, client.WithConsumerLogger(log))
	if err := consumer.Run(readCtx); err != nil || len(copies) != 2 {
		t.Fatalf("expected two broker messages: count=%d, err=%v", len(copies), err)
	}
	for _, msg := range copies {
		id, gen, err := jobs.ParseJobMessage(msg.Value)
		if err != nil || id != jobID || gen != generation || string(msg.Key) != jobID {
			t.Fatalf("redelivery identity: key=%q id=%q generation=%d err=%v", msg.Key, id, gen, err)
		}
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("down migration after outbox drained: %v", err)
	}
}

func TestImmediateDispatchAcceptanceAndRollback(t *testing.T) {
	check(t)
	ctx := t.Context()
	pool := schedulingDB(t, "raven_immediate_acceptance")
	log := quietRecoveryLog()
	addr, stopBroker := startTestBroker(t) // no execution topics yet
	defer stopBroker()
	producer := jobs.NewProducer(addr, log)
	defer producer.Close()
	api := startJobsAPI(t, pool, producer, log)
	if _, err := pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority) VALUES
		   ('source','send_email','{}','SUCCESS',4), ('dead','send_email','{}','DEAD',4);
		CREATE FUNCTION reject_pending() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'test: cannot record pending dispatch'; END $$;
		CREATE TRIGGER reject_pending BEFORE INSERT OR UPDATE ON scheduled_job_outbox
		FOR EACH ROW EXECUTE FUNCTION reject_pending()`); err != nil {
		t.Fatal(err)
	}
	create := &genjobs.CreateJobRequest{Type: "send_email", PayloadJson: "{}", Priority: 4, IdempotencyKey: "accept-once"}
	replay := &genjobs.ReplayJobRequest{Id: "source"}
	requeue := &genjobs.RequeueJobRequest{Id: "dead"}
	_, err := api.CreateJob(ctx, create)
	requireGRPCCode(t, err, codes.Unavailable)
	_, err = api.ReplayJob(ctx, replay)
	requireGRPCCode(t, err, codes.Unavailable)
	_, err = api.RequeueJob(ctx, requeue)
	requireGRPCCode(t, err, codes.Unavailable)

	var unchanged bool
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM jobs) = 2
		AND EXISTS (SELECT 1 FROM jobs WHERE id='dead' AND status='DEAD' AND execution_generation=1)
		AND NOT EXISTS (SELECT 1 FROM scheduled_job_outbox)`).Scan(&unchanged); err != nil || !unchanged {
		t.Fatalf("failed dispatch writes must roll back the job changes: unchanged=%t err=%v", unchanged, err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_pending ON scheduled_job_outbox"); err != nil {
		t.Fatal(err)
	}
	// each operation returns its committed job while the broker is unavailable.
	created, err := api.CreateJob(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	again, err := api.CreateJob(ctx, create)
	if err != nil || again.GetId() != created.GetId() {
		t.Fatalf("duplicate create during outage: id=%q err=%v", again.GetId(), err)
	}
	if _, err := api.ReplayJob(ctx, replay); err != nil {
		t.Fatal(err)
	}
	if _, err := api.RequeueJob(ctx, requeue); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM scheduled_job_outbox o JOIN jobs j
		ON o.job_id=j.id AND o.execution_generation=j.execution_generation WHERE j.status='QUEUED'`).Scan(&pending); err != nil || pending != 3 {
		t.Fatalf("pending current generations=%d want 3; err=%v", pending, err)
	}
	if _, err := api.CancelJob(ctx, &genjobs.CancelJobRequest{Id: created.GetId()}); err != nil {
		t.Fatal(err)
	}
	if err := jobs.EnsureTopics(ctx, addr, log); err != nil {
		t.Fatal(err)
	}
	if _, err := jobs.NewDispatcher(pool, producer, nil, log, nil, time.Second).DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := topicHighWatermark(t, addr, "jobs.p4"); n != 2 {
		t.Fatalf("expected only the replay and requeue to be published, got %d", n)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM scheduled_job_outbox").Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("outbox after recovery: pending=%d err=%v", pending, err)
	}
}
