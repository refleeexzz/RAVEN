//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

// the webhook blocks until the client disconnects, so each kill interrupts
// a worker inside the handler.
func TestWorkerCrashesExhaustAttemptBudget(t *testing.T) {
	check(t)
	const budget = 3
	ctx := t.Context()
	pool := recoveryDB(t, "raven_worker_crash_budget")
	addr, stopBroker := startTestBroker(t)
	t.Cleanup(stopBroker)
	log := quietRecoveryLog()
	if err := jobs.EnsureTopics(ctx, addr, log); err != nil {
		t.Fatal(err)
	}
	producer := jobs.NewProducer(addr, log)
	t.Cleanup(func() { _ = producer.Close() })
	api := startJobsAPI(t, pool, producer, log)
	sweeper := jobs.NewSweeper(pool, producer, nil, log, nil, time.Second)

	bin := filepath.Join(t.TempDir(), "worker.exe")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", bin, "../../cmd/worker").CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}
	startWorker := func(topic string) func() {
		t.Helper()
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(),
			"DATABASE_URL="+pool.Config().ConnString(), "BROKER_ADDR="+addr,
			"REDIS_ADDR="+env.rdb.Options().Addr, "HTTP_ADDR=127.0.0.1:0",
			"WORKER_PRIORITY_TOPICS="+topic, "WORKER_CONCURRENCY=1", "WORKER_JOB_LEASE_MS=600",
			"WORKER_WEBHOOK_ALLOW_PRIVATE=true", "LOG_LEVEL=warn", "OTEL_ENABLED=false")
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		kill := func() {
			t.Helper()
			if cmd.ProcessState != nil {
				return
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Errorf("kill worker: %v", err)
			}
			_ = cmd.Wait() // abrupt termination returns a non-zero exit status
		}
		t.Cleanup(kill)
		return kill
	}

	entered := make(chan struct{}, budget+1)
	var executions atomic.Int32
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/control" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		executions.Add(1)
		select {
		case entered <- struct{}{}:
		case <-r.Context().Done():
		}
		<-r.Context().Done()
	}))
	t.Cleanup(hook.Close)
	submit := func(path string) string {
		t.Helper()
		j, err := api.CreateJob(ctx, &genjobs.CreateJobRequest{
			Type: "webhook", Priority: 5, MaxAttempts: budget,
			PayloadJson: fmt.Sprintf(`{"url":%q}`, hook.URL+path),
		})
		if err != nil {
			t.Fatal(err)
		}
		return j.GetId()
	}
	id := submit("/crash")
	var owners []string
	// both topics carry the same job generation. alternating lanes also
	// exercises redelivery of earlier messages left by the dead consumer.
	for attempt, topic := range []string{"5", "legacy", "5"} {
		kill := startWorker(topic)
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
			t.Fatalf("worker never started execution %d", attempt+1)
		}
		state, owner, gen, _ := jobRow(t, pool, id)
		if state != "PROCESSING" || gen != attempt+1 || owner == "" {
			t.Fatalf("handler started in unexpected state: %s, gen %d, worker %q", state, gen, owner)
		}
		owners = append(owners, owner)
		kill()
		sweepExpiredJob(t, sweeper)
	}
	st, _, gen, charged := jobRow(t, pool, id)
	if st != "DEAD" || charged != budget || gen != budget+1 {
		t.Fatalf("after %d worker process crashes: got %s, attempts=%d, generation=%d; want DEAD, %d, %d",
			budget, st, charged, gen, budget, budget+1)
	}
	var attempts []int32
	var recorded []string
	var allLost bool
	if err := pool.QueryRow(ctx, `SELECT array_agg(attempt ORDER BY attempt), array_agg(worker_id ORDER BY attempt),
		bool_and(error='lease expired (worker lost)' AND finished_at IS NOT NULL AND finished_at >= started_at)
		FROM job_attempts WHERE job_id=$1`, id).Scan(&attempts, &recorded, &allLost); err != nil {
		t.Fatal(err)
	}
	if !allLost || !slices.Equal(attempts, []int32{1, 2, 3}) || !slices.Equal(recorded, owners) {
		t.Fatalf("lease-expiry history: attempts %v workers %v allLost=%t; want [1 2 3] with %v", attempts, recorded, allLost, owners)
	}
	if n, err := sweeper.SweepOnce(ctx); n != 0 || err != nil {
		t.Fatalf("terminal job was swept again: count=%d err=%v", n, err)
	}
	if n := topicRecords(t, addr, jobs.TopicDLQ); n != 1 {
		t.Fatalf("DLQ contains %d records, want 1", n)
	}
	if n := topicRecords(t, addr, jobs.TopicForPriority(5)); n != budget {
		t.Fatalf("work topic contains %d generations, want %d", n, budget)
	}
	startWorker("legacy")
	control := submit("/control")
	waitJobRow(t, pool, control, "SUCCESS", 1, 30*time.Second)
	if n := executions.Load(); n != budget {
		t.Fatalf("observed %d handler executions with budget %d", n, budget)
	}
}

// poll the same lease predicate and advisory lock used by the service.
func sweepExpiredJob(t *testing.T, s *jobs.Sweeper) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		n, err := s.SweepOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("worker lease was not recovered before the deadline")
}

func TestRetryLeaseExpiryPreservesAttemptBudget(t *testing.T) {
	check(t)
	t.Setenv("WORKER_WEBHOOK_ALLOW_PRIVATE", "true")
	ctx := t.Context()
	pool := recoveryDB(t, "raven_retry_lease_budget")
	addr, stopBroker := startTestBroker(t)
	t.Cleanup(stopBroker)
	log := quietRecoveryLog()
	if err := jobs.EnsureTopics(ctx, addr, log); err != nil {
		t.Fatal(err)
	}
	producer := jobs.NewProducer(addr, log)
	t.Cleanup(func() { _ = producer.Close() })
	api := startJobsAPI(t, pool, producer, log)
	started, fail := make(chan struct{}), make(chan struct{})
	var hits atomic.Int32
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if hits.Add(1) == 1 {
			close(started)
			select {
			case <-fail:
				w.WriteHeader(http.StatusServiceUnavailable)
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(hook.Close)
	w, stop := startRecoveryWorker(t, pool, addr, "first", time.Second, 10*time.Second, 10*time.Second)
	t.Cleanup(func() { stop(); w.HardStop() })
	job, err := api.CreateJob(ctx, &genjobs.CreateJobRequest{
		Type: "webhook", PayloadJson: fmt.Sprintf(`{"url":%q}`, hook.URL), Priority: 5, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("first worker did not reach the webhook")
	}
	stop() // no consumer can start the retry
	close(fail)
	id := job.GetId()
	waitJobRow(t, pool, id, "RETRYING", 1, 5*time.Second)
	waitWorkerIdle(t, w, time.Second)
	w.HardStop()
	var failure string
	if err := pool.QueryRow(ctx, "SELECT error FROM jobs WHERE id=$1", id).Scan(&failure); err != nil || failure == "" {
		t.Fatalf("first failure: error=%q query=%v", failure, err)
	}
	// the advisory lock and recovery transaction must share one connection.
	cfg := pool.Config()
	cfg.MaxConns = 1
	sweepPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sweepPool.Close)
	sweeper := jobs.NewSweeper(sweepPool, producer, nil, log, nil, time.Second)
	for wantGen := 2; wantGen <= 3; wantGen++ {
		if wantGen == 3 {
			// the recovery lease lasts 30s. advance just that deadline after
			// the first, naturally expired retry to exercise a second outage.
			if _, err := pool.Exec(ctx, "UPDATE jobs SET lease_until=now() WHERE id=$1", id); err != nil {
				t.Fatal(err)
			}
		}
		sweepExpiredJob(t, sweeper)
		st, _, gen, charged := jobRow(t, pool, id)
		var history int
		var lastError string
		if err := pool.QueryRow(ctx, `SELECT error, (SELECT count(*) FROM job_attempts WHERE job_id=$1)
			FROM jobs WHERE id=$1`, id).Scan(&lastError, &history); err != nil {
			t.Fatal(err)
		}
		if st != "RETRYING" || gen != wantGen || charged != 1 || history != 1 || lastError != failure {
			t.Fatalf("retry wait changed the recorded failure: state=%s gen=%d attempts=%d history=%d error=%q", st, gen, charged, history, lastError)
		}
	}
	next, stopNext := startRecoveryWorker(t, pool, addr, "next", time.Second, 10*time.Second, 5*time.Second)
	t.Cleanup(func() { stopNext(); next.Shutdown(5 * time.Second) })
	waitJobRow(t, pool, id, "SUCCESS", 3, 10*time.Second)
	_, _, _, charged := jobRow(t, pool, id)
	if charged != 2 || hits.Load() != 2 {
		t.Fatalf("after the replacement worker: charged=%d webhook calls=%d, want 2 each", charged, hits.Load())
	}
}
