//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

// admin requests reach the broker. this listener blocks publication by the
// first jobs process before it can append to the log.
type beforeAppendGate struct {
	*broker.Broker
	reached chan struct{}
	release chan struct{}
}

func (g *beforeAppendGate) Produce(ctx context.Context, _ *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	select {
	case g.reached <- struct{}{}:
	default:
	}
	select {
	case <-g.release:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func startDispatchProcess(t *testing.T, bin, dsn, brokerAddr string) (genjobs.JobServiceClient, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	// the test context expires before cleanup runs. give the process its
	// own deadline so cleanup can terminate it and wait for it to exit.
	lifetime, cancelProcess := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancelProcess)
	cmd := exec.CommandContext(lifetime, bin)
	cmd.Env = append(os.Environ(), "DATABASE_URL="+dsn, "BROKER_ADDR="+brokerAddr,
		"REDIS_ADDR="+env.rdb.Options().Addr, "GRPC_ADDR="+addr, "HTTP_ADDR=127.0.0.1:0",
		"LOG_LEVEL=warn", "OTEL_ENABLED=false", "JOBS_SCHEDULER_INTERVAL=0s",
		"JOBS_SWEEP_INTERVAL=0s", "JOBS_LEGACY_TOPIC_FANOUT=true")
	cmd.Stderr, cmd.Stdout = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	kill := func() {
		t.Helper()
		if cmd.ProcessState != nil {
			return
		}
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill jobs process: %v", err)
		}
		_ = cmd.Wait() // a forced exit returns a non-zero status
	}
	t.Cleanup(kill)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	api := genjobs.NewJobServiceClient(conn)
	ready, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if _, err := api.ListJobs(ready, &genjobs.ListJobsRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("jobs process did not become ready: %v", err)
	}
	return api, kill
}

func TestImmediateDispatchSurvivesJobsProcessCrash(t *testing.T) {
	check(t)
	t.Setenv("WORKER_WEBHOOK_ALLOW_PRIVATE", "true")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	bin := filepath.Join(t.TempDir(), "jobs.exe")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", bin, "../../cmd/jobs").CombinedOutput(); err != nil {
		t.Fatalf("build jobs service: %v\n%s", err, out)
	}
	pool := schedulingDB(t, "raven_immediate_crash")
	dsn := pool.Config().ConnString()
	if err := applyMigrationFile(ctx, dsn, "000005_webhook_deliveries.up.sql"); err != nil {
		t.Fatal(err)
	}
	b, stopBroker := startTestBrokerNode(t)
	t.Cleanup(stopBroker)
	gate := &beforeAppendGate{Broker: b, reached: make(chan struct{}, 1), release: make(chan struct{})}
	listener := server.New("127.0.0.1:0", gate, time.Second, quietRecoveryLog())
	gateCtx, stopGate := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- listener.Run(gateCtx) }()
	t.Cleanup(func() {
		stopGate()
		close(gate.release)
		if err := <-done; err != nil {
			t.Errorf("broker gate: %v", err)
		}
	})
	for listener.Addr() == "" {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	api, crash := startDispatchProcess(t, bin, dsn, listener.Addr())

	var mu sync.Mutex
	hits := make(map[string]int)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		hits[req.URL.Path]++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(hook.Close)
	payload := func(path string) string {
		return fmt.Sprintf(`{"url":%q}`, hook.URL+path)
	}
	const source = "job_replay_source"
	const dead = "job_dead_for_requeue"
	owner := uuid.NewString()
	_, err := pool.Exec(ctx, `
		INSERT INTO jobs (id,type,payload,status,priority,owner_id) VALUES
		($1,'webhook',$3,'SUCCESS',5,$5), ($2,'webhook',$4,'DEAD',5,$5)`,
		source, dead, payload("/replay"), payload("/requeue"), owner)
	if err != nil {
		t.Fatal(err)
	}
	idem := "dispatch-crash-" + uuid.NewString()
	callCtx := asUser(ctx, owner)
	create := &genjobs.CreateJobRequest{Type: "webhook", PayloadJson: payload("/create"), Priority: 5, IdempotencyKey: idem}
	results := make(chan error, 3)
	go func() { _, err := api.CreateJob(callCtx, create); results <- err }()
	go func() { _, err := api.ReplayJob(callCtx, &genjobs.ReplayJobRequest{Id: source}); results <- err }()
	go func() { _, err := api.RequeueJob(callCtx, &genjobs.RequeueJobRequest{Id: dead}); results <- err }()

	select {
	case <-gate.reached:
	case <-time.After(15 * time.Second):
		t.Fatal("first jobs process never attempted publication")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var queued int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='QUEUED'").Scan(&queued); err != nil {
			t.Fatal(err)
		}
		if queued == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 3 submissions committed QUEUED", queued)
		}
		time.Sleep(20 * time.Millisecond)
	}
	crash() // terminate the process before a graceful shutdown can publish the rows.
	for range 3 {
		if err := <-results; err != nil && status.Code(err) != codes.Unavailable {
			t.Fatalf("unexpected submission result at crash: %v", err)
		}
	}
	for _, topic := range []string{jobs.TopicForPriority(5), jobs.TopicJobs} {
		if n := topicHighWatermark(t, b.Addr(), topic); n != 0 {
			t.Fatalf("%s had %d records before replacement, want 0", topic, n)
		}
	}
	var created, replayed string
	if err := pool.QueryRow(ctx, "SELECT id FROM jobs WHERE idempotency_key=$1", idem).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT id FROM jobs WHERE replayed_from=$1", source).Scan(&replayed); err != nil {
		t.Fatal(err)
	}
	wantGen := map[string]int{created: 1, replayed: 1, dead: 2}
	for id, want := range wantGen {
		var gen int
		if err := pool.QueryRow(ctx, `SELECT execution_generation FROM scheduled_job_outbox
			WHERE job_id=$1`, id).Scan(&gen); err != nil || gen != want {
			t.Errorf("lost dispatch for %s after process crash: gen=%d, want %d, err=%v", id, gen, want, err)
		}
	}

	// the replacement connects to the same database and broker. scheduling
	// and lease recovery stay paused while the dispatch relay runs.
	replacement, _ := startDispatchProcess(t, bin, dsn, b.Addr())
	w, stopWorker := startRecoveryWorker(t, pool, b.Addr(), "dispatch-recovery", 10*time.Second, 5*time.Second, 2*time.Second)
	t.Cleanup(func() { stopWorker(); w.Shutdown(5 * time.Second) })
	for id, gen := range wantGen {
		waitJobRow(t, pool, id, "SUCCESS", gen, 20*time.Second)
		got, err := replacement.GetJob(callCtx, &genjobs.GetJobRequest{Id: id})
		if err != nil || got.GetStatus() != genjobs.JobStatus_JOB_STATUS_SUCCESS || got.GetAttempts() != 1 {
			t.Fatalf("recovered job %s: %+v err=%v", id, got, err)
		}
	}
	// the caller can lose the response when the process exits.
	again, err := replacement.CreateJob(callCtx, create)
	if err != nil || again.GetId() != created {
		t.Fatalf("create retry returned id=%q, want %q, err=%v", again.GetId(), created, err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		var pending int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM scheduled_job_outbox").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d dispatches still pending after job completion", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, path := range []string{"/create", "/replay", "/requeue"} {
		if hits[path] != 1 {
			t.Errorf("webhook %s: %d deliveries, want 1", path, hits[path])
		}
	}
}
