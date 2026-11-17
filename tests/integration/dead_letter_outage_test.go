//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

// disconnect this listener to cut every broker connection used by the first
// service and worker. the replacement connects to the same broker log.
func interruptibleBrokerEndpoint(t *testing.T, b *broker.Broker) (string, func()) {
	t.Helper()
	listener := server.New("127.0.0.1:0", b, 100*time.Millisecond, quietRecoveryLog())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- listener.Run(ctx) }()
	disconnect := sync.OnceFunc(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("broker listener: %v", err)
		}
	})
	t.Cleanup(disconnect)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if addr := listener.Addr(); addr != "" {
			return addr, disconnect
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("broker listener did not start")
	return "", nil
}

func TestDeadLetterSurvivesBrokerOutage(t *testing.T) {
	check(t)
	t.Setenv("WORKER_WEBHOOK_ALLOW_PRIVATE", "true")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	bin := filepath.Join(t.TempDir(), "jobs.exe")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", bin, "../../cmd/jobs").CombinedOutput(); err != nil {
		t.Fatalf("build jobs service: %v\n%s", err, out)
	}
	pool := schedulingDB(t, "raven_dead_letter_outage")
	dsn := pool.Config().ConnString()
	if err := applyMigrationFile(ctx, dsn, "000005_webhook_deliveries.up.sql"); err != nil {
		t.Fatal(err)
	}
	b, stopBroker := startTestBrokerNode(t)
	t.Cleanup(stopBroker)
	endpoint, disconnect := interruptibleBrokerEndpoint(t, b)
	api, crash := startDispatchProcess(t, bin, dsn, endpoint)

	calls := map[string]*atomic.Int32{"/dead": {}, "/requeue": {}}
	entered, release := make(chan string, len(calls)), make(chan struct{})
	failAttempts := sync.OnceFunc(func() { close(release) })
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, ok := calls[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if n.Add(1) == 1 {
			entered <- r.URL.Path
			select {
			case <-release:
				w.WriteHeader(http.StatusServiceUnavailable)
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(func() { failAttempts(); hook.Close() })
	worker, stop := startRecoveryWorker(t, pool, endpoint, "outage-worker", 30*time.Second, 25*time.Second, 20*time.Second)
	t.Cleanup(func() { stop(); worker.Shutdown(5 * time.Second) })
	owner := uuid.NewString()
	userCtx := asUser(ctx, owner)
	ids := make(map[string]string)
	for path := range calls {
		job, err := api.CreateJob(userCtx, &genjobs.CreateJobRequest{
			Type: "webhook", PayloadJson: fmt.Sprintf(`{"url":%q}`, hook.URL+path), Priority: 5, MaxAttempts: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		ids[path] = job.GetId()
	}
	for range calls {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("worker never reached both webhooks")
		}
	}
	stop()
	disconnect()
	failAttempts()
	for _, id := range ids {
		waitJobRow(t, pool, id, "DEAD", 1, 10*time.Second)
	}
	waitWorkerIdle(t, worker, 10*time.Second)
	if n := topicRecords(t, b.Addr(), jobs.TopicDLQ); n != 0 {
		t.Fatalf("broker accepted %d dead letters before recovery", n)
	}
	// requeuing resets the live row while its first generation still awaits publication.
	if _, err := api.RequeueJob(userCtx, &genjobs.RequeueJobRequest{Id: ids["/requeue"]}); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_dead_letter_outbox WHERE execution_generation=1").Scan(&pending); err != nil || pending != len(ids) {
		t.Fatalf("committed dead letters during outage: count=%d want=%d err=%v", pending, len(ids), err)
	}
	crash()
	startDispatchProcess(t, bin, dsn, b.Addr())
	next, stopNext := startRecoveryWorker(t, pool, b.Addr(), "replacement-worker", 15*time.Second, 5*time.Second, 2*time.Second)
	t.Cleanup(func() { stopNext(); next.Shutdown(5 * time.Second) })

	waitJobRow(t, pool, ids["/requeue"], "SUCCESS", 2, 20*time.Second)
	waitForDLQ(t, b.Addr(), uint64(len(ids)), 15*time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_dead_letter_outbox").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d dead letters still pending after acknowledgement", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st, _, _, attempts := jobRow(t, pool, ids["/dead"]); st != "DEAD" || attempts != 1 {
		t.Fatalf("terminal job after broker recovery: %s with %d attempts", st, attempts)
	}
	if n, r := calls["/dead"].Load(), calls["/requeue"].Load(); n != 1 || r != 2 {
		t.Fatalf("unexpected handler executions: terminal=%d requeued=%d", n, r)
	}

	for _, record := range readDeadLetters(t, b.Addr(), len(ids)) {
		var msg struct {
			ID         string `json:"id"`
			Status     string `json:"status"`
			Generation int    `json:"execution_generation"`
			Attempts   int    `json:"attempts"`
			WorkerID   string `json:"worker_id"`
			OwnerID    string `json:"owner_id"`
			Error      string `json:"error"`
			Payload    struct {
				URL string `json:"url"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(record.Value, &msg); err != nil {
			t.Fatal(err)
		}
		path := strings.TrimPrefix(msg.Payload.URL, hook.URL)
		id, ok := ids[path]
		if !ok || msg.ID != id || string(record.Key) != id || msg.Status != "DEAD" || msg.Generation != 1 ||
			msg.Attempts != 1 || msg.WorkerID != "outage-worker" || msg.OwnerID != owner || !strings.Contains(msg.Error, "503") {
			t.Fatalf("unexpected terminal snapshot (key %q): %s", record.Key, record.Value)
		}
		delete(ids, path)
	}
}

func TestDeadLetterRetriesAfterAck(t *testing.T) {
	check(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pool := recoveryDB(t, "raven_dead_letter_ack")
	addr, stopBroker := startTestBroker(t)
	t.Cleanup(stopBroker)
	log := quietRecoveryLog()
	if err := jobs.EnsureTopics(ctx, addr, log); err != nil {
		t.Fatal(err)
	}
	producer := jobs.NewProducer(addr, log)
	t.Cleanup(func() { _ = producer.Close() })
	const id = "job_after_ack"
	seedJob(t, pool, id, "PROCESSING", 0, 1, nil)
	now := time.Now()
	terminal, err := jobs.FinishJobFailure(ctx, pool, id, 1, 1, "ack-worker", "terminal failure", true, now, now, 0)
	if err != nil || terminal == nil {
		t.Fatalf("terminal write: job=%v err=%v", terminal, err)
	}

	// an overlapping relay must skip the generation while another transaction owns it.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	var lockedID string
	if err := holder.QueryRow(ctx, "SELECT job_id FROM job_dead_letter_outbox WHERE job_id=$1 FOR UPDATE", id).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	lockCtx, stopLockCheck := context.WithTimeout(ctx, 2*time.Second)
	worked, err := jobs.DispatchDeadLetter(lockCtx, pool, producer, nil)
	stopLockCheck()
	if err != nil || worked {
		t.Fatalf("competing relay: worked=%t err=%v", worked, err)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_dead_letter_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected dead letter commit failure'; END $$;
		CREATE CONSTRAINT TRIGGER reject_dead_letter_commit AFTER DELETE ON job_dead_letter_outbox
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_dead_letter_commit()`); err != nil {
		t.Fatal(err)
	}
	if _, err := jobs.DispatchDeadLetter(ctx, pool, producer, terminal); err == nil || !strings.Contains(err.Error(), "injected dead letter commit failure") {
		t.Fatalf("wanted commit failure after broker acknowledgement, got %v", err)
	}
	if n := topicRecords(t, addr, jobs.TopicDLQ); n != 1 {
		t.Fatalf("broker held %d records after commit failure, want 1", n)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_dead_letter_commit ON job_dead_letter_outbox"); err != nil {
		t.Fatal(err)
	}
	if ok, err := jobs.DispatchDeadLetter(ctx, pool, producer, nil); err != nil || !ok {
		t.Fatalf("retry after acknowledgement: worked=%t err=%v", ok, err)
	}
	if ok, err := jobs.DispatchDeadLetter(ctx, pool, producer, nil); err != nil || ok {
		t.Fatalf("scan after retry: worked=%t err=%v", ok, err)
	}
	copies := readDeadLetters(t, addr, 2)
	recordID, generation, err := jobs.ParseJobMessage(copies[0].Value)
	if err != nil || recordID != id || generation != terminal.ExecutionGeneration ||
		string(copies[0].Key) != id || string(copies[1].Key) != id || !bytes.Equal(copies[0].Value, copies[1].Value) {
		t.Fatalf("redelivery changed the terminal snapshot: first=%s second=%s", copies[0].Value, copies[1].Value)
	}
	if st, _, gen, attempts := jobRow(t, pool, id); st != "DEAD" || gen != generation || attempts != 1 {
		t.Fatalf("job changed during redelivery: status=%s generation=%d attempts=%d", st, gen, attempts)
	}
}

func readDeadLetters(t *testing.T, addr string, count int) []client.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var records []client.Message
	consumer := client.NewConsumer(addr, "verify-dead-letters", []string{jobs.TopicDLQ}, func(_ context.Context, m client.Message) error {
		records = append(records, m)
		if len(records) == count {
			cancel()
		}
		return nil
	}, client.WithConsumerLogger(quietRecoveryLog()))
	if err := consumer.Run(ctx); err != nil || len(records) != count {
		t.Fatalf("read dead letters: count=%d want=%d err=%v", len(records), count, err)
	}
	return records
}
