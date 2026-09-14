//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/services/jobs"
	workersvc "github.com/refleeexzz/RAVEN/services/worker"
)

// Priority-aware consumption: one worker with a single execution slot and
// one consumer per topic, all in the "workers" group. While the slot is
// busy, urgent messages queue ahead of cheap ones at dispatch; the observed
// execution order must follow topic rank (jobs.p1 -> jobs.p2 -> legacy).

// orderedGateServer serves webhook POSTs that block on per-name gates and
// records the arrival order of every request.
type orderedGateServer struct {
	mu     sync.Mutex
	gates  map[string]chan struct{}
	hits   []string
	server *httptest.Server
}

func newOrderedGateServer(t *testing.T) *orderedGateServer {
	t.Helper()
	g := &orderedGateServer{gates: map[string]chan struct{}{}}
	g.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("gate")
		_, _ = io.Copy(io.Discard, r.Body)
		g.mu.Lock()
		g.hits = append(g.hits, name)
		ch, ok := g.gates[name]
		g.mu.Unlock()
		if ok {
			select {
			case <-ch:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(g.server.Close)
	return g
}

// block makes requests with ?gate=name wait until release(name).
func (g *orderedGateServer) block(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gates[name] = make(chan struct{})
}

// release lets blocked requests with ?gate=name answer 200.
func (g *orderedGateServer) release(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ch, ok := g.gates[name]; ok {
		close(ch)
		delete(g.gates, name)
	}
}

// waitHit polls until at least n requests arrived.
func (g *orderedGateServer) waitHit(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		got := len(g.hits)
		g.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	t.Fatalf("only %d gate hits after %s, want %d (hits: %v)", len(g.hits), timeout, n, g.hits)
}

// order returns the recorded arrival order.
func (g *orderedGateServer) order() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.hits...)
}

// publishWebhookJob seeds a QUEUED job row (generation 1, one attempt) and
// publishes its message to the given topic.
func publishWebhookJob(t *testing.T, pool *pgxpool.Pool, producer *jobs.Producer, topic, id, url string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, attempts, max_attempts)
		VALUES ($1, 'webhook', $2::jsonb, 'QUEUED', 5, 0, 1)`, id, `{"url":"`+url+`"}`)
	if err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
	j := &jobs.Job{
		ID: id, Type: "webhook", Payload: `{"url":"` + url + `"}`,
		Status: jobs.StatusQueued, Priority: 5, MaxAttempts: 1,
		CreatedAt:           time.Now().UTC(),
		ExecutionGeneration: 1, // matches the migration 000003 column default
	}
	if err := producer.PublishJob(ctx, topic, j); err != nil {
		t.Fatalf("publish %s to %s: %v", id, topic, err)
	}
}

func TestPriorityConsumptionOrder(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool := recoveryDB(t, "raven_priority_order")
	brokerAddr, stopBroker := startTestBroker(t)
	defer stopBroker()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	admin := client.NewAdmin(brokerAddr)
	defer admin.Close()
	for _, topic := range []string{"jobs.p1", "jobs.p2", jobs.TopicJobs} {
		if _, err := admin.CreateTopic(ctx, topic, 0); err != nil {
			t.Fatalf("create %s: %v", topic, err)
		}
	}

	gate := newOrderedGateServer(t)
	gate.block("blocker")

	producer := jobs.NewProducer(brokerAddr, log)
	defer producer.Close()

	// One execution slot: dispatch order is fully determined by the gate.
	w := workersvc.New(workersvc.Params{
		Pool:                 pool,
		Producer:             producer,
		Log:                  log,
		Concurrency:          1,
		JobTimeout:           10 * time.Second,
		JobLease:             30 * time.Second,
		WorkerID:             "worker-priority",
		AllowPrivateWebhooks: true,
	})

	// One consumer per topic, same group, shared pipeline — exactly what
	// services/worker.Run wires from WORKER_PRIORITY_TOPICS.
	consumeCtx, stopConsume := context.WithCancel(context.Background())
	var consumersDone sync.WaitGroup
	for _, topic := range []string{"jobs.p1", "jobs.p2", jobs.TopicJobs} {
		c := client.NewConsumer(brokerAddr, "workers", []string{topic},
			w.Handle, client.WithConsumerLogger(log))
		consumersDone.Add(1)
		go func() { defer consumersDone.Done(); _ = c.Run(consumeCtx) }()
	}
	defer func() {
		stopConsume()
		consumersDone.Wait()
	}()

	urlFor := func(name string) string { return gate.server.URL + "?gate=" + name }

	// Phase 1: the blocker occupies the single slot.
	publishWebhookJob(t, pool, producer, jobs.TopicJobs, "job_blocker", urlFor("blocker"))
	gate.waitHit(t, 1, 15*time.Second)

	// Phase 2: while the slot is busy, cheap and urgent work arrives. The
	// legacy job was published FIRST on purpose: arrival order must lose to
	// rank order.
	publishWebhookJob(t, pool, producer, jobs.TopicJobs, "job_legacy", urlFor("legacy"))
	publishWebhookJob(t, pool, producer, "jobs.p2", "job_mid", urlFor("mid"))
	publishWebhookJob(t, pool, producer, "jobs.p1", "job_urgent", urlFor("urgent"))

	// Give the three consumers time to fetch and queue at the dispatch gate.
	time.Sleep(3 * time.Second)

	// Phase 3: release the blocker. The free slot must go to jobs.p1 first,
	// then jobs.p2, then legacy.
	gate.release("blocker")

	waitJobStatusFor(t, pool, "job_blocker", "SUCCESS", 15*time.Second)
	waitJobStatusFor(t, pool, "job_urgent", "SUCCESS", 15*time.Second)
	waitJobStatusFor(t, pool, "job_mid", "SUCCESS", 15*time.Second)
	waitJobStatusFor(t, pool, "job_legacy", "SUCCESS", 15*time.Second)

	got := gate.order()
	want := []string{"blocker", "urgent", "mid", "legacy"}
	if len(got) != len(want) {
		t.Fatalf("gate hits: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("execution order: got %v, want %v", got, want)
		}
	}
}

// TestWorkerRunPriorityTopics drives the real service wiring
// (services/worker.Run) with a custom WORKER_PRIORITY_TOPICS equivalent:
// the priority topics are ensured on the broker, all configured lanes are
// consumed, and shutdown is clean.
func TestWorkerRunPriorityTopics(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool := recoveryDB(t, "raven_priority_run")

	base, err := env.pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/raven_priority_run"
	dsn := u.String()

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

	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workersvc.Run(workerCtx, workersvc.Config{
			HTTPAddr:       "127.0.0.1:0",
			DatabaseURL:    dsn,
			RedisAddr:      redisOpt.Addr,
			BrokerAddr:     brokerAddr,
			LogLevel:       "warn",
			Concurrency:    2,
			JobTimeout:     10 * time.Second,
			PriorityTopics: "1..2+legacy",
		})
	}()

	// Wait until the worker's boot ensured the p2 lane (it ensures topics
	// itself), then publish. Legacy "jobs" comes from the same EnsureTopics.
	producer := jobs.NewProducer(brokerAddr, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer producer.Close()
	waitForTopic(t, brokerAddr, "jobs.p2", 15*time.Second)
	waitForTopic(t, brokerAddr, jobs.TopicJobs, 15*time.Second)

	publishWebhookJob(t, pool, producer, "jobs.p2", "job_lane_p2", "http://127.0.0.1:1/unreachable")
	publishWebhookJob(t, pool, producer, jobs.TopicJobs, "job_lane_legacy", "http://127.0.0.1:1/unreachable")

	// Both lanes are drained; the unreachable target makes the outcome a
	// fast DEAD (max_attempts 1) instead of a long backoff.
	waitJobStatusFor(t, pool, "job_lane_p2", "DEAD", 30*time.Second)
	waitJobStatusFor(t, pool, "job_lane_legacy", "DEAD", 30*time.Second)

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

// waitForTopic polls the broker until the topic exists.
func waitForTopic(t *testing.T, brokerAddr, topic string, timeout time.Duration) {
	t.Helper()
	admin := client.NewAdmin(brokerAddr)
	defer admin.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := admin.ListTopics(context.Background())
		if err == nil {
			for _, tp := range resp.Topics {
				if tp.Name == topic {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s did not appear in %s", topic, timeout)
}
