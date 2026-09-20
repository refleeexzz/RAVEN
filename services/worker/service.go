package worker

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/config"
	"github.com/refleeexzz/RAVEN/internal/database"
	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/httpserver"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	"github.com/refleeexzz/RAVEN/pkg/tracing"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

// Config carries everything the worker needs. cmd/worker fills it from
// environment variables documented in docs/contracts/ports-and-env.md.
type Config struct {
	HTTPAddr    string // :8085 (ops)
	DatabaseURL string
	RedisAddr   string
	BrokerAddr  string
	LogLevel    string
	Concurrency int           // WORKER_CONCURRENCY, default 8
	JobTimeout  time.Duration // WORKER_JOB_TIMEOUT, default 30s
	JobLease    time.Duration // WORKER_JOB_LEASE_MS, default 30s

	// Broker client protection (docs/broker-security.md), all optional.
	// Empty everywhere = open mode: plaintext, no AUTH frame, fully
	// compatible with a default broker.
	BrokerAPIKeyID          string // BROKER_API_KEY_ID
	BrokerAPIKeySecret      string // BROKER_API_KEY_SECRET
	BrokerTLSCAFile         string // BROKER_TLS_CA_FILE
	BrokerTLSServerName     string // BROKER_TLS_SERVER_NAME (optional)
	BrokerTLSClientCertFile string // BROKER_TLS_CLIENT_CERT_FILE (optional, mTLS)
	BrokerTLSClientKeyFile  string // BROKER_TLS_CLIENT_KEY_FILE (optional, mTLS)

	// WebhookAllowPrivate disables the webhook egress range checks
	// (WORKER_WEBHOOK_ALLOW_PRIVATE). Dev/test escape hatch — never in prod.
	WebhookAllowPrivate bool

	// PriorityTopics selects the topics to consume, WORKER_PRIORITY_TOPICS
	// format ("1..9+legacy" by default; see ParsePriorityTopics). Empty means
	// the default: the full jobs.p1..jobs.p9 family plus the legacy topic.
	PriorityTopics string

	// Tracing (OTel). Disabled by default locally; enabled in k8s via
	// the raven-config ConfigMap.
	OtelEndpoint string
	OtelEnabled  bool
}

// shutdownDrain bounds how long we wait for in-flight jobs at shutdown.
const shutdownDrain = 15 * time.Second

// Run starts the consumer, the registry heartbeat and the ops HTTP server,
// and blocks until ctx is cancelled (SIGINT/SIGTERM). Clean stop order:
// stop consuming -> drain in-flight jobs -> flush pending retries -> close
// the producer -> remove the registry entry.
func Run(ctx context.Context, cfg Config) error {
	log := logger.New("worker", cfg.LogLevel)

	// Tracing must be live before the Worker is built: New captures the
	// global tracer, and the extract→execute spans are what continue the
	// trace the jobs service injected into the record headers.
	shutdownTracing, err := tracing.Setup(ctx, "worker", cfg.OtelEndpoint, cfg.OtelEnabled)
	if err != nil {
		return fmt.Errorf("worker: tracing setup: %w", err)
	}
	defer func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = shutdownTracing(shutdownCtx)
	}()

	pool, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		// Events and the registry degrade; execution continues and /ready
		// reports Redis down until it recovers.
		log.Warn("redis ping failed at startup, continuing in degraded mode",
			slog.String("addr", cfg.RedisAddr), slog.Any("error", err))
	}

	// Topics are also ensured by the jobs service; doing it here too keeps the
	// worker bootable on its own. Existing topics are fine. The priority
	// topics get the same treatment so a worker running ahead of the jobs
	// service rollout can still join the group. A typo in the broker auth/TLS
	// config fails the boot right here instead of surfacing as perpetual
	// consume errors.
	bsec, err := jobs.NewBrokerSecurity(jobs.BrokerSecurityConfig{
		APIKeyID:          cfg.BrokerAPIKeyID,
		APIKeySecret:      cfg.BrokerAPIKeySecret,
		TLSCAFile:         cfg.BrokerTLSCAFile,
		TLSServerName:     cfg.BrokerTLSServerName,
		TLSClientCertFile: cfg.BrokerTLSClientCertFile,
		TLSClientKeyFile:  cfg.BrokerTLSClientKeyFile,
	})
	if err != nil {
		return fmt.Errorf("worker: broker security config: %w", err)
	}
	log.Info("broker client security", slog.String("mode", bsec.Mode()))

	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := jobs.EnsureTopicsWithSecurity(topicCtx, cfg.BrokerAddr, log, bsec); err != nil {
		cancel()
		return fmt.Errorf("worker: ensure broker topics: %w", err)
	}
	topics, err := ParsePriorityTopics(priorityTopicsSpec(cfg))
	if err != nil {
		cancel()
		return err
	}
	if err := ensureTopics(topicCtx, cfg.BrokerAddr, topics, log, bsec); err != nil {
		cancel()
		return fmt.Errorf("worker: ensure priority topics: %w", err)
	}
	cancel()

	producer := jobs.NewProducerWithSecurity(cfg.BrokerAddr, log, bsec)

	metr := metrics.New("worker")
	sm := NewServiceMetrics(metr)

	w := New(Params{
		Pool:                 pool,
		RDB:                  rdb,
		Producer:             producer,
		Log:                  log,
		Metrics:              sm,
		Concurrency:          cfg.Concurrency,
		JobTimeout:           cfg.JobTimeout,
		JobLease:             cfg.JobLease,
		AllowPrivateWebhooks: cfg.WebhookAllowPrivate,
	})
	if cfg.JobTimeout >= cfg.JobLease {
		// Renewals keep the lease alive, but a handler running right up to a
		// timeout that exceeds the lease leaves no margin for the finish
		// write: the sweeper could take the job over first. Keep
		// WORKER_JOB_TIMEOUT well below WORKER_JOB_LEASE_MS.
		log.Warn("job timeout >= lease: long jobs risk fencing by the sweeper",
			slog.Duration("job_timeout", cfg.JobTimeout),
			slog.Duration("job_lease", cfg.JobLease))
	}
	log.Info("worker starting", slog.String("worker_id", w.ID()),
		slog.Int("concurrency", cfg.Concurrency),
		slog.Duration("job_lease", cfg.JobLease),
		slog.Any("topics", topics))

	healthReg := health.NewRegistry(3 * time.Second)
	healthReg.Register("postgres", database.Checker(pool))
	healthReg.Register("redis", redisChecker(rdb))
	healthReg.Register("broker", jobs.BrokerCheckerWithSecurity(cfg.BrokerAddr, bsec))

	// Child contexts so shutdown can stop pieces in order.
	consumeCtx, stopConsume := context.WithCancel(context.Background())
	regCtx, stopRegistry := context.WithCancel(context.Background())

	// One consumer per topic, all in the "workers" group, all sharing the
	// same execution pipeline. The priority order is enforced at dispatch
	// (see priority.go): while urgent lanes saturate the execution slots,
	// cheaper lanes block in Handle and their offsets simply wait.
	consumerOpts := append([]client.ConsumerOption{client.WithConsumerLogger(log)}, bsec.ConsumerOptions()...)
	consumerDone := make(chan error, len(topics))
	for _, topic := range topics {
		consumer := client.NewConsumer(cfg.BrokerAddr, "workers", []string{topic},
			w.Handle, consumerOpts...)
		go func() { consumerDone <- consumer.Run(consumeCtx) }()
	}
	stopConsumers := func() {
		stopConsume()
		for range topics {
			if err := <-consumerDone; err != nil {
				log.Warn("consumer stopped with error", slog.Any("error", err))
			}
		}
	}

	reg := newRegistry(rdb, w.ID(), log, w.StartedAt(), func() (int64, int64) {
		return w.Processed(), w.InFlight()
	})
	registryDone := make(chan struct{})
	go func() { defer close(registryDone); reg.run(regCtx) }()

	errCh := make(chan error, 1)
	go func() {
		log.Info("ops http listening", slog.String("addr", cfg.HTTPAddr))
		errCh <- httpserver.ListenAndServe(ctx, cfg.HTTPAddr, opsHandler(healthReg, metr, w, log))
	}()

	select {
	case err := <-errCh:
		// Ops server died: shut everything down, then report.
		stopConsumers()
		w.Shutdown(shutdownDrain)
		_ = producer.Close()
		stopRegistry()
		<-registryDone
		return err
	case <-ctx.Done():
	}

	// Clean shutdown, in order.
	stopConsumers()
	w.Shutdown(shutdownDrain)
	if err := producer.Close(); err != nil {
		log.Warn("producer close failed", slog.Any("error", err))
	}
	stopRegistry()
	<-registryDone
	return <-errCh // httpserver returns nil after a clean shutdown
}

// opsHandler serves /health, /ready, /metrics and /debug/stats.
func opsHandler(healthReg *health.Registry, metr *metrics.Registry, w *Worker, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", healthReg.Liveness())
	mux.Handle("GET /ready", healthReg.Readiness())
	mux.Handle("GET /metrics", metr.Handler())
	mux.Handle("GET /debug/stats", http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(w.Stats())
	}))
	return middleware.Chain(metr.Middleware(mux),
		middleware.RequestID,
		middleware.Logging(log),
		middleware.Recovery(log),
	)
}

// priorityTopicsSpec resolves the topic spec: Config wins, then
// WORKER_PRIORITY_TOPICS, then the default. Reading the env here (like
// New does for WORKER_WEBHOOK_ALLOW_PRIVATE) keeps cmd/worker untouched.
func priorityTopicsSpec(cfg Config) string {
	if cfg.PriorityTopics != "" {
		return cfg.PriorityTopics
	}
	return config.Get("WORKER_PRIORITY_TOPICS", "")
}

// redisChecker pings Redis for the readiness probe.
func redisChecker(rdb redis.UniversalClient) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return rdb.Ping(pingCtx).Err()
	}
}

// ensureTopics creates the given topics, swallowing TOPIC_EXISTS. Same
// pattern as jobs.EnsureTopics, kept here so the worker can boot its
// priority lanes without waiting on a jobs-service rollout. sec carries the
// broker client auth/TLS settings (nil = open mode); auth failures are
// decorated by jobs.ExplainBrokerError so a bad key fails the boot fast.
func ensureTopics(ctx context.Context, addr string, topics []string, log *slog.Logger, sec *jobs.BrokerSecurity) error {
	admin := client.NewAdmin(addr, sec.AdminOptions()...)
	defer func() { _ = admin.Close() }()
	for _, topic := range topics {
		_, err := admin.CreateTopic(ctx, topic, 0) // 0 = broker default partitions
		if err == nil {
			log.Info("topic created", slog.String("topic", topic))
			continue
		}
		var pe *protocol.Error
		if stderrors.As(err, &pe) && pe.Code == protocol.CodeTopicExists {
			continue
		}
		return jobs.ExplainBrokerError("ensure topics", err)
	}
	return nil
}
