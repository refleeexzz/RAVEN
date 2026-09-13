package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/database"
	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/httpserver"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
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
}

// shutdownDrain bounds how long we wait for in-flight jobs at shutdown.
const shutdownDrain = 15 * time.Second

// Run starts the consumer, the registry heartbeat and the ops HTTP server,
// and blocks until ctx is cancelled (SIGINT/SIGTERM). Clean stop order:
// stop consuming -> drain in-flight jobs -> flush pending retries -> close
// the producer -> remove the registry entry.
func Run(ctx context.Context, cfg Config) error {
	log := logger.New("worker", cfg.LogLevel)

	pool, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		// Events and the registry degrade; execution continues and /ready
		// reports Redis down until it recovers.
		log.Warn("redis ping failed at startup, continuing in degraded mode",
			slog.String("addr", cfg.RedisAddr), slog.Any("error", err))
	}

	// Topics are also ensured by the jobs service; doing it here too keeps the
	// worker bootable on its own. Existing topics are fine.
	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := jobs.EnsureTopics(topicCtx, cfg.BrokerAddr, log); err != nil {
		cancel()
		return fmt.Errorf("worker: ensure broker topics: %w", err)
	}
	cancel()

	producer := jobs.NewProducer(cfg.BrokerAddr, log)

	metr := metrics.New("worker")
	sm := NewServiceMetrics(metr)

	w := New(Params{
		Pool:        pool,
		RDB:         rdb,
		Producer:    producer,
		Log:         log,
		Metrics:     sm,
		Concurrency: cfg.Concurrency,
		JobTimeout:  cfg.JobTimeout,
	})
	log.Info("worker starting", slog.String("worker_id", w.ID()),
		slog.Int("concurrency", cfg.Concurrency))

	healthReg := health.NewRegistry(3 * time.Second)
	healthReg.Register("postgres", database.Checker(pool))
	healthReg.Register("redis", redisChecker(rdb))
	healthReg.Register("broker", jobs.BrokerChecker(cfg.BrokerAddr))

	// Child contexts so shutdown can stop pieces in order.
	consumeCtx, stopConsume := context.WithCancel(context.Background())
	regCtx, stopRegistry := context.WithCancel(context.Background())

	consumer := client.NewConsumer(cfg.BrokerAddr, "workers", []string{jobs.TopicJobs},
		w.Handle, client.WithConsumerLogger(log))

	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(consumeCtx) }()

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
		stopConsume()
		<-consumerDone
		w.Shutdown(shutdownDrain)
		_ = producer.Close()
		stopRegistry()
		<-registryDone
		return err
	case <-ctx.Done():
	}

	// Clean shutdown, in order.
	stopConsume()
	if err := <-consumerDone; err != nil {
		log.Warn("consumer stopped with error", slog.Any("error", err))
	}
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

// redisChecker pings Redis for the readiness probe.
func redisChecker(rdb redis.UniversalClient) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return rdb.Ping(pingCtx).Err()
	}
}
