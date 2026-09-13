package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/refleeexzz/RAVEN/internal/database"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/httpserver"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	"github.com/refleeexzz/RAVEN/pkg/tracing"
)

// Config carries everything the jobs service needs. cmd/jobs fills it from
// environment variables documented in docs/contracts/ports-and-env.md.
type Config struct {
	GRPCAddr    string // :9083
	HTTPAddr    string // :8083 (ops)
	DatabaseURL string
	RedisAddr   string
	BrokerAddr  string
	LogLevel    string

	// SweepInterval is how often the stranded-job sweeper runs
	// (JOBS_SWEEP_INTERVAL, default 15s). <= 0 disables the sweeper (tests,
	// single-purpose debug deployments); disabling it reopens the kill -9
	// stranding window, so production should always run it.
	SweepInterval time.Duration

	// Tracing (OTel). Disabled by default locally; enabled in k8s via
	// the raven-config ConfigMap.
	OtelEndpoint string
	OtelEnabled  bool
}

// Run starts the gRPC service and the ops HTTP server and blocks until ctx
// is cancelled (SIGINT/SIGTERM) or a server fails. It returns nil on a clean
// shutdown.
func Run(ctx context.Context, cfg Config) error {
	log := logger.New("jobs", cfg.LogLevel)

	pool, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("jobs: %w", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		// Redis backs idempotency and job events. We start without it:
		// idempotency falls back to the Postgres unique index, events are
		// dropped, and /ready reports Redis down until it recovers.
		log.Warn("redis ping failed at startup, continuing in degraded mode",
			slog.String("addr", cfg.RedisAddr), slog.Any("error", err))
	}

	// The broker is NOT optional: without it we can store jobs but never run
	// them. Ensure the topics exist before accepting traffic.
	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := EnsureTopics(topicCtx, cfg.BrokerAddr, log); err != nil {
		cancel()
		return fmt.Errorf("jobs: ensure broker topics: %w", err)
	}
	cancel()

	producer := NewProducer(cfg.BrokerAddr, log)
	defer producer.Close()

	metr := metrics.New("jobs")
	sm := NewServiceMetrics(metr, countProcessingFunc(log, pool))

	healthReg := health.NewRegistry(3 * time.Second)
	healthReg.Register("postgres", database.Checker(pool))
	healthReg.Register("redis", redisChecker(rdb))
	healthReg.Register("broker", BrokerChecker(cfg.BrokerAddr))

	// Stranded-job sweeper: one goroutine per replica, exactly one active at
	// a time across replicas via the advisory lock inside SweepOnce.
	if cfg.SweepInterval > 0 {
		sweeper := NewSweeper(pool, producer, rdb, log, sm, cfg.SweepInterval)
		go sweeper.Run(ctx)
	} else {
		log.Warn("job sweeper disabled; kill -9 can strand jobs in PROCESSING/RETRYING")
	}

	srv := NewServer(pool, rdb, producer, log, sm)

	shutdownTracing, err := tracing.Setup(ctx, "jobs", cfg.OtelEndpoint, cfg.OtelEnabled)
	if err != nil {
		return fmt.Errorf("jobs: tracing setup: %w", err)
	}
	defer func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = shutdownTracing(shutdownCtx)
	}()

	// otelgrpc's server handler continues the trace the gateway started, so
	// one request shows up as a single trace across services in Jaeger.
	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			unaryRecoveryInterceptor(log),
			unaryLoggingInterceptor(log),
		),
	)
	genjobs.RegisterJobServiceServer(grpcSrv, srv)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("jobs: listen %s: %w", cfg.GRPCAddr, err)
	}

	// Cancel the child context on any fatal server error so the other server
	// stops too.
	ctx, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	errCh := make(chan error, 2)
	go func() {
		log.Info("grpc listening", slog.String("addr", lis.Addr().String()))
		if err := grpcSrv.Serve(lis); err != nil {
			errCh <- fmt.Errorf("jobs: grpc serve: %w", err)
		}
	}()
	go func() {
		log.Info("ops http listening", slog.String("addr", cfg.HTTPAddr))
		errCh <- httpserver.ListenAndServe(ctx, cfg.HTTPAddr, opsHandler(healthReg, metr, log))
	}()
	// Graceful gRPC stop: drain in-flight RPCs for up to 10s, then force.
	go func() {
		<-ctx.Done()
		timer := time.AfterFunc(10*time.Second, grpcSrv.Stop)
		defer timer.Stop()
		grpcSrv.GracefulStop()
	}()

	select {
	case err := <-errCh:
		cancel2()
		<-errCh
		return err
	case <-ctx.Done():
		return <-errCh
	}
}

// opsHandler serves /health, /ready and /metrics on the ops port.
func opsHandler(healthReg *health.Registry, metr *metrics.Registry, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", healthReg.Liveness())
	mux.Handle("GET /ready", healthReg.Readiness())
	mux.Handle("GET /metrics", metr.Handler())
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
