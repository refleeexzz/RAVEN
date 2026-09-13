package users

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/raven/platform/internal/database"
	genusers "github.com/raven/platform/internal/gen/users"
	"github.com/raven/platform/internal/health"
	"github.com/raven/platform/internal/httpserver"
	"github.com/raven/platform/internal/middleware"
	"github.com/raven/platform/pkg/errors"
	"github.com/raven/platform/pkg/logger"
	"github.com/raven/platform/pkg/metrics"
	"github.com/raven/platform/pkg/tracing"
)

// Config carries everything the users service needs. cmd/users fills it
// from environment variables documented in docs/contracts/ports-and-env.md.
type Config struct {
	GRPCAddr    string // :9082
	HTTPAddr    string // :8082 (ops)
	DatabaseURL string
	LogLevel    string
	BcryptCost  int // USERS_BCRYPT_COST; tests use bcrypt.MinCost (4)

	// Tracing (OTel). Disabled by default locally; enabled in k8s via
	// the raven-config ConfigMap.
	OtelEndpoint string
	OtelEnabled  bool
}

// Run starts the gRPC service and the ops HTTP server and blocks until ctx
// is cancelled (SIGINT/SIGTERM) or a server fails. It returns nil on a
// clean shutdown.
func Run(ctx context.Context, cfg Config) error {
	log := logger.New("users", cfg.LogLevel)

	pool, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("users: %w", err)
	}
	defer pool.Close()

	metr := metrics.New("users")
	sm := NewServiceMetrics(metr)

	healthReg := health.NewRegistry(3 * time.Second)
	healthReg.Register("postgres", database.Checker(pool))

	srv := NewServer(pool, log, cfg.BcryptCost, sm)

	shutdownTracing, err := tracing.Setup(ctx, "users", cfg.OtelEndpoint, cfg.OtelEnabled)
	if err != nil {
		return fmt.Errorf("users: tracing setup: %w", err)
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
	genusers.RegisterUserServiceServer(grpcSrv, srv)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("users: listen %s: %w", cfg.GRPCAddr, err)
	}

	// Cancel the child context on any fatal server error so the other
	// server stops too.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		log.Info("grpc listening", slog.String("addr", lis.Addr().String()))
		if err := grpcSrv.Serve(lis); err != nil {
			errCh <- fmt.Errorf("users: grpc serve: %w", err)
		}
	}()
	go func() {
		log.Info("ops http listening", slog.String("addr", cfg.HTTPAddr))
		errCh <- httpserver.ListenAndServe(ctx, cfg.HTTPAddr, opsHandler(healthReg, metr, log))
	}()
	// Graceful gRPC stop on shutdown: drain in-flight RPCs for up to 10s,
	// then force-close so a stuck stream cannot block the shutdown forever.
	go func() {
		<-ctx.Done()
		timer := time.AfterFunc(10*time.Second, grpcSrv.Stop)
		defer timer.Stop()
		grpcSrv.GracefulStop()
	}()

	select {
	case err := <-errCh:
		cancel()
		<-errCh // wait for the other server to wind down
		return err
	case <-ctx.Done():
		return <-errCh // httpserver returns nil after a clean shutdown
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

// unaryLoggingInterceptor logs one line per finished RPC.
func unaryLoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.WithContext(ctx, log).Debug("grpc call",
			slog.String("method", info.FullMethod),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return resp, err
	}
}

// unaryRecoveryInterceptor converts panics into Internal errors.
func unaryRecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("grpc panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", rec),
				)
				err = toStatus(errors.E(errors.KindUnknown, "internal", "internal error", nil))
			}
		}()
		return handler(ctx, req)
	}
}
