package auth

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

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/internal/database"
	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/httpserver"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	"github.com/refleeexzz/RAVEN/pkg/tracing"
)

// Config carries everything the auth service needs. cmd/auth fills it from
// environment variables documented in docs/contracts/ports-and-env.md.
type Config struct {
	GRPCAddr    string // :9081
	HTTPAddr    string // :8081 (ops)
	DatabaseURL string
	RedisAddr   string
	JWTSecret   string
	// JWTSecretPrevious (JWT_SECRET_PREVIOUS) opens the JWT rotation
	// window: verification falls back to it while signing keeps using
	// JWTSecret. Empty = single-secret operation (docs/security/rotation.md).
	JWTSecretPrevious string
	LogLevel          string
	BcryptCost        int // AUTH_BCRYPT_COST; clamped to [10, ∞) by HashPassword

	// Tracing (OTel). Disabled by default locally; enabled in k8s via
	// the raven-config ConfigMap.
	OtelEndpoint string
	OtelEnabled  bool
}

// Run starts the gRPC service and the ops HTTP server and blocks until ctx
// is cancelled (SIGINT/SIGTERM) or a server fails. It returns nil on a
// clean shutdown.
func Run(ctx context.Context, cfg Config) error {
	log := logger.New("auth", cfg.LogLevel)

	pool, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		// Redis backs the revocation denylist and the permission cache. The
		// service starts without it (see ValidateToken's fail-open comment);
		// readiness reports Redis down until it recovers.
		log.Warn("redis ping failed at startup, continuing in degraded mode",
			slog.String("addr", cfg.RedisAddr), slog.Any("error", err))
	}

	metr := metrics.New("auth")
	sm := NewServiceMetrics(metr)

	// Dual-secret JWT verification (docs/security/rotation.md): signing
	// always uses JWT_SECRET; JWT_SECRET_PREVIOUS, when set, keeps tokens
	// minted just before a rotation valid until they expire. The auth
	// service is the token issuer — booting without a primary secret is a
	// config error, so fail fast instead of serving 5xx on every login.
	verifier, err := ravenauth.NewVerifier(cfg.JWTSecret, cfg.JWTSecretPrevious, sm.previousSecretUsed)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	if verifier.HasPrevious() {
		log.Info("JWT rotation window open: verifying against JWT_SECRET_PREVIOUS as fallback",
			slog.String("metric", "raven_auth_jwt_previous_secret_used_total"))
	}

	healthReg := health.NewRegistry(3 * time.Second)
	healthReg.Register("postgres", database.Checker(pool))
	healthReg.Register("redis", redisChecker(rdb))

	srv := NewServerWithVerifier(pool, rdb, log, verifier, cfg.BcryptCost, sm)

	shutdownTracing, err := tracing.Setup(ctx, "auth", cfg.OtelEndpoint, cfg.OtelEnabled)
	if err != nil {
		return fmt.Errorf("auth: tracing setup: %w", err)
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
	genauth.RegisterAuthServiceServer(grpcSrv, srv)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("auth: listen %s: %w", cfg.GRPCAddr, err)
	}

	// Cancel the child context on any fatal server error so the other
	// server stops too.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		log.Info("grpc listening", slog.String("addr", lis.Addr().String()))
		if err := grpcSrv.Serve(lis); err != nil {
			errCh <- fmt.Errorf("auth: grpc serve: %w", err)
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
