// Package gateway is the RAVEN API gateway: the single public entrypoint of
// the platform. It translates REST to gRPC for the auth, users and jobs
// services, enforces authentication (via auth.ValidateToken + a short-lived
// cache), per-route permissions and rate limits, adds resilience (timeouts,
// bounded retries, circuit breakers), proxies /ws to the websocket service
// and serves the ops endpoints (/health, /ready, /metrics) that Prometheus
// scrapes. Everything listens on one port, 8080.
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/httpserver"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	"github.com/refleeexzz/RAVEN/pkg/tracing"
)

// apiTimeout bounds every API request (the /ws proxy is exempt — websockets
// are long-lived).
const apiTimeout = 10 * time.Second

// Config carries everything the gateway needs. cmd/gateway fills it from
// the environment variables in docs/contracts/ports-and-env.md.
type Config struct {
	HTTPAddr      string // :8080 — public API and ops endpoints share this port
	AuthGRPCAddr  string // localhost:9081
	UsersGRPCAddr string // localhost:9082
	JobsGRPCAddr  string // localhost:9083
	WSAddr        string // http://localhost:8084
	RedisAddr     string // localhost:6379
	// JWTSecret is part of the contract env table. Token validation itself
	// is delegated to the auth service over gRPC, so the gateway only keeps
	// it for parity/future local checks.
	JWTSecret      string
	LogLevel       string
	OtelEnabled    bool
	OtelEndpoint   string
	RateLimitRPM   int // RATE_LIMIT_RPM, default 100
	RateLimitBurst int // RATE_LIMIT_BURST, default 20
}

// server bundles the dependencies the route table and middleware need.
type server struct {
	log     *slog.Logger
	metr    *metrics.Registry
	metrics *serviceMetrics
	limiter *rateLimiter
	authn   *authenticator
	authH   *authHandlers
	usersH  *usersHandlers
	jobsH   *jobsHandlers
	health  *health.Registry
	wsProxy http.Handler
	otelMW  middleware.Middleware
}

// Run wires everything and serves until ctx is cancelled (SIGINT/SIGTERM).
// It returns nil on a clean shutdown.
func Run(ctx context.Context, cfg Config) error {
	log := logger.New("gateway", cfg.LogLevel)

	shutdownTracing, err := tracing.Setup(ctx, "gateway", cfg.OtelEndpoint, cfg.OtelEnabled)
	if err != nil {
		return fmt.Errorf("gateway: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()

	metr := metrics.New("gateway")
	gatewayMetrics := newServiceMetrics(metr)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		// Redis backs the worker registry endpoint. The gateway still starts;
		// readiness reports Redis down until it recovers.
		log.Warn("redis ping failed at startup, continuing in degraded mode",
			slog.String("addr", cfg.RedisAddr), slog.Any("error", err))
	}

	// One shared connection per upstream, each with its own circuit breaker.
	authUp, err := dialUpstream("auth", cfg.AuthGRPCAddr, log, gatewayMetrics)
	if err != nil {
		return err
	}
	defer authUp.conn.Close()
	usersUp, err := dialUpstream("users", cfg.UsersGRPCAddr, log, gatewayMetrics)
	if err != nil {
		return err
	}
	defer usersUp.conn.Close()
	jobsUp, err := dialUpstream("jobs", cfg.JobsGRPCAddr, log, gatewayMetrics)
	if err != nil {
		return err
	}
	defer jobsUp.conn.Close()

	// The janitors sweep the auth cache and the rate-limit buckets; both stop
	// when ctx is cancelled at shutdown.
	limiter := newRateLimiter(cfg.RateLimitRPM, cfg.RateLimitBurst)
	go limiter.sweep(ctx)
	cache := newAuthCache()
	go cache.sweep(ctx)

	wsProxy, err := newWSProxy(cfg.WSAddr, log)
	if err != nil {
		return err
	}

	healthReg := health.NewRegistry(3 * time.Second)
	healthReg.Register("auth_grpc", grpcConnectivityChecker(authUp.conn))
	healthReg.Register("users_grpc", grpcConnectivityChecker(usersUp.conn))
	healthReg.Register("jobs_grpc", grpcConnectivityChecker(jobsUp.conn))
	healthReg.Register("redis", redisChecker(rdb))

	s := &server{
		log:     log,
		metr:    metr,
		metrics: gatewayMetrics,
		limiter: limiter,
		authn: &authenticator{
			validator: newGRPCTokenValidator(authUp),
			cache:     cache,
			metrics:   gatewayMetrics,
		},
		authH:   newAuthHandlers(authUp),
		usersH:  newUsersHandlers(usersUp),
		jobsH:   newJobsHandlers(jobsUp, rdb),
		health:  healthReg,
		wsProxy: wsProxy,
		otelMW:  otelMiddleware(cfg.OtelEnabled),
	}

	log.Info("gateway listening", slog.String("addr", cfg.HTTPAddr))
	return httpserver.ListenAndServe(ctx, cfg.HTTPAddr, s.handler())
}

// handler assembles the whole public surface: API routes with the full
// middleware chain, ops endpoints with the light chain, and the /ws proxy
// with the minimal chain.
func (s *server) handler() http.Handler {
	mux := http.NewServeMux()

	for _, rt := range s.table() {
		mux.Handle(rt.method+" "+rt.path, s.wrapAPI(rt))
	}

	mux.Handle("GET /health", s.wrapOps(s.health.Liveness()))
	mux.Handle("GET /ready", s.wrapOps(s.health.Readiness()))
	mux.Handle("GET /metrics", s.wrapOps(s.metr.Handler()))

	// /ws gets only RequestID + Recovery. Logging, metrics and timeout wrap
	// the ResponseWriter with recorders that do not implement http.Hijacker,
	// which would break the websocket upgrade.
	ws := middleware.Chain(s.wsProxy, middleware.RequestID, middleware.Recovery(s.log))
	mux.Handle("GET /ws", ws)
	mux.Handle("GET /ws/", ws)

	// Outermost layer: CORS for the console's browser fetches. Header-only
	// (no ResponseWriter wrapping), so /ws hijacking still works.
	return cors(mux)
}

// wrapAPI applies the API middleware chain to one route:
//
//	RequestID → Logging → Recovery → metrics → otel (when enabled) →
//	Timeout(10s) → AuthN → AuthZ → RateLimit → handler
//
// AuthN/AuthZ are skipped on public routes; RateLimit then keys on the
// client IP instead of the user id.
func (s *server) wrapAPI(rt route) http.Handler {
	chain := []middleware.Middleware{
		middleware.RequestID,
		middleware.Logging(s.log),
		middleware.Recovery(s.log),
		s.metr.Middleware,
		s.otelMW,
		middleware.Timeout(apiTimeout),
	}
	if rt.perm != "" {
		chain = append(chain, s.authn.middleware, s.authorize(rt.perm))
	}
	chain = append(chain, s.rateLimit)
	return middleware.Chain(rt.handler, chain...)
}

// wrapOps applies the light chain to the ops endpoints: no auth, no rate
// limit, no timeout — Prometheus and the kubelet must always get through.
func (s *server) wrapOps(h http.Handler) http.Handler {
	return middleware.Chain(h,
		middleware.RequestID,
		middleware.Logging(s.log),
		middleware.Recovery(s.log),
		s.metr.Middleware,
	)
}

// otelMiddleware wraps requests with an otelhttp span when tracing is on,
// and is a pass-through otherwise. Span names use the route pattern to keep
// cardinality bounded.
func otelMiddleware(enabled bool) middleware.Middleware {
	if !enabled {
		return func(next http.Handler) http.Handler { return next }
	}
	return otelhttp.NewMiddleware("gateway",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if r.Pattern != "" {
				return r.Method + " " + r.Pattern
			}
			return r.Method + " " + r.URL.Path
		}),
	)
}

// grpcConnectivityChecker is the readiness probe for one gRPC upstream: it
// kicks the lazy connection and waits up to 1 s for it to become ready.
func grpcConnectivityChecker(conn *grpc.ClientConn) health.Checker {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		conn.Connect()
		for {
			state := conn.GetState()
			if state == connectivity.Ready {
				return nil
			}
			if !conn.WaitForStateChange(ctx, state) {
				return fmt.Errorf("connection not ready (state: %s)", conn.GetState())
			}
		}
	}
}

// redisChecker is the readiness probe for Redis.
func redisChecker(rdb redis.UniversalClient) health.Checker {
	return func(ctx context.Context) error {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return rdb.Ping(pingCtx).Err()
	}
}
