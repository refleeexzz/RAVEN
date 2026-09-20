// metrics.go holds the gateway-specific Prometheus collectors. The HTTP RED
// metrics (raven_gateway_http_requests_total, duration, in-flight) come free
// from pkg/metrics; these cover the gateway-only concerns: upstream gRPC
// latency, rate limiting, the auth cache and the circuit breakers.
package gateway

import (
	"github.com/prometheus/client_golang/prometheus"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

type serviceMetrics struct {
	upstreamDuration   *prometheus.HistogramVec // raven_gateway_upstream_duration_seconds{upstream,rpc,code}
	rateLimited        prometheus.Counter       // raven_gateway_rate_limited_total
	authCacheHits      prometheus.Counter       // raven_gateway_auth_cache_hits_total
	authCacheMisses    prometheus.Counter       // raven_gateway_auth_cache_misses_total
	rateLimitFallbacks prometheus.Counter       // raven_gateway_rate_limit_fallback_total
	breakerState       *prometheus.GaugeVec     // raven_gateway_circuit_breaker_state{upstream} 0/1/2
	// jwtPreviousSecretUsed is the rotation-window collector from
	// internal/auth, shared with the local token Verifier. Always
	// registered; it sits at 0 outside a JWT rotation window.
	jwtPreviousSecretUsed prometheus.Counter // raven_auth_jwt_previous_secret_used_total
}

func newServiceMetrics(reg *metrics.Registry) *serviceMetrics {
	m := &serviceMetrics{
		upstreamDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raven",
			Subsystem: "gateway",
			Name:      "upstream_duration_seconds",
			Help:      "Latency of upstream gRPC calls in seconds, by upstream, RPC and result code.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"upstream", "rpc", "code"}),
		rateLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "gateway",
			Name:      "rate_limited_total",
			Help:      "Requests rejected by the rate limiter.",
		}),
		authCacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "gateway",
			Name:      "auth_cache_hits_total",
			Help:      "Token validations served from the in-memory cache.",
		}),
		authCacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "gateway",
			Name:      "auth_cache_misses_total",
			Help:      "Token validations that had to call the auth service.",
		}),
		rateLimitFallbacks: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "gateway",
			Name:      "rate_limit_fallback_total",
			Help:      "Rate-limit decisions served by the in-process fallback because Redis errored (fail-open).",
		}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "gateway",
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state per upstream: 0 closed, 1 open, 2 half-open.",
		}, []string{"upstream"}),
		jwtPreviousSecretUsed: ravenauth.NewPreviousSecretUsedCounter(),
	}
	reg.Register(m.upstreamDuration, m.rateLimited, m.authCacheHits, m.authCacheMisses, m.rateLimitFallbacks, m.breakerState, m.jwtPreviousSecretUsed)
	return m
}
