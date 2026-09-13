// Package metrics wires Prometheus into every service the same way:
// one registry per process, RED-style HTTP metrics (rate, errors,
// duration) and a /metrics handler. Service-specific counters live in
// the services themselves; this package is only the shared plumbing.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry bundles the Prometheus registry with the standard HTTP
// instrumenter.
type Registry struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge
}

// New creates a registry pre-loaded with Go runtime metrics and the
// shared HTTP collectors, all namespaced by service name.
func New(service string) *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   "raven",
		Subsystem:   service,
		Name:        "http_requests_total",
		Help:        "Total HTTP requests by method, path and status.",
		ConstLabels: prometheus.Labels{"service": service},
	}, []string{"method", "path", "status"})

	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "raven",
		Subsystem:   service,
		Name:        "http_request_duration_seconds",
		Help:        "HTTP request latency in seconds.",
		Buckets:     prometheus.DefBuckets,
		ConstLabels: prometheus.Labels{"service": service},
	}, []string{"method", "path"})

	inFlight := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   "raven",
		Subsystem:   service,
		Name:        "http_requests_in_flight",
		Help:        "HTTP requests currently being served.",
		ConstLabels: prometheus.Labels{"service": service},
	})

	reg.MustRegister(requests, duration, inFlight)
	return &Registry{reg: reg, requests: requests, duration: duration, inFlight: inFlight}
}

// Register exposes the underlying registry so services can add their
// own collectors (jobs_total, broker_messages_pending, ...).
func (r *Registry) Register(cs ...prometheus.Collector) {
	r.reg.MustRegister(cs...)
}

// Middleware measures every request. path should be the route pattern
// ("/users/{id}"), not the raw URL, to keep cardinality bounded.
func (r *Registry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		r.inFlight.Inc()
		defer r.inFlight.Dec()

		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, req)

		path := req.Pattern
		if path == "" {
			path = "unknown"
		}
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		r.requests.WithLabelValues(req.Method, path, strconv.Itoa(status)).Inc()
		r.duration.WithLabelValues(req.Method, path).Observe(time.Since(start).Seconds())
	})
}

// Handler serves GET /metrics for Prometheus scrapes.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

// statusRecorder captures the response status. Kept local to avoid an
// import cycle with internal/middleware (middleware must not depend on
// metrics).
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}
