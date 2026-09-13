package users

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/raven/platform/pkg/metrics"
)

// ServiceMetrics bundles the users-specific Prometheus collectors.
type ServiceMetrics struct {
	requests *prometheus.CounterVec // raven_users_requests_total{op,result}
}

func NewServiceMetrics(reg *metrics.Registry) *ServiceMetrics {
	m := &ServiceMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "users",
			Name:      "requests_total",
			Help:      "User-service RPCs by operation and result (ok|error).",
		}, []string{"op", "result"}),
	}
	reg.Register(m.requests)
	return m
}

// observe increments the per-op counter. result is "ok" or "error".
func (m *ServiceMetrics) observe(op string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.requests.WithLabelValues(op, result).Inc()
}
