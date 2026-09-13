package auth

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/raven/platform/pkg/metrics"
)

// ServiceMetrics bundles the auth-specific Prometheus collectors. HTTP RED
// metrics come from pkg/metrics; these counters cover the business events.
type ServiceMetrics struct {
	logins          *prometheus.CounterVec // raven_auth_logins_total{result}
	tokensValidated *prometheus.CounterVec // raven_auth_tokens_validated_total{result}
	refreshRotated  prometheus.Counter     // raven_auth_refresh_rotations_total
}

func NewServiceMetrics(reg *metrics.Registry) *ServiceMetrics {
	m := &ServiceMetrics{
		logins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "auth",
			Name:      "logins_total",
			Help:      "Login attempts by result (success|failure).",
		}, []string{"result"}),
		tokensValidated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "auth",
			Name:      "tokens_validated_total",
			Help:      "Access-token validations by result (valid|invalid).",
		}, []string{"result"}),
		refreshRotated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "auth",
			Name:      "refresh_rotations_total",
			Help:      "Successful refresh-token rotations.",
		}),
	}
	reg.Register(m.logins, m.tokensValidated, m.refreshRotated)
	return m
}
