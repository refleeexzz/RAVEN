package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// ServiceMetrics bundles the jobs-specific Prometheus collectors. HTTP RED
// metrics come from pkg/metrics; these cover the job lifecycle.
type ServiceMetrics struct {
	jobsTotal  *prometheus.CounterVec // raven_jobs_total{type,status}
	created    prometheus.Counter     // raven_jobs_created_total
	processing prometheus.GaugeFunc   // raven_jobs_processing
}

// NewServiceMetrics registers the collectors. processing is measured at
// scrape time straight from Postgres, so the gauge survives restarts and
// stays true even though workers (other processes) do the transitions.
func NewServiceMetrics(reg *metrics.Registry, countProcessing func() (int64, error)) *ServiceMetrics {
	m := &ServiceMetrics{
		jobsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "jobs",
			Name:      "total",
			Help:      "Job status transitions performed by the jobs service, by type and resulting status.",
		}, []string{"type", "status"}),
		created: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "jobs",
			Name:      "created_total",
			Help:      "Jobs accepted by CreateJob (after validation, before broker produce).",
		}),
		processing: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "jobs",
			Name:      "processing",
			Help:      "Jobs currently in PROCESSING, read from Postgres at scrape time (-1 when the read fails).",
		}, func() float64 {
			n, err := countProcessing()
			if err != nil {
				return -1
			}
			return float64(n)
		}),
	}
	reg.Register(m.jobsTotal, m.created, m.processing)
	return m
}

// observeTransition counts one transition the jobs service performed.
func (m *ServiceMetrics) observeTransition(jobType string, to Status) {
	m.jobsTotal.WithLabelValues(jobType, string(to)).Inc()
}

// countProcessing queries Postgres with a bounded context. Kept as a small
// closure factory so the GaugeFunc stays testable.
func countProcessingFunc(log *slog.Logger, q querier) func() (int64, error) {
	return func() (int64, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		n, err := countByStatus(ctx, q, StatusProcessing)
		if err != nil {
			log.Warn("processing gauge query failed", slog.Any("error", err))
		}
		return n, err
	}
}
