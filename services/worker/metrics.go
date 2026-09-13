package worker

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// ServiceMetrics bundles the worker-specific Prometheus collectors. HTTP RED
// metrics come from pkg/metrics; these cover job execution.
type ServiceMetrics struct {
	processed    *prometheus.CounterVec   // raven_worker_jobs_processed_total{result}
	duration     *prometheus.HistogramVec // raven_worker_job_duration_seconds{type}
	inFlight     prometheus.Gauge         // raven_worker_in_flight
	fencedWrites *prometheus.CounterVec   // raven_worker_fenced_writes_total{op}
}

// NewServiceMetrics registers the collectors on reg.
func NewServiceMetrics(reg *metrics.Registry) *ServiceMetrics {
	m := &ServiceMetrics{
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "jobs_processed_total",
			Help:      "Job executions by result (success|retry|dead|skipped|poison|fenced|lease_lost).",
		}, []string{"result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "job_duration_seconds",
			Help:      "Handler execution time by job type.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"type"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "in_flight",
			Help:      "Jobs currently executing in this worker.",
		}),
		fencedWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "fenced_writes_total",
			Help:      "Finish writes rejected by the generation fence (a newer generation owns the job), by op (success|failure|dead).",
		}, []string{"op"}),
	}
	reg.Register(m.processed, m.duration, m.inFlight, m.fencedWrites)
	return m
}
