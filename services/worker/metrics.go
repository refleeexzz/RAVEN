package worker

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// ServiceMetrics bundles the worker-specific Prometheus collectors. HTTP RED
// metrics come from pkg/metrics; these cover job execution.
type ServiceMetrics struct {
	processed    *prometheus.CounterVec   // raven_worker_jobs_processed_total{result}
	duration     *prometheus.HistogramVec // raven_worker_job_duration_seconds{type}
	inFlightN    atomic.Int64             // backs raven_worker_in_flight + raven_worker_active_jobs
	retries      prometheus.Counter       // raven_worker_retries_total
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
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "retries_total",
			Help:      "Job failures scheduled for a retry republish (RETRYING transitions driven by this worker).",
		}),
		fencedWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "fenced_writes_total",
			Help:      "Finish writes rejected by the generation fence (a newer generation owns the job), by op (success|failure|dead).",
		}, []string{"op"}),
	}
	readInFlight := func() float64 { return float64(m.inFlightN.Load()) }
	reg.Register(m.processed, m.duration, m.retries, m.fencedWrites,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "in_flight",
			Help:      "Jobs currently executing in this worker.",
		}, readInFlight),
		// active_jobs is the roadmap (P2) name for the same series. Both
		// read one atomic, so they can never disagree; in_flight stays for
		// existing dashboards.
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "worker",
			Name:      "active_jobs",
			Help:      "Jobs currently executing in this worker. Alias of raven_worker_in_flight.",
		}, readInFlight),
	)
	return m
}

// incInFlight / decInFlight track the executing-job count behind both
// in_flight and active_jobs.
func (m *ServiceMetrics) incInFlight() { m.inFlightN.Add(1) }
func (m *ServiceMetrics) decInFlight() { m.inFlightN.Add(-1) }
