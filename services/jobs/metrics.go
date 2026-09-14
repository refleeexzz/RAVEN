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
	jobsTotal        *prometheus.CounterVec // raven_jobs_total{type,status}
	created          prometheus.Counter     // raven_jobs_created_total
	processing       prometheus.GaugeFunc   // raven_jobs_processing
	queued           prometheus.GaugeFunc   // raven_jobs_queued
	retrying         prometheus.GaugeFunc   // raven_jobs_retrying
	dead             prometheus.GaugeFunc   // raven_jobs_dead
	scheduled        prometheus.GaugeFunc   // raven_jobs_scheduled
	dispatched       prometheus.Counter     // raven_jobs_scheduled_dispatched_total
	sweeperRuns      prometheus.Counter     // raven_jobs_sweeper_runs_total
	sweeperRecovered *prometheus.CounterVec // raven_jobs_sweeper_recovered_total{outcome}
}

// NewServiceMetrics registers the collectors. The status gauges are measured
// at scrape time straight from Postgres, so they survive restarts and stay
// true even though workers (other processes) do the transitions.
func NewServiceMetrics(reg *metrics.Registry, countStatus func(Status) (int64, error)) *ServiceMetrics {
	statusGauge := func(name, help string, status Status) prometheus.GaugeFunc {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "jobs",
			Name:      name,
			Help:      help,
		}, func() float64 {
			n, err := countStatus(status)
			if err != nil {
				return -1
			}
			return float64(n)
		})
	}
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
		processing: statusGauge("processing",
			"Jobs currently in PROCESSING, read from Postgres at scrape time (-1 when the read fails).",
			StatusProcessing),
		queued: statusGauge("queued",
			"Jobs currently in QUEUED, read from Postgres at scrape time (-1 when the read fails).",
			StatusQueued),
		retrying: statusGauge("retrying",
			"Jobs currently in RETRYING, read from Postgres at scrape time (-1 when the read fails).",
			StatusRetrying),
		dead: statusGauge("dead",
			"Jobs currently in DEAD, read from Postgres at scrape time (-1 when the read fails).",
			StatusDead),
		scheduled: statusGauge("scheduled",
			"Jobs currently in SCHEDULED (delayed jobs waiting for their time), read from Postgres at scrape time (-1 when the read fails).",
			StatusScheduled),
		dispatched: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "jobs",
			Name:      "scheduled_dispatched_total",
			Help:      "Delayed jobs the dispatcher released to the broker (SCHEDULED -> QUEUED).",
		}),
		sweeperRuns: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "jobs",
			Name:      "sweeper_runs_total",
			Help:      "Sweeper passes that held the advisory lock and scanned for expired leases.",
		}),
		sweeperRecovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "jobs",
			Name:      "sweeper_recovered_total",
			Help:      "Stranded jobs taken over by the sweeper, by outcome (retry|dead).",
		}, []string{"outcome"}),
	}
	reg.Register(m.jobsTotal, m.created, m.processing, m.queued, m.retrying, m.dead,
		m.scheduled, m.dispatched, m.sweeperRuns, m.sweeperRecovered)
	return m
}

// observeTransition counts one transition the jobs service performed.
func (m *ServiceMetrics) observeTransition(jobType string, to Status) {
	m.jobsTotal.WithLabelValues(jobType, string(to)).Inc()
}

// countByStatusFunc queries Postgres with a bounded context. Kept as a small
// closure factory so the status GaugeFuncs stay testable.
func countByStatusFunc(log *slog.Logger, q querier) func(Status) (int64, error) {
	return func(status Status) (int64, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		n, err := countByStatus(ctx, q, status)
		if err != nil {
			log.Warn("status gauge query failed",
				slog.String("status", string(status)), slog.Any("error", err))
		}
		return n, err
	}
}
