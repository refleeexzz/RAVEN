package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/semaphore"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

// Worker consumes the jobs topic and executes jobs with bounded concurrency.
//
// Dispatch model: the broker client invokes Handle sequentially per record;
// Handle acquires a semaphore slot and runs the job in a goroutine, so up to
// `concurrency` jobs execute at once. The offset commits when Handle returns
// (at dispatch). That means a hard kill can strand up to `concurrency`
// dispatched-but-unfinished jobs in QUEUED/RETRYING/PROCESSING — the same is
// true for the one job a synchronous handler was mid-way through when it
// died. Postgres is the source of truth for recovery: the status fence skips
// duplicates on redelivery, and a sweeper for stranded jobs is a documented
// TODO. Graceful shutdown (the normal path) drains in-flight jobs and
// pending retries before exiting, so nothing strands on SIGTERM.
type Worker struct {
	id       string
	pool     *pgxpool.Pool
	rdb      redis.UniversalClient
	producer *jobs.Producer
	handlers map[string]Handler
	sem      *semaphore.Weighted
	timeout  time.Duration
	log      *slog.Logger
	metrics  *ServiceMetrics

	concurrency int
	startedAt   time.Time
	inFlight    atomic.Int64
	processed   atomic.Int64 // terminal jobs (SUCCESS + DEAD)

	timersMu sync.Mutex
	timers   map[string]*retryTask // pending retry republishes, by job id
	closed   atomic.Bool           // set when shutdown starts
}

// retryTask is a scheduled republish of one job message.
type retryTask struct {
	timer *time.Timer
	raw   []byte
}

// Params carries the worker dependencies.
type Params struct {
	Pool        *pgxpool.Pool
	RDB         redis.UniversalClient
	Producer    *jobs.Producer
	Log         *slog.Logger
	Metrics     *ServiceMetrics
	Concurrency int           // semaphore size (WORKER_CONCURRENCY, default 8)
	JobTimeout  time.Duration // per-job deadline (WORKER_JOB_TIMEOUT, default 30s)
	WorkerID    string        // optional override (tests); empty = generated
	HTTPClient  *http.Client  // optional override (tests); nil = 5s timeout client
}

// New builds a Worker with the default handler registry.
func New(p Params) *Worker {
	if p.Concurrency <= 0 {
		p.Concurrency = 8
	}
	if p.JobTimeout <= 0 {
		p.JobTimeout = 30 * time.Second
	}
	if p.Log == nil {
		p.Log = slog.Default()
	}
	if p.HTTPClient == nil {
		p.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	id := p.WorkerID
	if id == "" {
		host, _ := os.Hostname()
		id = fmt.Sprintf("worker-%s-%s", host, uuid.NewString()[:6])
	}
	return &Worker{
		id:          id,
		pool:        p.Pool,
		rdb:         p.RDB,
		producer:    p.Producer,
		handlers:    newHandlers(p.Log, p.HTTPClient),
		sem:         semaphore.NewWeighted(int64(p.Concurrency)),
		timeout:     p.JobTimeout,
		log:         p.Log,
		metrics:     p.Metrics,
		concurrency: p.Concurrency,
		startedAt:   time.Now().UTC(),
		timers:      make(map[string]*retryTask),
	}
}

// ID is the worker's registry identity: worker-<hostname>-<uuid6>.
func (w *Worker) ID() string { return w.id }

// StartedAt is when the worker process started (registry field).
func (w *Worker) StartedAt() time.Time { return w.startedAt }

// Processed is the number of jobs this worker drove to a terminal state.
func (w *Worker) Processed() int64 { return w.processed.Load() }

// InFlight is the number of jobs currently executing.
func (w *Worker) InFlight() int64 { return w.inFlight.Load() }

// Handle is the broker client handler. It blocks on the semaphore (consumer
// backpressure: a full worker stops fetching), then dispatches the job to a
// goroutine and returns nil so the offset commits.
func (w *Worker) Handle(ctx context.Context, msg client.Message) error {
	if err := w.sem.Acquire(ctx, 1); err != nil {
		// Shutting down while acquiring: return the error so this batch is
		// not committed and the message is redelivered to another member.
		return err
	}
	w.inFlight.Add(1)
	if w.metrics != nil {
		w.metrics.inFlight.Inc()
	}
	go func() {
		defer w.sem.Release(1)
		defer w.inFlight.Add(-1)
		if w.metrics != nil {
			defer w.metrics.inFlight.Dec()
		}
		w.execute(msg)
	}()
	return nil
}

// execute runs one message end to end: parse, fence, handler, outcome.
func (w *Worker) execute(msg client.Message) {
	jobID, err := jobs.ParseJobMessageID(msg.Value)
	if err != nil {
		// Poison message: no job id, retrying can never parse it. DLQ the raw
		// bytes so an operator can inspect them, and move on.
		w.log.Warn("unparseable job message, sending to DLQ",
			slog.String("topic", msg.Topic), slog.Uint64("offset", msg.Offset))
		w.publishRawDLQ(msg.Key, msg.Value)
		w.countProcessed("poison")
		return
	}

	// The fence doubles as the cancel/duplicate check: only QUEUED/RETRYING
	// jobs proceed. A cancelled or already-finished job skips silently.
	fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	job, err := jobs.StartJobFence(fenceCtx, w.pool, jobID, w.id)
	fenceCancel()
	if err != nil {
		// Transient database trouble. The offset already committed, so
		// compensate: republish the message after a short delay. When the
		// database recovers, the next delivery goes through.
		w.log.Error("fence update failed, requeueing message",
			slog.String("job_id", jobID), slog.Any("error", err))
		w.scheduleRawRepublish(jobID, msg.Value, 2*time.Second)
		return
	}
	if job == nil {
		w.log.Info("job skipped by fence (cancelled, duplicate or finished)",
			slog.String("job_id", jobID))
		w.countProcessed("skipped")
		return
	}

	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, job)

	handler, err := lookupHandler(w.handlers, job.Type)
	if err != nil {
		// Unknown type: straight to DEAD, no retries.
		w.finishDead(job, job.Attempts+1, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	startedAt := time.Now()
	runErr := handler(ctx, job)
	cancel()
	finishedAt := time.Now()

	if w.metrics != nil {
		w.metrics.duration.WithLabelValues(job.Type).Observe(finishedAt.Sub(startedAt).Seconds())
	}

	switch {
	case runErr == nil:
		w.finishSuccess(job, job.Attempts+1, startedAt, finishedAt)
	case isPermanent(runErr):
		w.finishDeadWithTimes(job, job.Attempts+1, runErr.Error(), startedAt, finishedAt)
	default:
		w.finishFailed(job, job.Attempts+1, runErr, startedAt, finishedAt)
	}
}

// finishSuccess records the attempt and moves the job to SUCCESS.
func (w *Worker) finishSuccess(job *jobs.Job, attempt int, startedAt, finishedAt time.Time) {
	updated, err := jobs.FinishJobSuccess(context.Background(), w.pool,
		job.ID, attempt, w.id, startedAt, finishedAt)
	if err != nil {
		w.log.Error("could not record success",
			slog.String("job_id", job.ID), slog.Any("error", err))
		return
	}
	if updated == nil {
		return // job moved on under us; the attempt row is recorded
	}
	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, updated)
	w.processed.Add(1)
	w.countProcessed("success")
	w.log.Info("job succeeded",
		slog.String("job_id", job.ID), slog.Int("attempt", attempt))
}

// finishFailed records the attempt. When attempts remain, the job goes
// RETRYING and is republished after a backoff; otherwise it goes DEAD and a
// copy lands on jobs.dlq.
func (w *Worker) finishFailed(job *jobs.Job, attempt int, runErr error, startedAt, finishedAt time.Time) {
	dead := attempt >= job.MaxAttempts
	updated, err := jobs.FinishJobFailure(context.Background(), w.pool,
		job.ID, attempt, w.id, runErr.Error(), dead, startedAt, finishedAt)
	if err != nil {
		w.log.Error("could not record failure",
			slog.String("job_id", job.ID), slog.Any("error", err))
		return
	}
	if updated == nil {
		return
	}
	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, updated)

	if dead {
		w.sendToDLQ(updated)
		w.processed.Add(1)
		w.countProcessed("dead")
		w.log.Warn("job is DEAD",
			slog.String("job_id", job.ID), slog.Int("attempts", attempt),
			slog.Any("error", runErr))
		return
	}

	delay := retryBackoff(attempt)
	w.countProcessed("retry")
	w.log.Info("job failed, scheduling retry",
		slog.String("job_id", job.ID), slog.Int("attempt", attempt),
		slog.Duration("backoff", delay), slog.Any("error", runErr))
	w.scheduleRetryRepublish(updated, delay)
}

// finishDead sends a job straight to DEAD (permanent failures that never
// entered a handler run, e.g. unknown type).
func (w *Worker) finishDead(job *jobs.Job, attempt int, errMsg string) {
	now := time.Now()
	w.finishDeadWithTimes(job, attempt, errMsg, now, now)
}

// finishDeadWithTimes is finishDead with explicit attempt timestamps.
func (w *Worker) finishDeadWithTimes(job *jobs.Job, attempt int, errMsg string, startedAt, finishedAt time.Time) {
	updated, err := jobs.FinishJobFailure(context.Background(), w.pool,
		job.ID, attempt, w.id, errMsg, true, startedAt, finishedAt)
	if err != nil {
		w.log.Error("could not mark job dead",
			slog.String("job_id", job.ID), slog.Any("error", err))
		return
	}
	if updated == nil {
		return
	}
	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, updated)
	w.sendToDLQ(updated)
	w.processed.Add(1)
	w.countProcessed("dead")
	w.log.Warn("job is DEAD (permanent failure)",
		slog.String("job_id", job.ID), slog.String("error", errMsg))
}

// sendToDLQ copies a dead job to jobs.dlq. Best effort: the Postgres row is
// the real dead-letter record; the broker copy is for tooling and inspection.
func (w *Worker) sendToDLQ(j *jobs.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.producer.PublishJob(ctx, jobs.TopicDLQ, j); err != nil {
		w.log.Error("DLQ produce failed",
			slog.String("job_id", j.ID), slog.Any("error", err))
	}
}

// publishRawDLQ copies an unparseable message to the DLQ, best effort.
func (w *Worker) publishRawDLQ(key, value []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.producer.PublishRaw(ctx, jobs.TopicDLQ, key, value); err != nil {
		w.log.Error("DLQ produce failed for poison message", slog.Any("error", err))
	}
}

// ---------------------------------------------------------------------------
// Retry scheduler
// ---------------------------------------------------------------------------

// scheduleRetryRepublish republishes job j to the jobs topic after delay.
// The consumer loop never sleeps: a timer fires the republish later.
func (w *Worker) scheduleRetryRepublish(j *jobs.Job, delay time.Duration) {
	raw, err := j.MarshalMessage()
	if err != nil {
		w.log.Error("could not encode retry message",
			slog.String("job_id", j.ID), slog.Any("error", err))
		return
	}
	w.scheduleRawRepublish(j.ID, raw, delay)
}

// scheduleRawRepublish schedules raw for republication after delay. Timers
// are tracked per job id so Shutdown can flush them instead of stranding
// RETRYING jobs.
func (w *Worker) scheduleRawRepublish(jobID string, raw []byte, delay time.Duration) {
	t := &retryTask{raw: raw}
	t.timer = time.AfterFunc(delay, func() {
		w.timersMu.Lock()
		delete(w.timers, jobID)
		w.timersMu.Unlock()
		w.republish(jobID, raw)
	})

	w.timersMu.Lock()
	defer w.timersMu.Unlock()
	if w.closed.Load() {
		// Shutdown already started: fire now instead of scheduling.
		t.timer.Stop()
		go w.republish(jobID, raw)
		return
	}
	w.timers[jobID] = t
}

// republish puts the message back on the jobs topic.
func (w *Worker) republish(jobID string, raw []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.producer.PublishRaw(ctx, jobs.TopicJobs, []byte(jobID), raw); err != nil {
		// The job row stays RETRYING with no message in flight. Log loudly;
		// a stranded-job sweeper is a documented TODO.
		w.log.Error("retry republish failed; job stays RETRYING until requeued",
			slog.String("job_id", jobID), slog.Any("error", err))
	}
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// Shutdown drains the worker: it stops new scheduling, waits for in-flight
// jobs up to the deadline, then flushes pending retry timers by republishing
// immediately. The caller stops the consumer before calling this.
func (w *Worker) Shutdown(deadline time.Duration) {
	w.closed.Store(true)

	dead := time.After(deadline)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for w.inFlight.Load() > 0 {
		select {
		case <-dead:
			w.log.Warn("shutdown deadline hit with jobs still in flight",
				slog.Int64("in_flight", w.inFlight.Load()))
			w.flushRetries()
			return
		case <-tick.C:
		}
	}
	w.flushRetries()
}

// flushRetries stops every pending retry timer and republishes immediately,
// so a graceful restart never strands a RETRYING job.
func (w *Worker) flushRetries() {
	w.timersMu.Lock()
	pending := w.timers
	w.timers = make(map[string]*retryTask)
	w.timersMu.Unlock()
	for jobID, t := range pending {
		t.timer.Stop()
		w.republish(jobID, t.raw)
	}
	if len(pending) > 0 {
		w.log.Info("flushed pending retries at shutdown", slog.Int("count", len(pending)))
	}
}

// ---------------------------------------------------------------------------
// Debug stats
// ---------------------------------------------------------------------------

// Stats is the /debug/stats payload.
type Stats struct {
	WorkerID       string `json:"worker_id"`
	InFlight       int64  `json:"in_flight"`
	Processed      int64  `json:"processed"`
	PendingRetries int    `json:"pending_retries"`
	Concurrency    int    `json:"concurrency"`
	StartedAt      string `json:"started_at"`
	UptimeSeconds  int64  `json:"uptime_seconds"`
}

// Stats returns a snapshot of the worker's counters.
func (w *Worker) Stats() Stats {
	w.timersMu.Lock()
	pending := len(w.timers)
	w.timersMu.Unlock()
	return Stats{
		WorkerID:       w.id,
		InFlight:       w.inFlight.Load(),
		Processed:      w.processed.Load(),
		PendingRetries: pending,
		Concurrency:    w.concurrency,
		StartedAt:      w.startedAt.Format(time.RFC3339),
		UptimeSeconds:  int64(time.Since(w.startedAt).Seconds()),
	}
}

func (w *Worker) countProcessed(result string) {
	if w.metrics != nil {
		w.metrics.processed.WithLabelValues(result).Inc()
	}
}
