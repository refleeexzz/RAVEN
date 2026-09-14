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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/config"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

// Worker consumes the jobs topic and executes jobs with bounded concurrency.
//
// Dispatch model: the broker client invokes Handle sequentially per record;
// Handle acquires a semaphore slot and runs the job in a goroutine, so up to
// `concurrency` jobs execute at once. The offset commits when Handle returns
// (at dispatch). That means a hard kill can strand up to `concurrency`
// dispatched-but-unfinished jobs in PROCESSING/RETRYING — which is exactly
// what the lease machinery covers: every claimed job carries lease_until, the
// worker renews it while executing, and the jobs-service sweeper requeues
// whatever stops renewing (ADR 009). Generation fencing rejects a late write
// from a worker the sweeper already replaced. Graceful shutdown (the normal
// path) drains in-flight jobs and pending retries before exiting, so nothing
// strands on SIGTERM.
type Worker struct {
	id       string
	pool     *pgxpool.Pool
	rdb      redis.UniversalClient
	producer *jobs.Producer
	handlers map[string]Handler
	sem      *semaphore.Weighted
	timeout  time.Duration
	lease    time.Duration
	log      *slog.Logger
	metrics  *ServiceMetrics
	tracer   trace.Tracer

	concurrency int
	startedAt   time.Time
	inFlight    atomic.Int64
	processed   atomic.Int64 // terminal jobs (SUCCESS + DEAD)

	aliveCtx context.Context    // cancelled by HardStop: simulates SIGKILL
	kill     context.CancelFunc // test hook; never called on the graceful path

	timersMu sync.Mutex
	timers   map[string]*retryTask // pending retry republishes, by job id
	closed   atomic.Bool           // set when shutdown starts
}

// retryTask is a scheduled republish of one job message. headers carry
// the W3C trace context captured when the retry was scheduled, so the
// republished message continues the original trace instead of starting a
// disconnected one.
type retryTask struct {
	timer   *time.Timer
	raw     []byte
	headers []protocol.Header
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
	JobLease    time.Duration // claimed-job lease (WORKER_JOB_LEASE_MS, default 30s)
	WorkerID    string        // optional override (tests); empty = generated
	HTTPClient  *http.Client  // optional override (tests); nil = guarded 5s-timeout client

	// AllowPrivateWebhooks disables the webhook egress range checks
	// (WORKER_WEBHOOK_ALLOW_PRIVATE). Dev/test escape hatch: httptest servers
	// live on loopback. Never set in production — that reopens JOBS-01 (SSRF).
	AllowPrivateWebhooks bool
}

// New builds a Worker with the default handler registry.
func New(p Params) *Worker {
	if p.Concurrency <= 0 {
		p.Concurrency = 8
	}
	if p.JobTimeout <= 0 {
		p.JobTimeout = 30 * time.Second
	}
	if p.JobLease <= 0 {
		p.JobLease = 30 * time.Second
	}
	if p.Log == nil {
		p.Log = slog.Default()
	}
	// Webhook egress guard (JOBS-01). Secure by default: private/loopback/
	// link-local/reserved targets are refused. The env escape hatch keeps
	// httptest-based integration tests working without touching Params.
	guard := NewEgressGuard(p.AllowPrivateWebhooks ||
		config.GetBool("WORKER_WEBHOOK_ALLOW_PRIVATE", false))
	if guard.AllowPrivate() {
		p.Log.Warn("webhook egress guard allows private targets (dev/test mode)")
	}
	if p.HTTPClient == nil {
		p.HTTPClient = guard.HTTPClient(5 * time.Second)
	} else {
		p.HTTPClient = guard.WrapClient(p.HTTPClient)
	}
	id := p.WorkerID
	if id == "" {
		host, _ := os.Hostname()
		id = fmt.Sprintf("worker-%s-%s", host, uuid.NewString()[:6])
	}
	aliveCtx, kill := context.WithCancel(context.Background())
	return &Worker{
		id:          id,
		pool:        p.Pool,
		rdb:         p.RDB,
		producer:    p.Producer,
		handlers:    newHandlers(p.Log, p.HTTPClient, guard),
		sem:         semaphore.NewWeighted(int64(p.Concurrency)),
		timeout:     p.JobTimeout,
		lease:       p.JobLease,
		log:         p.Log,
		metrics:     p.Metrics,
		tracer:      otel.Tracer("raven/worker"),
		concurrency: p.Concurrency,
		startedAt:   time.Now().UTC(),
		aliveCtx:    aliveCtx,
		kill:        kill,
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
		w.metrics.incInFlight()
	}
	go func() {
		defer w.sem.Release(1)
		defer w.inFlight.Add(-1)
		if w.metrics != nil {
			defer w.metrics.decInFlight()
		}
		w.execute(msg)
	}()
	return nil
}

// execute runs one message end to end: parse, fence, handler, outcome.
//
// Tracing: the W3C context the producer injected into the record headers
// is extracted, and "job execute" continues that trace — Gateway → Jobs →
// Broker → Worker lands as one trace in Jaeger. Child spans wrap the
// handler run ("job handler") and the Postgres finish transaction
// ("job db commit"). Attributes stay low-cardinality (job type, status,
// topic, partition) — never payloads, emails or job ids.
func (w *Worker) execute(msg client.Message) {
	execCtx := protocol.ExtractTraceContext(context.Background(), msg.Headers)
	execCtx, execSpan := w.tracer.Start(execCtx, "job execute",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "raven-broker"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.Int("messaging.partition", int(msg.Partition)),
		))
	defer execSpan.End()

	jobID, msgGen, err := jobs.ParseJobMessage(msg.Value)
	if err != nil {
		// Poison message: no job id, retrying can never parse it. DLQ the raw
		// bytes so an operator can inspect them, and move on.
		w.log.Warn("unparseable job message, sending to DLQ",
			slog.String("topic", msg.Topic), slog.Uint64("offset", msg.Offset))
		execSpan.RecordError(err)
		execSpan.SetStatus(codes.Error, "poison message")
		execSpan.SetAttributes(attribute.String("job.status", "poison"))
		w.publishRawDLQ(execCtx, msg.Key, msg.Value)
		w.countProcessed("poison")
		return
	}

	// The fence doubles as the cancel/duplicate/stale-generation check: only
	// QUEUED/RETRYING jobs whose generation still matches the message
	// proceed, and the claim writes the lease the sweeper watches. A
	// cancelled, already-finished or already-requeued job skips silently.
	fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	job, err := jobs.StartJobFence(fenceCtx, w.pool, jobID, w.id, msgGen, w.lease)
	fenceCancel()
	if err != nil {
		// Transient database trouble. The offset already committed, so
		// compensate: republish the message after a short delay. When the
		// database recovers, the next delivery goes through.
		w.log.Error("fence update failed, requeueing message",
			slog.String("job_id", jobID), slog.Any("error", err))
		execSpan.RecordError(err)
		execSpan.SetStatus(codes.Error, "fence update failed")
		w.scheduleRawRepublish(jobID, msg.Value, 2*time.Second,
			protocol.InjectTraceContext(execCtx, nil))
		return
	}
	if job == nil {
		w.log.Info("job skipped by fence (cancelled, duplicate, finished or stale generation)",
			slog.String("job_id", jobID))
		execSpan.SetAttributes(attribute.String("job.status", "skipped"))
		w.countProcessed("skipped")
		return
	}

	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, job)

	handler, err := lookupHandler(w.handlers, job.Type)
	if err != nil {
		// Unknown type: straight to DEAD, no retries.
		execSpan.RecordError(err)
		execSpan.SetStatus(codes.Error, "unknown job type")
		execSpan.SetAttributes(attribute.String("job.status", "dead"))
		w.finishDead(execCtx, job, job.ExecutionGeneration, job.Attempts+1, err.Error())
		return
	}

	execSpan.SetAttributes(attribute.String("job.type", job.Type))
	ctx, cancel := context.WithTimeout(execCtx, w.timeout)
	handlerCtx, handlerSpan := w.tracer.Start(ctx, "job handler",
		trace.WithAttributes(
			attribute.String("job.type", job.Type),
			attribute.Int("job.attempt", job.Attempts+1),
		))
	// Webhook attempts are observed end to end: the handler fills this
	// recorder with status/latency/snippet/blocked and it is flushed to
	// webhook_deliveries below, whatever the job outcome.
	var drec *webhookDeliveryRecorder
	if job.Type == "webhook" {
		handlerCtx, drec = withDeliveryRecorder(handlerCtx)
	}
	var lostLease atomic.Bool
	renewDone := w.startLeaseRenewal(ctx, cancel, &lostLease, job.ID, job.ExecutionGeneration)

	startedAt := time.Now()
	runErr := handler(handlerCtx, job)
	cancel()
	<-renewDone
	finishedAt := time.Now()
	if runErr != nil {
		handlerSpan.RecordError(runErr)
		handlerSpan.SetStatus(codes.Error, runErr.Error())
	}
	handlerSpan.End()

	if drec != nil {
		// Best effort and outside the job state machine: even a delivery
		// whose lease was lost mid-flight really happened, so it is
		// recorded before the lostLease branch below.
		w.recordDelivery(job, job.Attempts+1, drec)
	}

	if w.metrics != nil {
		w.metrics.duration.WithLabelValues(job.Type).Observe(finishedAt.Sub(startedAt).Seconds())
	}

	if lostLease.Load() {
		// The sweeper declared this worker dead mid-execution and another
		// generation owns the job now. Do NOT write results: the finish
		// fence would reject them, and skipping the write keeps the attempt
		// history honest.
		w.log.Warn("job lease lost mid-execution, abandoning result",
			slog.String("job_id", job.ID),
			slog.Int("generation", job.ExecutionGeneration))
		execSpan.SetAttributes(attribute.String("job.status", "lease_lost"))
		w.countProcessed("lease_lost")
		return
	}

	switch {
	case runErr == nil:
		execSpan.SetAttributes(attribute.String("job.status", "success"))
		w.finishSuccess(execCtx, job, job.ExecutionGeneration, job.Attempts+1, startedAt, finishedAt)
	case isPermanent(runErr):
		execSpan.SetStatus(codes.Error, "permanent failure")
		execSpan.SetAttributes(attribute.String("job.status", "dead"))
		w.finishDeadWithTimes(execCtx, job, job.ExecutionGeneration, job.Attempts+1, runErr.Error(), startedAt, finishedAt)
	default:
		execSpan.SetStatus(codes.Error, runErr.Error())
		execSpan.SetAttributes(attribute.String("job.status", "retry"))
		w.finishFailed(execCtx, job, job.ExecutionGeneration, job.Attempts+1, runErr, startedAt, finishedAt)
	}
}

// renewInterval is how often the lease is renewed while a handler runs:
// lease/3, so two renewals can fail back to back before the lease actually
// expires. The floor keeps short test leases renewing usefully.
func renewInterval(lease time.Duration) time.Duration {
	const floor = 50 * time.Millisecond
	if d := lease / 3; d > floor {
		return d
	}
	return floor
}

// startLeaseRenewal renews the job's lease every lease/3 until the handler's
// ctx is done. Losing the lease (0 rows updated: the sweeper bumped the
// generation, or the job left PROCESSING) flips lost, cancels the handler
// ctx and stops. Transient database errors are logged and retried on the
// next tick — if they persist, the lease expires and the next successful
// renew observes the takeover. The returned channel closes when the loop
// exits, so execute never writes a result while a renewal is in flight.
func (w *Worker) startLeaseRenewal(ctx context.Context, cancel context.CancelFunc, lost *atomic.Bool, jobID string, generation int) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(renewInterval(w.lease))
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.aliveCtx.Done():
				return
			case <-t.C:
				renewCtx, renewCancel := context.WithTimeout(context.Background(), 5*time.Second)
				ok, err := jobs.RenewJobLease(renewCtx, w.pool, jobID, generation, w.lease)
				renewCancel()
				if err != nil {
					w.log.Warn("lease renewal failed, retrying on next tick",
						slog.String("job_id", jobID), slog.Any("error", err))
					continue
				}
				if !ok {
					lost.Store(true)
					w.log.Warn("job lease lost (newer generation owns the job)",
						slog.String("job_id", jobID), slog.Int("generation", generation))
					cancel()
					return
				}
			}
		}
	}()
	return done
}

// finishSuccess records the attempt and moves the job to SUCCESS. The
// Postgres transaction is wrapped in a "job db commit" span, the last hop
// of the Gateway → Jobs → Broker → Worker → Postgres trace.
func (w *Worker) finishSuccess(ctx context.Context, job *jobs.Job, generation, attempt int, startedAt, finishedAt time.Time) {
	commitCtx, commitSpan := w.tracer.Start(ctx, "job db commit",
		trace.WithAttributes(
			attribute.String("job.type", job.Type),
			attribute.String("job.status", string(jobs.StatusSuccess)),
		))
	updated, err := jobs.FinishJobSuccess(commitCtx, w.pool,
		job.ID, generation, attempt, w.id, startedAt, finishedAt)
	if err != nil {
		commitSpan.RecordError(err)
		commitSpan.SetStatus(codes.Error, "finish write failed")
		commitSpan.End()
		w.log.Error("could not record success",
			slog.String("job_id", job.ID), slog.Any("error", err))
		return
	}
	commitSpan.End()
	if updated == nil {
		w.countFenced(job, "success")
		return
	}
	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, updated)
	w.processed.Add(1)
	w.countProcessed("success")
	w.log.Info("job succeeded",
		slog.String("job_id", job.ID), slog.Int("attempt", attempt))
}

// finishFailed records the attempt. When attempts remain, the job goes
// RETRYING and is republished after a backoff; otherwise it goes DEAD and a
// copy lands on jobs.dlq. The Postgres transaction runs inside a
// "job db commit" span.
func (w *Worker) finishFailed(ctx context.Context, job *jobs.Job, generation, attempt int, runErr error, startedAt, finishedAt time.Time) {
	dead := attempt >= job.MaxAttempts
	delay := retryBackoff(attempt)
	commitStatus := string(jobs.StatusRetrying)
	if dead {
		commitStatus = string(jobs.StatusDead)
	}
	commitCtx, commitSpan := w.tracer.Start(ctx, "job db commit",
		trace.WithAttributes(
			attribute.String("job.type", job.Type),
			attribute.String("job.status", commitStatus),
		))
	// RETRYING keeps a lease covering the backoff plus a full lease window:
	// if this worker dies before its retry timer fires, the sweeper takes
	// over once that window passes.
	updated, err := jobs.FinishJobFailure(commitCtx, w.pool,
		job.ID, generation, attempt, w.id, runErr.Error(), dead, startedAt, finishedAt,
		delay+w.lease)
	if err != nil {
		commitSpan.RecordError(err)
		commitSpan.SetStatus(codes.Error, "finish write failed")
		commitSpan.End()
		w.log.Error("could not record failure",
			slog.String("job_id", job.ID), slog.Any("error", err))
		return
	}
	commitSpan.End()
	if updated == nil {
		w.countFenced(job, "failure")
		return
	}
	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, updated)

	if dead {
		w.sendToDLQ(ctx, updated)
		w.processed.Add(1)
		w.countProcessed("dead")
		w.log.Warn("job is DEAD",
			slog.String("job_id", job.ID), slog.Int("attempts", attempt),
			slog.Any("error", runErr))
		return
	}

	w.countProcessed("retry")
	if w.metrics != nil {
		w.metrics.retries.Inc()
	}
	w.log.Info("job failed, scheduling retry",
		slog.String("job_id", job.ID), slog.Int("attempt", attempt),
		slog.Duration("backoff", delay), slog.Any("error", runErr))
	w.scheduleRetryRepublish(ctx, updated, delay)
}

// finishDead sends a job straight to DEAD (permanent failures that never
// entered a handler run, e.g. unknown type).
func (w *Worker) finishDead(ctx context.Context, job *jobs.Job, generation, attempt int, errMsg string) {
	now := time.Now()
	w.finishDeadWithTimes(ctx, job, generation, attempt, errMsg, now, now)
}

// finishDeadWithTimes is finishDead with explicit attempt timestamps.
func (w *Worker) finishDeadWithTimes(ctx context.Context, job *jobs.Job, generation, attempt int, errMsg string, startedAt, finishedAt time.Time) {
	commitCtx, commitSpan := w.tracer.Start(ctx, "job db commit",
		trace.WithAttributes(
			attribute.String("job.type", job.Type),
			attribute.String("job.status", string(jobs.StatusDead)),
		))
	updated, err := jobs.FinishJobFailure(commitCtx, w.pool,
		job.ID, generation, attempt, w.id, errMsg, true, startedAt, finishedAt, 0)
	if err != nil {
		commitSpan.RecordError(err)
		commitSpan.SetStatus(codes.Error, "finish write failed")
		commitSpan.End()
		w.log.Error("could not mark job dead",
			slog.String("job_id", job.ID), slog.Any("error", err))
		return
	}
	commitSpan.End()
	if updated == nil {
		w.countFenced(job, "dead")
		return
	}
	jobs.PublishJobEvent(context.Background(), w.rdb, w.log, updated)
	w.sendToDLQ(ctx, updated)
	w.processed.Add(1)
	w.countProcessed("dead")
	w.log.Warn("job is DEAD (permanent failure)",
		slog.String("job_id", job.ID), slog.String("error", errMsg))
}

// countFenced handles a finish write the generation fence rejected: a newer
// generation owns the job now (the sweeper declared this worker dead and
// requeued it). The write is a no-op by design — the other generation's
// outcome is the truth — so the handler flow treats it as success.
func (w *Worker) countFenced(job *jobs.Job, op string) {
	w.log.Warn("finish write rejected by fence: newer generation owns the job",
		slog.String("job_id", job.ID), slog.String("op", op),
		slog.Int("generation", job.ExecutionGeneration))
	if w.metrics != nil {
		w.metrics.fencedWrites.WithLabelValues(op).Inc()
	}
	w.countProcessed("fenced")
}

// sendToDLQ copies a dead job to jobs.dlq. Best effort: the Postgres row is
// the real dead-letter record; the broker copy is for tooling and inspection.
// The DLQ message re-uses the job message so PublishJob's span + header
// injection keep the dead letter inside the original trace.
func (w *Worker) sendToDLQ(ctx context.Context, j *jobs.Job) {
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.producer.PublishJob(publishCtx, jobs.TopicDLQ, j); err != nil {
		w.log.Error("DLQ produce failed",
			slog.String("job_id", j.ID), slog.Any("error", err))
	}
}

// publishRawDLQ copies an unparseable message to the DLQ, best effort. The
// incoming trace context is forwarded in the headers so even poison messages
// stay linked to the trace that produced them.
func (w *Worker) publishRawDLQ(ctx context.Context, key, value []byte) {
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	headers := protocol.InjectTraceContext(ctx, nil)
	if err := w.producer.PublishRawWithHeaders(publishCtx, jobs.TopicDLQ, key, value, headers); err != nil {
		w.log.Error("DLQ produce failed for poison message", slog.Any("error", err))
	}
}

// ---------------------------------------------------------------------------
// Retry scheduler
// ---------------------------------------------------------------------------

// scheduleRetryRepublish republishes job j to the jobs topic after delay.
// The consumer loop never sleeps: a timer fires the republish later. The
// trace context of the current "job execute" span is injected now and
// stored with the task, so the delayed message still carries it.
func (w *Worker) scheduleRetryRepublish(ctx context.Context, j *jobs.Job, delay time.Duration) {
	raw, err := j.MarshalMessage()
	if err != nil {
		w.log.Error("could not encode retry message",
			slog.String("job_id", j.ID), slog.Any("error", err))
		return
	}
	w.scheduleRawRepublish(j.ID, raw, delay, protocol.InjectTraceContext(ctx, nil))
}

// scheduleRawRepublish schedules raw for republication after delay. Timers
// are tracked per job id so Shutdown can flush them instead of stranding
// RETRYING jobs. A hard-stopped worker (SIGKILL simulation) drops them —
// the sweeper recovers the job once its lease expires.
func (w *Worker) scheduleRawRepublish(jobID string, raw []byte, delay time.Duration, headers []protocol.Header) {
	t := &retryTask{raw: raw, headers: headers}
	t.timer = time.AfterFunc(delay, func() {
		w.timersMu.Lock()
		delete(w.timers, jobID)
		w.timersMu.Unlock()
		w.republish(jobID, t.raw, t.headers)
	})

	w.timersMu.Lock()
	defer w.timersMu.Unlock()
	if w.aliveCtx.Err() != nil {
		// Hard-stopped: a dead process has no timers. Drop, do not schedule.
		t.timer.Stop()
		return
	}
	if w.closed.Load() {
		// Shutdown already started: fire now instead of scheduling.
		t.timer.Stop()
		go w.republish(jobID, t.raw, t.headers)
		return
	}
	w.timers[jobID] = t
}

// republish puts the message back on the jobs topic. The stored headers
// (with the original trace context) go with it, so the next delivery's
// "job execute" span joins the same trace.
func (w *Worker) republish(jobID string, raw []byte, headers []protocol.Header) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.producer.PublishRawWithHeaders(ctx, jobs.TopicJobs, []byte(jobID), raw, headers); err != nil {
		// The job row stays RETRYING with no message in flight. Log loudly;
		// the sweeper picks it up once the lease written at finish expires.
		w.log.Error("retry republish failed; job stays RETRYING until the sweeper recovers it",
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
		w.republish(jobID, t.raw, t.headers)
	}
	if len(pending) > 0 {
		w.log.Info("flushed pending retries at shutdown", slog.Int("count", len(pending)))
	}
}

// HardStop simulates a SIGKILL for chaos tests. Lease renewals stop (the
// lease starts ticking towards expiry), pending retry timers are dropped
// without republishing, and in-flight handlers are left wherever they are —
// nothing is drained, flushed or fenced off politely. If a handler
// eventually returns, its finish write still has to pass the generation
// fence, which is exactly what the sweeper's generation bump rejects.
//
// Test hook only: production shutdown is Shutdown. The caller must also stop
// the consumer itself; HardStop does not touch broker state.
func (w *Worker) HardStop() {
	w.kill()
	w.timersMu.Lock()
	pending := w.timers
	w.timers = make(map[string]*retryTask)
	w.timersMu.Unlock()
	for _, t := range pending {
		t.timer.Stop()
	}
	w.closed.Store(true)
	w.log.Warn("worker hard-stopped (SIGKILL simulation)",
		slog.Int("dropped_retries", len(pending)),
		slog.Int64("in_flight", w.inFlight.Load()))
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
