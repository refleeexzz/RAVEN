package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/refleeexzz/RAVEN/services/jobs"
)

// Webhook delivery observability.
//
// Every webhook attempt leaves one row in webhook_deliveries (migration
// 000005): success, HTTP error, transport error and egress-guard refusal
// (JOBS-01) alike. The worker writes straight to Postgres — it already owns
// a pool for the lease machinery, and a direct insert keeps execution
// decoupled from jobs-service availability. The write is best effort: a
// failed insert is logged and the job flow goes on untouched.
//
// Plumbing: execute puts a webhookDeliveryRecorder in the handler context,
// the webhook handler fills it with the attempt facts (it is the only place
// that sees the response), and execute flushes it once the handler returns.
// The handler signature stays untouched, so the security suite driving
// WebhookHandler directly simply finds no recorder and records nothing.

// Outcomes of raven_worker_webhook_deliveries_total{outcome}.
const (
	deliveryOutcomeSuccess   = "success"         // 2xx response
	deliveryOutcomeHTTPError = "http_error"      // response came back, non-2xx
	deliveryOutcomeTransport = "transport_error" // no response (dial, DNS, timeout, ctx)
	deliveryOutcomeBlocked   = "blocked"         // egress guard refused the target
	deliveryOutcomeInvalid   = "invalid"         // never dispatched (bad payload/URL)
)

// deliveryCtxKey is the context key carrying the attempt recorder.
type deliveryCtxKey struct{}

// webhookDeliveryRecorder collects the facts of one webhook attempt. Written
// only by the webhook handler while the attempt runs, read by execute after
// the handler returns — single-goroutine at a time, no locking needed.
type webhookDeliveryRecorder struct {
	url        string
	dispatched bool // an HTTP request actually left the worker
	statusCode int
	latency    time.Duration
	snippet    string // response body prefix, capped at 1 KiB by the handler
	blocked    bool   // egress-guard refusal
	invalid    bool   // failed before dispatch (bad payload, bad URL)
	errMsg     string
}

// withDeliveryRecorder returns a context carrying a fresh recorder.
func withDeliveryRecorder(ctx context.Context) (context.Context, *webhookDeliveryRecorder) {
	rec := &webhookDeliveryRecorder{}
	return context.WithValue(ctx, deliveryCtxKey{}, rec), rec
}

// recorderFrom extracts the recorder, nil when the caller is not observing
// deliveries (unit tests, the security suite).
func recorderFrom(ctx context.Context) *webhookDeliveryRecorder {
	rec, _ := ctx.Value(deliveryCtxKey{}).(*webhookDeliveryRecorder)
	return rec
}

// outcome maps the recorded facts to the metric label value.
func (r *webhookDeliveryRecorder) outcome() string {
	switch {
	case r.blocked:
		return deliveryOutcomeBlocked
	case r.invalid:
		return deliveryOutcomeInvalid
	case !r.dispatched:
		// Never reached the wire and not a policy refusal: DNS failures and
		// request-build-time context cancellations land here.
		return deliveryOutcomeTransport
	case r.errMsg != "" && r.statusCode == 0:
		return deliveryOutcomeTransport
	case r.statusCode >= 200 && r.statusCode < 300 && r.errMsg == "":
		return deliveryOutcomeSuccess
	default:
		return deliveryOutcomeHTTPError
	}
}

// recordDelivery flushes the recorder: one insert into webhook_deliveries
// plus one tick of raven_worker_webhook_deliveries_total{outcome}. Best
// effort — the insert must never block or fail the job, so it runs on a
// fresh context (the handler's is already cancelled) with a short timeout
// and errors only reach the log.
func (w *Worker) recordDelivery(job *jobs.Job, attempt int, rec *webhookDeliveryRecorder) {
	outcome := rec.outcome()
	if w.metrics != nil {
		w.metrics.deliveries.WithLabelValues(outcome).Inc()
	}
	if w.pool == nil {
		// Unit tests without a database: the metric above is the observable
		// part; there is nowhere to insert into.
		return
	}

	d := &jobs.WebhookDelivery{
		JobID:           job.ID,
		Attempt:         attempt,
		URL:             rec.url,
		ResponseSnippet: rec.snippet,
		Blocked:         rec.blocked,
		Error:           rec.errMsg,
	}
	if rec.dispatched && rec.statusCode > 0 {
		code := int32(rec.statusCode)
		d.StatusCode = &code
		lat := int32(rec.latency.Milliseconds())
		d.LatencyMS = &lat
	}

	insertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := jobs.InsertWebhookDelivery(insertCtx, w.pool, d); err != nil {
		w.log.Warn("could not record webhook delivery (job flow unaffected)",
			slog.String("job_id", job.ID), slog.Int("attempt", attempt),
			slog.String("outcome", outcome), slog.Any("error", err))
	}
}
