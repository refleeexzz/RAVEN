// Package audit is the platform-wide audit trail: an append-only event
// stream of who-did-what at the API edge, persisted to the audit_events
// table (migration 000006).
//
// The design constraint that shapes everything here: auditing must never
// hurt the request path. Producers call Emit, which is a non-blocking
// channel send; a single background goroutine batches rows into Postgres.
// If the buffer is full (database slow or down) events are dropped with a
// metric and a warn log — the API keeps answering. Shutdown drains whatever
// is buffered, bounded by a timeout.
//
// Reads (GET /api/audit) go through Store. The gateway owns the wiring; see
// services/gateway/audit_middleware.go and docs/audit.md.
package audit

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// Outcome is how the audited call ended. The values are the wire/database
// contract (a CHECK constraint enforces them in audit_events).
type Outcome string

const (
	OutcomeSuccess Outcome = "success" // 2xx
	OutcomeFailure Outcome = "failure" // everything else non-2xx
	OutcomeDenied  Outcome = "denied"  // 401/403 on a protected route
)

// Actor sentinels for events without an authenticated user id.
const (
	ActorAnonymous = "anonymous" // public or unauthenticated calls
	ActorSystem    = "system"    // platform-internal emitters
)

// Writer defaults. The buffer is sized so a burst of requests rides out a
// slow database without a single blocked request; past that, dropping is
// the documented trade-off (an audit trail that can take the API down is a
// bug, not a feature).
const (
	defaultBufferSize    = 4096
	defaultBatchSize     = 100
	defaultFlushInterval = 2 * time.Second
	defaultInsertTimeout = 10 * time.Second
	defaultDrainTimeout  = 10 * time.Second
)

// Event is one audit record. Field names mirror the audit_events columns.
// JSON tags exist for the read API (GET /api/audit); inserts never marshal
// the struct itself.
type Event struct {
	ID           int64          `json:"id,omitempty"`
	TS           time.Time      `json:"ts"`
	ActorID      string         `json:"actor_id"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type,omitempty"`
	ResourceID   string         `json:"resource_id,omitempty"`
	Outcome      Outcome        `json:"outcome"`
	IP           string         `json:"ip,omitempty"`
	UserAgent    string         `json:"user_agent,omitempty"`
	TraceID      string         `json:"trace_id,omitempty"`
	Detail       map[string]any `json:"detail,omitempty"`
}

// Metrics bundles the audit counters. Every field is optional (nil-safe),
// so unit tests and small binaries can pass a zero value. The gateway
// registers the real collectors as raven_audit_* in its own registry.
type Metrics struct {
	Dropped      prometheus.Counter // events lost because the buffer was full
	Written      prometheus.Counter // events successfully inserted
	InsertErrors prometheus.Counter // batch inserts that failed (rows dropped)
}

func (m Metrics) incDropped() {
	if m.Dropped != nil {
		m.Dropped.Inc()
	}
}

func (m Metrics) addWritten(n int) {
	if m.Written != nil {
		m.Written.Add(float64(n))
	}
}

func (m Metrics) incInsertErrors() {
	if m.InsertErrors != nil {
		m.InsertErrors.Inc()
	}
}

// batchSender is the slice of *pgxpool.Pool the writer needs. It exists so
// unit tests can fake Postgres (a hung database, a failing one) without
// containers.
type batchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Options tunes the writer. Zero values get the package defaults.
type Options struct {
	BufferSize    int           // channel capacity (AUDIT_BUFFER), default 4096
	BatchSize     int           // rows per insert, default 100
	FlushInterval time.Duration // max time a row waits in memory, default 2s
	InsertTimeout time.Duration // per-batch insert deadline, default 10s
	DrainTimeout  time.Duration // shutdown drain deadline, default 10s
}

func (o Options) withDefaults() Options {
	if o.BufferSize <= 0 {
		o.BufferSize = defaultBufferSize
	}
	if o.BatchSize <= 0 {
		o.BatchSize = defaultBatchSize
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = defaultFlushInterval
	}
	if o.InsertTimeout <= 0 {
		o.InsertTimeout = defaultInsertTimeout
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = defaultDrainTimeout
	}
	return o
}

// Writer is the asynchronous audit sink. Emit is safe for concurrent use
// and never blocks; Run is the single consumer goroutine.
type Writer struct {
	db   batchSender
	log  *slog.Logger
	met  Metrics
	opts Options

	ch    chan Event
	wg    sync.WaitGroup
	drops atomic.Int64 // monotonic drop counter, for sampled warn logs
}

// NewWriter builds a writer. db is typically a *pgxpool.Pool from
// internal/database.NewPool. Call Run in a goroutine; call Wait to block
// until the final drain completes.
func NewWriter(db batchSender, log *slog.Logger, met Metrics, opts Options) *Writer {
	opts = opts.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Writer{
		db:   db,
		log:  log,
		met:  met,
		opts: opts,
		ch:   make(chan Event, opts.BufferSize),
	}
}

// Emit enqueues e and returns immediately. When the buffer is full the
// event is dropped: the drop counter and metric always move, and a warn is
// logged on the first drop and then once every 1000 drops so a dead
// database cannot turn the audit trail into a log flood.
func (w *Writer) Emit(e Event) {
	if e.TS.IsZero() {
		e.TS = time.Now().UTC()
	}
	if e.ActorID == "" {
		e.ActorID = ActorAnonymous
	}
	select {
	case w.ch <- e:
	default:
		w.met.incDropped()
		if n := w.drops.Add(1); n == 1 || n%1000 == 0 {
			w.log.Warn("audit event dropped: buffer full",
				slog.Int64("dropped_total", n),
				slog.String("action", e.Action))
		}
	}
}

// Run consumes events until ctx is cancelled, then drains the buffer on a
// best-effort basis and returns. Events emitted after the drain gives up
// are left in the channel and garbage-collected — documented loss, never a
// panic or a block. Run must be called exactly once.
func (w *Writer) Run(ctx context.Context) {
	w.wg.Add(1)
	defer w.wg.Done()

	ticker := time.NewTicker(w.opts.FlushInterval)
	defer ticker.Stop()

	buf := make([]Event, 0, w.opts.BatchSize)
	for {
		select {
		case <-ctx.Done():
			w.drain(buf)
			return
		case <-ticker.C:
			buf = w.flush(buf)
		case e := <-w.ch:
			buf = append(buf, e)
			if len(buf) >= w.opts.BatchSize {
				buf = w.flush(buf)
			}
		}
	}
}

// Wait blocks until Run has returned (final drain included).
func (w *Writer) Wait() {
	w.wg.Wait()
}

// drain pulls whatever is buffered right now and flushes it under the
// drain deadline. It deliberately does not wait for stragglers: shutdown
// must stay fast even while requests are still finishing.
func (w *Writer) drain(buf []Event) {
	deadline := time.Now().Add(w.opts.DrainTimeout)
	for {
		select {
		case e := <-w.ch:
			buf = append(buf, e)
			if len(buf) >= w.opts.BatchSize {
				buf = w.flushUntil(buf, deadline)
			}
		default:
			_ = w.flushUntil(buf, deadline)
			return
		}
	}
}

// flush inserts the buffered batch and returns an empty buffer, whether or
// not the insert worked.
func (w *Writer) flush(buf []Event) []Event {
	return w.flushUntil(buf, time.Now().Add(w.opts.InsertTimeout))
}

// flushUntil inserts buf in one batch. A failed insert is logged and
// counted and the rows are dropped — auditing never retries into a sick
// database and never panics.
func (w *Writer) flushUntil(buf []Event, deadline time.Time) []Event {
	if len(buf) == 0 {
		return buf
	}

	b := &pgx.Batch{}
	for _, e := range buf {
		b.Queue(`INSERT INTO audit_events
			(ts, actor_id, action, resource_type, resource_id, outcome,
			 ip, user_agent, trace_id, detail)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			e.TS, e.ActorID, e.Action, e.ResourceType, e.ResourceID,
			string(e.Outcome), e.IP, e.UserAgent, e.TraceID, marshalDetail(e.Detail))
	}

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	err := w.db.SendBatch(ctx, b).Close()
	if err != nil {
		w.met.incInsertErrors()
		w.log.Error("audit batch insert failed, dropping events",
			slog.Int("events", len(buf)),
			slog.Any("error", err))
		return buf[:0]
	}
	w.met.addWritten(len(buf))
	return buf[:0]
}

// marshalDetail renders the detail column. nil/empty becomes '{}' (the
// column default) and anything unmarshalable degrades to '{}' too — the
// audit row matters more than its decoration.
func marshalDetail(d map[string]any) json.RawMessage {
	if len(d) == 0 {
		return json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(d)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
