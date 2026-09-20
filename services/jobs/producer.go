package jobs

import (
	"context"
	stderrors "errors"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// Producer wraps the broker client with job-aware helpers. Safe for
// concurrent use.
type Producer struct {
	p   *client.Producer
	log *slog.Logger

	// legacyFanout mirrors every execution publish onto the legacy "jobs"
	// topic in addition to the pinned jobs.p<priority> topic. Default on so
	// pre-priority workers keep consuming while the fleet migrates; flip off
	// (JOBS_LEGACY_TOPIC_FANOUT=false) once workers subscribe to the
	// priority topics directly.
	legacyFanout atomic.Bool
}

// NewProducer builds a Producer for the broker at addr. The legacy-topic
// fanout starts enabled: the safe compatibility default.
func NewProducer(addr string, log *slog.Logger) *Producer {
	p := &Producer{
		p:   client.NewProducer(addr, client.WithProducerLogger(log)),
		log: log,
	}
	p.legacyFanout.Store(true)
	return p
}

// NewProducerWithSecurity is NewProducer with the broker client's auth/TLS
// options applied (docs/broker-security.md). A nil sec is open mode and
// behaves exactly like NewProducer.
func NewProducerWithSecurity(addr string, log *slog.Logger, sec *BrokerSecurity) *Producer {
	opts := append([]client.ProducerOption{client.WithProducerLogger(log)}, sec.ProducerOptions()...)
	p := &Producer{
		p:   client.NewProducer(addr, opts...),
		log: log,
	}
	p.legacyFanout.Store(true)
	return p
}

// SetLegacyFanout toggles the duplicate publish onto the legacy "jobs"
// topic (see the Producer docs).
func (p *Producer) SetLegacyFanout(on bool) { p.legacyFanout.Store(on) }

// LegacyFanout reports whether the legacy-topic fanout is enabled.
func (p *Producer) LegacyFanout() bool { return p.legacyFanout.Load() }

// Close closes the underlying producer.
func (p *Producer) Close() error { return p.p.Close() }

// PublishJob marshals j and produces it to topic with the job id as key, so
// all messages of one job land on the same partition (per-job ordering).
//
// Tracing: a "job publish" span continues whatever trace ctx carries (the
// CreateJob gRPC span when the gateway called, a root otherwise), and its
// W3C context rides along in the record headers so the worker's "job
// execute" span becomes its child — the trace crosses the broker intact.
func (p *Producer) PublishJob(ctx context.Context, topic string, j *Job) error {
	raw, err := j.message()
	if err != nil {
		return err
	}
	return p.publish(ctx, topic, []byte(j.ID), raw, j.Type)
}

// PublishExecution is the standard way to put a job in front of workers: it
// routes the message to the pinned priority topic jobs.p<priority> and,
// while the legacy fanout is on, also to the legacy "jobs" topic so old
// workers keep seeing the work. The same encoded message goes to every
// topic, so the worker's claim fence simply ignores the duplicate copy.
func (p *Producer) PublishExecution(ctx context.Context, j *Job) error {
	raw, err := j.message()
	if err != nil {
		return err
	}
	for _, topic := range executionTopics(j.Priority, p.LegacyFanout()) {
		if err := p.publish(ctx, topic, []byte(j.ID), raw, j.Type); err != nil {
			return err
		}
	}
	return nil
}

// publish is the single produce path: span, inject, produce.
func (p *Producer) publish(ctx context.Context, topic string, key, raw []byte, jobType string) error {
	ctx, span := otel.Tracer("raven/jobs").Start(ctx, "job publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "raven-broker"),
			attribute.String("messaging.destination.name", topic),
			// Low-cardinality only: job type, never the payload.
			attribute.String("job.type", jobType),
		))
	defer span.End()

	headers := protocol.InjectTraceContext(ctx, nil)
	_, err := p.p.ProduceWithHeaders(ctx, topic, key, raw, headers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "broker produce failed")
	}
	return err
}

// PublishRaw produces an already-encoded value. Used for DLQ copies of
// messages we could not even parse.
func (p *Producer) PublishRaw(ctx context.Context, topic string, key, value []byte) error {
	_, err := p.p.Produce(ctx, topic, key, value)
	return err
}

// PublishRawWithHeaders is PublishRaw with record headers. The worker uses
// it for retry republishes and DLQ copies so the trace context extracted
// from the incoming message keeps flowing: a retried job stays in the
// original trace instead of starting a disconnected one.
func (p *Producer) PublishRawWithHeaders(ctx context.Context, topic string, key, value []byte, headers []protocol.Header) error {
	_, err := p.p.ProduceWithHeaders(ctx, topic, key, value, headers)
	return err
}

// EnsureTopics creates the job topics at startup: the legacy topics plus the
// pinned priority family jobs.p1..jobs.p9. Already-existing topics are
// fine (TOPIC_EXISTS is swallowed); anything else is fatal at boot.
func EnsureTopics(ctx context.Context, addr string, log *slog.Logger) error {
	return EnsureTopicsWithSecurity(ctx, addr, log, nil)
}

// EnsureTopicsWithSecurity is EnsureTopics with the broker client's auth/TLS
// options applied (docs/broker-security.md). A nil sec is open mode. Broker
// auth failures come back decorated by ExplainBrokerError, so a misconfigured
// key fails the boot fast with a readable message.
func EnsureTopicsWithSecurity(ctx context.Context, addr string, log *slog.Logger, sec *BrokerSecurity) error {
	admin := client.NewAdmin(addr, sec.AdminOptions()...)
	defer func() { _ = admin.Close() }()

	topics := []string{TopicJobs, TopicDLQ, TopicRetry}
	for prio := 1; prio <= MaxPriority; prio++ {
		topics = append(topics, TopicForPriority(prio))
	}
	for _, topic := range topics {
		_, err := admin.CreateTopic(ctx, topic, 0) // 0 = broker default partitions
		if err == nil {
			log.Info("topic created", slog.String("topic", topic))
			continue
		}
		var pe *protocol.Error
		if stderrors.As(err, &pe) && pe.Code == protocol.CodeTopicExists {
			continue
		}
		return ExplainBrokerError("ensure topics", err)
	}
	return nil
}

// BrokerChecker returns a health checker that lists topics with a short
// timeout. Good enough to know the broker answers the protocol.
func BrokerChecker(addr string) func(ctx context.Context) error {
	return BrokerCheckerWithSecurity(addr, nil)
}

// BrokerCheckerWithSecurity is BrokerChecker with the broker client's
// auth/TLS options applied. A nil sec is open mode.
func BrokerCheckerWithSecurity(addr string, sec *BrokerSecurity) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		admin := client.NewAdmin(addr, sec.AdminOptions()...)
		defer func() { _ = admin.Close() }()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := admin.ListTopics(ctx)
		return ExplainBrokerError("broker health check", err)
	}
}
