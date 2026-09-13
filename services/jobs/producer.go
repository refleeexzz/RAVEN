package jobs

import (
	"context"
	stderrors "errors"
	"log/slog"
	"time"

	"github.com/raven/platform/internal/broker/client"
	"github.com/raven/platform/internal/broker/protocol"
)

// Producer wraps the broker client with job-aware helpers. Safe for
// concurrent use.
type Producer struct {
	p   *client.Producer
	log *slog.Logger
}

// NewProducer builds a Producer for the broker at addr.
func NewProducer(addr string, log *slog.Logger) *Producer {
	return &Producer{
		p:   client.NewProducer(addr, client.WithProducerLogger(log)),
		log: log,
	}
}

// Close closes the underlying producer.
func (p *Producer) Close() error { return p.p.Close() }

// PublishJob marshals j and produces it to topic with the job id as key, so
// all messages of one job land on the same partition (per-job ordering).
func (p *Producer) PublishJob(ctx context.Context, topic string, j *Job) error {
	raw, err := j.message()
	if err != nil {
		return err
	}
	_, err = p.p.Produce(ctx, topic, []byte(j.ID), raw)
	return err
}

// PublishRaw produces an already-encoded value. Used for DLQ copies of
// messages we could not even parse.
func (p *Producer) PublishRaw(ctx context.Context, topic string, key, value []byte) error {
	_, err := p.p.Produce(ctx, topic, key, value)
	return err
}

// EnsureTopics creates the job topics at startup. Already-existing topics are
// fine (TOPIC_EXISTS is swallowed); anything else is fatal at boot.
func EnsureTopics(ctx context.Context, addr string, log *slog.Logger) error {
	admin := client.NewAdmin(addr)
	defer admin.Close()

	for _, topic := range []string{TopicJobs, TopicDLQ, TopicRetry} {
		_, err := admin.CreateTopic(ctx, topic, 0) // 0 = broker default partitions
		if err == nil {
			log.Info("topic created", slog.String("topic", topic))
			continue
		}
		var pe *protocol.Error
		if stderrors.As(err, &pe) && pe.Code == protocol.CodeTopicExists {
			continue
		}
		return err
	}
	return nil
}

// BrokerChecker returns a health checker that lists topics with a short
// timeout. Good enough to know the broker answers the protocol.
func BrokerChecker(addr string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		admin := client.NewAdmin(addr)
		defer admin.Close()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := admin.ListTopics(ctx)
		return err
	}
}
