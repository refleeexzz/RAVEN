package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// Producer produces records to the broker. Safe for concurrent use.
type Producer struct {
	t *transport

	tlsCfg              *tls.Config
	authID, authSecret string

	batch      bool
	flushEvery time.Duration
	flushCount int
	batchCh    chan batchMsg
	batchDone  chan struct{}
	closeOnce  sync.Once
	log        *slog.Logger
}

type batchMsg struct {
	topic string
	msg   protocol.Message
	resp  chan produceResult
}

type produceResult struct {
	partition int32
	offset    uint64
	err       error
}

// ProducerOption customizes a Producer.
type ProducerOption func(*Producer)

// WithBatching enables micro-batching: Produce calls are grouped into
// one PRODUCE request per topic every flushEvery or every flushCount
// messages, whichever comes first. Higher throughput, slightly higher
// latency. Defaults suggested: 5ms / 64.
func WithBatching(flushEvery time.Duration, flushCount int) ProducerOption {
	return func(p *Producer) {
		p.batch = true
		p.flushEvery = flushEvery
		p.flushCount = flushCount
	}
}

// WithProducerLogger sets a logger (default: slog.Default).
func WithProducerLogger(log *slog.Logger) ProducerOption {
	return func(p *Producer) { p.log = log }
}

// WithProducerTLS upgrades the broker connection to TLS (cfg is cloned
// per dial; ServerName defaults to the dial host). Pass a config
// trusting the broker's CA; use WithProducerAuth as well when the
// broker also requires API-key authentication.
func WithProducerTLS(cfg *tls.Config) ProducerOption {
	return func(p *Producer) { p.tlsCfg = cfg }
}

// WithProducerAuth sends an AUTH frame (API key id + plaintext secret)
// right after every connect, before any PRODUCE. Against an open-mode
// broker the AUTH is a no-op, so the same client config works in dev
// and prod. Pair with WithProducerTLS on real networks: the secret
// travels inside the frame payload.
func WithProducerAuth(id, secret string) ProducerOption {
	return func(p *Producer) {
		p.authID = id
		p.authSecret = secret
	}
}

// NewProducer creates a producer. It dials lazily on the first Produce
// and reconnects automatically with exponential backoff.
func NewProducer(addr string, opts ...ProducerOption) *Producer {
	p := &Producer{
		t:     newTransport(addr, 5*time.Second, nil),
		log:   slog.Default(),
		batch: false,
	}
	for _, o := range opts {
		o(p)
	}
	p.t.log = p.log
	p.t.tlsCfg = p.tlsCfg
	p.t.authID = p.authID
	p.t.authSecret = p.authSecret
	if p.batch {
		p.batchCh = make(chan batchMsg, p.flushCount*4)
		p.batchDone = make(chan struct{})
		go p.batchLoop()
	}
	return p
}

// Produce appends one record. A key selects the partition by hash
// (same key → same partition → per-key order); an empty key goes
// round-robin. It returns the assigned offset.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) (uint64, error) {
	_, off, err := p.produce(ctx, topic, protocol.Message{Key: key, Value: value})
	return off, err
}

// ProduceMessage is Produce with headers.
func (p *Producer) ProduceMessage(ctx context.Context, topic string, m protocol.Message) (uint64, error) {
	_, off, err := p.produce(ctx, topic, m)
	return off, err
}

// ProduceWithHeaders is Produce with record headers (e.g. W3C
// traceparent for trace propagation). Backward-compatible convenience
// over ProduceMessage.
func (p *Producer) ProduceWithHeaders(ctx context.Context, topic string, key, value []byte, headers []protocol.Header) (uint64, error) {
	return p.ProduceMessage(ctx, topic, protocol.Message{Key: key, Value: value, Headers: headers})
}

func (p *Producer) produce(ctx context.Context, topic string, m protocol.Message) (int32, uint64, error) {
	if p.batch {
		msg := batchMsg{topic: topic, msg: m, resp: make(chan produceResult, 1)}
		select {
		case p.batchCh <- msg:
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
		select {
		case res := <-msg.resp:
			return res.partition, res.offset, res.err
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
	resp, err := p.sendBatch(ctx, topic, []protocol.Message{m})
	if err != nil {
		return 0, 0, err
	}
	return resp.Results[0].Partition, resp.Results[0].Offset, nil
}

// sendBatch ships one produce request and returns the decoded response.
func (p *Producer) sendBatch(ctx context.Context, topic string, records []protocol.Message) (*protocol.ProduceResponse, error) {
	payload := protocol.EncodeProduceRequest(&protocol.ProduceRequest{
		Topic:     topic,
		Partition: -1, // broker picks per record (hash key / round-robin)
		Records:   records,
	})
	frame, err := p.t.call(ctx, protocol.OpProduce, payload)
	if err != nil {
		return nil, err
	}
	if err := checkError(frame); err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeProduceResponse(frame.Payload)
	if err != nil {
		return nil, fmt.Errorf("client: decode produce response: %w", err)
	}
	if len(resp.Results) != len(records) {
		return nil, fmt.Errorf("client: produce response has %d results for %d records", len(resp.Results), len(records))
	}
	return resp, nil
}

// batchLoop collects messages and flushes per topic. It exits on
// Close, flushing everything pending first.
func (p *Producer) batchLoop() {
	defer close(p.batchDone)
	ticker := time.NewTicker(p.flushEvery)
	defer ticker.Stop()
	pending := make(map[string][]batchMsg)
	count := 0
	flush := func() {
		for topic, msgs := range pending {
			p.flushTopic(topic, msgs)
		}
		pending = make(map[string][]batchMsg)
		count = 0
	}
	for {
		select {
		case m, ok := <-p.batchCh:
			if !ok {
				flush()
				return
			}
			pending[m.topic] = append(pending[m.topic], m)
			count++
			if count >= p.flushCount {
				flush()
			}
		case <-ticker.C:
			if count > 0 {
				flush()
			}
		}
	}
}

// flushTopic produces one topic batch and delivers results to every
// waiting Produce caller.
func (p *Producer) flushTopic(topic string, msgs []batchMsg) {
	records := make([]protocol.Message, len(msgs))
	for i, m := range msgs {
		records[i] = m.msg
	}
	// The batch ctx is generous: callers hold their own ctx, results
	// land in buffered channels, so a slow broker never blocks Close.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	resp, err := p.sendBatch(ctx, topic, records)
	cancel()
	if err != nil {
		for _, m := range msgs {
			m.resp <- produceResult{err: err}
		}
		return
	}
	for i, m := range msgs {
		m.resp <- produceResult{partition: resp.Results[i].Partition, offset: resp.Results[i].Offset}
	}
}

// Close flushes pending batches and closes the connection.
func (p *Producer) Close() error {
	p.closeOnce.Do(func() {
		if p.batch {
			close(p.batchCh)
			<-p.batchDone
		}
	})
	return p.t.Close()
}
