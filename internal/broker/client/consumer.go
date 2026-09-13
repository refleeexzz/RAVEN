package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// Message is one consumed record.
type Message struct {
	Topic     string
	Partition int32
	Offset    uint64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []protocol.Header
}

// Handler processes one message. A nil return commits (the batch's)
// offset; a non-nil return commits nothing, so the batch is redelivered
// after the next fetch, rejoin or restart — at-least-once delivery.
type Handler func(ctx context.Context, msg Message) error

// Consumer is a consumer-group client. Run manages join, heartbeats,
// fetches, commits and rebalances internally.
type Consumer struct {
	t       *transport
	group   string
	topics  []string
	handler Handler

	memberID       string
	heartbeatEvery time.Duration
	pollInterval   time.Duration
	retryBackoff   time.Duration
	maxRecords     int
	maxBytes       int
	log            *slog.Logger
}

// ConsumerOption customizes a Consumer.
type ConsumerOption func(*Consumer)

// WithConsumerLogger sets the logger.
func WithConsumerLogger(log *slog.Logger) ConsumerOption {
	return func(c *Consumer) { c.log = log }
}

// WithFetchLimits sets per-fetch batch bounds (defaults 100 records / 1 MiB).
func WithFetchLimits(maxRecords, maxBytes int) ConsumerOption {
	return func(c *Consumer) {
		c.maxRecords = maxRecords
		c.maxBytes = maxBytes
	}
}

// WithPollInterval sets the idle poll sleep (default 200ms).
func WithPollInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithRetryBackoff sets the sleep after a handler error (default 500ms).
func WithRetryBackoff(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.retryBackoff = d }
}

// WithMemberID overrides the generated member id (useful in tests).
func WithMemberID(id string) ConsumerOption {
	return func(c *Consumer) { c.memberID = id }
}

// WithHeartbeatEvery sets the heartbeat cadence (default 3s). Must stay
// well below the broker's session timeout (default 10s).
func WithHeartbeatEvery(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.heartbeatEvery = d }
}

// NewConsumer builds a consumer for group over topics. handler runs per
// message; see Handler for the commit contract.
func NewConsumer(addr, group string, topics []string, handler Handler, opts ...ConsumerOption) *Consumer {
	host, _ := os.Hostname()
	c := &Consumer{
		t:              newTransport(addr, 5*time.Second, nil),
		group:          group,
		topics:         topics,
		handler:        handler,
		memberID:       fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8]),
		heartbeatEvery: 3 * time.Second,
		pollInterval:   200 * time.Millisecond,
		retryBackoff:   500 * time.Millisecond,
		maxRecords:     100,
		maxBytes:       1 << 20,
		log:            slog.Default(),
	}
	for _, o := range opts {
		o(c)
	}
	c.t.log = c.log
	return c
}

// topicPartition identifies one assigned partition.
type topicPartition struct {
	topic     string
	partition int32
}

// Run consumes until ctx is cancelled. It re-joins on every rebalance
// and returns nil on a clean stop. Before returning it leaves the group
// so the broker rebalances immediately instead of waiting for the
// session timeout.
func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		leaveCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = c.leave(leaveCtx)
		cancel()
		_ = c.t.Close()
	}()
	for {
		if ctx.Err() != nil {
			return nil
		}
		gen, assignments, err := c.join(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Topic missing or broker down: wait and retry. Topics are
			// often created after consumers boot.
			c.log.Debug("join failed, retrying", slog.Any("err", err))
			if !sleepCtx(ctx, c.retryBackoff) {
				return nil
			}
			continue
		}
		c.runGeneration(ctx, gen, assignments)
		// Generation ended: rebalance → loop and re-join, or ctx done.
	}
}

// join performs JOIN_GROUP and returns the generation and assignment.
func (c *Consumer) join(ctx context.Context) (int32, []protocol.Assignment, error) {
	payload, _ := json.Marshal(protocol.JoinGroupRequest{Group: c.group, MemberID: c.memberID, Topics: c.topics})
	frame, err := c.t.call(ctx, protocol.OpJoinGroup, payload)
	if err != nil {
		return 0, nil, err
	}
	if err := checkError(frame); err != nil {
		return 0, nil, err
	}
	var resp protocol.JoinGroupResponse
	if err := json.Unmarshal(frame.Payload, &resp); err != nil {
		return 0, nil, fmt.Errorf("client: decode join response: %w", err)
	}
	c.log.Info("joined group",
		slog.String("group", c.group),
		slog.String("member", c.memberID),
		slog.Int("generation", int(resp.Generation)))
	return resp.Generation, resp.Assignments, nil
}

func (c *Consumer) leave(ctx context.Context) error {
	payload, _ := json.Marshal(protocol.LeaveGroupRequest{Group: c.group, MemberID: c.memberID})
	frame, err := c.t.call(ctx, protocol.OpLeaveGroup, payload)
	if err != nil {
		return err
	}
	return checkError(frame)
}

// runGeneration fetches and processes until the generation ends
// (rebalance) or ctx is cancelled. Heartbeats run in a goroutine that
// cancels genCtx when the broker reports REBALANCE; it always exits
// before runGeneration returns.
func (c *Consumer) runGeneration(ctx context.Context, gen int32, assignments []protocol.Assignment) {
	genCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		c.heartbeatLoop(genCtx, gen, cancel)
	}()
	defer func() { cancel(); <-hbDone }()

	// Starting positions: committed offsets (0 when never committed).
	pos := make(map[topicPartition]uint64)
	for _, a := range assignments {
		for _, p := range a.Partitions {
			tp := topicPartition{a.Topic, p}
			off, err := c.fetchOffset(genCtx, tp)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.log.Debug("fetch offset failed", slog.Any("err", err))
				return // re-join and retry
			}
			pos[tp] = off
		}
	}

	for genCtx.Err() == nil {
		work := false
		failed := false
		for tp, off := range pos {
			resp, err := c.fetch(genCtx, tp, off, gen)
			if err != nil {
				if isRebalance(err) {
					return // re-join
				}
				if genCtx.Err() != nil {
					return
				}
				// Connection trouble: the transport reconnects lazily;
				// keep this generation and retry.
				c.log.Debug("fetch failed", slog.Any("err", err))
				failed = true
				continue
			}
			if len(resp.Records) == 0 {
				continue
			}
			work = true
			allOK := true
			for i := range resp.Records {
				rec := &resp.Records[i]
				msg := Message{
					Topic:     tp.topic,
					Partition: tp.partition,
					Offset:    rec.Offset,
					Timestamp: time.UnixMilli(rec.Timestamp),
					Key:       rec.Key,
					Value:     rec.Value,
					Headers:   rec.Headers,
				}
				if err := c.handler(genCtx, msg); err != nil {
					c.log.Warn("handler error, batch not committed (will redeliver)",
						slog.String("topic", tp.topic),
						slog.Int("partition", int(tp.partition)),
						slog.Uint64("offset", rec.Offset),
						slog.Any("err", err))
					allOK = false
					break
				}
			}
			if !allOK {
				failed = true
				continue
			}
			next := resp.Records[len(resp.Records)-1].Offset + 1
			if err := c.commit(genCtx, tp, gen, next); err != nil {
				if isRebalance(err) {
					return // re-join
				}
				if genCtx.Err() != nil {
					return
				}
				// Commit lost: records will be redelivered. Safe under
				// at-least-once.
				c.log.Debug("commit failed", slog.Any("err", err))
				continue
			}
			pos[tp] = next
		}
		switch {
		case !work:
			if !sleepCtx(genCtx, c.pollInterval) {
				return
			}
		case failed:
			if !sleepCtx(genCtx, c.retryBackoff) {
				return
			}
		}
	}
}

// heartbeatLoop sends heartbeats until the generation ends. A REBALANCE
// answer cancels the generation so the consumer re-joins immediately.
func (c *Consumer) heartbeatLoop(ctx context.Context, gen int32, cancel context.CancelFunc) {
	t := time.NewTicker(c.heartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		payload, _ := json.Marshal(protocol.HeartbeatRequest{Group: c.group, MemberID: c.memberID, Generation: gen})
		frame, err := c.t.call(ctx, protocol.OpHeartbeat, payload)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Debug("heartbeat failed", slog.Any("err", err))
			continue // transient; the broker reaps us if it persists
		}
		if err := checkError(frame); err != nil {
			if isRebalance(err) {
				cancel() // generation is stale: re-join
				return
			}
			c.log.Debug("heartbeat rejected", slog.Any("err", err))
		}
	}
}

func (c *Consumer) fetch(ctx context.Context, tp topicPartition, offset uint64, gen int32) (*protocol.FetchResponse, error) {
	payload := protocol.EncodeFetchRequest(&protocol.FetchRequest{
		Topic:      tp.topic,
		Partition:  tp.partition,
		Offset:     offset,
		MaxRecords: uint32(c.maxRecords),
		MaxBytes:   uint32(c.maxBytes),
		Group:      c.group,
		MemberID:   c.memberID,
		Generation: gen,
	})
	frame, err := c.t.call(ctx, protocol.OpFetch, payload)
	if err != nil {
		return nil, err
	}
	if err := checkError(frame); err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeFetchResponse(frame.Payload)
	if err != nil {
		return nil, fmt.Errorf("client: decode fetch response: %w", err)
	}
	return resp, nil
}

func (c *Consumer) fetchOffset(ctx context.Context, tp topicPartition) (uint64, error) {
	payload, _ := json.Marshal(protocol.FetchOffsetRequest{Group: c.group, Topic: tp.topic, Partition: tp.partition})
	frame, err := c.t.call(ctx, protocol.OpFetchOffset, payload)
	if err != nil {
		return 0, err
	}
	if err := checkError(frame); err != nil {
		return 0, err
	}
	var resp protocol.FetchOffsetResponse
	if err := json.Unmarshal(frame.Payload, &resp); err != nil {
		return 0, fmt.Errorf("client: decode fetch-offset response: %w", err)
	}
	return resp.Offset, nil
}

func (c *Consumer) commit(ctx context.Context, tp topicPartition, gen int32, offset uint64) error {
	payload, _ := json.Marshal(protocol.CommitOffsetRequest{
		Group: c.group, MemberID: c.memberID,
		Topic: tp.topic, Partition: tp.partition,
		Offset: offset, Generation: gen,
	})
	frame, err := c.t.call(ctx, protocol.OpCommitOffset, payload)
	if err != nil {
		return err
	}
	return checkError(frame)
}

// isRebalance reports whether err means "re-join the group".
func isRebalance(err error) bool {
	pe, ok := err.(*protocol.Error)
	return ok && (pe.Code == protocol.CodeRebalance || pe.Code == protocol.CodeUnknownMember)
}

// sleepCtx sleeps d or until ctx is done. Returns false when ctx ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
