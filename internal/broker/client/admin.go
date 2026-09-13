package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/raven/platform/internal/broker/protocol"
)

// Admin wraps topic management (create/list). Most services only need
// Producer and Consumer; Admin is for setup code and tooling.
type Admin struct {
	t *transport
}

// NewAdmin creates an admin client (lazy dial).
func NewAdmin(addr string) *Admin {
	return &Admin{t: newTransport(addr, 5*time.Second, nil)}
}

// CreateTopic creates a topic. partitions <= 0 uses the broker default
// (3). Returns a *protocol.Error with code TOPIC_EXISTS when present.
func (a *Admin) CreateTopic(ctx context.Context, topic string, partitions int32) (*protocol.CreateTopicResponse, error) {
	payload, _ := json.Marshal(protocol.CreateTopicRequest{Topic: topic, Partitions: partitions})
	frame, err := a.t.call(ctx, protocol.OpCreateTopic, payload)
	if err != nil {
		return nil, err
	}
	if err := checkError(frame); err != nil {
		return nil, err
	}
	var resp protocol.CreateTopicResponse
	if err := json.Unmarshal(frame.Payload, &resp); err != nil {
		return nil, fmt.Errorf("client: decode create-topic response: %w", err)
	}
	return &resp, nil
}

// ListTopics returns every topic with per-partition high-water marks.
func (a *Admin) ListTopics(ctx context.Context) (*protocol.ListTopicsResponse, error) {
	frame, err := a.t.call(ctx, protocol.OpListTopics, nil)
	if err != nil {
		return nil, err
	}
	if err := checkError(frame); err != nil {
		return nil, err
	}
	var resp protocol.ListTopicsResponse
	if err := json.Unmarshal(frame.Payload, &resp); err != nil {
		return nil, fmt.Errorf("client: decode list-topics response: %w", err)
	}
	return &resp, nil
}

// Close closes the connection.
func (a *Admin) Close() error { return a.t.Close() }
