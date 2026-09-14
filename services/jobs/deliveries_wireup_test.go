package jobs

import (
	"testing"
	"time"
)

// deliveryToProto is the only new mapping in the wire-up; pin it, above all
// the NULL pass-through (the blocked/transport rows carry NULL status and
// latency, and that must survive onto the wire as absent optionals).
func TestDeliveryToProto(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	status := int32(200)
	latency := int32(42)

	full := deliveryToProto(&WebhookDelivery{
		ID: 7, JobID: "job-1", Attempt: 2, URL: "https://hook.example/x",
		StatusCode: &status, LatencyMS: &latency,
		ResponseSnippet: "ok", Blocked: false, Error: "", Ts: ts,
	})
	if full.GetId() != 7 || full.GetJobId() != "job-1" || full.GetAttempt() != 2 {
		t.Errorf("scalars: got %+v", full)
	}
	if full.StatusCode == nil || *full.StatusCode != 200 {
		t.Errorf("status_code: got %v, want 200 present", full.StatusCode)
	}
	if full.LatencyMs == nil || *full.LatencyMs != 42 {
		t.Errorf("latency_ms: got %v, want 42 present", full.LatencyMs)
	}
	if full.GetTs() != ts.Unix() {
		t.Errorf("ts: got %d, want %d", full.GetTs(), ts.Unix())
	}

	// NULL columns stay absent — a real 0 must not be invented here.
	blocked := deliveryToProto(&WebhookDelivery{
		ID: 8, JobID: "job-2", Attempt: 1, URL: "ftp://nope",
		Blocked: true, Error: "scheme not allowed", Ts: ts,
	})
	if blocked.StatusCode != nil || blocked.LatencyMs != nil {
		t.Errorf("blocked row must carry absent optionals, got status=%v latency=%v",
			blocked.StatusCode, blocked.LatencyMs)
	}
	if !blocked.GetBlocked() || blocked.GetError() == "" {
		t.Errorf("blocked row lost its flags: %+v", blocked)
	}

	// A genuine 0 ms round trip stays a present 0 (loopback webhooks exist).
	zero := int32(0)
	withZero := deliveryToProto(&WebhookDelivery{ID: 9, JobID: "j", LatencyMS: &zero, Ts: ts})
	if withZero.LatencyMs == nil || *withZero.LatencyMs != 0 {
		t.Errorf("real 0 ms latency must survive as present 0, got %v", withZero.LatencyMs)
	}
}
