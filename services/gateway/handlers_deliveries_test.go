package gateway

import (
	"testing"
	"time"

	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
)

// deliveryToJSON must preserve the proto3-optional NULL semantics on the
// REST wire: absent optionals become JSON null, present ones (including a
// real 0) become numbers.
func TestDeliveryToJSON(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC).Unix()
	status := int32(500)
	latency := int32(0) // a real, present 0 ms

	full := deliveryToJSON(&genjobs.WebhookDelivery{
		Id: 3, JobId: "job-1", Attempt: 4, Url: "https://hook.example",
		StatusCode: &status, LatencyMs: &latency,
		ResponseSnippet: "kaput", Error: "http 500", Ts: ts,
	})
	if full.StatusCode == nil || *full.StatusCode != 500 {
		t.Errorf("status_code: got %v, want 500", full.StatusCode)
	}
	if full.LatencyMS == nil || *full.LatencyMS != 0 {
		t.Errorf("latency_ms: got %v, want present 0 (not null)", full.LatencyMS)
	}
	if full.TS != ts || full.Attempt != 4 || full.Error != "http 500" {
		t.Errorf("scalars: got %+v", full)
	}

	blocked := deliveryToJSON(&genjobs.WebhookDelivery{
		Id: 4, JobId: "job-2", Blocked: true, Error: "scheme not allowed", Ts: ts,
	})
	if blocked.StatusCode != nil || blocked.LatencyMS != nil {
		t.Errorf("blocked row must render nulls, got status=%v latency=%v",
			blocked.StatusCode, blocked.LatencyMS)
	}
}
