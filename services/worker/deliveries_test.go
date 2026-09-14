package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/pkg/metrics"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

func TestDeliveryOutcomeMapping(t *testing.T) {
	cases := []struct {
		name string
		rec  webhookDeliveryRecorder
		want string
	}{
		{"success", webhookDeliveryRecorder{dispatched: true, statusCode: 200}, deliveryOutcomeSuccess},
		{"http error", webhookDeliveryRecorder{dispatched: true, statusCode: 500, errMsg: "got status 500"}, deliveryOutcomeHTTPError},
		{"transport error", webhookDeliveryRecorder{dispatched: true, errMsg: "connection refused"}, deliveryOutcomeTransport},
		{"dns failure never dispatched", webhookDeliveryRecorder{errMsg: "resolve: no such host"}, deliveryOutcomeTransport},
		{"blocked pre-flight", webhookDeliveryRecorder{blocked: true, errMsg: "loopback"}, deliveryOutcomeBlocked},
		{"blocked wins over dispatched", webhookDeliveryRecorder{dispatched: true, blocked: true, errMsg: "redirect"}, deliveryOutcomeBlocked},
		{"invalid payload", webhookDeliveryRecorder{invalid: true, errMsg: "not an object"}, deliveryOutcomeInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rec.outcome(); got != tc.want {
				t.Errorf("outcome() = %s, want %s", got, tc.want)
			}
		})
	}
}

// runWebhookWithRecorder drives the real webhook handler with a recorder in
// the context, like execute does, and returns the filled recorder.
func runWebhookWithRecorder(t *testing.T, h Handler, payload string) *webhookDeliveryRecorder {
	t.Helper()
	ctx, rec := withDeliveryRecorder(context.Background())
	_ = h(ctx, &jobs.Job{ID: "job_t", Payload: payload})
	return rec
}

func TestWebhookHandlerRecordsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	guard := NewEgressGuard(true)
	h := WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	rec := runWebhookWithRecorder(t, h, `{"url":"`+server.URL+`"}`)

	if rec.outcome() != deliveryOutcomeSuccess {
		t.Errorf("outcome: got %s, want success (err %q)", rec.outcome(), rec.errMsg)
	}
	if rec.url != server.URL {
		t.Errorf("url: got %q", rec.url)
	}
	if !rec.dispatched || rec.statusCode != 200 {
		t.Errorf("dispatched %v status %d, want true/200", rec.dispatched, rec.statusCode)
	}
	if rec.latency <= 0 {
		t.Errorf("latency: got %v, want > 0", rec.latency)
	}
	if !strings.Contains(rec.snippet, `"ok":true`) {
		t.Errorf("snippet: got %q", rec.snippet)
	}
	if rec.blocked || rec.errMsg != "" {
		t.Errorf("blocked %v errMsg %q, want clean success", rec.blocked, rec.errMsg)
	}
}

func TestWebhookHandlerRecordsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream is down"))
	}))
	defer server.Close()

	guard := NewEgressGuard(true)
	h := WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	rec := runWebhookWithRecorder(t, h, `{"url":"`+server.URL+`"}`)

	if rec.outcome() != deliveryOutcomeHTTPError {
		t.Errorf("outcome: got %s, want http_error", rec.outcome())
	}
	if rec.statusCode != http.StatusBadGateway {
		t.Errorf("status: got %d, want 502", rec.statusCode)
	}
	if rec.snippet != "upstream is down" {
		t.Errorf("snippet: got %q", rec.snippet)
	}
	if !strings.Contains(rec.errMsg, "502") {
		t.Errorf("errMsg: got %q", rec.errMsg)
	}
}

func TestWebhookHandlerRecordsBlocked(t *testing.T) {
	// Strict guard + loopback target: a pre-flight refusal.
	guard := NewEgressGuard(false)
	h := WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	rec := runWebhookWithRecorder(t, h, `{"url":"http://127.0.0.1:1/hook"}`)

	if !rec.blocked {
		t.Errorf("blocked: got false, want true (strict guard must refuse loopback)")
	}
	if rec.outcome() != deliveryOutcomeBlocked {
		t.Errorf("outcome: got %s, want blocked", rec.outcome())
	}
	if rec.statusCode != 0 || rec.dispatched {
		t.Errorf("dispatched %v status %d: a refused target must never reach the wire",
			rec.dispatched, rec.statusCode)
	}
	if rec.errMsg == "" {
		t.Error("errMsg must carry the refusal reason")
	}
}

func TestWebhookHandlerRecordsTransportError(t *testing.T) {
	guard := NewEgressGuard(true) // allow private so the dial actually happens
	h := WebhookHandler(guard.HTTPClient(500*time.Millisecond), guard)
	// Port 1 on loopback: connection refused.
	rec := runWebhookWithRecorder(t, h, `{"url":"http://127.0.0.1:1/hook"}`)

	if rec.outcome() != deliveryOutcomeTransport {
		t.Errorf("outcome: got %s, want transport_error", rec.outcome())
	}
	if rec.statusCode != 0 {
		t.Errorf("status: got %d, want 0 (no response)", rec.statusCode)
	}
	if rec.errMsg == "" {
		t.Error("errMsg must carry the transport failure")
	}
}

func TestWebhookHandlerRecordsInvalid(t *testing.T) {
	guard := NewEgressGuard(true)
	h := WebhookHandler(guard.HTTPClient(time.Second), guard)

	rec := runWebhookWithRecorder(t, h, `{"not":"an object with url"}`)
	if rec.outcome() != deliveryOutcomeInvalid {
		t.Errorf("missing url: got %s, want invalid", rec.outcome())
	}

	rec = runWebhookWithRecorder(t, h, `[1,2,3]`)
	if rec.outcome() != deliveryOutcomeInvalid {
		t.Errorf("non-object payload: got %s, want invalid", rec.outcome())
	}
}

func TestWebhookHandlerSnippetCappedAt1KiB(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 4*1024)))
	}))
	defer server.Close()

	guard := NewEgressGuard(true)
	h := WebhookHandler(guard.HTTPClient(2*time.Second), guard)
	rec := runWebhookWithRecorder(t, h, `{"url":"`+server.URL+`"}`)

	if len(rec.snippet) != jobs.MaxDeliverySnippetBytes {
		t.Errorf("snippet length: got %d, want exactly %d (1 KiB cap)",
			len(rec.snippet), jobs.MaxDeliverySnippetBytes)
	}
}

func TestWebhookHandlerNoRecorderNoPanic(t *testing.T) {
	// The security suite drives the handler directly: no recorder in the
	// context, and the handler must behave exactly as before.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	guard := NewEgressGuard(true)
	h := WebhookHandler(guard.HTTPClient(time.Second), guard)
	if err := h(context.Background(), &jobs.Job{ID: "job_1", Payload: `{"url":"` + server.URL + `"}`}); err != nil {
		t.Fatalf("handler without recorder: %v", err)
	}
}

func TestRecordDeliveryCountsMetric(t *testing.T) {
	reg := metrics.New("worker-test")
	sm := NewServiceMetrics(reg)
	w := New(Params{Log: quietLog(), Metrics: sm}) // no pool: metric only

	rec := &webhookDeliveryRecorder{dispatched: true, statusCode: 200}
	w.recordDelivery(&jobs.Job{ID: "job_m"}, 1, rec)
	w.recordDelivery(&jobs.Job{ID: "job_m"}, 2, &webhookDeliveryRecorder{blocked: true, errMsg: "loopback"})

	resp := httptest.NewRecorder()
	reg.Handler().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(resp.Result().Body)
	text := string(body)

	if !strings.Contains(text, `raven_worker_webhook_deliveries_total{outcome="success"} 1`) {
		t.Errorf("missing success series in /metrics:\n%s", text)
	}
	if !strings.Contains(text, `raven_worker_webhook_deliveries_total{outcome="blocked"} 1`) {
		t.Errorf("missing blocked series in /metrics:\n%s", text)
	}
}
