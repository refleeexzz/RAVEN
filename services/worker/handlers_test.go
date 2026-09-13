package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/services/jobs"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestHandlerRegistry(t *testing.T) {
	reg := newHandlers(quietLog(), &http.Client{Timeout: time.Second})

	for _, typ := range []string{"send_email", "resize_image", "webhook"} {
		if _, err := lookupHandler(reg, typ); err != nil {
			t.Errorf("lookupHandler(%s): %v", typ, err)
		}
	}

	// Unknown type: error must be permanent so the job goes DEAD without
	// burning attempts.
	_, err := lookupHandler(reg, "mine_crypto")
	if err == nil {
		t.Fatal("want error for unknown type")
	}
	if !isPermanent(err) {
		t.Errorf("unknown type error must be permanent, got %v", err)
	}
}

func TestIsPermanent(t *testing.T) {
	if !isPermanent(permanentf("nope")) {
		t.Error("PermanentError must be permanent")
	}
	// Wrapped permanent errors still count.
	if !isPermanent(errors.Join(errors.New("ctx"), permanentf("nope"))) {
		t.Error("wrapped PermanentError must be permanent")
	}
	if isPermanent(errors.New("boom")) {
		t.Error("plain error must be retryable")
	}
}

func TestSendEmailHandler(t *testing.T) {
	h := sendEmailHandler(quietLog())

	// Happy path: sleeps 200-800ms, then nil.
	start := time.Now()
	err := h(context.Background(), &jobs.Job{ID: "job_1", Payload: `{"to":"a@b.c","subject":"hi"}`})
	if err != nil {
		t.Fatalf("send_email: %v", err)
	}
	if d := time.Since(start); d < 200*time.Millisecond {
		t.Errorf("send_email returned too fast: %v (want >= 200ms of fake SMTP latency)", d)
	}

	// Bad payloads are permanent.
	if err := h(context.Background(), &jobs.Job{ID: "job_2", Payload: `{"subject":"hi"}`}); !isPermanent(err) {
		t.Errorf("missing 'to' must be permanent, got %v", err)
	}
	if err := h(context.Background(), &jobs.Job{ID: "job_3", Payload: `[1,2]`}); !isPermanent(err) {
		t.Errorf("non-object payload must be permanent, got %v", err)
	}

	// Cancelled context interrupts the sleep and is retryable.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h(ctx, &jobs.Job{ID: "job_4", Payload: `{"to":"a@b.c"}`}); err == nil || isPermanent(err) {
		t.Errorf("ctx cancel must be a retryable error, got %v", err)
	}
}

func TestResizeImageHandler(t *testing.T) {
	h := resizeImageHandler()

	start := time.Now()
	if err := h(context.Background(), &jobs.Job{ID: "job_1", Payload: `{"image_id":"x","width":64}`}); err != nil {
		t.Fatalf("resize_image: %v", err)
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Errorf("resize_image returned too fast: %v (want ~300ms of CPU work)", d)
	}

	if err := h(context.Background(), &jobs.Job{ID: "job_2", Payload: `[1]`}); !isPermanent(err) {
		t.Errorf("non-object payload must be permanent, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := h(ctx, &jobs.Job{ID: "job_3", Payload: `{"image_id":"x"}`}); err == nil {
		t.Error("ctx cancel during CPU work must return an error")
	}
}

func TestWebhookHandler(t *testing.T) {
	var calls atomic.Int64
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		gotBody, _ = io.ReadAll(r.Body)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type: got %q", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &http.Client{Timeout: time.Second}
	h := webhookHandler(client)

	job := &jobs.Job{ID: "job_1", Payload: `{"url":"` + server.URL + `","event":"test"}`}
	if err := h(context.Background(), job); err != nil {
		t.Fatalf("webhook 200: %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls: got %d, want 1", calls.Load())
	}
	if !strings.Contains(string(gotBody), `"event":"test"`) {
		t.Errorf("body: got %s", gotBody)
	}

	// Non-2xx: retryable.
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer fail.Close()
	err := h(context.Background(), &jobs.Job{ID: "job_2", Payload: `{"url":"` + fail.URL + `"}`})
	if err == nil || isPermanent(err) {
		t.Errorf("500 must be a retryable error, got %v", err)
	}

	// Missing URL: permanent.
	if err := h(context.Background(), &jobs.Job{ID: "job_3", Payload: `{}`}); !isPermanent(err) {
		t.Errorf("missing url must be permanent, got %v", err)
	}
	// Garbage URL: permanent.
	if err := h(context.Background(), &jobs.Job{ID: "job_4", Payload: `{"url":"://bad"}`}); !isPermanent(err) {
		t.Errorf("bad url must be permanent, got %v", err)
	}
	// Unreachable host: retryable (network may recover).
	err = h(context.Background(), &jobs.Job{ID: "job_5", Payload: `{"url":"http://127.0.0.1:1/nope"}`})
	if err == nil || isPermanent(err) {
		t.Errorf("connection refused must be retryable, got %v", err)
	}
}

func TestWebhookRespectsClientTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	h := webhookHandler(&http.Client{Timeout: 100 * time.Millisecond})
	start := time.Now()
	err := h(context.Background(), &jobs.Job{ID: "job_1", Payload: `{"url":"` + slow.URL + `"}`})
	if err == nil {
		t.Fatal("want timeout error")
	}
	if isPermanent(err) {
		t.Errorf("timeout must be retryable, got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("timeout took too long: %v", d)
	}
}
