package jobs

import (
	"strings"
	"testing"

	gencommon "github.com/raven/platform/internal/gen/common"
	genjobs "github.com/raven/platform/internal/gen/jobs"
	"github.com/raven/platform/pkg/errors"
)

// The state machine guards are the pure form of the SQL fences. These tables
// pin them down; if one changes, the SQL and this test must change together.

func TestStartable(t *testing.T) {
	cases := []struct {
		status Status
		want   bool
	}{
		{StatusQueued, true},
		{StatusRetrying, true},
		{StatusProcessing, false},
		{StatusSuccess, false},
		{StatusFailed, false},
		{StatusCancelled, false},
		{StatusDead, false},
	}
	for _, c := range cases {
		if got := startable(c.status); got != c.want {
			t.Errorf("startable(%s) = %v, want %v", c.status, got, c.want)
		}
	}
}

func TestCancellable(t *testing.T) {
	cases := []struct {
		status Status
		want   bool
	}{
		{StatusQueued, true},
		{StatusRetrying, true},
		{StatusProcessing, false}, // too late: a worker owns it
		{StatusSuccess, false},
		{StatusFailed, false},
		{StatusCancelled, false},
		{StatusDead, false},
	}
	for _, c := range cases {
		if got := cancellable(c.status); got != c.want {
			t.Errorf("cancellable(%s) = %v, want %v", c.status, got, c.want)
		}
	}
}

func TestRequeueable(t *testing.T) {
	for _, s := range []Status{StatusQueued, StatusProcessing, StatusSuccess,
		StatusFailed, StatusRetrying, StatusCancelled} {
		if requeueable(s) {
			t.Errorf("requeueable(%s) = true, want false (only DEAD)", s)
		}
	}
	if !requeueable(StatusDead) {
		t.Error("requeueable(DEAD) = false, want true")
	}
}

func TestTerminal(t *testing.T) {
	cases := []struct {
		status Status
		want   bool
	}{
		{StatusSuccess, true},
		{StatusFailed, true},
		{StatusCancelled, true},
		{StatusDead, true},
		{StatusQueued, false},
		{StatusProcessing, false},
		{StatusRetrying, false},
	}
	for _, c := range cases {
		if got := terminal(c.status); got != c.want {
			t.Errorf("terminal(%s) = %v, want %v", c.status, got, c.want)
		}
	}
}

func TestLegalTransitions(t *testing.T) {
	type transition struct {
		from, to Status
	}
	legal := []transition{
		{StatusQueued, StatusProcessing},
		{StatusQueued, StatusCancelled},
		{StatusQueued, StatusFailed}, // broker down at create time
		{StatusProcessing, StatusSuccess},
		{StatusProcessing, StatusRetrying},
		{StatusProcessing, StatusDead},
		{StatusRetrying, StatusProcessing},
		{StatusRetrying, StatusCancelled},
		{StatusDead, StatusQueued}, // RequeueJob
	}
	for _, tr := range legal {
		if !legalTransition(tr.from, tr.to) {
			t.Errorf("legalTransition(%s, %s) = false, want true", tr.from, tr.to)
		}
	}

	illegal := []transition{
		{StatusQueued, StatusSuccess},   // must run first
		{StatusQueued, StatusDead},      // must attempt first
		{StatusProcessing, StatusQueued}, // no direct requeue while running
		{StatusSuccess, StatusQueued},
		{StatusCancelled, StatusQueued},
		{StatusDead, StatusProcessing}, // must pass through QUEUED
		{StatusRetrying, StatusSuccess},
		{StatusFailed, StatusQueued}, // FAILED is terminal here
	}
	for _, tr := range illegal {
		if legalTransition(tr.from, tr.to) {
			t.Errorf("legalTransition(%s, %s) = true, want false", tr.from, tr.to)
		}
	}
}

func TestValidateCreate(t *testing.T) {
	cases := []struct {
		name        string
		jobType     string
		payload     string
		priority    int32
		maxAttempts int32
		wantErr     string // expected machine code; "" means success
		wantPrio    int
		wantMax     int
	}{
		{name: "valid with defaults", jobType: "send_email", payload: `{"to":"a@b.c"}`,
			wantPrio: 5, wantMax: 4},
		{name: "valid explicit", jobType: "webhook", payload: `{"url":"http://x"}`, priority: 9, maxAttempts: 2,
			wantPrio: 9, wantMax: 2},
		{name: "unknown type", jobType: "mine_crypto", payload: `{}`, wantErr: "job_type_unknown"},
		{name: "empty payload", jobType: "webhook", payload: "", wantErr: "payload_required"},
		{name: "payload not json", jobType: "webhook", payload: "{nope", wantErr: "payload_invalid_json"},
		{name: "payload scalar json ok", jobType: "webhook", payload: `"just-a-string"`, wantPrio: 5, wantMax: 4},
		{name: "priority too low", jobType: "webhook", payload: `{}`, priority: -1, wantErr: "priority_out_of_range"},
		{name: "priority too high", jobType: "webhook", payload: `{}`, priority: 11, wantErr: "priority_out_of_range"},
		{name: "max attempts negative", jobType: "webhook", payload: `{}`, maxAttempts: -2, wantErr: "max_attempts_out_of_range"},
		{name: "max attempts too high", jobType: "webhook", payload: `{}`, maxAttempts: 100, wantErr: "max_attempts_out_of_range"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prio, maxA, err := validateCreate(c.jobType, c.payload, c.priority, c.maxAttempts)
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("want error %s, got nil", c.wantErr)
				}
				if got := errors.CodeOf(err); got != c.wantErr {
					t.Fatalf("error code: got %s, want %s", got, c.wantErr)
				}
				if errors.KindOf(err) != errors.KindInvalid {
					t.Fatalf("kind: got %v, want KindInvalid", errors.KindOf(err))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if prio != c.wantPrio || maxA != c.wantMax {
				t.Errorf("got (prio %d, max %d), want (%d, %d)", prio, maxA, c.wantPrio, c.wantMax)
			}
		})
	}
}

func TestNormalizeIdempotencyKey(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "empty means no idempotency", in: "", want: ""},
		{name: "whitespace means no idempotency", in: "   ", want: ""},
		{name: "plain key", in: "order-123", want: "order-123"},
		{name: "trimmed", in: "  order-123  ", want: "order-123"},
		{name: "max length", in: strings.Repeat("k", 255), want: strings.Repeat("k", 255)},
		{name: "too long", in: strings.Repeat("k", 256), wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := normalizeIdempotencyKey(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				if got := errors.CodeOf(err); got != "idempotency_key_too_long" {
					t.Fatalf("error code: got %s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestNormalizePage(t *testing.T) {
	cases := []struct {
		name             string
		in               *genjobs.ListJobsRequest
		wantPage, wantSz int32
	}{
		{name: "nil page", in: &genjobs.ListJobsRequest{}, wantPage: 1, wantSz: 20},
		{name: "explicit", in: listReq(3, 50), wantPage: 3, wantSz: 50},
		{name: "size capped at 100", in: listReq(1, 500), wantPage: 1, wantSz: 100},
		{name: "zero values default", in: listReq(0, 0), wantPage: 1, wantSz: 20},
		{name: "negative page defaults", in: listReq(-2, 10), wantPage: 1, wantSz: 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			page, size := normalizePage(c.in.GetPage())
			if page != c.wantPage || size != c.wantSz {
				t.Errorf("got (page %d, size %d), want (%d, %d)", page, size, c.wantPage, c.wantSz)
			}
		})
	}
}

func listReq(page, size int32) *genjobs.ListJobsRequest {
	return &genjobs.ListJobsRequest{Page: &gencommon.PageRequest{Page: page, PageSize: size}}
}

func TestStatusProtoRoundTrip(t *testing.T) {
	// Every status must map to a proto enum and back.
	for s, p := range statusToProto {
		back, ok := protoToStatus[p]
		if !ok || back != s {
			t.Errorf("round trip failed for %s", s)
		}
	}
	if len(statusToProto) != 7 {
		t.Errorf("statusToProto covers %d statuses, want 7", len(statusToProto))
	}
	if _, ok := statusToProto[StatusDead]; !ok {
		t.Error("DEAD must map to JOB_STATUS_DEAD")
	}
}

func TestParseJobMessageID(t *testing.T) {
	if _, err := ParseJobMessageID([]byte("not json")); err == nil {
		t.Error("want error for garbage")
	}
	if _, err := ParseJobMessageID([]byte(`{"type":"webhook"}`)); err == nil {
		t.Error("want error for missing id")
	}
	id, err := ParseJobMessageID([]byte(`{"id":"job_abc","type":"webhook"}`))
	if err != nil || id != "job_abc" {
		t.Errorf("got (%q, %v), want (job_abc, nil)", id, err)
	}
}

func TestJobMessageRoundTrip(t *testing.T) {
	j := &Job{
		ID: "job_x", Type: "send_email", Payload: `{"to":"a@b.c"}`,
		Status: StatusQueued, Priority: 5, MaxAttempts: 4,
	}
	raw, err := j.MarshalMessage()
	if err != nil {
		t.Fatal(err)
	}
	id, err := ParseJobMessageID(raw)
	if err != nil || id != j.ID {
		t.Fatalf("round trip: got (%q, %v)", id, err)
	}
	// The payload must survive as raw JSON, not a stringified blob.
	if !strings.Contains(string(raw), `"payload":{"to":"a@b.c"}`) {
		t.Errorf("payload not embedded as raw JSON: %s", raw)
	}
}
