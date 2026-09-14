package jobs

import (
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// ValidateScheduledAt is the delayed-job input guard. The table pins the
// documented rules: 0 = now, negative = invalid, distant past = invalid,
// inside the one-minute tolerance = now, future = scheduled.
func TestValidateScheduledAt(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name     string
		unix     int64
		wantNil  bool   // normalized to "run now"
		wantErr  string // machine code
		wantTime time.Time
	}{
		{name: "zero means run now", unix: 0, wantNil: true},
		{name: "negative rejected", unix: -5, wantErr: "scheduled_at_invalid"},
		{name: "distant past rejected", unix: now.Add(-2 * time.Minute).Unix(),
			wantErr: "scheduled_at_in_past"},
		{name: "epoch is a distant past", unix: 1, wantErr: "scheduled_at_in_past"},
		{name: "inside past tolerance runs now", unix: now.Add(-30 * time.Second).Unix(), wantNil: true},
		{name: "exactly now runs now", unix: now.Unix(), wantNil: true},
		{name: "past tolerance boundary runs now", unix: now.Add(-scheduledAtPastTolerance).Unix(), wantNil: true},
		{name: "near future is scheduled", unix: now.Add(time.Minute).Unix(),
			wantTime: now.Add(time.Minute)},
		{name: "far future is scheduled", unix: now.Add(365 * 24 * time.Hour).Unix(),
			wantTime: now.Add(365 * 24 * time.Hour)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ValidateScheduledAt(c.unix, now)
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("want error %s, got nil", c.wantErr)
				}
				if code := errors.CodeOf(err); code != c.wantErr {
					t.Fatalf("error code: got %s, want %s", code, c.wantErr)
				}
				if errors.KindOf(err) != errors.KindInvalid {
					t.Fatalf("kind: got %v, want KindInvalid", errors.KindOf(err))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.wantNil {
				if got != nil {
					t.Errorf("got %v, want nil (run now)", got)
				}
				return
			}
			if got == nil || !got.Equal(c.wantTime) {
				t.Errorf("got %v, want %v", got, c.wantTime)
			}
			if got != nil && got.Location() != time.UTC {
				t.Errorf("location: got %v, want UTC", got.Location())
			}
		})
	}
}

// SCHEDULED joins the state machine: the dispatcher releases it to QUEUED,
// a user can cancel it, and nothing else may touch it.
func TestScheduledTransitions(t *testing.T) {
	for _, to := range []Status{StatusQueued, StatusCancelled} {
		if !legalTransition(StatusScheduled, to) {
			t.Errorf("legalTransition(SCHEDULED, %s) = false, want true", to)
		}
	}
	for _, to := range []Status{StatusProcessing, StatusSuccess, StatusFailed,
		StatusRetrying, StatusDead, StatusScheduled} {
		if legalTransition(StatusScheduled, to) {
			t.Errorf("legalTransition(SCHEDULED, %s) = true, want false", to)
		}
	}
	// A scheduled job must not be worker-startable and is not terminal.
	if startable(StatusScheduled) {
		t.Error("startable(SCHEDULED) = true, want false (only the dispatcher releases it)")
	}
	if terminal(StatusScheduled) {
		t.Error("terminal(SCHEDULED) = true, want false")
	}
	if !cancellable(StatusScheduled) {
		t.Error("cancellable(SCHEDULED) = false, want true (cancel drops the schedule)")
	}
}

// SCHEDULED must round-trip through the proto enum like every other status.
func TestScheduledProtoMapping(t *testing.T) {
	p, ok := statusToProto[StatusScheduled]
	if !ok {
		t.Fatal("statusToProto has no SCHEDULED entry")
	}
	back, ok := protoToStatus[p]
	if !ok || back != StatusScheduled {
		t.Errorf("round trip failed: %v -> %v", p, back)
	}
}

// toProto carries the scheduling fields: scheduled_at as unix seconds,
// replayed_from as the source id; both zero-ish when unset.
func TestToProtoSchedulingFields(t *testing.T) {
	at := time.Date(2026, 3, 10, 15, 0, 0, 0, time.UTC)
	src := "job_source123"
	j := &Job{
		ID: "job_s", Type: "webhook", Payload: `{}`, Status: StatusScheduled,
		Priority: 5, MaxAttempts: 4, CreatedAt: time.Now(),
		ScheduledAt: &at, ReplayedFrom: &src,
	}
	p := j.toProto()
	if p.GetScheduledAt() != at.Unix() {
		t.Errorf("scheduled_at: got %d, want %d", p.GetScheduledAt(), at.Unix())
	}
	if p.GetReplayedFrom() != src {
		t.Errorf("replayed_from: got %q, want %q", p.GetReplayedFrom(), src)
	}

	plain := &Job{ID: "job_p", Type: "webhook", Payload: `{}`,
		Status: StatusQueued, Priority: 5, MaxAttempts: 4, CreatedAt: time.Now()}
	pp := plain.toProto()
	if pp.GetScheduledAt() != 0 || pp.GetReplayedFrom() != "" {
		t.Errorf("unset scheduling fields: got scheduled_at %d replayed_from %q, want zeroes",
			pp.GetScheduledAt(), pp.GetReplayedFrom())
	}
}

// The legacy status filter must accept the new SCHEDULED filter value over
// gRPC: protoToStatus drives ListJobs' filter validation.
func TestScheduledStatusFilterMaps(t *testing.T) {
	st, ok := protoToStatus[8] // JOB_STATUS_SCHEDULED
	if !ok || st != StatusScheduled {
		t.Errorf("protoToStatus[8]: got (%v, %v), want (SCHEDULED, true)", st, ok)
	}
}
