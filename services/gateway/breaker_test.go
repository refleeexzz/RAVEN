package gateway

import (
	"testing"
	"time"

	"github.com/raven/platform/pkg/errors"
)

// breakerEvent records one onChange call.
type breakerEvent struct {
	from, to breakerState
}

// newTestBreaker builds a breaker with a fake clock and an event recorder.
func newTestBreaker(threshold int, openFor time.Duration) (*breaker, *fakeClock, *[]breakerEvent) {
	clock := newFakeClock()
	events := &[]breakerEvent{}
	b := newBreaker("test", threshold, openFor, func(_ string, from, to breakerState) {
		*events = append(*events, breakerEvent{from: from, to: to})
	})
	b.now = clock.Now
	return b, clock, events
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	t.Parallel()

	b, _, events := newTestBreaker(5, 30*time.Second)

	// Four failures: still closed, calls flow.
	for i := 0; i < 4; i++ {
		if err := b.before(); err != nil {
			t.Fatalf("failure %d: breaker should still be closed: %v", i+1, err)
		}
		b.after(false)
	}
	if b.state != breakerClosed {
		t.Fatalf("state after 4 failures: got %s, want closed", b.state)
	}

	// Fifth failure trips it.
	if err := b.before(); err != nil {
		t.Fatalf("before: %v", err)
	}
	b.after(false)
	if b.state != breakerOpen {
		t.Fatalf("state after 5 failures: got %s, want open", b.state)
	}

	if len(*events) != 1 || (*events)[0] != (breakerEvent{breakerClosed, breakerOpen}) {
		t.Errorf("events: got %+v, want one closed→open", *events)
	}
}

func TestBreakerRejectsWhileOpen(t *testing.T) {
	t.Parallel()

	b, clock, _ := newTestBreaker(2, 30*time.Second)

	b.before()
	b.after(false)
	b.before()
	b.after(false) // now open

	err := b.before()
	if err == nil {
		t.Fatal("open breaker must reject calls")
	}
	if got := errors.KindOf(err); got != errors.KindUnavailable {
		t.Errorf("kind: got %v, want Unavailable", got)
	}
	if got := errors.CodeOf(err); got != "circuit_open" {
		t.Errorf("code: got %q, want circuit_open", got)
	}

	// One second before the cool-down ends: still rejecting.
	clock.Advance(29 * time.Second)
	if err := b.before(); err == nil {
		t.Fatal("breaker must still reject before openFor elapses")
	}
}

func TestBreakerHalfOpenProbeThenCloseOnSuccess(t *testing.T) {
	t.Parallel()

	b, clock, events := newTestBreaker(2, 30*time.Second)

	b.before()
	b.after(false)
	b.before()
	b.after(false) // open

	clock.Advance(30 * time.Second)

	// First call after cool-down is the single probe.
	if err := b.before(); err != nil {
		t.Fatalf("probe should be allowed: %v", err)
	}
	if b.state != breakerHalfOpen {
		t.Fatalf("state: got %s, want half-open", b.state)
	}

	// While the probe is in flight, other calls are rejected.
	if err := b.before(); err == nil {
		t.Fatal("half-open breaker must allow exactly one probe")
	}

	// Probe succeeds → closed, failure counter reset.
	b.after(true)
	if b.state != breakerClosed {
		t.Fatalf("state after successful probe: got %s, want closed", b.state)
	}
	if b.failures != 0 {
		t.Errorf("failures after close: got %d, want 0", b.failures)
	}

	wantEvents := []breakerEvent{
		{breakerClosed, breakerOpen},
		{breakerOpen, breakerHalfOpen},
		{breakerHalfOpen, breakerClosed},
	}
	if len(*events) != len(wantEvents) {
		t.Fatalf("events: got %+v, want %+v", *events, wantEvents)
	}
	for i, e := range *events {
		if e != wantEvents[i] {
			t.Errorf("event %d: got %+v, want %+v", i, e, wantEvents[i])
		}
	}
}

func TestBreakerHalfOpenProbeFailureReopens(t *testing.T) {
	t.Parallel()

	b, clock, _ := newTestBreaker(2, 30*time.Second)

	b.before()
	b.after(false)
	b.before()
	b.after(false) // open

	clock.Advance(30 * time.Second)
	if err := b.before(); err != nil {
		t.Fatalf("probe should be allowed: %v", err)
	}

	// Probe fails: straight back to open for another full cool-down.
	b.after(false)
	if b.state != breakerOpen {
		t.Fatalf("state after failed probe: got %s, want open", b.state)
	}
	if err := b.before(); err == nil {
		t.Fatal("reopened breaker must reject")
	}

	// The cool-down restarted: 29 s is not enough, another 1 s is.
	clock.Advance(29 * time.Second)
	if err := b.before(); err == nil {
		t.Fatal("cool-down must restart after a failed probe")
	}
	clock.Advance(time.Second)
	if err := b.before(); err != nil {
		t.Fatalf("after the new cool-down a probe should be allowed: %v", err)
	}
}

func TestBreakerSuccessResetsConsecutiveFailures(t *testing.T) {
	t.Parallel()

	b, _, _ := newTestBreaker(5, 30*time.Second)

	for i := 0; i < 4; i++ {
		b.before()
		b.after(false)
	}
	// One success in the middle resets the streak.
	b.before()
	b.after(true)
	for i := 0; i < 4; i++ {
		b.before()
		b.after(false)
	}
	if b.state != breakerClosed {
		t.Fatalf("state: got %s, want closed (4+4 failures split by a success)", b.state)
	}
}

func TestBreakerStateString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state breakerState
		want  string
	}{
		{breakerClosed, "closed"},
		{breakerOpen, "open"},
		{breakerHalfOpen, "half-open"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("state %d: got %q, want %q", int(tt.state), got, tt.want)
		}
	}
}
