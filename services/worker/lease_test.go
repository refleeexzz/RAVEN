package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/services/jobs"
)

// renewInterval drives the lease heartbeat cadence: lease/3, so two renewals
// can fail back to back before the lease actually expires.
func TestRenewInterval(t *testing.T) {
	cases := []struct {
		lease time.Duration
		want  time.Duration
	}{
		{30 * time.Second, 10 * time.Second},
		{3 * time.Second, 1 * time.Second},
		{300 * time.Millisecond, 100 * time.Millisecond},
		{150 * time.Millisecond, 50 * time.Millisecond}, // exactly at the floor
		{100 * time.Millisecond, 50 * time.Millisecond}, // floored
		{10 * time.Millisecond, 50 * time.Millisecond},  // floored
	}
	for _, c := range cases {
		if got := renewInterval(c.lease); got != c.want {
			t.Errorf("renewInterval(%v) = %v, want %v", c.lease, got, c.want)
		}
	}
}

// HardStop is the SIGKILL simulation: pending retry timers die with the
// process instead of firing later, and anything scheduled afterwards is
// dropped (a dead worker does not republish). The nil producer makes any
// accidental republish panic, which would fail the test.
func TestHardStopDropsPendingRetries(t *testing.T) {
	w := New(Params{WorkerID: "worker-test", Log: quietLog()})

	raw := []byte(`{"id":"job_x","execution_generation":1}`)
	w.scheduleRawRepublish("job_x", raw, jobs.TopicJobs, time.Hour, nil)
	if got := w.Stats().PendingRetries; got != 1 {
		t.Fatalf("pending retries before stop: got %d, want 1", got)
	}

	w.HardStop()
	if got := w.Stats().PendingRetries; got != 0 {
		t.Errorf("pending retries after hard stop: got %d, want 0 (dropped, not flushed)", got)
	}

	// Scheduling after the stop is a drop, not a timer: give a would-be
	// timer plenty of time to fire (it must not).
	w.scheduleRawRepublish("job_y", raw, jobs.TopicJobs, time.Millisecond, nil)
	time.Sleep(100 * time.Millisecond)
	if got := w.Stats().PendingRetries; got != 0 {
		t.Errorf("post-stop schedule stored a timer: got %d, want 0", got)
	}
}

// HardStop must stop lease renewals too — otherwise a "dead" worker would
// keep its leases alive and the sweeper could never take over. A 30s lease
// means no renewal tick fires during the test, so the loop exits only via
// the kill signal.
func TestHardStopStopsLeaseRenewal(t *testing.T) {
	w := New(Params{WorkerID: "worker-test", JobLease: 30 * time.Second, Log: quietLog()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var lost atomic.Bool
	done := w.startLeaseRenewal(ctx, cancel, &lost, "job_x", 1)

	w.HardStop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal loop did not stop after HardStop")
	}
	if lost.Load() {
		t.Error("lease must not be reported lost when the worker itself died")
	}
}

// The normal path: the handler finished and its ctx was cancelled, so the
// renewal loop must exit promptly without flagging the lease as lost.
func TestLeaseRenewalStopsWithHandlerContext(t *testing.T) {
	w := New(Params{WorkerID: "worker-test", JobLease: 30 * time.Second, Log: quietLog()})

	ctx, cancel := context.WithCancel(context.Background())
	var lost atomic.Bool
	done := w.startLeaseRenewal(ctx, cancel, &lost, "job_x", 1)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal loop did not stop when the handler context ended")
	}
	if lost.Load() {
		t.Error("a clean handler end must not flag the lease as lost")
	}
}
