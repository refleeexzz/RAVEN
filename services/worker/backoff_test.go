package worker

import (
	"testing"
	"time"
)

// The exact schedule is a contract with operators reading logs; pin it.
func TestRetryBackoffSchedule(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 250 * time.Millisecond},
		{3, 500 * time.Millisecond},
		{4, 1 * time.Second},
		{5, 2 * time.Second},
		{6, 4 * time.Second},
		{7, 5 * time.Second},  // capped
		{8, 5 * time.Second},  // stays capped
		{50, 5 * time.Second}, // far past the cap, no shift overflow
		{0, 100 * time.Millisecond},
		{-3, 100 * time.Millisecond},
	}
	for _, c := range cases {
		if got := retryBackoff(c.failures); got != c.want {
			t.Errorf("retryBackoff(%d) = %v, want %v", c.failures, got, c.want)
		}
	}
}

func TestRetryBackoffMonotonicUntilCap(t *testing.T) {
	prev := time.Duration(0)
	for n := 1; n <= 7; n++ {
		d := retryBackoff(n)
		if d < prev {
			t.Fatalf("backoff shrank at %d: %v < %v", n, d, prev)
		}
		if d > retryCap {
			t.Fatalf("backoff over cap at %d: %v", n, d)
		}
		prev = d
	}
}
