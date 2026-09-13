package worker

import "time"

// Retry backoff schedule. After failure number n (1-based) we wait
// retryBackoff(n) before republishing the job:
//
//	100ms, 250ms, 500ms, 1s, 2s, 4s, then capped at 5s.
//
// The cap keeps a job that keeps failing from disappearing into a huge delay
// while still giving downstreams room to recover. Pure function: the unit
// tests pin the exact schedule.
var retrySchedule = []time.Duration{
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
}

const retryCap = 5 * time.Second

// retryBackoff returns the delay after the n-th failed attempt (n >= 1).
func retryBackoff(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	if failures <= len(retrySchedule) {
		return retrySchedule[failures-1]
	}
	if failures >= 7 {
		return retryCap // 1s<<3 would pass the cap; stop shifting early
	}
	return time.Second << (failures - 4) // 5 -> 2s, 6 -> 4s
}
