//go:build !windows

package benchmarks

import "time"

// Non-Windows platforms get high-resolution monotonic clocks from
// time.Since out of the box.
var hiresBase = time.Now()

// hiresNow is a monotonic nanosecond reading from an arbitrary epoch.
// Only deltas are meaningful.
func hiresNow() time.Duration { return time.Since(hiresBase) }
