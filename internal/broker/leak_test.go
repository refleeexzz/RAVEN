package broker_test

import (
	"bytes"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// checkLeaks snapshots the goroutine count and returns a verification
// func: it polls until the count returns to baseline or the timeout
// expires, then dumps full stacks on failure. go.uber.org/goleak is not
// in go.mod and go.mod is frozen for this sprint, so this is the local
// equivalent: same idea (snapshot, settle, diff), stdlib only.
func checkLeaks(t *testing.T, timeout time.Duration) func() {
	t.Helper()
	baseline := runtime.NumGoroutine()
	return func() {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if runtime.NumGoroutine() <= baseline {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
		t.Errorf("goroutine leak: baseline %d, now %d\n%s",
			baseline, runtime.NumGoroutine(), buf.String())
	}
}
