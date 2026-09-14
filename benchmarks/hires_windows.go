//go:build windows

package benchmarks

import (
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// On this Windows build time.Now() ticks at the ~0.5 ms interrupt
// quantum, which turns per-message latency percentiles into 0s and
// 1ms cliffs. QueryPerformanceCounter ticks at 10 MHz (100 ns), so the
// produce path (~tens of µs) is measurable. Monotonic by definition.
var (
	qpcProc = windows.NewLazySystemDLL("kernel32.dll")
	qpc     = qpcProc.NewProc("QueryPerformanceCounter")
	qpf     = qpcProc.NewProc("QueryPerformanceFrequency")
	qpfFreq = func() int64 {
		var f int64
		_, _, _ = qpf.Call(uintptr(unsafe.Pointer(&f)))
		if f <= 0 {
			f = 10_000_000 // safe default: 10 MHz is universal on modern Windows
		}
		return f
	}()
)

// hiresNow is a monotonic nanosecond reading from an arbitrary epoch.
// Only deltas are meaningful.
func hiresNow() time.Duration {
	var c int64
	_, _, _ = qpc.Call(uintptr(unsafe.Pointer(&c)))
	return time.Duration(c * 1_000_000_000 / qpfFreq)
}
