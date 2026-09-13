// breaker.go implements the per-upstream circuit breaker. When an upstream
// fails too many times in a row the breaker opens and calls fail fast with
// 503 instead of piling onto a dying service. After a cool-down it lets one
// probe through; if the probe succeeds the breaker closes again.
package gateway

import (
	"sync"
	"time"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// Breaker tuning, fixed by design (see the gateway spec): five consecutive
// failures open the circuit, it stays open for 30 s, then a single half-open
// probe decides whether to close again.
const (
	breakerThreshold = 5
	breakerOpenFor   = 30 * time.Second
)

// breakerState is exported to the raven_gateway_circuit_breaker_state gauge
// as 0, 1 or 2.
type breakerState int

const (
	breakerClosed   breakerState = iota // 0 — calls flow normally
	breakerOpen                         // 1 — calls fail fast
	breakerHalfOpen                     // 2 — one probe in flight
)

func (s breakerState) String() string {
	switch s {
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// breaker guards one upstream. The zero value is not usable; build it with
// newBreaker. now is a clock injection point for tests.
type breaker struct {
	mu          sync.Mutex
	name        string
	state       breakerState
	failures    int // consecutive failures while closed
	threshold   int
	openFor     time.Duration
	openedAt    time.Time
	probeActive bool // a half-open probe is currently in flight
	now         func() time.Time
	onChange    func(upstream string, from, to breakerState)
}

// newBreaker builds a closed breaker. onChange fires on every state change
// (used for logging + the gauge); it must not call back into the breaker.
func newBreaker(name string, threshold int, openFor time.Duration, onChange func(string, breakerState, breakerState)) *breaker {
	return &breaker{
		name:      name,
		threshold: threshold,
		openFor:   openFor,
		now:       time.Now,
		onChange:  onChange,
	}
}

// setStateLocked moves the breaker and reports the change. Caller holds mu.
func (b *breaker) setStateLocked(to breakerState) {
	if b.state == to {
		return
	}
	from := b.state
	b.state = to
	if b.onChange != nil {
		b.onChange(b.name, from, to)
	}
}

// before asks permission to call the upstream. While open it rejects with a
// KindUnavailable error until openFor has passed; then it lets exactly one
// probe through in half-open state.
func (b *breaker) before() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerOpen:
		if b.now().Sub(b.openedAt) < b.openFor {
			return b.openError()
		}
		b.probeActive = true
		b.setStateLocked(breakerHalfOpen)
		return nil
	case breakerHalfOpen:
		if b.probeActive {
			return b.openError()
		}
		b.probeActive = true
		return nil
	default:
		return nil
	}
}

// after reports the outcome of a call that before allowed. Any response from
// the upstream — even an application error like NotFound — counts as success:
// the breaker only cares whether the upstream is alive, and that is judged by
// the caller (only transport-level failures are reported as failures).
func (b *breaker) after(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if success {
		b.failures = 0
		b.probeActive = false
		b.setStateLocked(breakerClosed)
		return
	}

	switch b.state {
	case breakerHalfOpen:
		// Probe failed: the upstream is still sick, stay open for another
		// full cool-down.
		b.probeActive = false
		b.failures = b.threshold
		b.openedAt = b.now()
		b.setStateLocked(breakerOpen)
	case breakerClosed:
		b.failures++
		if b.failures >= b.threshold {
			b.openedAt = b.now()
			b.setStateLocked(breakerOpen)
		}
	case breakerOpen:
		// A call that started before the breaker tripped finished now.
		// Nothing to do; the open window is already running.
	}
}

// openError is what fast-failing calls return.
func (b *breaker) openError() error {
	return errors.E(errors.KindUnavailable, "circuit_open",
		"upstream "+b.name+" is temporarily unavailable", nil)
}
