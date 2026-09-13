package gateway

import (
	"sync"
	"testing"
	"time"

	"github.com/raven/platform/pkg/metrics"
)

// fakeClock is a manual clock for the limiter and breaker tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// testMetrics builds real collectors on a throwaway registry.
func testMetrics(t *testing.T) *serviceMetrics {
	t.Helper()
	return newServiceMetrics(metrics.New("gateway-test-" + t.Name()))
}
