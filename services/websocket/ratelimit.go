package websocket

import (
	"sync"
	"time"
)

// Client message rate limits: 20 messages/second sustained, burst of 40.
// Exceeding the budget costs an error frame, not a disconnect.
const (
	ratePerSecond = 20.0
	rateBurst     = 40
)

// tokenBucket is a small per-connection rate limiter. A connection starts
// with a full bucket; every inbound frame costs one token; tokens refill at
// rate per second up to burst. Not shared across connections: one instance
// per conn, so the mutex is never contended.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64 // max accumulated tokens
	tokens float64
	last   time.Time
	now    func() time.Time // injectable clock for tests
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
		now:    time.Now,
	}
}

// allow spends one token, refilling first based on elapsed time.
func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
