package websocket

import (
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	type step struct {
		advance time.Duration // move the fake clock forward before this check
		want    bool
	}
	tests := []struct {
		name  string
		rate  float64
		burst int
		steps []step
	}{
		{
			name: "burst allowed then denied",
			rate: 20, burst: 3,
			steps: []step{{0, true}, {0, true}, {0, true}, {0, false}, {0, false}},
		},
		{
			name: "refill after interval",
			rate: 20, burst: 2,
			steps: []step{
				{0, true}, {0, true}, {0, false},
				{100 * time.Millisecond, true}, // +2 tokens at 20/s
				{0, true}, {0, false},
			},
		},
		{
			name: "accumulation capped at burst",
			rate: 100, burst: 2,
			steps: []step{{time.Hour, true}, {0, true}, {0, false}},
		},
		{
			name: "zero rate never refills",
			rate: 0, burst: 1,
			steps: []step{{0, true}, {time.Minute, false}},
		},
		{
			name: "spec defaults allow 40 then throttle",
			rate: ratePerSecond, burst: rateBurst,
			steps: func() []step {
				steps := make([]step, 0, rateBurst+1)
				for i := 0; i < rateBurst; i++ {
					steps = append(steps, step{0, true})
				}
				return append(steps, step{0, false})
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			b := newTokenBucket(tt.rate, tt.burst)
			b.last = now
			b.now = func() time.Time { return now }
			for i, s := range tt.steps {
				now = now.Add(s.advance)
				if got := b.allow(); got != s.want {
					t.Errorf("step %d: allow() = %v, want %v", i, got, s.want)
				}
			}
		})
	}
}
