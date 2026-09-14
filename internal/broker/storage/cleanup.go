package storage

import (
	"context"
	"log/slog"
	"time"
)

// CleanupStats reports what the cleaner did to one partition in one
// sweep. The broker turns this into metrics; the storage layer only
// reports facts.
type CleanupStats struct {
	Topic     string
	Partition int32
	Retention RetentionStats
}

// PolicyFunc resolves the cleanup policy for one topic. Called once per
// topic per sweep, so config changes apply without a restart.
type PolicyFunc func(topic string) Policy

// SafetyFunc snapshots the smallest committed offset per topic/partition
// across every consumer group. Called once per sweep. A partition absent
// from the map has no committed consumer and gets no protection.
type SafetyFunc func() map[string]map[int32]uint64

// CleanupObserver receives one CleanupStats per partition per sweep,
// including zero stats. It must not block; it runs on the cleaner
// goroutine.
type CleanupObserver func(CleanupStats)

// RunCleaner is the store's background cleanup loop: every interval it
// sweeps every partition, applying retention. It returns when ctx is
// cancelled; the broker waits for it before closing the store, so no
// cleanup ever touches a closed partition.
//
// The loop is the only caller of ApplyRetention and it processes one
// partition at a time, so deletion never races itself.
func (s *Store) RunCleaner(ctx context.Context, interval time.Duration, policy PolicyFunc, safety SafetyFunc, obs CleanupObserver) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.SweepOnce(ctx, policy, safety, obs)
		}
	}
}

// SweepOnce runs a single cleanup pass over every partition of every
// topic. Exported so tests can drive it deterministically (no ticker)
// and so operators could trigger a manual sweep. It checks ctx between
// partitions, so a shutdown stops the sweep promptly.
func (s *Store) SweepOnce(ctx context.Context, policy PolicyFunc, safety SafetyFunc, obs CleanupObserver) {
	var min map[string]map[int32]uint64
	if safety != nil {
		min = safety()
	}
	now := time.Now()
	for _, t := range s.Topics() {
		if ctx.Err() != nil {
			return
		}
		pol := policy(t.Name)
		for _, p := range t.Partitions {
			if ctx.Err() != nil {
				return
			}
			stats := CleanupStats{Topic: t.Name, Partition: p.ID()}
			minSafe := uint64(noSafeOffset)
			if parts, ok := min[t.Name]; ok {
				if off, ok := parts[p.ID()]; ok {
					minSafe = off
				}
			}
			rs, err := p.ApplyRetention(pol, minSafe, now)
			if err != nil {
				s.log.Error("retention failed",
					slog.String("topic", t.Name),
					slog.Int("partition", int(p.ID())),
					slog.Any("err", err))
			} else {
				stats.Retention = rs
				if rs.SegmentsDeleted > 0 {
					s.log.Info("retention deleted segments",
						slog.String("topic", t.Name),
						slog.Int("partition", int(p.ID())),
						slog.Int("segments", rs.SegmentsDeleted),
						slog.Int64("bytes_freed", rs.BytesFreed))
				}
			}
			if obs != nil {
				obs(stats)
			}
		}
	}
}
