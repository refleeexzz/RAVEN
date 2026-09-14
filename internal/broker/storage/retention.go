package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

// Policy is the resolved cleanup policy for one topic. The broker
// computes it from global defaults plus per-topic overrides and hands it
// to the store; the storage layer itself has no notion of topics'
// configuration sources.
type Policy struct {
	// RetentionMaxAge deletes closed segments whose newest record is
	// older than this. Zero disables time-based retention.
	RetentionMaxAge time.Duration
	// RetentionMaxBytes caps the partition's total log bytes: while over
	// the cap, the oldest closed segments are deleted until it fits.
	// Zero or negative disables size-based retention.
	RetentionMaxBytes int64
	// Compact marks the topic for log compaction (see compaction.go).
	Compact bool
}

// RetentionStats reports what one ApplyRetention call removed.
type RetentionStats struct {
	SegmentsDeleted int
	BytesFreed      int64
}

// noSafeOffset means "no consumer group has committed on this
// partition", so retention is free to delete any closed segment that the
// time/size rules select.
const noSafeOffset = math.MaxUint64

// ApplyRetention deletes expired closed segments and returns what was
// freed. Two independent rules select a segment for deletion:
//
//   - time: the segment's newest record timestamp (maxTs) is older than
//     pol.RetentionMaxAge.
//   - size: the partition is over pol.RetentionMaxBytes and the segment
//     is one of the oldest.
//
// Two hard safety rules override both, always:
//
//   - the active (last) segment is never deleted;
//   - a segment is deleted only when every offset it holds is below
//     minSafeOffset — the smallest committed offset across all consumer
//     groups with commits on this partition. Committed offsets are the
//     next offset to consume, so deleting anything at or above the
//     smallest one could strand a group on OFFSET_OUT_OF_RANGE forever.
//     Callers pass math.MaxUint64 (noSafeOffset) when no group has
//     committed: nothing is protected.
//
// A group that never committed on this exact partition is NOT protected:
// it would start wherever the client chooses anyway. This mirrors
// Kafka's consumer-facing behavior while being strictly safer than
// Kafka's retention, which ignores consumer offsets entirely.
func (p *Partition) ApplyRetention(pol Policy, minSafeOffset uint64, now time.Time) (RetentionStats, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return RetentionStats{}, fmt.Errorf("storage: partition closed")
	}
	// Candidates: every segment except the active one, oldest first.
	var stats RetentionStats
	totalLogBytes := int64(0)
	for _, s := range p.segments {
		totalLogBytes += s.size
	}
	deleteUpTo := 0 // segments[0:deleteUpTo] are selected for deletion
	for i := 0; i < len(p.segments)-1; i++ {
		seg := p.segments[i]
		segEnd := p.segments[i+1].baseOffset // one past the last offset in seg
		// Safety rule 2: never delete data a committed group still needs.
		if segEnd > minSafeOffset {
			break
		}
		expired := pol.RetentionMaxAge > 0 &&
			now.Sub(time.UnixMilli(seg.maxTs)) > pol.RetentionMaxAge
		overSize := pol.RetentionMaxBytes > 0 &&
			totalLogBytes > pol.RetentionMaxBytes
		if !expired && !overSize {
			// Deletion candidates are a prefix of the segment list: the
			// size rule only ever removes the oldest segments, and the
			// time rule is monotone in age, so the first segment that
			// fails both rules ends the scan.
			break
		}
		totalLogBytes -= seg.size
		deleteUpTo = i + 1
	}
	if deleteUpTo == 0 {
		return stats, nil
	}
	freed, err := p.deleteSegmentsLocked(deleteUpTo)
	if err != nil {
		return stats, err
	}
	stats.SegmentsDeleted = deleteUpTo
	stats.BytesFreed = freed
	return stats, nil
}

// deleteSegmentsLocked closes, removes and forgets the first n segments
// (all closed). The caller holds p.mu and has already checked the safety
// rules. Returns the real on-disk bytes freed (.log + .index).
func (p *Partition) deleteSegmentsLocked(n int) (int64, error) {
	if n <= 0 || n >= len(p.segments) {
		return 0, fmt.Errorf("storage: invalid segment deletion count %d (have %d)", n, len(p.segments))
	}
	var freed int64
	var firstErr error
	for _, s := range p.segments[:n] {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		for _, name := range []string{segmentLogName(s.baseOffset), segmentIndexName(s.baseOffset)} {
			path := filepath.Join(p.dir, name)
			if fi, err := os.Stat(path); err == nil {
				freed += fi.Size()
			}
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = fmt.Errorf("delete segment %d: %w", s.baseOffset, err)
			}
		}
	}
	p.segments = append([]*segment(nil), p.segments[n:]...)
	// dirtySegStart indexes into p.segments; keep it valid after the
	// prefix shift.
	p.dirtySegStart -= n
	if p.dirtySegStart < 0 {
		p.dirtySegStart = 0
	}
	return freed, firstErr
}
