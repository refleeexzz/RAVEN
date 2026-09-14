package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// compactedMarkerName is a flag file written into the partition dir by
// the first successful compaction. Its presence means segments may have
// non-contiguous offsets (compaction removes records without
// renumbering), so index rebuilds must tolerate gaps. The marker is
// never removed: compaction is a one-way property of the data.
const compactedMarkerName = "_compacted"

// maxCompactionKeys bounds the in-memory offset map built during one
// compaction pass (one entry per distinct key in the closed segments).
// Past the cap the pass aborts and the partition is left untouched;
// compaction is an optimization, never worth an OOM. A var (not a
// const) so tests can shrink it.
var maxCompactionKeys = 4 << 20

// CompactionStats reports what one Compact call did.
type CompactionStats struct {
	SegmentsRewritten int
	SegmentsDeleted   int // segments left with zero survivors
	RecordsKept       int
	RecordsDropped    int
	BytesFreed        int64
}

// Swap names are the temporary files compaction writes before the
// atomic swap.
func segmentLogSwapName(base uint64) string   { return segmentLogName(base) + ".swap" }
func segmentIndexSwapName(base uint64) string { return segmentIndexName(base) + ".swap" }

// Compact rewrites the closed segments of one partition keeping, for
// every key, only the record with the highest offset — Kafka-style log
// compaction. Keyless records are always kept (there is no key to
// dedupe on). Empty-value records are NOT tombstones in v1: they are
// kept like any other latest value, because the record format cannot
// distinguish "null" from "empty". Offsets are preserved: dropped
// records leave holes, nothing is renumbered, and the high-water mark
// does not move.
//
// Only closed segments are compacted; the active segment is never
// touched. A segment that ends up with zero survivors is deleted
// entirely.
//
// Crash safety: every rewritten segment goes to <base>.log.swap +
// <base>.index.swap first, fsynced, and only then swapped over the old
// files (close → remove → rename). A crash anywhere leaves a state that
// resolveSwapFiles repairs at the next open: either the old pair or the
// new pair survives whole, never a mix.
//
// Concurrency: the scan and the swap-file writes run WITHOUT holding
// the partition lock (closed segments are immutable), so appends and
// reads continue during compaction. Only the final swap takes the
// write lock, and only for a few file renames per segment.
func (p *Partition) Compact(ctx context.Context) (CompactionStats, error) {
	var stats CompactionStats

	// Phase 1: snapshot the closed segments. Rotation during later
	// phases only appends newer segments, which are not targets.
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return stats, errors.New("storage: partition closed")
	}
	targets := make([]*segment, 0, len(p.segments))
	for _, s := range p.segments[:len(p.segments)-1] {
		if s.size > 0 {
			targets = append(targets, s)
		}
	}
	sizes := make(map[uint64]int64, len(targets))
	for _, s := range targets {
		sizes[s.baseOffset] = s.size
	}
	p.mu.RUnlock()
	if len(targets) == 0 {
		return stats, nil
	}

	// Phase 2: build the offset map — key → highest offset in the closed
	// range. Aborts (leaving everything untouched) if the map would grow
	// past maxCompactionKeys.
	last := make(map[string]uint64)
	for _, s := range targets {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		full := false
		err := streamRecords(s.log, 0, s.size, false, func(r *Record, _, _ int64) bool {
			if len(r.Key) == 0 {
				return true
			}
			if _, seen := last[string(r.Key)]; !seen && len(last) >= maxCompactionKeys {
				full = true
				return false
			}
			last[string(r.Key)] = r.Offset
			return true
		})
		if err != nil {
			return stats, fmt.Errorf("compaction scan segment %d: %w", s.baseOffset, err)
		}
		if full {
			return stats, fmt.Errorf("storage: too many distinct keys (>%d) in closed segments: compaction aborted", maxCompactionKeys)
		}
	}
	if len(last) == 0 {
		return stats, nil // no keyed records: nothing can be dropped
	}

	// Phase 3: rewrite each segment's survivors to swap files.
	type rewrite struct {
		base      uint64
		survivors int
		maxTs     int64
	}
	var rewrites []rewrite
	var emptyBases []uint64
	swapPaths := func(base uint64) (string, string) {
		return filepath.Join(p.dir, segmentLogSwapName(base)),
			filepath.Join(p.dir, segmentIndexSwapName(base))
	}
	removeSwaps := func() {
		for _, rw := range rewrites {
			ls, is := swapPaths(rw.base)
			_ = os.Remove(ls)
			_ = os.Remove(is)
		}
	}
	for _, s := range targets {
		if err := ctx.Err(); err != nil {
			removeSwaps()
			return stats, err
		}
		logSwap, indexSwap := swapPaths(s.baseOffset)
		logF, err := os.OpenFile(logSwap, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			removeSwaps()
			return stats, fmt.Errorf("create swap log: %w", err)
		}
		indexF, err := os.OpenFile(indexSwap, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			_ = logF.Close()
			removeSwaps()
			return stats, fmt.Errorf("create swap index: %w", err)
		}
		var logBuf, indexBuf []byte
		kept, dropped := 0, 0
		var maxTs int64
		bytesSinceIndex := int64(0)
		var encodeErr error
		scanErr := streamRecords(s.log, 0, s.size, false, func(r *Record, _, _ int64) bool {
			if len(r.Key) > 0 && last[string(r.Key)] != r.Offset {
				dropped++
				return true // superseded by a newer record with the same key
			}
			start := len(logBuf)
			logBuf, encodeErr = EncodeRecord(logBuf, r)
			if encodeErr != nil {
				return false
			}
			recSize := int64(len(logBuf) - start)
			if bytesSinceIndex >= s.indexInterval {
				indexBuf = appendIndexEntry(indexBuf, uint32(r.Offset-s.baseOffset), uint32(start))
				bytesSinceIndex = 0
			}
			bytesSinceIndex += recSize
			kept++
			if r.TimestampMs > maxTs {
				maxTs = r.TimestampMs
			}
			return true
		})
		switch {
		case encodeErr != nil:
			_ = logF.Close()
			_ = indexF.Close()
			removeSwaps()
			return stats, encodeErr
		case scanErr != nil:
			_ = logF.Close()
			_ = indexF.Close()
			removeSwaps()
			return stats, fmt.Errorf("compaction rewrite segment %d: %w", s.baseOffset, scanErr)
		}
		stats.RecordsKept += kept
		stats.RecordsDropped += dropped
		if kept == 0 {
			// Zero survivors: the segment will be deleted, not swapped.
			_ = logF.Close()
			_ = indexF.Close()
			_ = os.Remove(logSwap)
			_ = os.Remove(indexSwap)
			emptyBases = append(emptyBases, s.baseOffset)
			continue
		}
		if dropped == 0 {
			// Nothing to remove: skip the rewrite entirely, keep the
			// original files. Repeated compaction passes stay cheap.
			_ = logF.Close()
			_ = indexF.Close()
			_ = os.Remove(logSwap)
			_ = os.Remove(indexSwap)
			continue
		}
		if _, err := logF.Write(logBuf); err != nil {
			_ = logF.Close()
			_ = indexF.Close()
			removeSwaps()
			return stats, fmt.Errorf("write swap log: %w", err)
		}
		if _, err := indexF.Write(indexBuf); err != nil {
			_ = logF.Close()
			_ = indexF.Close()
			removeSwaps()
			return stats, fmt.Errorf("write swap index: %w", err)
		}
		if err := logF.Sync(); err != nil {
			_ = logF.Close()
			_ = indexF.Close()
			removeSwaps()
			return stats, fmt.Errorf("sync swap log: %w", err)
		}
		if err := indexF.Sync(); err != nil {
			_ = logF.Close()
			_ = indexF.Close()
			removeSwaps()
			return stats, fmt.Errorf("sync swap index: %w", err)
		}
		if err := logF.Close(); err != nil {
			_ = indexF.Close()
			removeSwaps()
			return stats, err
		}
		if err := indexF.Close(); err != nil {
			removeSwaps()
			return stats, err
		}
		rewrites = append(rewrites, rewrite{base: s.baseOffset, survivors: kept, maxTs: maxTs})
	}

	if len(rewrites) == 0 && len(emptyBases) == 0 {
		return stats, nil // every segment was already compacted
	}

	// Phase 4: the atomic swap. Marker first (durable before any renamed
	// segment can exist), then per segment: close, remove old pair,
	// rename swap pair into place, reopen.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		// The partition closed under us (shutdown). Leave the swap
		// files: resolveSwapFiles at the next open discards them and the
		// old segments stay authoritative.
		return stats, errors.New("storage: partition closed")
	}
	if !p.gapsAllowed {
		if err := writeMarker(p.dir); err != nil {
			removeSwaps()
			return stats, fmt.Errorf("write compaction marker: %w", err)
		}
		p.gapsAllowed = true
	}
	rewriteByBase := make(map[uint64]rewrite, len(rewrites))
	for _, rw := range rewrites {
		rewriteByBase[rw.base] = rw
	}
	emptySet := make(map[uint64]bool, len(emptyBases))
	for _, b := range emptyBases {
		emptySet[b] = true
	}
	newSegs := make([]*segment, 0, len(p.segments))
	removedBeforeDirty := 0
	for i, s := range p.segments {
		isActive := i == len(p.segments)-1
		if isActive {
			newSegs = append(newSegs, s)
			continue
		}
		if emptySet[s.baseOffset] && sizes[s.baseOffset] == s.size {
			freed, err := removeSegmentFiles(p.dir, s)
			if err != nil {
				removeSwaps()
				return stats, err
			}
			stats.BytesFreed += freed
			stats.SegmentsDeleted++
			if i < p.dirtySegStart {
				removedBeforeDirty++
			}
			continue
		}
		rw, ok := rewriteByBase[s.baseOffset]
		if !ok || sizes[s.baseOffset] != s.size {
			// Not a target (or changed under us — should not happen):
			// keep the old segment; its swap files, if any, are removed
			// below.
			newSegs = append(newSegs, s)
			continue
		}
		if err := s.close(); err != nil {
			removeSwaps()
			return stats, fmt.Errorf("close segment %d for swap: %w", s.baseOffset, err)
		}
		oldBytes, err := removeSegmentPair(p.dir, s.baseOffset)
		if err != nil {
			removeSwaps()
			return stats, err
		}
		logSwap, indexSwap := swapPaths(s.baseOffset)
		if err := os.Rename(logSwap, filepath.Join(p.dir, segmentLogName(s.baseOffset))); err != nil {
			return stats, fmt.Errorf("swap log %d: %w", s.baseOffset, err)
		}
		if err := os.Rename(indexSwap, filepath.Join(p.dir, segmentIndexName(s.baseOffset))); err != nil {
			return stats, fmt.Errorf("swap index %d: %w", s.baseOffset, err)
		}
		ns, err := openSegment(p.dir, s.baseOffset, p.opts.IndexIntervalBytes)
		if err != nil {
			return stats, fmt.Errorf("reopen compacted segment %d: %w", s.baseOffset, err)
		}
		ns.maxTs = rw.maxTs // survivor timestamps, not the rename's mtime
		newSegs = append(newSegs, ns)
		stats.SegmentsRewritten++
		stats.BytesFreed += oldBytes - (ns.size + int64(len(ns.entries)*indexEntrySize))
	}
	p.segments = newSegs
	p.dirtySegStart -= removedBeforeDirty
	if p.dirtySegStart < 0 {
		p.dirtySegStart = 0
	}
	// Any swap files left over belong to segments we skipped; remove
	// them so the next open has nothing to resolve.
	removeSwaps()
	return stats, nil
}

// writeMarker creates the _compacted flag file and fsyncs it. It must
// be durable before the first compacted segment lands, because it is
// what lets later index rebuilds tolerate offset gaps.
func writeMarker(dir string) error {
	path := filepath.Join(dir, compactedMarkerName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString("compaction v1: segments may have offset gaps\n"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// appendIndexEntry appends one sparse-index entry to buf.
func appendIndexEntry(buf []byte, relOffset, position uint32) []byte {
	var e [indexEntrySize]byte
	binary.BigEndian.PutUint32(e[0:4], relOffset)
	binary.BigEndian.PutUint32(e[4:8], position)
	return append(buf, e[:]...)
}

// removeSegmentPair deletes the .log and .index files of a segment and
// returns their combined on-disk size. The segment's files must already
// be closed.
func removeSegmentPair(dir string, base uint64) (int64, error) {
	var freed int64
	for _, name := range []string{segmentLogName(base), segmentIndexName(base)} {
		path := filepath.Join(dir, name)
		if fi, err := os.Stat(path); err == nil {
			freed += fi.Size()
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return freed, fmt.Errorf("remove %s: %w", name, err)
		}
	}
	return freed, nil
}

// removeSegmentFiles closes a segment's files and removes them.
func removeSegmentFiles(dir string, s *segment) (int64, error) {
	if err := s.close(); err != nil {
		return 0, fmt.Errorf("close segment %d: %w", s.baseOffset, err)
	}
	return removeSegmentPair(dir, s.baseOffset)
}

// resolveSwapFiles repairs compaction leftovers after a crash. The swap
// protocol (fsync swap pair → write marker → close → remove old pair →
// rename swap pair) guarantees that for each segment the on-disk state
// is always one of:
//
//   - old .log + old .index (+ maybe swap files): crash before the
//     delete phase. The old pair is authoritative; swaps are discarded.
//   - some pair file missing, swap files present: crash mid-swap. The
//     swap is completed: for each file of the pair, the swap replaces
//     whatever real file is left.
//
// Anything else (e.g. a missing pair file with no swap to complete it)
// means someone hand-edited the data dir; we refuse to guess.
func resolveSwapFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list partition dir: %w", err)
	}
	type pair struct{ log, index bool }
	swaps := make(map[uint64]*pair)
	for _, e := range entries {
		name := e.Name()
		var baseStr string
		var isLog bool
		switch {
		case strings.HasSuffix(name, ".log.swap"):
			baseStr = strings.TrimSuffix(name, ".log.swap")
			isLog = true
		case strings.HasSuffix(name, ".index.swap"):
			baseStr = strings.TrimSuffix(name, ".index.swap")
		default:
			continue
		}
		base, err := strconv.ParseUint(baseStr, 10, 64)
		if err != nil {
			continue
		}
		sp, ok := swaps[base]
		if !ok {
			sp = &pair{}
			swaps[base] = sp
		}
		if isLog {
			sp.log = true
		} else {
			sp.index = true
		}
	}
	for base, sw := range swaps {
		logReal := filepath.Join(dir, segmentLogName(base))
		indexReal := filepath.Join(dir, segmentIndexName(base))
		_, logStatErr := os.Stat(logReal)
		_, indexStatErr := os.Stat(indexReal)
		if logStatErr == nil && indexStatErr == nil {
			// Old pair intact: crash before the delete phase.
			if sw.log {
				_ = os.Remove(logReal + ".swap")
			}
			if sw.index {
				_ = os.Remove(indexReal + ".swap")
			}
			continue
		}
		// Mid-swap crash: complete the swap per file.
		if sw.log {
			if logStatErr == nil {
				if err := os.Remove(logReal); err != nil {
					return fmt.Errorf("resolve swap %d: remove stale log: %w", base, err)
				}
			}
			if err := os.Rename(logReal+".swap", logReal); err != nil {
				return fmt.Errorf("resolve swap %d: rename log: %w", base, err)
			}
		} else if logStatErr != nil {
			return fmt.Errorf("resolve swap %d: log and log.swap both missing", base)
		}
		if sw.index {
			if indexStatErr == nil {
				if err := os.Remove(indexReal); err != nil {
					return fmt.Errorf("resolve swap %d: remove stale index: %w", base, err)
				}
			}
			if err := os.Rename(indexReal+".swap", indexReal); err != nil {
				return fmt.Errorf("resolve swap %d: rename index: %w", base, err)
			}
		} else if indexStatErr != nil {
			return fmt.Errorf("resolve swap %d: index and index.swap both missing", base)
		}
	}
	return nil
}
