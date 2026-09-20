package cluster

import (
	"sync"
)

// This file holds the assignment overlay: operator-driven reassignments
// that take precedence over the deterministic ring placement. The write
// path (OpReassignPartition) lands with the reassignment controller;
// the read path below is part of AssignmentFor from day one so every
// placement decision goes through exactly one lookup.
//
// The overlay lives on the Cluster instance (not a package global): the
// chaos suite runs several brokers in one process and each must see its
// own view.

// overlayKey identifies one partition in the overlay.
type overlayKey struct {
	topic     string
	partition int32
}

// overlay is the per-cluster map of operator-assigned replica sets,
// guarded by its own mutex because placement lookups happen on the
// produce hot path.
type overlay struct {
	mu sync.RWMutex
	m  map[overlayKey]Assignment
}

func newOverlay() *overlay {
	return &overlay{m: make(map[overlayKey]Assignment)}
}

// assignmentOverlay returns the operator-set assignment for the
// partition, if one exists.
func (c *Cluster) assignmentOverlay(topic string, partition int32) (Assignment, bool) {
	c.overlay.mu.RLock()
	defer c.overlay.mu.RUnlock()
	a, ok := c.overlay.m[overlayKey{topic, partition}]
	return a, ok
}

// setAssignmentOverlay records (or replaces) an assignment.
func (c *Cluster) setAssignmentOverlay(a Assignment) {
	c.overlay.mu.Lock()
	c.overlay.m[overlayKey{a.Topic, a.Partition}] = a
	c.overlay.mu.Unlock()
}

// clearAssignmentOverlay drops the overlay entry, returning the
// partition to deterministic ring placement.
func (c *Cluster) clearAssignmentOverlay(topic string, partition int32) {
	c.overlay.mu.Lock()
	delete(c.overlay.m, overlayKey{topic, partition})
	c.overlay.mu.Unlock()
}

// overlaySnapshot copies every overlay assignment (ops + persistence).
func (c *Cluster) overlaySnapshot() []Assignment {
	c.overlay.mu.RLock()
	defer c.overlay.mu.RUnlock()
	out := make([]Assignment, 0, len(c.overlay.m))
	for _, a := range c.overlay.m {
		cp := Assignment{Topic: a.Topic, Partition: a.Partition, Leader: a.Leader}
		cp.Replicas = append(cp.Replicas, a.Replicas...)
		out = append(out, cp)
	}
	return out
}
