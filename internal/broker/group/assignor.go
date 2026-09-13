// Package group implements consumer groups: membership, heartbeats,
// broker-side partition assignment (range assignor), rebalances and
// committed offsets.
package group

import (
	"sort"

	"github.com/raven/platform/internal/broker/protocol"
)

// memberInfo is the assignor's view of one member.
type memberInfo struct {
	id     string
	topics []string
}

// rangeAssign computes the classic range assignment: for each topic,
// partitions 0..n-1 are dealt to the subscribed members (sorted by id)
// in contiguous ranges. When partitions don't divide evenly, the first
// members get one extra partition. Members with no matching partitions
// get an empty assignment — they stay in the group and get work after
// the next rebalance.
func rangeAssign(members []memberInfo, partitionsFor func(topic string) (int, bool)) map[string][]protocol.Assignment {
	sorted := append([]memberInfo(nil), members...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].id < sorted[j].id })

	// Union of subscribed topics, sorted for deterministic output.
	topicSet := map[string]bool{}
	for _, m := range sorted {
		for _, t := range m.topics {
			topicSet[t] = true
		}
	}
	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	out := make(map[string][]protocol.Assignment, len(sorted))
	for _, m := range sorted {
		out[m.id] = nil // members without partitions get an empty assignment
	}
	for _, topic := range topics {
		n, ok := partitionsFor(topic)
		if !ok || n <= 0 {
			continue
		}
		var subs []string
		for _, m := range sorted {
			for _, t := range m.topics {
				if t == topic {
					subs = append(subs, m.id)
					break
				}
			}
		}
		if len(subs) == 0 {
			continue
		}
		per := n / len(subs)
		extra := n % len(subs)
		start := 0
		for i, id := range subs {
			count := per
			if i < extra {
				count++
			}
			parts := make([]int32, 0, count)
			for p := start; p < start+count; p++ {
				parts = append(parts, int32(p))
			}
			start += count
			out[id] = append(out[id], protocol.Assignment{Topic: topic, Partitions: parts})
		}
	}
	return out
}
