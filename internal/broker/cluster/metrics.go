package cluster

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles the cluster control-plane collectors. All names carry
// the raven_broker namespace so they sit next to the broker's own
// series on the ops endpoint.
type Metrics struct {
	// heartbeats counts ping round-trips by peer and result.
	heartbeats *prometheus.CounterVec
	// transitions counts membership state flips by resulting state.
	transitions *prometheus.CounterVec
	// leaderChanges counts per-partition leadership flips (elections,
	// transfers), by topic.
	leaderChanges *prometheus.CounterVec
	// replicationLag tracks follower lag in offsets, by topic/partition/peer.
	replicationLag *prometheus.GaugeVec
	// committedOffset tracks the committed offset (stable HWM) per
	// partition as known by this node.
	committedOffset *prometheus.GaugeVec
	// followerOffset tracks each replica's replicated offset as known by
	// this node (leaders track followers, followers track themselves).
	followerOffset *prometheus.GaugeVec
}

func newMetrics() *Metrics {
	return &Metrics{
		heartbeats: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "cluster_heartbeats_total",
			Help:      "Cluster heartbeat round-trips, by peer node and result (ok/fail).",
		}, []string{"peer", "result"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "cluster_member_transitions_total",
			Help:      "Cluster membership state transitions observed by this node, by resulting state.",
		}, []string{"state"}),
		leaderChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "cluster_leader_changes_total",
			Help:      "Partition leadership changes observed by this node (elections and transfers), by topic.",
		}, []string{"topic"}),
		replicationLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "cluster_replication_lag",
			Help:      "Follower replication lag in offsets (leader end minus replica offset), by topic/partition/replica.",
		}, []string{"topic", "partition", "replica"}),
		committedOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "cluster_committed_offset",
			Help:      "Committed offset (stable high-water mark) per partition as known by this node.",
		}, []string{"topic", "partition"}),
		followerOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "cluster_follower_offset",
			Help:      "Replicated offset of each replica as known by this node, by topic/partition/replica.",
		}, []string{"topic", "partition", "replica"}),
	}
}

// Collectors returns every cluster collector for registration.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.heartbeats,
		m.transitions,
		m.leaderChanges,
		m.replicationLag,
		m.committedOffset,
		m.followerOffset,
	}
}

// memberStateDesc builds the per-member state gauge descriptor. The
// member table is dynamic, so a custom collector beats cached gauges.
type memberCollector struct {
	c          *Cluster
	stateDesc  *prometheus.Desc
	lastSeenDs *prometheus.Desc
}

func (mc *memberCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.stateDesc
	ch <- mc.lastSeenDs
}

func (mc *memberCollector) Collect(ch chan<- prometheus.Metric) {
	for _, mem := range mc.c.Members() {
		ch <- prometheus.MustNewConstMetric(
			mc.stateDesc, prometheus.GaugeValue, float64(mem.State),
			string(mem.ID), mem.State.String(),
		)
		if !mem.LastSeen.IsZero() {
			ch <- prometheus.MustNewConstMetric(
				mc.lastSeenDs, prometheus.GaugeValue, float64(mem.LastSeen.Unix()),
				string(mem.ID),
			)
		}
	}
}

// RegisterMetrics wires every cluster collector into the broker's
// registry. Called by the broker only in cluster mode.
func (c *Cluster) RegisterMetrics(reg interface{ Register(...prometheus.Collector) }) {
	reg.Register(c.metrics.Collectors()...)
	reg.Register(&memberCollector{
		c: c,
		stateDesc: prometheus.NewDesc(
			"raven_broker_cluster_member_state",
			"Cluster member state as seen by this node (0=JOINING 1=ACTIVE 2=SUSPECT 3=DEAD 4=LEAVING), with the state name as a label.",
			[]string{"node", "state"}, nil,
		),
		lastSeenDs: prometheus.NewDesc(
			"raven_broker_cluster_member_last_seen_unix",
			"Unix timestamp of the last valid frame received from the member, as seen by this node.",
			[]string{"node"}, nil,
		),
	})
}

// partLabel renders a partition id for metric labels.
func partLabel(p int32) string { return strconv.Itoa(int(p)) }

// ---- data-plane metric observers (called by the broker's replication
// manager; all cheap gauge/counter sets) ----

// ObserveReplicationLag records how far a replica trails, in offsets.
func (c *Cluster) ObserveReplicationLag(topic string, part int32, replica NodeID, lag uint64) {
	c.metrics.replicationLag.WithLabelValues(topic, partLabel(part), string(replica)).Set(float64(lag))
}

// ObserveCommittedOffset records the committed offset (stable HWM) this
// node knows for a partition.
func (c *Cluster) ObserveCommittedOffset(topic string, part int32, off uint64) {
	c.metrics.committedOffset.WithLabelValues(topic, partLabel(part)).Set(float64(off))
}

// ObserveFollowerOffset records one replica's replicated offset as
// known by this node.
func (c *Cluster) ObserveFollowerOffset(topic string, part int32, replica NodeID, off uint64) {
	c.metrics.followerOffset.WithLabelValues(topic, partLabel(part), string(replica)).Set(float64(off))
}

// ObserveLeaderChange counts a leadership flip for a topic.
func (c *Cluster) ObserveLeaderChange(topic string) {
	c.metrics.leaderChanges.WithLabelValues(topic).Inc()
}
