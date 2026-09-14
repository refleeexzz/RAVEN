package broker

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles the broker's Prometheus collectors.
type Metrics struct {
	produced      *prometheus.CounterVec
	fetched       *prometheus.CounterVec
	consumed      *prometheus.CounterVec
	appendLatency *prometheus.HistogramVec
}

// CollectorRegistrar is satisfied by pkg/metrics.Registry; defined as
// an interface so tests can pass a plain prometheus registry or nil.
type CollectorRegistrar interface {
	Register(...prometheus.Collector)
}

func newMetrics() *Metrics {
	return &Metrics{
		produced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "messages_produced_total",
			Help:      "Total messages appended, by topic and partition.",
		}, []string{"topic", "partition"}),
		fetched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "messages_fetched_total",
			Help:      "Total messages delivered to fetches, by topic and partition.",
		}, []string{"topic", "partition"}),
		// consumed mirrors fetched: in this broker every fetched record is
		// delivered to a consumer, so the two move together. Both names
		// stay live — fetched is the historical series, consumed is the
		// roadmap (P2) name dashboards are migrating to.
		consumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "messages_consumed_total",
			Help:      "Total messages delivered to consumers, by topic and partition. Alias of messages_fetched_total.",
		}, []string{"topic", "partition"}),
		appendLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "append_latency",
			Help:      "Time in seconds to append one produce batch to the partition log (encode + write, before fsync), by topic and partition.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		}, []string{"topic", "partition"}),
	}
}

// registerMetrics wires every collector into the service registry. It
// runs after b.server exists, so the connection gauge never reads a nil
// server.
func (b *Broker) registerMetrics(reg CollectorRegistrar) {
	reg.Register(
		b.metrics.produced,
		b.metrics.fetched,
		b.metrics.consumed,
		b.metrics.appendLatency,
		&offsetCollector{
			b: b,
			pendingDesc: prometheus.NewDesc(
				"raven_broker_messages_pending",
				"Consumer lag: high-water mark minus committed offset, per topic/partition/group. Alias of raven_broker_consumer_lag.",
				[]string{"topic", "partition", "group"}, nil,
			),
			lagDesc: prometheus.NewDesc(
				"raven_broker_consumer_lag",
				"Consumer lag: partition end offset minus committed consumer offset, per topic/partition/group.",
				[]string{"topic", "partition", "group"}, nil,
			),
			partitionOffsetDesc: prometheus.NewDesc(
				"raven_broker_partition_offset",
				"Partition end offset (high-water mark: next offset to be assigned), per topic/partition.",
				[]string{"topic", "partition"}, nil,
			),
			consumerOffsetDesc: prometheus.NewDesc(
				"raven_broker_consumer_offset",
				"Last committed consumer offset (next record to consume), per topic/partition/group.",
				[]string{"topic", "partition", "group"}, nil,
			),
		},
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "active_groups",
			Help:      "Consumer groups with at least one live member.",
		}, func() float64 {
			return float64(b.groups.ActiveGroups())
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "active_connections",
			Help:      "Client TCP connections currently held by the broker.",
		}, func() float64 {
			return float64(b.server.ActiveConnections())
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "disk_usage",
			Help:      "Bytes held by the broker data dir: segment logs and indexes plus the committed-offsets file.",
		}, func() float64 {
			total := b.store.DiskUsageBytes()
			// offsets.jsonl lives next to topics/; count it too so the
			// gauge really is "data dir bytes".
			if fi, err := os.Stat(filepath.Join(b.cfg.DataDir, "offsets.jsonl")); err == nil {
				total += fi.Size()
			}
			return float64(total)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "segment_count",
			Help:      "Segment log files across every topic and partition.",
		}, func() float64 {
			return float64(b.store.SegmentCount())
		}),
	)
}

// offsetCollector computes the lag trio on scrape: partition end offsets
// for every known partition, and committed offset + lag per group. Values
// change constantly, so a dynamic collector beats a cached gauge. One
// collector emits all four series (messages_pending kept as a compatible
// alias of consumer_lag) so the offset snapshot is taken once per scrape.
type offsetCollector struct {
	b                   *Broker
	pendingDesc         *prometheus.Desc
	lagDesc             *prometheus.Desc
	partitionOffsetDesc *prometheus.Desc
	consumerOffsetDesc  *prometheus.Desc
}

func (c *offsetCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pendingDesc
	ch <- c.lagDesc
	ch <- c.partitionOffsetDesc
	ch <- c.consumerOffsetDesc
}

func (c *offsetCollector) Collect(ch chan<- prometheus.Metric) {
	// Partition end offsets: every partition of every topic, whether or
	// not any group consumes it yet.
	hwm := make(map[string]map[int32]uint64)
	for _, t := range c.b.store.Topics() {
		parts := make(map[int32]uint64, len(t.Partitions))
		for _, p := range t.Partitions {
			part := strconv.Itoa(int(p.ID()))
			end := p.HighWatermark()
			parts[p.ID()] = end
			ch <- prometheus.MustNewConstMetric(
				c.partitionOffsetDesc, prometheus.GaugeValue, float64(end),
				t.Name, part,
			)
		}
		hwm[t.Name] = parts
	}

	// Committed offsets and lag, per group.
	for _, groupID := range c.b.groups.OffsetGroupIDs() {
		snapshot := c.b.groups.GroupOffsets(groupID)
		for topic, parts := range snapshot {
			topicHWM, ok := hwm[topic]
			if !ok {
				continue // topic dropped between the two snapshots
			}
			for part, committed := range parts {
				end, ok := topicHWM[part]
				if !ok {
					continue // partition id no longer exists
				}
				partStr := strconv.Itoa(int(part))
				lag := uint64(0)
				if end > committed {
					lag = end - committed
				}
				ch <- prometheus.MustNewConstMetric(
					c.consumerOffsetDesc, prometheus.GaugeValue, float64(committed),
					topic, partStr, groupID,
				)
				ch <- prometheus.MustNewConstMetric(
					c.lagDesc, prometheus.GaugeValue, float64(lag),
					topic, partStr, groupID,
				)
				ch <- prometheus.MustNewConstMetric(
					c.pendingDesc, prometheus.GaugeValue, float64(lag),
					topic, partStr, groupID,
				)
			}
		}
	}
}
