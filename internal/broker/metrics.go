package broker

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles the broker's Prometheus collectors.
type Metrics struct {
	produced *prometheus.CounterVec
	fetched  *prometheus.CounterVec
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
	}
}

// registerMetrics wires every collector into the service registry.
func (b *Broker) registerMetrics(reg CollectorRegistrar) {
	reg.Register(
		b.metrics.produced,
		b.metrics.fetched,
		&lagCollector{
			b: b,
			desc: prometheus.NewDesc(
				"raven_broker_messages_pending",
				"Consumer lag: high-water mark minus committed offset, per topic/partition/group.",
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
	)
}

// lagCollector computes consumer lag on scrape. Values change
// constantly, so a dynamic collector beats a cached gauge.
type lagCollector struct {
	b    *Broker
	desc *prometheus.Desc
}

func (c *lagCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *lagCollector) Collect(ch chan<- prometheus.Metric) {
	for _, groupID := range c.b.groups.OffsetGroupIDs() {
		snapshot := c.b.groups.GroupOffsets(groupID)
		for topic, parts := range snapshot {
			t, err := c.b.store.Topic(topic)
			if err != nil {
				continue
			}
			for part, committed := range parts {
				if part < 0 || int(part) >= len(t.Partitions) {
					continue
				}
				hwm := t.Partitions[part].HighWatermark()
				lag := uint64(0)
				if hwm > committed {
					lag = hwm - committed
				}
				ch <- prometheus.MustNewConstMetric(
					c.desc, prometheus.GaugeValue, float64(lag),
					topic, strconv.Itoa(int(part)), groupID,
				)
			}
		}
	}
}
