package broker

import (
	"time"

	"github.com/raven/platform/internal/config"
)

// Config holds every broker knob. All of it comes from env vars
// (12-factor); see docs/contracts/ports-and-env.md.
type Config struct {
	// TCPAddr is the client protocol listener (contract: 9100).
	TCPAddr string
	// DataDir holds topics/ and offsets.jsonl (env BROKER_DATA_DIR).
	DataDir string
	// DefaultPartitions is used when CREATE_TOPIC omits the count.
	DefaultPartitions int32
	// MaxSegmentBytes rotates the active segment past this size.
	MaxSegmentBytes int64
	// IndexIntervalBytes: one sparse index entry per this many log bytes.
	IndexIntervalBytes int64
	// FsyncEvery: background flush+fsync cadence.
	FsyncEvery time.Duration
	// FsyncRecords: flush+fsync after this many appended records,
	// whichever limit hits first.
	FsyncRecords int
	// ProduceQueueSize bounds the per-partition writer channel. A full
	// queue rejects PRODUCE with BROKER_BUSY (backpressure).
	ProduceQueueSize int
	// SessionTimeout: max gap between member heartbeats before the
	// group expels the member and rebalances.
	SessionTimeout time.Duration
	// DrainTimeout bounds graceful connection draining at shutdown.
	DrainTimeout time.Duration
}

// ConfigFromEnv loads the broker configuration from the environment.
func ConfigFromEnv() Config {
	return Config{
		TCPAddr:            config.Get("BROKER_TCP_ADDR", ":9100"),
		DataDir:            config.Get("BROKER_DATA_DIR", "./data"),
		DefaultPartitions:  int32(config.GetInt("BROKER_DEFAULT_PARTITIONS", 3)),
		MaxSegmentBytes:    int64(config.GetInt("BROKER_SEGMENT_BYTES", 64<<20)),
		IndexIntervalBytes: int64(config.GetInt("BROKER_INDEX_INTERVAL_BYTES", 4096)),
		FsyncEvery:         time.Duration(config.GetInt("BROKER_FSYNC_MS", 100)) * time.Millisecond,
		FsyncRecords:       config.GetInt("BROKER_FSYNC_RECORDS", 256),
		ProduceQueueSize:   config.GetInt("BROKER_PRODUCE_QUEUE", 1024),
		SessionTimeout:     time.Duration(config.GetInt("BROKER_SESSION_TIMEOUT_MS", 10000)) * time.Millisecond,
		DrainTimeout:       config.GetDuration("BROKER_DRAIN_TIMEOUT", 5*time.Second),
	}
}

// withDefaults fills zero fields so tests can build partial configs.
func (c Config) withDefaults() Config {
	def := Config{
		TCPAddr:            ":9100",
		DataDir:            "./data",
		DefaultPartitions:  3,
		MaxSegmentBytes:    64 << 20,
		IndexIntervalBytes: 4096,
		FsyncEvery:         100 * time.Millisecond,
		FsyncRecords:       256,
		ProduceQueueSize:   1024,
		SessionTimeout:     10 * time.Second,
		DrainTimeout:       5 * time.Second,
	}
	if c.TCPAddr != "" {
		def.TCPAddr = c.TCPAddr
	}
	if c.DataDir != "" {
		def.DataDir = c.DataDir
	}
	if c.DefaultPartitions > 0 {
		def.DefaultPartitions = c.DefaultPartitions
	}
	if c.MaxSegmentBytes > 0 {
		def.MaxSegmentBytes = c.MaxSegmentBytes
	}
	if c.IndexIntervalBytes > 0 {
		def.IndexIntervalBytes = c.IndexIntervalBytes
	}
	if c.FsyncEvery > 0 {
		def.FsyncEvery = c.FsyncEvery
	}
	if c.FsyncRecords > 0 {
		def.FsyncRecords = c.FsyncRecords
	}
	if c.ProduceQueueSize > 0 {
		def.ProduceQueueSize = c.ProduceQueueSize
	}
	if c.SessionTimeout > 0 {
		def.SessionTimeout = c.SessionTimeout
	}
	if c.DrainTimeout > 0 {
		def.DrainTimeout = c.DrainTimeout
	}
	return def
}
