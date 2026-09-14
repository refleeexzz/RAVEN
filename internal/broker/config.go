package broker

import (
	"time"

	"github.com/refleeexzz/RAVEN/internal/config"
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
	// MaxConnections caps simultaneous client TCP connections. Beyond
	// the cap, new connections get a BROKER_BUSY error frame and are
	// closed (env BROKER_MAX_CONNECTIONS, default 1024).
	MaxConnections int
	// MaxTopics caps topic creation (env BROKER_MAX_TOPICS, default
	// 1024). Every topic costs directories, file handles and one writer
	// goroutine per partition, so unauthenticated creation cannot be
	// free.
	MaxTopics int
	// MaxGroups caps live consumer-group states (env BROKER_MAX_GROUPS,
	// default 1024). Memberless groups are garbage-collected (committed
	// offsets survive), so the cap bounds only groups with state.
	MaxGroups int
	// IdleTimeout closes connections that sent nothing for this long
	// (env BROKER_IDLE_TIMEOUT, default 5m). It is a read deadline
	// refreshed per frame, so active clients never notice it.
	IdleTimeout time.Duration
	// WriteTimeout bounds one frame write to a client (env
	// BROKER_WRITE_TIMEOUT, default 30s). A client that stops reading
	// loses its connection instead of pinning a goroutine forever.
	WriteTimeout time.Duration
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
		MaxConnections:     config.GetInt("BROKER_MAX_CONNECTIONS", 1024),
		MaxTopics:          config.GetInt("BROKER_MAX_TOPICS", 1024),
		MaxGroups:          config.GetInt("BROKER_MAX_GROUPS", 1024),
		IdleTimeout:        config.GetDuration("BROKER_IDLE_TIMEOUT", 5*time.Minute),
		WriteTimeout:       config.GetDuration("BROKER_WRITE_TIMEOUT", 30*time.Second),
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
		MaxConnections:     1024,
		MaxTopics:          1024,
		MaxGroups:          1024,
		IdleTimeout:        5 * time.Minute,
		WriteTimeout:       30 * time.Second,
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
	if c.MaxConnections > 0 {
		def.MaxConnections = c.MaxConnections
	}
	if c.MaxTopics > 0 {
		def.MaxTopics = c.MaxTopics
	}
	if c.MaxGroups > 0 {
		def.MaxGroups = c.MaxGroups
	}
	if c.IdleTimeout > 0 {
		def.IdleTimeout = c.IdleTimeout
	}
	if c.WriteTimeout > 0 {
		def.WriteTimeout = c.WriteTimeout
	}
	return def
}
