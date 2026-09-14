package broker

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/storage"
	"github.com/refleeexzz/RAVEN/internal/config"
)

// TopicOverride holds per-topic cleanup settings. It rides the
// BROKER_TOPIC_CONFIGS env var as JSON, e.g.:
//
//	BROKER_TOPIC_CONFIGS={"jobs-dlq":{"retention_ms":86400000},"state":{"compact":true}}
//
// Fields absent from the JSON inherit the global defaults
// (RetentionMaxAge / RetentionMaxBytes). Overrides for topics that do
// not exist are ignored at sweep time. Compaction is opt-in per topic
// only — there is deliberately no global "compact everything" switch.
type TopicOverride struct {
	RetentionMs    *int64 `json:"retention_ms,omitempty"`
	RetentionBytes *int64 `json:"retention_bytes,omitempty"`
	Compact        bool   `json:"compact,omitempty"`
}

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
	// RetentionMaxAge is the global default for time-based retention:
	// closed segments whose newest record is older than this are deleted
	// (env BROKER_RETENTION_MS, default 0 = disabled). The active
	// segment is never deleted, and committed consumer offsets are
	// always protected (see storage.ApplyRetention).
	RetentionMaxAge time.Duration
	// RetentionMaxBytes is the global default for size-based retention:
	// while a partition exceeds this many log bytes, the oldest closed
	// segments are deleted (env BROKER_RETENTION_BYTES, default 0 =
	// disabled). Same safety rules as RetentionMaxAge.
	RetentionMaxBytes int64
	// CleanupInterval is the retention/compaction sweep cadence (env
	// BROKER_CLEANUP_INTERVAL_MS, default 300000 = 5m).
	CleanupInterval time.Duration
	// TopicConfigs holds per-topic cleanup overrides (env
	// BROKER_TOPIC_CONFIGS, JSON object keyed by topic name).
	TopicConfigs map[string]TopicOverride
	// TLSCertFile and TLSKeyFile enable TLS on the client listener when
	// both are set (env BROKER_TLS_CERT_FILE / BROKER_TLS_KEY_FILE).
	// Both empty (the default) keeps plaintext for dev compatibility.
	TLSCertFile string
	TLSKeyFile  string
	// TLSClientCAFile turns on mTLS: every client must present a
	// certificate signed by this CA (env BROKER_TLS_CLIENT_CA_FILE,
	// RequireAndVerifyClientCert). Only meaningful with TLS on.
	TLSClientCAFile string
	// TLSReloadInterval is the certificate hot-reload poll cadence (env
	// BROKER_TLS_RELOAD_SEC, default 5s). See certReloader for why this
	// is polling rather than stat-per-handshake.
	TLSReloadInterval time.Duration
}

// policyFor resolves the effective cleanup policy for one topic: global
// defaults plus the per-topic override, when one exists.
func (c Config) policyFor(topic string) storage.Policy {
	pol := storage.Policy{
		RetentionMaxAge:   c.RetentionMaxAge,
		RetentionMaxBytes: c.RetentionMaxBytes,
	}
	if ov, ok := c.TopicConfigs[topic]; ok {
		if ov.RetentionMs != nil {
			pol.RetentionMaxAge = time.Duration(*ov.RetentionMs) * time.Millisecond
		}
		if ov.RetentionBytes != nil {
			pol.RetentionMaxBytes = *ov.RetentionBytes
		}
		pol.Compact = ov.Compact
	}
	return pol
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
		RetentionMaxAge:    time.Duration(config.GetInt("BROKER_RETENTION_MS", 0)) * time.Millisecond,
		RetentionMaxBytes:  int64(config.GetInt("BROKER_RETENTION_BYTES", 0)),
		CleanupInterval:    time.Duration(config.GetInt("BROKER_CLEANUP_INTERVAL_MS", 300000)) * time.Millisecond,
		TopicConfigs:       parseTopicConfigs(config.Get("BROKER_TOPIC_CONFIGS", "")),
		TLSCertFile:        config.Get("BROKER_TLS_CERT_FILE", ""),
		TLSKeyFile:         config.Get("BROKER_TLS_KEY_FILE", ""),
		TLSClientCAFile:    config.Get("BROKER_TLS_CLIENT_CA_FILE", ""),
		TLSReloadInterval:  time.Duration(config.GetInt("BROKER_TLS_RELOAD_SEC", 5)) * time.Second,
	}
}

// parseTopicConfigs decodes BROKER_TOPIC_CONFIGS. Bad JSON is not fatal:
// the broker keeps its global defaults and warns loudly, because a
// broker that refuses to boot over a typo is worse than one that runs
// with defaults and tells you.
func parseTopicConfigs(raw string) map[string]TopicOverride {
	if raw == "" {
		return nil
	}
	var out map[string]TopicOverride
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		slog.Default().Warn("BROKER_TOPIC_CONFIGS is not valid JSON; ignoring it",
			slog.Any("err", err))
		return nil
	}
	return out
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
		CleanupInterval:    5 * time.Minute,
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
	// Retention defaults are "disabled", which is the zero value, so
	// there is nothing to fill: an explicit zero keeps cleanup off.
	def.RetentionMaxAge = c.RetentionMaxAge
	def.RetentionMaxBytes = c.RetentionMaxBytes
	if c.CleanupInterval > 0 {
		def.CleanupInterval = c.CleanupInterval
	}
	if c.TopicConfigs != nil {
		def.TopicConfigs = c.TopicConfigs
	}
	// TLS is opt-in: empty file paths pass through as "off".
	def.TLSCertFile = c.TLSCertFile
	def.TLSKeyFile = c.TLSKeyFile
	def.TLSClientCAFile = c.TLSClientCAFile
	def.TLSReloadInterval = 5 * time.Second
	if c.TLSReloadInterval > 0 {
		def.TLSReloadInterval = c.TLSReloadInterval
	}
	return def
}
