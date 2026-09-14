// Package broker ties the message broker together: storage, consumer
// groups, the TCP server, the fsync policy and metrics. Broker
// implements server.Backend, so every wire op lands here.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/refleeexzz/RAVEN/internal/broker/group"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
	"github.com/refleeexzz/RAVEN/internal/broker/storage"
)

// topicPart keys the per-partition writer map.
type topicPart struct {
	topic     string
	partition int32
}

// appendRequest is one produce batch waiting for the partition writer.
type appendRequest struct {
	records []storage.Record
	resp    chan appendResult // buffered(1): the writer never blocks on it
}

type appendResult struct {
	base uint64
	err  error
}

// partWriter is the single writer goroutine of one partition. It owns
// the append path: every PRODUCE for the partition flows through its
// bounded channel, which is what guarantees per-partition order and
// gives the broker its backpressure point.
type partWriter struct {
	topic        string
	partitionID  int32
	partition    *storage.Partition
	in           chan appendRequest
	fsyncRecords int
	metrics      *Metrics
	log          *slog.Logger
	done         chan struct{}
}

// run consumes append requests until the channel is closed, then
// drains whatever is left and exits. Every request always gets an
// answer, so handlers never hang during shutdown.
func (pw *partWriter) run() {
	defer close(pw.done)
	for req := range pw.in {
		start := time.Now()
		base, err := pw.partition.Append(req.records)
		if pw.metrics != nil {
			part := strconv.Itoa(int(pw.partitionID))
			pw.metrics.appendLatency.WithLabelValues(pw.topic, part).Observe(time.Since(start).Seconds())
			if err == nil {
				pw.metrics.produced.WithLabelValues(pw.topic, part).Add(float64(len(req.records)))
			}
		}
		req.resp <- appendResult{base: base, err: err}
		if pw.partition.RecordsSinceFlush() >= pw.fsyncRecords {
			if err := pw.partition.Flush(); err != nil {
				pw.log.Error("record-count flush failed", slog.Any("err", err))
			}
		}
	}
}

// Broker is the whole service in one type.
type Broker struct {
	cfg     Config
	log     *slog.Logger
	store   *storage.Store
	groups  *group.Coordinator
	server  *server.Server
	metrics *Metrics

	// retentionFreed counts bytes deleted by retention sweeps
	// (raven_broker_retention_bytes_freed_total). Kept on the Broker, not
	// in Metrics, so metrics.go stays untouched for parallel work.
	retentionFreed *prometheus.CounterVec
	// compactionFreed counts bytes reclaimed by log compaction;
	// compactionDropped counts superseded records removed.
	compactionFreed   *prometheus.CounterVec
	compactionDropped *prometheus.CounterVec

	// tlsReloader swaps the serving certificate on file rotation; nil
	// when TLS is off. Started by Run.
	tlsReloader *certReloader

	// writers holds one partWriter per partition. Channels are closed
	// only during shutdown, after the server has fully drained, so no
	// handler ever sends on a closed channel.
	writersMu sync.RWMutex
	writers   map[topicPart]*partWriter

	flushDone   chan struct{}
	cleanupDone chan struct{}
}

// New builds a broker: opens the store (with crash recovery), loads
// committed offsets, starts per-partition writers and prepares the TCP
// server. reg may be nil (tests).
func New(cfg Config, log *slog.Logger, reg CollectorRegistrar) (*Broker, error) {
	if log == nil {
		log = slog.Default()
	}
	cfg = cfg.withDefaults()
	store, err := storage.OpenStore(cfg.DataDir, storage.Options{
		MaxSegmentBytes:    cfg.MaxSegmentBytes,
		IndexIntervalBytes: cfg.IndexIntervalBytes,
	}, cfg.DefaultPartitions, log)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	partitionsFor := func(topic string) (int, bool) {
		t, err := store.Topic(topic)
		if err != nil {
			return 0, false
		}
		return t.NumPartitions(), true
	}
	groups, err := group.NewCoordinator(cfg.DataDir, partitionsFor, cfg.SessionTimeout, log,
		group.WithMaxGroups(cfg.MaxGroups))
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open coordinator: %w", err)
	}
	// TLS is resolved before the server exists so a bad cert/key/CA
	// fails the boot instead of the first handshake.
	tlsCfg, tlsReloader, err := buildServerTLSConfig(cfg, log)
	if err != nil {
		_ = groups.Close()
		_ = store.Close()
		return nil, err
	}
	// Authentication is resolved the same way: a malformed
	// BROKER_API_KEYS must fail the boot (fail closed) — silently
	// starting in open mode would turn a config typo into an outage of
	// the security model, which nobody notices until it matters.
	if cfg.apiKeysErr != nil {
		_ = groups.Close()
		_ = store.Close()
		return nil, fmt.Errorf("BROKER_API_KEYS: %w", cfg.apiKeysErr)
	}
	var authenticator server.Authenticator
	if len(cfg.APIKeys) > 0 {
		authenticator, err = newStaticAuthenticator(cfg.APIKeys)
		if err != nil {
			_ = groups.Close()
			_ = store.Close()
			return nil, fmt.Errorf("BROKER_API_KEYS: %w", err)
		}
		log.Info("broker API-key authentication enabled", slog.Int("keys", len(cfg.APIKeys)))
	} else {
		log.Warn("broker authentication DISABLED: no BROKER_API_KEYS configured — " +
			"any client that can reach the TCP port can produce, consume and admin. " +
			"Fine for local dev, wrong for anything else.")
	}
	b := &Broker{
		cfg:       cfg,
		log:       log,
		store:     store,
		groups:    groups,
		metrics:   newMetrics(),
		writers:   make(map[topicPart]*partWriter),
		flushDone: make(chan struct{}),
		retentionFreed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "retention_bytes_freed_total",
			Help:      "Total bytes deleted by retention sweeps (.log + .index), by topic and partition.",
		}, []string{"topic", "partition"}),
		compactionFreed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "compaction_bytes_freed_total",
			Help:      "Total bytes reclaimed by log compaction, by topic and partition.",
		}, []string{"topic", "partition"}),
		compactionDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "broker",
			Name:      "compaction_records_dropped_total",
			Help:      "Total superseded records removed by log compaction, by topic and partition.",
		}, []string{"topic", "partition"}),
		tlsReloader: tlsReloader,
		cleanupDone: make(chan struct{}),
	}
	for _, t := range store.Topics() {
		for _, p := range t.Partitions {
			b.registerWriter(t.Name, p)
		}
	}
	// The server must exist before registerMetrics: the
	// active_connections gauge reads b.server at scrape time.
	b.server = server.New(cfg.TCPAddr, b, cfg.DrainTimeout, log,
		server.WithMaxConnections(cfg.MaxConnections),
		server.WithIdleTimeout(cfg.IdleTimeout),
		server.WithWriteTimeout(cfg.WriteTimeout),
		server.WithTLS(tlsCfg),
		server.WithAuthenticator(authenticator),
		server.WithSecurityHooks(server.SecurityHooks{
			OnAuthFailure: func() { b.metrics.authFailures.Inc() },
		}),
	)
	if reg != nil {
		b.registerMetrics(reg)
		reg.Register(b.retentionFreed, b.compactionFreed, b.compactionDropped)
	}
	return b, nil
}

// Addr returns the TCP listen address (after Run started).
func (b *Broker) Addr() string { return b.server.Addr() }

// Run starts the fsync ticker, the group reaper, the retention cleaner
// and the TCP server, and blocks until ctx is cancelled. Shutdown order
// matters:
//
//  1. server stops and drains connections (no handler touches writers)
//  2. cleaner finishes its current sweep step and exits (ctx done)
//  3. writer channels close; writers drain their queues and exit
//  4. store flushes and closes every file
//  5. coordinator compacts the offsets file
func (b *Broker) Run(ctx context.Context) error {
	b.groups.Start(ctx)
	if b.tlsReloader != nil {
		go b.tlsReloader.run(ctx, b.cfg.TLSReloadInterval)
		b.log.Info("broker TLS hot-reload watching",
			slog.String("cert_file", b.cfg.TLSCertFile),
			slog.String("reload_interval", b.cfg.TLSReloadInterval.String()),
			slog.Bool("mtls", b.cfg.TLSClientCAFile != ""))
	}
	go func() {
		defer close(b.flushDone)
		t := time.NewTicker(b.cfg.FsyncEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.store.FlushAll()
			}
		}
	}()
	go func() {
		defer close(b.cleanupDone)
		b.store.RunCleaner(ctx, b.cfg.CleanupInterval, b.cfg.policyFor,
			b.minCommittedSnapshot, b.observeCleanup)
	}()

	serveErr := b.server.Run(ctx)

	<-b.flushDone
	// The cleaner may be mid-sweep holding a partition lock; wait for it
	// before closing the store underneath it.
	<-b.cleanupDone
	b.writersMu.Lock()
	for _, w := range b.writers {
		close(w.in)
	}
	writers := make([]*partWriter, 0, len(b.writers))
	for _, w := range b.writers {
		writers = append(writers, w)
	}
	b.writersMu.Unlock()
	for _, w := range writers {
		<-w.done
	}
	if err := b.store.Close(); err != nil {
		b.log.Error("store close", slog.Any("err", err))
	}
	if err := b.groups.Close(); err != nil {
		b.log.Error("coordinator close", slog.Any("err", err))
	}
	b.log.Info("broker stopped")
	return serveErr
}

// registerWriter starts the writer goroutine for one partition.
func (b *Broker) registerWriter(topic string, p *storage.Partition) {
	w := &partWriter{
		topic:        topic,
		partitionID:  p.ID(),
		partition:    p,
		in:           make(chan appendRequest, b.cfg.ProduceQueueSize),
		fsyncRecords: b.cfg.FsyncRecords,
		metrics:      b.metrics,
		log:          b.log.With(slog.String("topic", topic), slog.Int("partition", int(p.ID()))),
		done:         make(chan struct{}),
	}
	b.writersMu.Lock()
	b.writers[topicPart{topic, p.ID()}] = w
	b.writersMu.Unlock()
	go w.run()
}

// Healthy is the readiness check: the data dir must be accessible.
func (b *Broker) Healthy(_ context.Context) error {
	if _, err := os.Stat(b.cfg.DataDir); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}
	return nil
}

// ---- server.Backend implementation ----

func (b *Broker) CreateTopic(_ context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	// BRKR-04: bound the number of topics. Each topic costs directories,
	// file handles and one writer goroutine per partition, so topic
	// creation cannot be free. The existence check comes first so a
	// duplicate create still answers TOPIC_EXISTS at the cap. The count
	// check races with concurrent creates by design: it is load
	// shedding, not an exact quota.
	if _, err := b.store.Topic(req.Topic); errors.Is(err, storage.ErrTopicNotFound) && len(b.store.Topics()) >= b.cfg.MaxTopics {
		return nil, protocol.NewError(protocol.CodeBadRequest,
			fmt.Sprintf("topic limit reached (%d)", b.cfg.MaxTopics))
	}
	t, err := b.store.CreateTopic(req.Topic, req.Partitions)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrTopicExists):
			return nil, protocol.NewError(protocol.CodeTopicExists, req.Topic)
		case errors.Is(err, storage.ErrInvalidTopicName):
			return nil, protocol.NewError(protocol.CodeBadRequest, err.Error())
		case errors.Is(err, storage.ErrTooManyPartitions):
			return nil, protocol.NewError(protocol.CodeBadRequest, err.Error())
		default:
			return nil, fmt.Errorf("create topic: %w", err)
		}
	}
	for _, p := range t.Partitions {
		b.registerWriter(t.Name, p)
	}
	b.log.Info("topic created", slog.String("topic", t.Name), slog.Int("partitions", t.NumPartitions()))
	return &protocol.CreateTopicResponse{Topic: t.Name, Partitions: int32(t.NumPartitions())}, nil
}

func (b *Broker) ListTopics(_ context.Context, _ *protocol.ListTopicsRequest) (*protocol.ListTopicsResponse, error) {
	topics := b.store.Topics()
	resp := &protocol.ListTopicsResponse{Topics: make([]protocol.TopicInfo, 0, len(topics))}
	for _, t := range topics {
		info := protocol.TopicInfo{Name: t.Name, Partitions: make([]protocol.PartitionInfo, 0, len(t.Partitions))}
		for _, p := range t.Partitions {
			info.Partitions = append(info.Partitions, protocol.PartitionInfo{ID: p.ID(), HighWatermark: p.HighWatermark()})
		}
		resp.Topics = append(resp.Topics, info)
	}
	return resp, nil
}

func (b *Broker) Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	if len(req.Records) == 0 {
		return nil, protocol.NewError(protocol.CodeBadRequest, "empty produce batch")
	}
	topic, err := b.store.Topic(req.Topic)
	if errors.Is(err, storage.ErrTopicNotFound) {
		return nil, protocol.NewError(protocol.CodeTopicNotFound, req.Topic)
	}
	if err != nil {
		return nil, fmt.Errorf("produce lookup: %w", err)
	}
	n := int32(topic.NumPartitions())

	// Assign a partition per record up front. Per-key hashing keeps
	// per-key order; round-robin spreads keyless records.
	parts := make([]int32, len(req.Records))
	for i := range req.Records {
		p := req.Partition
		if p < 0 {
			p = topic.PickPartition(req.Records[i].Key)
		}
		if p >= n {
			return nil, protocol.NewError(protocol.CodeBadRequest,
				fmt.Sprintf("partition %d out of range (topic has %d)", p, n))
		}
		parts[i] = p
	}

	// Group records by partition, remembering original positions.
	type indexed struct {
		idx int
		rec storage.Record
	}
	batches := make(map[int32][]indexed)
	order := make([]int32, 0, 4)
	for i := range req.Records {
		m := &req.Records[i]
		p := parts[i]
		if _, seen := batches[p]; !seen {
			order = append(order, p)
		}
		batches[p] = append(batches[p], indexed{idx: i, rec: storage.Record{
			Key:     m.Key,
			Value:   m.Value,
			Headers: convertHeaders(m.Headers),
		}})
	}

	resp := &protocol.ProduceResponse{Results: make([]protocol.ProduceResult, len(req.Records))}
	for _, p := range order {
		batch := batches[p]
		w := b.writerFor(req.Topic, p)
		if w == nil {
			return nil, protocol.NewError(protocol.CodeInternal, "no writer for partition")
		}
		recs := make([]storage.Record, len(batch))
		for j, ir := range batch {
			recs[j] = ir.rec
		}
		areq := appendRequest{records: recs, resp: make(chan appendResult, 1)}
		// Bounded queue = backpressure: a full writer rejects the
		// produce instead of growing memory.
		select {
		case w.in <- areq:
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, protocol.NewError(protocol.CodeBrokerBusy, "produce queue full")
		}
		select {
		case res := <-areq.resp:
			if res.err != nil {
				return nil, fmt.Errorf("append to %s/%d: %w", req.Topic, p, res.err)
			}
			for j, ir := range batch {
				resp.Results[ir.idx] = protocol.ProduceResult{Partition: p, Offset: res.base + uint64(j)}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return resp, nil
}

func (b *Broker) Fetch(_ context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	topic, err := b.store.Topic(req.Topic)
	if errors.Is(err, storage.ErrTopicNotFound) {
		return nil, protocol.NewError(protocol.CodeTopicNotFound, req.Topic)
	}
	if err != nil {
		return nil, fmt.Errorf("fetch lookup: %w", err)
	}
	if req.Partition < 0 || req.Partition >= int32(topic.NumPartitions()) {
		return nil, protocol.NewError(protocol.CodeBadRequest,
			fmt.Sprintf("partition %d out of range", req.Partition))
	}
	if req.Group != "" {
		if err := b.groups.CheckFetch(req.Group, req.MemberID, req.Generation); err != nil {
			return nil, protocol.NewError(protocol.CodeRebalance,
				"stale generation or unknown member: re-join the group")
		}
	}
	p := topic.Partitions[req.Partition]
	maxRecords := int(req.MaxRecords)
	if maxRecords <= 0 {
		maxRecords = 100
	}
	if maxRecords > 10000 {
		maxRecords = 10000
	}
	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	// The response must fit in one 4 MiB frame; leave headroom.
	if maxBytes > 3<<20 {
		maxBytes = 3 << 20
	}
	recs, err := p.Read(req.Offset, maxRecords, maxBytes)
	if errors.Is(err, storage.ErrOffsetOutOfRange) {
		return nil, protocol.NewError(protocol.CodeOffsetOutOfRange,
			fmt.Sprintf("offset %d beyond high-water mark %d", req.Offset, p.HighWatermark()))
	}
	if err != nil {
		return nil, fmt.Errorf("fetch read: %w", err)
	}
	resp := &protocol.FetchResponse{
		HighWatermark: p.HighWatermark(),
		Records:       make([]protocol.FetchedMessage, 0, len(recs)),
	}
	for _, r := range recs {
		resp.Records = append(resp.Records, protocol.FetchedMessage{
			Offset:    r.Offset,
			Timestamp: r.TimestampMs,
			Key:       r.Key,
			Value:     r.Value,
			Headers:   convertHeadersBack(r.Headers),
		})
	}
	if len(recs) > 0 {
		part := strconv.Itoa(int(req.Partition))
		b.metrics.fetched.WithLabelValues(req.Topic, part).Add(float64(len(recs)))
		b.metrics.consumed.WithLabelValues(req.Topic, part).Add(float64(len(recs)))
	}
	return resp, nil
}

func (b *Broker) CommitOffset(_ context.Context, req *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error) {
	t, err := b.checkTopicPartition(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}
	// BRKR-02: never commit past the high-water mark. A committed offset
	// beyond the HWM wedges the group: every later FETCH answers
	// OFFSET_OUT_OF_RANGE and the consumer can make no progress. The
	// check is taken at request time; the HWM only grows, so a race with
	// a concurrent produce can at most accept an offset that was valid a
	// moment ago.
	hwm := t.Partitions[req.Partition].HighWatermark()
	if req.Offset > hwm {
		return nil, protocol.NewError(protocol.CodeOffsetOutOfRange,
			fmt.Sprintf("offset %d beyond high-water mark %d", req.Offset, hwm))
	}
	err = b.groups.Commit(req.Group, req.MemberID, req.Topic, req.Partition, req.Offset, req.Generation)
	switch {
	case errors.Is(err, group.ErrRebalance):
		return nil, protocol.NewError(protocol.CodeRebalance, "stale generation: re-join the group")
	case errors.Is(err, group.ErrUnknownMember):
		return nil, protocol.NewError(protocol.CodeUnknownMember, req.MemberID)
	case errors.Is(err, group.ErrNotAssigned):
		return nil, protocol.NewError(protocol.CodeBadRequest,
			fmt.Sprintf("partition %s/%d is not assigned to member %s", req.Topic, req.Partition, req.MemberID))
	case err != nil:
		return nil, fmt.Errorf("commit offset: %w", err)
	}
	return &protocol.CommitOffsetResponse{}, nil
}

func (b *Broker) FetchOffset(_ context.Context, req *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error) {
	if _, err := b.checkTopicPartition(req.Topic, req.Partition); err != nil {
		return nil, err
	}
	// BRKR-03: offset reads are membership-bound. Without this any
	// client could read any group's committed offsets by naming its id.
	err := b.groups.CheckOffsetAccess(req.Group, req.MemberID, req.Generation)
	switch {
	case errors.Is(err, group.ErrUnknownMember):
		return nil, protocol.NewError(protocol.CodeUnknownMember, req.MemberID)
	case errors.Is(err, group.ErrRebalance):
		return nil, protocol.NewError(protocol.CodeRebalance, "unknown group or stale generation: re-join")
	case err != nil:
		return nil, fmt.Errorf("fetch offset: %w", err)
	}
	return &protocol.FetchOffsetResponse{Offset: b.groups.Committed(req.Group, req.Topic, req.Partition)}, nil
}

func (b *Broker) JoinGroup(_ context.Context, req *protocol.JoinGroupRequest) (*protocol.JoinGroupResponse, error) {
	gen, assignments, err := b.groups.Join(req.Group, req.MemberID, req.Topics)
	if errors.Is(err, group.ErrUnknownTopic) {
		return nil, protocol.NewError(protocol.CodeTopicNotFound, err.Error())
	}
	if err != nil {
		return nil, protocol.NewError(protocol.CodeBadRequest, err.Error())
	}
	return &protocol.JoinGroupResponse{
		Generation:  gen,
		MemberID:    req.MemberID,
		Assignments: assignments,
	}, nil
}

func (b *Broker) LeaveGroup(_ context.Context, req *protocol.LeaveGroupRequest) (*protocol.LeaveGroupResponse, error) {
	if err := b.groups.Leave(req.Group, req.MemberID); err != nil {
		return nil, fmt.Errorf("leave group: %w", err)
	}
	return &protocol.LeaveGroupResponse{}, nil
}

func (b *Broker) Heartbeat(_ context.Context, req *protocol.HeartbeatRequest) (*protocol.HeartbeatResponse, error) {
	err := b.groups.Heartbeat(req.Group, req.MemberID, req.Generation)
	if errors.Is(err, group.ErrRebalance) {
		return nil, protocol.NewError(protocol.CodeRebalance, "stale generation or unknown member: re-join")
	}
	if err != nil {
		return nil, fmt.Errorf("heartbeat: %w", err)
	}
	return &protocol.HeartbeatResponse{}, nil
}

// ---- helpers ----

func (b *Broker) checkTopicPartition(topic string, partition int32) (*storage.Topic, error) {
	t, err := b.store.Topic(topic)
	if errors.Is(err, storage.ErrTopicNotFound) {
		return nil, protocol.NewError(protocol.CodeTopicNotFound, topic)
	}
	if err != nil {
		return nil, err
	}
	if partition < 0 || partition >= int32(t.NumPartitions()) {
		return nil, protocol.NewError(protocol.CodeBadRequest, fmt.Sprintf("partition %d out of range", partition))
	}
	return t, nil
}

func (b *Broker) writerFor(topic string, partition int32) *partWriter {
	b.writersMu.RLock()
	defer b.writersMu.RUnlock()
	return b.writers[topicPart{topic, partition}]
}

// minCommittedSnapshot feeds the retention cleaner: for every
// topic/partition, the smallest committed offset across every consumer
// group with a commit there. Retention never deletes at or above it, so
// a committed group can never be stranded on OFFSET_OUT_OF_RANGE by a
// cleanup sweep. Partitions with no commits are absent from the map and
// get no protection.
func (b *Broker) minCommittedSnapshot() map[string]map[int32]uint64 {
	out := make(map[string]map[int32]uint64)
	for _, g := range b.groups.OffsetGroupIDs() {
		for topic, parts := range b.groups.GroupOffsets(g) {
			for part, off := range parts {
				m, ok := out[topic]
				if !ok {
					m = make(map[int32]uint64)
					out[topic] = m
				}
				if cur, ok := m[part]; !ok || off < cur {
					m[part] = off
				}
			}
		}
	}
	return out
}

// observeCleanup turns one partition's sweep stats into metrics.
func (b *Broker) observeCleanup(stats storage.CleanupStats) {
	part := strconv.Itoa(int(stats.Partition))
	if stats.Retention.BytesFreed > 0 {
		b.retentionFreed.WithLabelValues(stats.Topic, part).Add(float64(stats.Retention.BytesFreed))
	}
	if stats.Compaction.BytesFreed > 0 {
		b.compactionFreed.WithLabelValues(stats.Topic, part).Add(float64(stats.Compaction.BytesFreed))
	}
	if stats.Compaction.RecordsDropped > 0 {
		b.compactionDropped.WithLabelValues(stats.Topic, part).Add(float64(stats.Compaction.RecordsDropped))
	}
}

func convertHeaders(hs []protocol.Header) []storage.Header {
	if len(hs) == 0 {
		return nil
	}
	out := make([]storage.Header, len(hs))
	for i, h := range hs {
		out[i] = storage.Header{Key: h.Key, Value: h.Value}
	}
	return out
}

func convertHeadersBack(hs []storage.Header) []protocol.Header {
	if len(hs) == 0 {
		return nil
	}
	out := make([]protocol.Header, len(hs))
	for i, h := range hs {
		out[i] = protocol.Header{Key: h.Key, Value: h.Value}
	}
	return out
}

// ---- ops snapshot (feeds GET /topics and the console UI) ----

// GroupOffsetStatus is one group's committed position on a partition.
type GroupOffsetStatus struct {
	Committed uint64 `json:"committed"`
	Lag       uint64 `json:"lag"`
}

// PartitionStatus reports a partition's high-water mark plus every
// group's committed offset and lag.
type PartitionStatus struct {
	ID            int32                        `json:"id"`
	HighWatermark uint64                       `json:"high_watermark"`
	Groups        map[string]GroupOffsetStatus `json:"groups,omitempty"`
}

// TopicStatus is one topic in the ops snapshot.
type TopicStatus struct {
	Name       string            `json:"name"`
	Partitions []PartitionStatus `json:"partitions"`
}

// GroupStatus reports one consumer group.
type GroupStatus struct {
	ID         string `json:"id"`
	Generation int32  `json:"generation"`
	Members    int    `json:"members"`
}

// Status is the GET /topics payload.
type Status struct {
	Topics []TopicStatus `json:"topics"`
	Groups []GroupStatus `json:"groups"`
}

// Status builds the ops snapshot.
func (b *Broker) Status() Status {
	st := Status{Topics: []TopicStatus{}, Groups: []GroupStatus{}}
	groupIDs := b.groups.OffsetGroupIDs()
	snapshots := make(map[string]map[string]map[int32]uint64, len(groupIDs))
	for _, g := range groupIDs {
		snapshots[g] = b.groups.GroupOffsets(g)
	}
	for _, t := range b.store.Topics() {
		ts := TopicStatus{Name: t.Name, Partitions: make([]PartitionStatus, 0, len(t.Partitions))}
		for _, p := range t.Partitions {
			hwm := p.HighWatermark()
			ps := PartitionStatus{ID: p.ID(), HighWatermark: hwm}
			for _, g := range groupIDs {
				topics := snapshots[g]
				committed, ok := topics[t.Name][p.ID()]
				if !ok {
					continue
				}
				lag := uint64(0)
				if hwm > committed {
					lag = hwm - committed
				}
				if ps.Groups == nil {
					ps.Groups = make(map[string]GroupOffsetStatus)
				}
				ps.Groups[g] = GroupOffsetStatus{Committed: committed, Lag: lag}
			}
			ts.Partitions = append(ts.Partitions, ps)
		}
		st.Topics = append(st.Topics, ts)
	}
	for _, gi := range b.groups.GroupsInfo() {
		st.Groups = append(st.Groups, GroupStatus{ID: gi.ID, Generation: gi.Generation, Members: gi.Members})
	}
	return st
}
