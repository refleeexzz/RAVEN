package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/cluster"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/storage"
)

// This file is the replication data plane: the leader side of
// REPLICATE, the per-partition follower loops, topic metadata sync and
// the committed-offset bookkeeping the Fetch path caps reads at.
//
// Roles come from cluster.AssignmentFor: the leader serves REPLICATE
// and tracks each follower's progress; followers pull in a loop and
// apply batches with AppendReplica, truncating to the leader's
// committed offset whenever the logs diverge (log matching).
//
// In this sub-feature leadership is the static ring placement; the
// election layer (docs/raft.md) takes over leader selection later
// without changing anything below the AssignmentFor line.

// partitionRepl is the replication runtime of one partition on this
// node.
type partitionRepl struct {
	// committed is the stable high-water mark known by this node. On
	// the leader it is quorum-driven: it advances only while a majority
	// of the replica set (the ISR) confirms the offset. On a follower
	// it is learned from the leader. It never decreases.
	committed atomic.Uint64

	// commitCh is a generation channel: closed and replaced on every
	// committed raise, so acks=all waiters wake up. Guarded by mu.
	commitCh chan struct{}

	// Leader-side per-replica progress: the last fetch offset each
	// replica reported. Drives lag metrics, the ISR set and the
	// quorum-commit computation.
	mu              sync.Mutex
	followerOffsets map[cluster.NodeID]uint64
}

// replicationManager owns every replication loop and handler.
type replicationManager struct {
	b   *Broker
	cl  *cluster.Cluster
	cfg cluster.Config
	log *slog.Logger

	mu        sync.Mutex
	parts     map[topicPart]*partitionRepl
	followCtl map[topicPart]context.CancelFunc

	wg sync.WaitGroup
}

// clusterStatusResponse is the CLUSTER_STATUS ops payload. It is also
// how followers discover topics created on other nodes (topic sync).
type clusterStatusResponse struct {
	Node       cluster.NodeID           `json:"node"`
	Members    []cluster.Member         `json:"members"`
	Topics     []clusterStatusTopic     `json:"topics"`
	Partitions []clusterStatusPartition `json:"partitions"`
}

type clusterStatusTopic struct {
	Name       string `json:"name"`
	Partitions int32  `json:"partitions"`
}

type followerProgress struct {
	ID     cluster.NodeID `json:"id"`
	Offset uint64         `json:"offset"`
	Lag    uint64         `json:"lag"`
	InISR  bool           `json:"in_isr"`
}

type clusterStatusPartition struct {
	Topic     string             `json:"topic"`
	Partition int32              `json:"partition"`
	Leader    cluster.NodeID     `json:"leader"`
	Replicas  []cluster.NodeID   `json:"replicas"`
	ISR       []cluster.NodeID   `json:"isr"`
	Role      string             `json:"role"` // "leader" or "follower"
	HWM       uint64             `json:"hwm"`
	Committed uint64             `json:"committed"`
	Followers []followerProgress `json:"followers,omitempty"`
}

// newReplicationManager builds the manager and registers the cluster
// opcode handlers. Called from New only in cluster mode.
func newReplicationManager(b *Broker) *replicationManager {
	m := &replicationManager{
		b:         b,
		cl:        b.cluster,
		cfg:       b.cluster.Config(),
		log:       b.log.With(slog.String("component", "replication")),
		parts:     make(map[topicPart]*partitionRepl),
		followCtl: make(map[topicPart]context.CancelFunc),
	}
	b.cluster.Handle(protocol.OpReplicate, m.handleReplicate)
	b.cluster.Handle(protocol.OpClusterStatus, m.handleClusterStatus)
	return m
}

// run starts the reconcile and topic-sync loops. They exit with ctx;
// wait blocks until they (and every follower loop) have stopped.
func (m *replicationManager) run(ctx context.Context) {
	m.wg.Add(2)
	go func() {
		defer m.wg.Done()
		m.reconcileLoop(ctx)
	}()
	go func() {
		defer m.wg.Done()
		m.topicSyncLoop(ctx)
	}()
}

// wait blocks until every replication goroutine has exited. The broker
// calls it before closing the store.
func (m *replicationManager) wait() { m.wg.Wait() }

// part returns (creating on demand) the partition's replication state.
func (m *replicationManager) part(topic string, partition int32) *partitionRepl {
	key := topicPart{topic, partition}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.parts[key]
	if !ok {
		r = &partitionRepl{
			commitCh:        make(chan struct{}),
			followerOffsets: make(map[cluster.NodeID]uint64),
		}
		m.parts[key] = r
	}
	return r
}

// committedFor returns the committed offset the Fetch path caps reads
// at — the quorum-confirmed stable mark (leader) or the value learned
// from the leader (follower), never above the local log end.
func (m *replicationManager) committedFor(topic string, partition int32) uint64 {
	t, err := m.b.store.Topic(topic)
	if err != nil || partition >= int32(t.NumPartitions()) {
		return 0
	}
	p := t.Partitions[partition]
	c := m.part(topic, partition).committed.Load()
	if hwm := p.HighWatermark(); c > hwm {
		return hwm
	}
	return c
}

// setCommitted raises the locally known committed offset. It never
// decreases: committed is a stable mark. Every raise wakes acks=all
// waiters (leader) and updates the committed metric (both roles).
func (m *replicationManager) setCommitted(topic string, partition int32, off uint64) {
	r := m.part(topic, partition)
	for {
		cur := r.committed.Load()
		if off <= cur {
			return
		}
		if r.committed.CompareAndSwap(cur, off) {
			r.mu.Lock()
			close(r.commitCh)
			r.commitCh = make(chan struct{})
			r.mu.Unlock()
			m.cl.ObserveCommittedOffset(topic, partition, off)
			return
		}
	}
}

// waitCommitted blocks until the partition's committed offset reaches
// off, the quorum-ack deadline passes, or ctx ends. This is the
// acks=all confirmation path: NOT_ENOUGH_REPLICAS means the batch is on
// the leader's WAL but the ISR quorum could not confirm it in time —
// it commits later, when the ISR recovers (same contract Kafka gives).
func (m *replicationManager) waitCommitted(ctx context.Context, topic string, partition int32, off uint64) error {
	timer := time.NewTimer(m.cfg.QuorumAckTimeout)
	defer timer.Stop()
	for {
		if m.committedFor(topic, partition) >= off {
			return nil
		}
		r := m.part(topic, partition)
		r.mu.Lock()
		ch := r.commitCh
		r.mu.Unlock()
		select {
		case <-ch:
			continue
		case <-timer.C:
			return protocol.NewError(protocol.CodeNotEnoughReplicas,
				fmt.Sprintf("partition %s/%d: ISR quorum did not confirm offset %d within %s",
					topic, partition, off, m.cfg.QuorumAckTimeout))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// maybeAdvanceCommit recomputes the leader-side committed offset: the
// minimum offset across the ISR, advanced only while the ISR holds a
// majority of the replica set (quorum = floor(R/2)+1). A slow or dead
// replica leaves the ISR (lag threshold / liveness) and the quorum
// recomputes without it — that is what lets a 3-replica partition keep
// committing with one replica down, and what freezes commits (never
// rolls them back) when only a minority remains.
func (m *replicationManager) maybeAdvanceCommit(topic string, partition int32) {
	if !m.cl.IsLeaderFor(topic, partition) {
		return
	}
	t, err := m.b.store.Topic(topic)
	if err != nil || partition >= int32(t.NumPartitions()) {
		return
	}
	p := t.Partitions[partition]
	hwm := p.HighWatermark()
	a := m.cl.AssignmentFor(topic, partition)
	quorum := len(a.Replicas)/2 + 1
	progress := m.followerProgressSnapshot(topic, partition)
	isrMin := hwm // the leader's own offset
	isrSize := 1
	for _, id := range a.Replicas {
		if id == m.cl.Self() {
			continue
		}
		off, ok := progress[id]
		if !ok || !m.cl.MemberState(id).Alive() {
			continue // not in the ISR: absent, dead or leaving
		}
		if hwm-off > m.cfg.ReplicaLagMax {
			continue // lagging beyond the ISR threshold
		}
		isrSize++
		if off < isrMin {
			isrMin = off
		}
	}
	if isrSize < quorum {
		return // frozen until the ISR recovers a majority
	}
	m.setCommitted(topic, partition, isrMin)
}

// ---- leader side: REPLICATE handler ----

func (m *replicationManager) handleReplicate(_ context.Context, f *protocol.Frame) *protocol.Frame {
	var req protocol.ReplicateRequest
	if err := json.Unmarshal(f.Payload, &req); err != nil {
		return errFrame(f, protocol.CodeBadRequest, err.Error())
	}
	respond := func(status byte, first, hwm, committed uint64, recs []protocol.FetchedMessage) *protocol.Frame {
		return &protocol.Frame{
			Opcode:        protocol.OpReplicate,
			CorrelationID: f.CorrelationID,
			Payload: protocol.EncodeReplicateResponse(&protocol.ReplicateResponse{
				Status: status, FirstOffset: first, HighWatermark: hwm,
				Committed: committed, Records: recs,
			}),
		}
	}
	if !m.cl.IsLeaderFor(req.Topic, req.Partition) {
		return respond(protocol.ReplicateNotLeader, 0, 0, 0, nil)
	}
	t, err := m.b.store.Topic(req.Topic)
	if errors.Is(err, storage.ErrTopicNotFound) {
		return respond(protocol.ReplicateTopicUnknown, 0, 0, 0, nil)
	}
	if err != nil || req.Partition < 0 || req.Partition >= int32(t.NumPartitions()) {
		return respond(protocol.ReplicateTopicUnknown, 0, 0, 0, nil)
	}
	p := t.Partitions[req.Partition]
	hwm := p.HighWatermark()
	first := p.FirstOffset()
	committed := m.committedFor(req.Topic, req.Partition)

	// Divergence: the follower holds records the leader does not (they
	// were never committed) — tell it to truncate to our end.
	if req.Offset > hwm {
		return respond(protocol.ReplicateTruncate, first, hwm, committed, nil)
	}
	maxRecords := int(req.MaxRecords)
	if maxRecords <= 0 || maxRecords > 10000 {
		maxRecords = 1000
	}
	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 || maxBytes > m.cfg.FetchMaxBytes {
		maxBytes = m.cfg.FetchMaxBytes
	}
	recs, err := p.Read(req.Offset, maxRecords, maxBytes)
	if errors.Is(err, storage.ErrOffsetOutOfRange) {
		// The follower sits below our first offset (retention deleted
		// records it never fetched): it must jump forward.
		return respond(protocol.ReplicateTruncate, first, hwm, committed, nil)
	}
	if err != nil {
		return errFrame(f, protocol.CodeInternal, fmt.Sprintf("replicate read: %v", err))
	}
	out := make([]protocol.FetchedMessage, 0, len(recs))
	for _, r := range recs {
		out = append(out, protocol.FetchedMessage{
			Offset:    r.Offset,
			Timestamp: r.TimestampMs,
			Key:       r.Key,
			Value:     r.Value,
			Headers:   convertHeadersBack(r.Headers),
		})
	}
	m.noteFollowerProgress(req.Topic, req.Partition, cluster.NodeID(req.From),
		req.Offset+uint64(len(recs)), hwm)
	return respond(protocol.ReplicateOK, first, hwm, committed, out)
}

// noteFollowerProgress records where a replica stands (leader side) and
// feeds the lag metrics.
func (m *replicationManager) noteFollowerProgress(topic string, partition int32,
	id cluster.NodeID, offset, hwm uint64) {
	if id == "" || id == m.cl.Self() {
		return
	}
	r := m.part(topic, partition)
	r.mu.Lock()
	r.followerOffsets[id] = offset
	r.mu.Unlock()
	lag := uint64(0)
	if hwm > offset {
		lag = hwm - offset
	}
	m.cl.ObserveFollowerOffset(topic, partition, id, offset)
	m.cl.ObserveReplicationLag(topic, partition, id, lag)
	// Every follower progress report may move the quorum commit forward.
	m.maybeAdvanceCommit(topic, partition)
}

// followerProgressSnapshot copies the leader-side progress table.
func (m *replicationManager) followerProgressSnapshot(topic string, partition int32) map[cluster.NodeID]uint64 {
	r := m.part(topic, partition)
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[cluster.NodeID]uint64, len(r.followerOffsets))
	for id, off := range r.followerOffsets {
		out[id] = off
	}
	return out
}

// isr computes the in-sync replica set for the status surface: the
// leader plus every replica whose known offset trails by at most
// ReplicaLagMax and whose node is alive. The acks sub-feature reuses
// this exact rule for quorum decisions.
func (m *replicationManager) isr(a cluster.Assignment, hwm uint64) []cluster.NodeID {
	progress := m.followerProgressSnapshot(a.Topic, a.Partition)
	out := make([]cluster.NodeID, 0, len(a.Replicas))
	for _, id := range a.Replicas {
		if id == m.cl.Self() {
			out = append(out, id)
			continue
		}
		off, ok := progress[id]
		if !ok || !m.cl.MemberState(id).Alive() {
			continue
		}
		if hwm-off <= m.cfg.ReplicaLagMax {
			out = append(out, id)
		}
	}
	return out
}

// ---- follower side ----

// reconcileLoop keeps the running follower loops in sync with the
// current assignments: start loops for partitions this node follows,
// stop loops for partitions it now leads (or no longer holds).
func (m *replicationManager) reconcileLoop(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	m.reconcile(ctx)
	for {
		select {
		case <-ctx.Done():
			m.stopAllFollowers()
			return
		case <-t.C:
			m.reconcile(ctx)
		}
	}
}

func (m *replicationManager) reconcile(ctx context.Context) {
	self := m.cl.Self()
	for _, topic := range m.b.store.Topics() {
		for _, p := range topic.Partitions {
			key := topicPart{topic.Name, p.ID()}
			a := m.cl.AssignmentFor(topic.Name, p.ID())
			isReplica := false
			for _, id := range a.Replicas {
				if id == self {
					isReplica = true
					break
				}
			}
			m.mu.Lock()
			_, following := m.followCtl[key]
			m.mu.Unlock()
			switch {
			case !isReplica || a.Leader == self:
				if following {
					m.stopFollower(key)
				}
			default:
				if !following {
					m.startFollower(ctx, topic.Name, p.ID(), a.Leader)
				}
			}
		}
	}
}

func (m *replicationManager) startFollower(ctx context.Context, topic string, partition int32, leader cluster.NodeID) {
	key := topicPart{topic, partition}
	fctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.followCtl[key] = cancel
	m.mu.Unlock()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.followLoop(fctx, topic, partition, leader)
	}()
	m.log.Info("following partition leader",
		slog.String("topic", topic), slog.Int("partition", int(partition)),
		slog.String("leader", string(leader)))
}

func (m *replicationManager) stopFollower(key topicPart) {
	m.mu.Lock()
	cancel, ok := m.followCtl[key]
	delete(m.followCtl, key)
	m.mu.Unlock()
	if ok {
		cancel()
		m.log.Info("stopped following partition (leadership or assignment changed)",
			slog.String("topic", key.topic), slog.Int("partition", int(key.partition)))
	}
}

func (m *replicationManager) stopAllFollowers() {
	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(m.followCtl))
	for k, c := range m.followCtl {
		cancels = append(cancels, c)
		delete(m.followCtl, k)
	}
	m.mu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// followLoop pulls the partition's log from its leader, applying
// batches with log matching: divergence always resolves toward the
// leader's committed offset, never the other way.
func (m *replicationManager) followLoop(ctx context.Context, topic string, partition int32, leader cluster.NodeID) {
	for {
		if ctx.Err() != nil {
			return
		}
		progressed, keepGoing := m.followOnce(ctx, topic, partition, leader)
		if !keepGoing {
			return
		}
		if progressed {
			continue // catch up as fast as the leader serves
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.cfg.FetchInterval):
		}
	}
}

// followOnce performs one fetch/apply round. progressed reports that
// records were applied (so the loop should poll again immediately);
// keepGoing reports whether the loop should continue at all.
func (m *replicationManager) followOnce(ctx context.Context, topic string, partition int32, leader cluster.NodeID) (progressed, keepGoing bool) {
	t, err := m.b.store.Topic(topic)
	if err != nil || partition >= int32(t.NumPartitions()) {
		// Topic not here yet (topic sync will create it) — back off.
		return false, true
	}
	p := t.Partitions[partition]
	localHWM := p.HighWatermark()
	reqBody, _ := json.Marshal(protocol.ReplicateRequest{
		From:       string(m.cl.Self()),
		Topic:      topic,
		Partition:  partition,
		Offset:     localHWM,
		MaxRecords: 1000,
		MaxBytes:   uint32(m.cfg.FetchMaxBytes),
	})
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	payload, err := m.cl.CallPeer(callCtx, leader, protocol.OpReplicate, reqBody)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return false, false
		}
		return false, true // leader unreachable: retry next tick
	}
	resp, err := protocol.DecodeReplicateResponse(payload)
	if err != nil {
		m.log.Warn("bad replicate response", slog.Any("err", err))
		return false, true
	}
	switch resp.Status {
	case protocol.ReplicateNotLeader:
		// Roles changed underneath us; reconcile will sort it out.
		return false, false
	case protocol.ReplicateTopicUnknown:
		return false, true
	case protocol.ReplicateTruncate:
		m.applyTruncate(topic, partition, localHWM, resp)
		return true, true
	}
	// ReplicateOK.
	if len(resp.Records) > 0 {
		recs := make([]storage.Record, 0, len(resp.Records))
		for i := range resp.Records {
			fm := &resp.Records[i]
			recs = append(recs, storage.Record{
				Offset:      fm.Offset,
				TimestampMs: fm.Timestamp,
				Key:         fm.Key,
				Value:       fm.Value,
				Headers:     convertHeaders(fm.Headers),
			})
		}
		if err := p.AppendReplica(recs); err != nil {
			if errors.Is(err, storage.ErrDivergedAppend) {
				// Belt and suspenders: the leader thought we were in
				// sync but we are not. Resolve toward committed, per
				// the log-matching rule.
				m.applyTruncate(topic, partition, localHWM, resp)
				return true, true
			}
			m.log.Error("replica append failed",
				slog.String("topic", topic), slog.Int("partition", int(partition)),
				slog.Any("err", err))
			return false, true
		}
	}
	newHWM := p.HighWatermark()
	committed := resp.Committed
	if committed > newHWM {
		committed = newHWM
	}
	m.setCommitted(topic, partition, committed)
	lag := uint64(0)
	if resp.HighWatermark > newHWM {
		lag = resp.HighWatermark - newHWM
	}
	self := m.cl.Self()
	m.cl.ObserveFollowerOffset(topic, partition, self, newHWM)
	m.cl.ObserveReplicationLag(topic, partition, self, lag)
	return len(resp.Records) > 0 && newHWM < resp.HighWatermark, true
}

// applyTruncate executes the leader's truncate instruction: cut the
// divergent tail (local end ahead of the leader's) or jump forward
// over a retention hole (local end below the leader's first offset).
func (m *replicationManager) applyTruncate(topic string, partition int32, localHWM uint64, resp *protocol.ReplicateResponse) {
	t, err := m.b.store.Topic(topic)
	if err != nil || partition >= int32(t.NumPartitions()) {
		return
	}
	p := t.Partitions[partition]
	target := resp.HighWatermark
	if localHWM <= resp.HighWatermark {
		// We are not ahead: this is the forward-hole case.
		target = resp.FirstOffset
		if target <= localHWM {
			target = resp.HighWatermark
		}
	}
	if err := p.TruncateTo(target); err != nil {
		m.log.Error("replica truncate failed",
			slog.String("topic", topic), slog.Int("partition", int(partition)),
			slog.Uint64("target", target), slog.Any("err", err))
		return
	}
	m.log.Warn("replica log truncated to match leader",
		slog.String("topic", topic), slog.Int("partition", int(partition)),
		slog.Uint64("from", localHWM), slog.Uint64("to", target))
	committed := resp.Committed
	if hwm := p.HighWatermark(); committed > hwm {
		committed = hwm
	}
	m.setCommitted(topic, partition, committed)
}

// ---- topic metadata sync ----

// topicSyncLoop discovers topics created on other nodes and creates the
// ones this node holds replicas for. CREATE_TOPIC lands on one node;
// every replica learns about it here within about a second.
func (m *replicationManager) topicSyncLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.syncTopics(ctx)
		}
	}
}

func (m *replicationManager) syncTopics(ctx context.Context) {
	local := make(map[string]int32)
	for _, t := range m.b.store.Topics() {
		local[t.Name] = int32(t.NumPartitions())
	}
	for _, mem := range m.cl.Members() {
		if mem.ID == m.cl.Self() || !mem.State.Alive() {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		payload, err := m.cl.CallPeer(callCtx, mem.ID, protocol.OpClusterStatus, []byte("{}"))
		cancel()
		if err != nil {
			continue
		}
		var status clusterStatusResponse
		if err := json.Unmarshal(payload, &status); err != nil {
			continue
		}
		for _, topic := range status.Topics {
			known, exists := local[topic.Name]
			if exists {
				if known != topic.Partitions {
					// A real metadata conflict: two nodes created the
					// same topic with different partition counts. v1
					// keeps the local truth and screams — operators must
					// recreate the topic consistently.
					m.log.Error("topic partition-count conflict with peer; keeping local",
						slog.String("topic", topic.Name),
						slog.Int("local", int(known)),
						slog.Int("peer", int(topic.Partitions)),
						slog.String("peer_node", string(mem.ID)))
				}
				continue
			}
			if !m.holdsAnyReplica(topic.Name, topic.Partitions) {
				continue
			}
			if err := m.b.ensureTopicLocal(topic.Name, topic.Partitions); err != nil {
				m.log.Error("topic sync create failed",
					slog.String("topic", topic.Name), slog.Any("err", err))
				continue
			}
			m.log.Info("topic synced from peer",
				slog.String("topic", topic.Name),
				slog.Int("partitions", int(topic.Partitions)),
				slog.String("peer_node", string(mem.ID)))
			local[topic.Name] = topic.Partitions
		}
	}
}

// holdsAnyReplica reports whether this node is in the replica set of at
// least one partition of the named topic.
func (m *replicationManager) holdsAnyReplica(topic string, partitions int32) bool {
	for p := int32(0); p < partitions; p++ {
		if m.cl.IsReplicaFor(topic, p) {
			return true
		}
	}
	return false
}

// ---- ops: CLUSTER_STATUS handler ----

func (m *replicationManager) handleClusterStatus(_ context.Context, f *protocol.Frame) *protocol.Frame {
	resp := clusterStatusResponse{
		Node:       m.cl.Self(),
		Members:    m.cl.Members(),
		Topics:     []clusterStatusTopic{},
		Partitions: []clusterStatusPartition{},
	}
	self := m.cl.Self()
	for _, t := range m.b.store.Topics() {
		resp.Topics = append(resp.Topics, clusterStatusTopic{Name: t.Name, Partitions: int32(t.NumPartitions())})
		for _, p := range t.Partitions {
			a := m.cl.AssignmentFor(t.Name, p.ID())
			hwm := p.HighWatermark()
			ps := clusterStatusPartition{
				Topic:     t.Name,
				Partition: p.ID(),
				Leader:    a.Leader,
				Replicas:  a.Replicas,
				ISR:       m.isr(a, hwm),
				HWM:       hwm,
				Committed: m.committedFor(t.Name, p.ID()),
			}
			if a.Leader == self {
				ps.Role = "leader"
				progress := m.followerProgressSnapshot(t.Name, p.ID())
				for _, id := range a.Replicas {
					if id == self {
						continue
					}
					off := progress[id]
					fp := followerProgress{ID: id, Offset: off}
					if hwm > off {
						fp.Lag = hwm - off
					}
					for _, in := range ps.ISR {
						if in == id {
							fp.InISR = true
						}
					}
					ps.Followers = append(ps.Followers, fp)
				}
			} else {
				ps.Role = "follower"
			}
			resp.Partitions = append(resp.Partitions, ps)
		}
	}
	payload, err := json.Marshal(resp)
	if err != nil {
		return errFrame(f, protocol.CodeInternal, err.Error())
	}
	return &protocol.Frame{Opcode: protocol.OpClusterStatus, CorrelationID: f.CorrelationID, Payload: payload}
}

// errFrame builds an OpError frame with a stable code (broker-local
// helper for the replication handlers).
func errFrame(f *protocol.Frame, code, msg string) *protocol.Frame {
	return protocol.ErrorFrame(f.CorrelationID, protocol.NewError(code, msg))
}
