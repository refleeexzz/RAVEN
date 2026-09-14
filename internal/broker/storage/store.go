package storage

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Sentinel errors; the broker maps them onto wire error codes.
var (
	ErrTopicExists      = errors.New("storage: topic already exists")
	ErrTopicNotFound    = errors.New("storage: topic not found")
	ErrInvalidTopicName = errors.New("storage: invalid topic name")
	// ErrTooManyPartitions rejects CREATE_TOPIC past MaxPartitionsPerTopic.
	ErrTooManyPartitions = errors.New("storage: partition count exceeds limit")
)

// topicNameRe keeps topic names filesystem-safe and URL-friendly.
var topicNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,128}$`)

// MaxPartitionsPerTopic caps CREATE_TOPIC. Every partition costs a
// directory, at least two open files and one writer goroutine, so the
// count must stay small enough that one connection cannot exhaust file
// descriptors by spraying topics with huge partition counts.
const MaxPartitionsPerTopic = 64

// validTopicName enforces the wire-level topic name rules. The charset
// alone is not enough: "." and ".." pass it but are path metacharacters
// that escape topics/ once filepath.Join cleans them (BRKR-01). Names
// ending in a dot are rejected too: Windows refuses to create them (and
// silently strips trailing dots), so allowing them would make the wire
// contract platform-dependent.
func validTopicName(name string) error {
	if !topicNameRe.MatchString(name) {
		return fmt.Errorf("%w %q: allowed [a-zA-Z0-9._-], max 128 chars", ErrInvalidTopicName, name)
	}
	if strings.HasSuffix(name, ".") {
		return fmt.Errorf("%w %q: reserved path name or trailing dot", ErrInvalidTopicName, name)
	}
	return nil
}

// topicsDir is the one directory every topic lives under.
func (s *Store) topicsDir() string { return filepath.Join(s.dir, "topics") }

// topicDirFor resolves the on-disk directory for a topic and proves the
// result stays directly inside topicsDir. Defense in depth behind
// validTopicName: even a future caller that skips name validation cannot
// make the store touch a path outside topics/.
func (s *Store) topicDirFor(name string) (string, error) {
	base := s.topicsDir()
	dir := filepath.Join(base, name)
	rel, err := filepath.Rel(base, dir)
	if err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) ||
		strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w %q: resolved path escapes topics dir", ErrInvalidTopicName, name)
	}
	return dir, nil
}

// Topic is a named set of partitions.
type Topic struct {
	Name       string
	Partitions []*Partition
	rr         atomic.Uint32 // round-robin cursor for keyless produce
}

// NumPartitions returns the partition count.
func (t *Topic) NumPartitions() int { return len(t.Partitions) }

// PickPartition routes a record: hash of the key when present,
// round-robin otherwise. This is the only partitioning strategy; it is
// deterministic per key, which preserves per-key order.
func (t *Topic) PickPartition(key []byte) int32 {
	n := uint32(len(t.Partitions))
	if len(key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(key)
		return int32(h.Sum32() % n)
	}
	return int32(t.rr.Add(1) % n)
}

// Store owns every topic and partition under the data dir.
type Store struct {
	mu                sync.RWMutex
	dir               string
	opts              Options
	defaultPartitions int32
	topics            map[string]*Topic
	log               *slog.Logger
	closed            bool
}

// OpenStore scans dir/topics and opens every topic and partition found,
// running crash recovery per partition.
func OpenStore(dir string, opts Options, defaultPartitions int32, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	topicsDir := filepath.Join(dir, "topics")
	if err := os.MkdirAll(topicsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create topics dir: %w", err)
	}
	s := &Store{
		dir:               dir,
		opts:              opts,
		defaultPartitions: defaultPartitions,
		topics:            make(map[string]*Topic),
		log:               log,
	}
	entries, err := os.ReadDir(topicsDir)
	if err != nil {
		return nil, fmt.Errorf("list topics dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() && validTopicName(e.Name()) == nil {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		topic, err := s.openTopic(name)
		if err != nil {
			return nil, fmt.Errorf("open topic %s: %w", name, err)
		}
		s.topics[name] = topic
	}
	return s, nil
}

// openTopic opens all partitions of one topic from disk.
func (s *Store) openTopic(name string) (*Topic, error) {
	topicDir, err := s.topicDirFor(name)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "partition-") {
			continue
		}
		id, err := strconv.Atoi(strings.TrimPrefix(e.Name(), "partition-"))
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	// A topic always has at least one partition: CreateTopic never makes
	// fewer. A dir without partition-N subdirs is corruption (or the
	// leftover of the "." traversal, BRKR-01) — refuse to guess around
	// it, because a zero-partition topic panics PickPartition with a
	// division by zero on the first PRODUCE.
	if len(ids) == 0 {
		return nil, fmt.Errorf("topic %q has no partitions on disk", name)
	}
	// Partition ids must be exactly 0..n-1; anything else means someone
	// hand-edited the data dir, which we refuse to guess around.
	for i, id := range ids {
		if id != i {
			return nil, fmt.Errorf("non-contiguous partition ids: %v", ids)
		}
	}
	topic := &Topic{Name: name, Partitions: make([]*Partition, 0, len(ids))}
	for _, id := range ids {
		p, err := OpenPartition(partitionDir(topicDir, id), name, int32(id), s.opts, s.log)
		if err != nil {
			return nil, err
		}
		topic.Partitions = append(topic.Partitions, p)
	}
	return topic, nil
}

func partitionDir(topicDir string, id int) string {
	return filepath.Join(topicDir, "partition-"+strconv.Itoa(id))
}

// CreateTopic creates a topic with the given partition count (or the
// store default when partitions <= 0).
func (s *Store) CreateTopic(name string, partitions int32) (*Topic, error) {
	if err := validTopicName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("storage: store closed")
	}
	if _, ok := s.topics[name]; ok {
		return nil, ErrTopicExists
	}
	if partitions <= 0 {
		partitions = s.defaultPartitions
	}
	if partitions > MaxPartitionsPerTopic {
		return nil, fmt.Errorf("%w: %d exceeds limit %d", ErrTooManyPartitions, partitions, MaxPartitionsPerTopic)
	}
	topicDir, err := s.topicDirFor(name)
	if err != nil {
		return nil, err
	}
	topic := &Topic{Name: name, Partitions: make([]*Partition, 0, partitions)}
	for i := int32(0); i < partitions; i++ {
		p, err := OpenPartition(partitionDir(topicDir, int(i)), name, i, s.opts, s.log)
		if err != nil {
			for _, open := range topic.Partitions {
				_ = open.Close()
			}
			return nil, err
		}
		topic.Partitions = append(topic.Partitions, p)
	}
	s.topics[name] = topic
	return topic, nil
}

// Topic returns the named topic or ErrTopicNotFound.
func (s *Store) Topic(name string) (*Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return nil, ErrTopicNotFound
	}
	return t, nil
}

// Topics lists all topics sorted by name.
func (s *Store) Topics() []*Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Topic, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// DiskUsageBytes sums the on-disk segment bytes of every topic and
// partition in the store. The broker adds the offsets file on top.
func (s *Store) DiskUsageBytes() int64 {
	var total int64
	for _, t := range s.Topics() {
		for _, p := range t.Partitions {
			total += p.DiskUsageBytes()
		}
	}
	return total
}

// SegmentCount sums the segment files across every topic and partition.
func (s *Store) SegmentCount() int {
	total := 0
	for _, t := range s.Topics() {
		for _, p := range t.Partitions {
			total += p.SegmentCount()
		}
	}
	return total
}

// FlushAll fsyncs every dirty partition. Called by the broker's fsync
// ticker and at shutdown.
func (s *Store) FlushAll() {
	for _, t := range s.Topics() {
		for _, p := range t.Partitions {
			if err := p.Flush(); err != nil {
				s.log.Error("flush partition failed",
					slog.String("topic", t.Name),
					slog.Int("partition", int(p.id)),
					slog.Any("err", err))
			}
		}
	}
}

// Close flushes and closes every partition.
func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true
	topics := make([]*Topic, 0, len(s.topics))
	for _, t := range s.topics {
		topics = append(topics, t)
	}
	s.mu.Unlock()

	var firstErr error
	for _, t := range topics {
		for _, p := range t.Partitions {
			if err := p.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
