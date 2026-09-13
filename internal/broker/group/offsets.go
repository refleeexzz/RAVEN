package group

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// offsetLine is one committed-offset record in the offsets file.
type offsetLine struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// compactAfterLines triggers a full rewrite of the offsets file once
// this many lines accumulated. Keeps the file small without fsyncing a
// rewrite on every commit.
const compactAfterLines = 8192

// offsetStore persists committed offsets as JSON lines in
// $DATA_DIR/offsets.jsonl. Every commit appends one line and fsyncs.
// On boot the file is replayed; a corrupt trailing line (crash
// mid-write) is skipped, at worst losing the last commit — safe under
// at-least-once semantics, which redeliver it.
type offsetStore struct {
	mu    sync.Mutex
	path  string
	data  map[string]map[string]map[int32]uint64 // group → topic → partition → offset
	file  *os.File
	lines int // lines appended since the last compaction
}

func openOffsetStore(dir string) (*offsetStore, error) {
	path := filepath.Join(dir, "offsets.jsonl")
	s := &offsetStore{path: path, data: make(map[string]map[string]map[int32]uint64)}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open offsets file: %w", err)
	}
	s.file = f
	if err := s.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// replay loads every line; the last commit per key wins. A corrupt
// trailing line is skipped (torn write on crash).
func (s *offsetStore) replay() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(s.file)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		var l offsetLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			continue // torn trailing line after a crash
		}
		s.setMem(l.Group, l.Topic, l.Partition, l.Offset)
	}
	return sc.Err()
}

func (s *offsetStore) setMem(group, topic string, partition int32, offset uint64) {
	t, ok := s.data[group]
	if !ok {
		t = make(map[string]map[int32]uint64)
		s.data[group] = t
	}
	p, ok := t[topic]
	if !ok {
		p = make(map[int32]uint64)
		t[topic] = p
	}
	p[partition] = offset
}

// Set records and persists one committed offset.
func (s *offsetStore) Set(group, topic string, partition int32, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setMem(group, topic, partition, offset)
	line, err := json.Marshal(offsetLine{Group: group, Topic: topic, Partition: partition, Offset: offset})
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := s.file.Write(line); err != nil {
		return fmt.Errorf("append offset commit: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("fsync offsets: %w", err)
	}
	s.lines++
	if s.lines >= compactAfterLines {
		return s.compactLocked()
	}
	return nil
}

// Get returns the committed offset (next to consume) for a key.
func (s *offsetStore) Get(group, topic string, partition int32) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.data[group]
	if !ok {
		return 0, false
	}
	p, ok := t[topic]
	if !ok {
		return 0, false
	}
	off, ok := p[partition]
	return off, ok
}

// Snapshot returns a copy of all offsets for one group.
func (s *offsetStore) Snapshot(group string) map[string]map[int32]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[int32]uint64, len(s.data[group]))
	for topic, parts := range s.data[group] {
		cp := make(map[int32]uint64, len(parts))
		for p, off := range parts {
			cp[p] = off
		}
		out[topic] = cp
	}
	return out
}

// GroupIDs lists groups that have at least one committed offset.
func (s *offsetStore) GroupIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.data))
	for g := range s.data {
		out = append(out, g)
	}
	return out
}

// compactLocked rewrites the file with exactly one line per key.
// Caller holds s.mu.
func (s *offsetStore) compactLocked() error {
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("compact offsets: %w", err)
	}
	w := bufio.NewWriter(f)
	for group, topics := range s.data {
		for topic, parts := range topics {
			for partition, offset := range parts {
				line, _ := json.Marshal(offsetLine{Group: group, Topic: topic, Partition: partition, Offset: offset})
				if _, err := w.Write(append(line, '\n')); err != nil {
					_ = f.Close()
					return err
				}
			}
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("compact offsets rename: %w", err)
	}
	nf, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.file = nf
	s.lines = 0
	return nil
}

// Close compacts and closes the file.
func (s *offsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.compactLocked(); err != nil {
		_ = s.file.Close()
		return err
	}
	return s.file.Close()
}
