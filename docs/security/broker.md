# Broker security audit — findings register

Scope: `internal/broker/**` (protocol, server, storage, group, client).
Attacker model: an unauthenticated client with a raw TCP connection to
the broker port (`:9100`). No auth on that port is a design choice (see
BRKR-08), so every check below had to hold against a fully hostile
client.

Audit date: 2026-09-13. Tests live in `tests/security/broker_test.go`
(every test name starts with `TestSecBroker` / `TestSecStore`; the fuzz
target is `FuzzBrokerFrame`). Reproduce any finding with:

```sh
go test -run TestSec ./tests/security/
go test -run '^$' -fuzz FuzzBrokerFrame -fuzztime 30s ./tests/security/
```

Summary: 4 real vulnerabilities found and fixed (BRKR-01..04), 1
hardening fix (BRKR-06), everything else verified clean or documented
by design.

---

## BRKR-01 — Path traversal via dot-only topic names — HIGH — fixed

**Impact.** `CREATE_TOPIC` names went straight into
`$DATA_DIR/topics/<name>/partition-<n>/`. The charset whitelist
(`^[a-zA-Z0-9._-]{1,128}$`) already killed the classic flavors — `../`,
`..\`, `/abs`, `a/b`, NUL bytes — but `.` and `..` are pure charset
citizens and passed. `filepath.Join` then cleaned them away:

- topic `..` → partitions created in the **data-dir root**, outside
  `topics/`;
- topic `.` → `partition-N` dirs created **directly inside `topics/`**.

The `.` flavor was the nasty one: on the next boot, `OpenStore` scans
`topics/` and happily re-opens `partition-0` as a **topic with zero
partitions**. A `PRODUCE` to it hits `PickPartition`'s `% 0` —
divide-by-zero panic in a request goroutine, whole broker down. So one
unauthenticated frame turned into persistent state corruption plus a
post-restart remote crash. Pre-fix repro (red tests):

- `TestSecStoreDotNamesEscape` — `CreateTopic("..")` succeeded and
  `partition-0` appeared in the data-dir root;
- `TestSecBrokerTopicTraversalRejected` — `.` and `..` were accepted
  over the wire;
- `TestSecStoreRejectsPartitionlessTopic` — a partitionless topic dir
  opened without complaint (the phantom-topic time bomb).

**Fix.** Three layers:

1. `validTopicName` rejects `.`/`..` explicitly and any name ending in
   a dot (Windows refuses or mangles those names — the wire contract
   must not depend on the OS).
2. Defense in depth at the storage boundary: `topicDirFor` resolves the
   path and proves with `filepath.Rel` that it stays directly inside
   `topics/`. A future caller that skips name validation still cannot
   escape.
3. `openTopic` refuses a topic dir with no `partition-N` subdirectories
   — corruption now fails loudly at boot instead of arming the
   divide-by-zero.

**Regression tests.** `TestSecBrokerTopicTraversalRejected` (14
traversal/injection flavors over the wire + disk-layout assertion),
`TestSecStoreDotNamesEscape`, `TestSecStoreRejectsPartitionlessTopic`.

**Scope note.** Full escape above the data dir was never reachable (the
charset blocks every separator, so at most one `..` segment exists and
`filepath.Join` caps it at the data-dir root). The severity comes from
the crash chain, not from arbitrary filesystem writes. Symlink tricks
need local filesystem access — out of scope for the wire threat model.

---

## BRKR-02 — Committed offset beyond the high-water mark — MEDIUM — fixed

**Impact.** `COMMIT_OFFSET` checked topic, partition, member and
generation — but not the offset value itself. A joined member in the
current generation could commit `2^64-1` (or anything past the
high-water mark). The group's progress was then wedged for good: every
later `FETCH` answers `OFFSET_OUT_OF_RANGE`, so consumers stall or spin
re-joining. One frame → permanent denial of service for that group.

**Fix.** The broker rejects `offset > high_watermark` with
`OFFSET_OUT_OF_RANGE` before the commit lands. `offset == HWM` stays
legal (normal "consumed everything"). Rewinds to any older offset stay
legal too — that's the replay feature, not a bug. Negative offsets are
impossible: the wire type is `uint64`.

**Regression test.** `TestSecBrokerCommitOffsetBeyondHighWatermark`
(HWM boundary accepted, HWM+1 and `MaxUint64` rejected, stored offset
untouched). Clean bill pinned by `TestSecBrokerFetchOutOfRangeOffsets`:
fetches at/past the HWM, including `MaxUint64`, were already
bounds-checked.

---

## BRKR-03 — Consumer-group isolation bypass — HIGH — fixed

**Impact, part A — offset reads.** `FETCH_OFFSET` had no `member_id` at
all: the broker returned any group's committed offsets to anyone who
named the group id. Cross-group reads, zero proof of membership.

**Impact, part B — offset writes.** `COMMIT_OFFSET` checked that the
member is joined and the generation is current (zombie fencing, already
solid) — but never checked **which partition** the member owns. Any live
member could commit for partitions owned by another member of the same
group: push the offset forward to skip unprocessed messages, or rewind
it to force mass redelivery. It could also commit under topics the group
never subscribed to.

**Fix.** Both offset ops are now bound to a member that is joined and
in-generation:

- `FETCH_OFFSET` gained `member_id` + `generation` (wire change, JSON —
  old clients keep parsing, but requests without the fields are now
  rejected; acceptable break before 1.0). Checked via
  `Coordinator.CheckOffsetAccess`: unknown member → `UNKNOWN_MEMBER`,
  unknown group / stale generation → `REBALANCE`. Reads are
  intentionally *not* assignment-bound — inspecting a sibling
  partition's lag is legit.
- `COMMIT_OFFSET` additionally requires the (topic, partition) to be in
  the member's current assignment (`ErrNotAssigned` → `BAD_REQUEST`).
  Assignments can't change inside a generation, so a correct client
  never trips this; a malicious one can't pass it.

**Regression tests.** `TestSecBrokerFetchOffsetRequiresMembership`
(legacy no-member shape, ghost member, stale generation, foreign-group
member — all rejected; legit read works).
`TestSecBrokerCommitUnassignedPartitionRejected` (m2 committing over
m1's partition, commit under an unsubscribed topic — rejected; own
commits land).

---

## BRKR-04 — Unbounded topic / partition / group creation — MEDIUM — fixed

**Impact.** One connection could spray `CREATE_TOPIC` forever (no topic
cap) with up to 1024 partitions each, and every partition costs a
directory, two file handles and a writer goroutine. Group states were
worse: unbounded **and** never collected — `Leave` kept the empty group
forever, so random group ids grew the coordinator map without limit.

**Fix.**

- Partitions per topic capped at 64 (`storage.MaxPartitionsPerTopic`;
  the old 1024 cap was far past anything a partition should cost).
  `partitions <= 0` still means "broker default" (3) — that's the
  documented contract.
- Topics capped at `BROKER_MAX_TOPICS` (default 1024) — past the cap the
  broker answers `BAD_REQUEST`. The count check deliberately races with
  concurrent creates: it sheds load, it is not an exact quota.
- Groups capped at `BROKER_MAX_GROUPS` (default 1024) **plus** empty
  groups are garbage-collected on last-member leave/expiry. Committed
  offsets live in the offset store and survive the GC; a re-join simply
  starts a fresh group, and stale-generation fencing still rejects the
  old zombies.

**Regression tests.** `TestSecBrokerPartitionCountCap` (0/-8 → default;
1 and 64 pass; 65, 1000, 100000, `MaxInt32` rejected),
`TestSecBrokerTopicCountCap`, `TestSecBrokerGroupCountCap` (cap hit →
rejection; leave empties the group → GC → new group fits; committed
offsets survive).

---

## BRKR-06 — Group/member id log injection — LOW — fixed

**Impact.** Group and member ids flow into log lines and
`offsets.jsonl`, and had no validation at all: `JOIN_GROUP` with
`"bad\ngroup"` forged log lines, and 200-char ids bloated the offsets
file. JSON escaping kept the file itself parseable, so this is log
hygiene + resource hygiene, not corruption.

**Fix.** Both ids now get the topic-name charset
(`^[a-zA-Z0-9._-]{1,128}$`) at `JOIN_GROUP` — the one operation that
creates state. Everywhere else an unknown id just misses the maps.

**Regression test.** `TestSecBrokerIDValidation`.

---

## Verified clean / documented trade-offs

### BRKR-05 — Protocol desync & fuzzing — clean bill, verified

- Frame length is a u32 capped at 4 MiB **before** allocation; oversize
  → the connection is dropped (no resync attempt on a poisoned stream).
- Pipelining: responses may return out of order and are matched by
  correlation id; ids are per-connection, so no cross-client confusion.
  There is no connection-level "error state" — a bad frame never bricks
  the connection for the next one.
- `TestSecBrokerMutatedFramesOverWire`: deterministic mutation table
  (truncations, opcode/length/payload corruption, 0xFF floods) over
  live sockets — every damaged frame got a clean error frame, a close,
  or a wait-for-the-rest; the broker served normal traffic afterwards.
- `TestSecBrokerPipelinedCorrelationIntegrity`: 32 pipelined requests
  (valid + unknown opcode + bad JSON) → 32 responses, each with its own
  correlation id, zero cross-talk.
- `FuzzBrokerFrame`: 30 s bounded run, ~1.5 M execs across the frame
  reader, PRODUCE/FETCH binary decoders, JSON control payloads and the
  on-disk record decoder — no panic, no oversize acceptance.
- Binary decode uses unsigned lengths only; the sticky-error reader
  bounds every `take`; no int casts can go negative.

### BRKR-07 — Resource exhaustion — bounded, with documented leftovers

- Connections: `BROKER_MAX_CONNECTIONS` (default 1024) with a clean
  `BROKER_BUSY` error frame — `TestSecBrokerConnectionCap`,
  `TestMaxConnections`. Per-connection handler goroutines are capped at
  64 in-flight, so goroutines are bounded by ~66 × conn cap.
- Produce: 4 MiB frame cap; record-count decode guard at 1 M per batch;
  full writer queue → `BROKER_BUSY` backpressure.
- Fetch: `max_bytes` clamped to 3 MiB so one response always fits the
  4 MiB frame budget — `TestSecBrokerFetchMaxBytesClamped` (16 ×
  256 KiB in, exactly 12 out). Read path streams in 256 KiB chunks — no
  cache to grow.
- Idle (5 min) and write (30 s) deadlines reap half-open and stalled
  connections.
- `offsets.jsonl`: one fsynced line per commit, compacted every 8192
  lines and on shutdown — bounded.
- **Documented leftover:** no retention — a producer that never stops
  fills the disk. P3 roadmap item; disk usage is already exported via
  `raven_broker_disk_usage_bytes` for alerting.

### BRKR-08 — No authentication / TLS on the broker port — documented (by design)

The broker port is a cluster-internal ClusterIP service; auth and TLS
are P3 roadmap items, and the infra auditor owns the NetworkPolicy that
keeps `:9100` inside the cluster (see `docs/security/infra.md` and the
README threat model). Residual risk after this audit: any workload that
reaches the port can still read and write message payloads, but it can
no longer escape the data dir (BRKR-01), wedge or clobber consumer
groups (BRKR-02/03), or exhaust resources unbounded (BRKR-04/07).

### BRKR-09 — Duplicate delivery / replay — info / documented

At-least-once delivery is the design: retries, rebalances, rewinds and
commit-loss redelivery all produce duplicates. Idempotency is the
consumer's job — the jobs layer does it with fences (see the jobs
auditor's register).

### BRKR-10 — Crash recovery / WAL corruption — clean bill, verified

Per-record CRC-32C, torn-tail truncation on open, sparse-index rebuild
with per-entry validation, offset-gap detection, corrupt-tail skip in
`offsets.jsonl`. Covered by `recovery_test.go`, `fsync_test.go`,
`hardening_e2e_test.go` — all green under `-race`, before and after this
audit's changes.

---

## Limitations

- One `DATA RACE` warning fired once in `TestRecoveryMissingIndex`
  during a full-suite run while the machine was under heavy parallel
  load. It did not reproduce in 8 follow-up runs (isolated ×5, full
  suite ×3) nor at the pre-change baseline; no race detector output
  pointed at code touched here. Flagged as a flake to watch, not a
  finding.
- At gate time, full-repo `go build ./...` and package-mode
  `go test ./tests/security/` were blocked by other auditors'
  in-progress files (`services/jobs`, `services/users`,
  `tests/security/jobs_test.go`). The broker gates ran clean in domain
  scope (`go build/vet ./internal/broker/...`, `go test -race
  ./internal/broker/...`, and the security suite in single-file mode).
- The `FETCH_OFFSET` wire change (BRKR-03) rejects clients that omit
  `member_id`/`generation`. Accepted deliberately: pre-1.0, and the
  in-repo client is updated.
