# Broker internals

RAVEN's broker is a small Kafka-style log broker written from scratch in Go.
This doc explains how it works under the hood. I wrote it the way I'd explain
it to a friend: short sentences, no marketing words, honest about what is good
and what is missing.

## The big picture

Producers send records over TCP. The broker appends them to files on disk,
ordered per partition. Consumers join a group, get partitions assigned, fetch
records, and commit their progress. If a consumer dies, the group notices and
moves its partitions to the survivors. That's the whole idea.

Two ports:

- **9100** — the custom binary protocol (clients talk here).
- **9101** — ops HTTP: `/health`, `/ready`, `/metrics`, `/topics`.

## Wire protocol

Every message on the wire is a frame:

```
[u32 payload_len][u8 opcode][u64 correlation_id][payload]
```

All big-endian. `payload_len` counts only the payload, not the 13-byte header.
Max payload is 4 MiB. Bigger frames get the connection dropped — a broken
client can't make us allocate giant buffers.

Opcodes: `CREATE_TOPIC`, `LIST_TOPICS`, `PRODUCE`, `FETCH`, `COMMIT_OFFSET`,
`FETCH_OFFSET`, `JOIN_GROUP`, `LEAVE_GROUP`, `HEARTBEAT`, plus `ERROR` for
failed responses.

Rules:

- Every request gets exactly one response with the same correlation id.
- Pipelining is fine: you can have many requests in flight on one connection.
  Responses may come back out of order; match them by correlation id.
- Control ops carry JSON. `PRODUCE` and `FETCH` carry a compact binary format
  because they're the hot path and JSON + base64 would burn CPU and bytes on
  every message.

Errors have stable machine codes: `TOPIC_EXISTS`, `TOPIC_NOT_FOUND`,
`BROKER_BUSY`, `REBALANCE`, `UNKNOWN_MEMBER`, `OFFSET_OUT_OF_RANGE`,
`BAD_REQUEST`, `UNKNOWN_OPCODE`, `INTERNAL`. The client library checks codes,
not strings.

## Record format on disk

```
[u32 crc][u64 offset][i64 timestamp_ms][u32 key_len][key]
[u32 val_len][value][u16 headers_count][headers...]
header := [u16 key_len][key][u16 val_len][value]
```

CRC-32C over everything after the crc field. Every read checks it. If a record
fails the check during recovery, we truncate the file right there (more on that
below). Headers exist in the format but clients rarely use them today.

## Storage layout

```
$DATA_DIR/topics/<topic>/partition-<n>/<base_offset>.log
$DATA_DIR/topics/<topic>/partition-<n>/<base_offset>.index
$DATA_DIR/offsets.jsonl
```

Each partition is an append-only log split into **segments**. When the active
segment passes 64 MiB, we close it and start a new one named by its first
offset (`00000000000000000064.log` style). Rotation happens between batches, so
a segment can overshoot a bit. That's fine.

The `.index` file is **sparse**: one entry (`u32 rel_offset → u32 position`)
per 4 KiB of log data. To find offset 5000, we binary-search the index for the
closest entry before it and scan forward from there. Scanning a few KB beats
reading the whole file.

### Crash recovery

Writes can be torn if the process dies mid-append. On startup we scan the
active segment record by record, checking CRCs and that offsets are contiguous.
The first bad record wins: we truncate the file at the last good byte and
rebuild the index. We lose the torn tail (the producer never got an ack for it
anyway, so nothing acknowledged is lost) and keep serving.

Older (inactive) segments are trusted. We only rebuild their index if the index
file itself is missing or corrupt. Mid-segment corruption in an old segment is
detected on read (CRC error comes back to the consumer) but not auto-repaired.
I wrote this in the limitations because it's a real trade-off.

### fsync policy

Data is readable right after append, but only durable after fsync. We fsync
every 100 ms **or** every 256 records, whichever comes first (env:
`BROKER_FSYNC_MS`, `BROKER_FSYNC_RECORDS`). A crash can lose up to that window.
This is the same deal Kafka offers; syncing every record would be much slower.

## Retention

Logs no longer grow forever. A background **cleaner** sweeps every partition
on a cadence (`BROKER_CLEANUP_INTERVAL_MS`, default 5 min) and deletes closed
segments that break one of two policies:

- **`retention.time`** (`BROKER_RETENTION_MS`) — a closed segment expires when
  its newest record is older than this. Zero (the default) means off.
- **`retention.bytes`** (`BROKER_RETENTION_BYTES`) — while a partition holds
  more log bytes than this, the oldest closed segments are deleted until it
  fits. Zero (the default) means off.

Both defaults are **off** on purpose: turning on deletion should be a decision,
not a surprise after an upgrade. Per-topic overrides ride
`BROKER_TOPIC_CONFIGS` as JSON, e.g.
`BROKER_TOPIC_CONFIGS={"jobs-dlq":{"retention_ms":86400000}}` keeps the DLQ for
one day while everything else stays unlimited.

Deletion is segment-granular (whole `.log` + `.index` files), never
record-granular, and follows three hard safety rules:

1. **The active segment is never deleted.** Ever. Only closed ones can go.
2. **Committed consumers are protected.** A segment goes only if every offset
   it holds is below the smallest committed offset across all groups with
   commits on that partition. Committed offset = next record to consume, so
   deleting above it would strand a group on `OFFSET_OUT_OF_RANGE` forever.
   This is stricter than Kafka, which ignores consumer offsets entirely.
3. **Offsets never rewind.** Deleting old segments moves the low-water mark
   forward; the high-water mark keeps climbing. New records never reuse an
   offset.

Recovery after cleanup is boring on purpose: the remaining segment files are
untouched, so the normal boot path works as-is. The one new rule is about
reading *below* the low-water mark — a fetch for a deleted offset gets
`OFFSET_OUT_OF_RANGE` (same answer Kafka gives). A fetch at the low-water
mark works normally. Groups that committed are safe by rule 2; a group that
never committed has no position to protect, so it would rejoin and start
wherever the client chooses anyway.

How old is a segment? Each segment tracks the max record timestamp it has
seen. One caveat: for a closed segment opened from disk without a scan we use
the file's mtime as an approximation — good enough for deletion, and we
document it here so nobody is surprised.

Metrics: `raven_broker_retention_bytes_freed_total` (counter per
topic/partition) tells you how much the cleaner freed;
`raven_broker_segment_count` (existing gauge) drops as segments are deleted.

## Concurrency design

- One goroutine per connection reads frames and dispatches handlers.
- Each partition has **one writer goroutine** fed by a bounded channel
  (default 1024, `BROKER_PRODUCE_QUEUE`). Single writer = order within a
  partition, no locks on the append path beyond one RWMutex.
- The bounded channel is the backpressure: when it's full, `PRODUCE` gets a
  `BROKER_BUSY` error instead of us buffering forever and eating RAM.
- Reads (FETCH) take the partition's RWMutex in read mode and go straight to
  the segment files with `ReadAt`/`WriteAt`, so there's no shared seek state
  to race over.
- Graceful shutdown: stop the listener, finish in-flight requests with a
  5 s deadline, close writer channels (they drain their queues first), flush
  + fsync everything, close files, compact the offsets file.

## Consumer groups

The broker is the coordinator. It keeps all group state in memory plus one
offsets file on disk.

- `JOIN_GROUP(group, member_id, topics)` → the broker rebalances: generation
  goes up by one and every member's assignment is recomputed with the **range
  assignor** (sort members, deal each topic's partitions out in contiguous
  ranges; first members get the remainder).
- Members must `HEARTBEAT` within the session timeout (10 s default,
  `BROKER_SESSION_TIMEOUT_MS`). A reaper goroutine expels silent members and
  rebalances.
- `FETCH` and `COMMIT_OFFSET` carry the generation. Stale generation →
  `REBALANCE` error → the client re-joins. This stops a zombie member from an
  old generation from committing over the new generation's progress.
- Committed offsets are the **next offset to consume**, stored per
  (group, topic, partition) in `offsets.jsonl` (one JSON line per commit,
  fsynced; compacted at shutdown and after 8192 lines). Restored on boot.

### Delivery semantics: at-least-once

The client fetches from its committed offset, calls your handler per message,
and commits only if the whole batch succeeded. Handler error → nothing is
committed → the batch comes back after rejoin or restart. So: **messages can
be delivered twice** (crash after processing but before committing is the
classic case). Your handler must cope with duplicates. We never lose
acknowledged messages; we sometimes repeat them. That's the honest trade-off
and it matches what the jobs service wants (retry is a feature, not a bug).

## Client package

`internal/broker/client` is what other services import:

```go
p := client.NewProducer(addr)                      // dial timeout + reconnect backoff
off, err := p.Produce(ctx, "jobs", key, value)     // key → hash partition; "" → round-robin

c := client.NewConsumer(addr, "workers", []string{"jobs"}, handler)
err := c.Run(ctx)                                  // join/heartbeat/fetch/commit/rebalance inside
```

The producer has optional micro-batching (`WithBatching(5ms, 64)`): same API,
but calls group into one request per topic. Reconnects use exponential backoff
(50 ms doubling to 2 s) and are bounded by your context. A request whose write
already started is never silently retried — that would duplicate records.

## Ops HTTP

`GET /topics` returns topics with per-partition high-water marks and, per
group, committed offset + lag. The console UI reads this.

Prometheus metrics: `raven_broker_messages_produced_total`,
`raven_broker_messages_fetched_total` (counters per topic/partition),
`raven_broker_messages_pending` (lag gauge per topic/partition/group, computed
at scrape time), `raven_broker_active_groups`.

## Current limitations (being honest)

1. **Single node.** No replication. The disk dies, data is gone.
2. **No compaction.** Retention deletes old segments (see above), but
   superseded values inside a segment are never cleaned by key.
3. **No idempotent/exactly-once produce.** Retrying a produce after an
   ambiguous failure can duplicate records.
4. **Static partition count.** You pick it at topic creation; there's no
   repartitioning.
5. **Group state is in memory.** Committed offsets survive a restart, but
   membership/generations don't (members just re-join — cheap here).
6. **Old-segment corruption is detected on read, not repaired.** Only the
   active segment gets the truncate-the-tail recovery.
7. **One record ≤ 4 MiB** (frame cap). Headers are capped at 64 KiB each.
8. **No auth/encryption on 9100.** It's a private cluster port; the gateway
   never exposes it.
9. At-least-once means your consumer handler will see duplicates sometimes.
   Design for it.

## If you're debugging it

- `LOG_LEVEL=debug` shows connection churn and rebalance details.
- `/topics` + `raven_broker_messages_pending` answer "is my consumer stuck?"
  in one look: lag growing means the consumer is behind or dead.
- Corrupt-tail truncations are logged as WARN with the byte range dropped.

## The rebalance storm bug (found in production-like k8s)

What we saw: 7 worker pods in group `workers` on a 12-partition topic, and the
group generation would not stop climbing. It hit 453,427 and kept going, about
+3 per second **per member**. Every pod logged "joined group ... generation N,
N+1, N+2..." several times a second, fetches died with "context canceled"
mid-batch, nothing was committed, and the queue stopped draining. Scaling down
to one pod made it mostly stable, but the generation still crept up ~1/second.
A healthy group should sit on the same generation forever.

Root cause: `Coordinator.Join` bumped the generation on **every** join, even
when the exact same member re-joined with the exact same topics. That made the
system feed on itself. One member has a stale heartbeat (normal — happens right
after any legit rebalance). The broker correctly rejects only that member's
heartbeat with `REBALANCE`. The member re-joins — and the join bumps the
generation, which makes **every other member's** heartbeat stale. They all
re-join, each re-join bumps again, and the loop never stops. Two members are
enough to ping-pong forever. The assignor was deterministic, member identity
was stable, the reaper timing was fine — the bug was purely "re-join counts as
a membership change".

The fix: one rule now lives in `Join` — **the generation only moves when the
membership or a subscription actually changes** (new member, leave, session
timeout, different topics). Re-joining with an unchanged subscription just
refreshes liveness and returns the current generation and assignment. Stale
FETCH/COMMIT/HEARTBEAT still fail with `REBALANCE`, so the safety contract is
untouched; the difference is that recovering from it no longer disturbs anyone
else. The single-member creep is gone too, by construction: with a stable
member set there is simply nothing left that can bump the generation.

Regression tests: `TestRejoinUnchangedMemberKeepsGeneration` (coordinator unit
test) and `TestE2ENoRebalanceStorm` (5 consumers, 12 partitions, asserts the
generation is constant for 3s, then one member leaves → exactly one bump →
stable again). Before the fix the e2e test saw the generation jump from 127 to
1059 in under 100ms — roughly 9,000 rebalances a second, in-process.

Lesson: idempotency is not only for APIs your users call. Every internal
recovery path — and re-join is the recovery path — has to be a no-op when
nothing actually changed, or your error handling becomes your worst load
generator.

## Hardening (P0 sweep)

This section is the "what happens when the network and the disk are out to
get us" chapter. Everything here has a test that breaks if the protection
stops working.

### TCP / protocol

- **Connection cap** (`BROKER_MAX_CONNECTIONS`, default 1024). Past the cap a
  new connection gets a real answer — an `ERROR` frame with `BROKER_BUSY` —
  and then a close. No silent drops, no unbounded file descriptors.
- **Idle timeout** (`BROKER_IDLE_TIMEOUT`, default 5m). Every frame read gets
  a fresh read deadline, so a connection that goes completely silent (client
  crashed without closing the socket) is reaped, while any active client
  never notices. TCP keepalive alone takes hours to notice a half-open conn;
  this takes minutes.
- **Write timeout** (`BROKER_WRITE_TIMEOUT`, default 30s). One frame write may
  never take longer. A client that stops reading gets its connection closed —
  the writer goroutine closes the socket on a write error, which wakes the
  read loop, so the whole connection is reaped at the deadline instead of
  lingering until the idle timeout.
- **Slow clients are contained.** A connection that floods its pipelining cap
  (64 in-flight requests) or never reads responses pins only its own bounded
  goroutines (one reader, one writer, at most 64 handlers). Other connections
  are served normally. Proven by `TestSlowReaderDoesNotBlockOthers` and
  `TestWriteDeadlineDropsStalledReader` (goroutine counts return to baseline).
- **Hostile input is boring.** Byte-by-byte drip feeds, several frames in one
  TCP write, EOF mid-header, EOF mid-payload, RST mid-payload, garbage JSON,
  truncated binary payloads, unknown opcodes, oversize frames announced
  byte-by-byte — all handled, all tested in
  `internal/broker/server/hardening_test.go` and `protocol_test.go`.

### Storage / WAL / recovery

- **Index files are never trusted blindly.** At open, every sparse index entry
  is validated against the log: entries must be sorted, positions must be
  strictly increasing and inside the file, and — the strong check — we read 12
  bytes at each position and verify the record there really has the offset the
  entry claims. Costs one tiny read per entry, only at boot, and the index is
  sparse (one entry per 4 KiB), so it stays cheap.
- **Missing or corrupt indexes rebuild themselves.** If validation fails (or
  the file is simply gone), the segment is scanned and the index rebuilt from
  the log, then persisted. This works for the active segment (which always
  gets a full recovery scan anyway) and for old inactive ones. Before this
  check, a structurally valid but wrong index on an old segment would have
  been trusted forever.
- **Crash shapes tested**: empty segment (crash before append), half-written
  record mid-batch, truncated header, single dangling byte, bad CRC in the
  middle (everything after it is dropped), torn tail on the last of many
  segments, and offsets staying monotonic across repeated crash/reopen cycles.
  See `internal/broker/storage/recovery_test.go`.
- **fsync policy is proven, not assumed.** One test watches the record-count
  policy (`BROKER_FSYNC_RECORDS`) fire the flush at exactly the threshold;
  another runs the full broker and watches the interval ticker
  (`BROKER_FSYNC_MS`) flush dirty partitions on cadence. See
  `internal/broker/fsync_test.go`.

### Consumer groups

- **One partition, one owner — always.** A property-style test hammers the
  coordinator with a deterministic pseudo-random mix of joins, leaves and
  subscription changes (300 steps) and checks after every single step that no
  partition is assigned to two members and nothing is out of range.
- **Zombies are fenced.** A member that misses its session timeout is expelled
  and its partitions move. When it wakes up, its old generation is useless:
  FETCH and COMMIT both come back `REBALANCE`/`UNKNOWN_MEMBER`. It cannot
  consume or commit over the new owner. Tested at the coordinator level
  (`TestZombieMemberFencedOut`) and end-to-end over the wire
  (`TestE2EZombieMemberFencedOut`, which drives JOIN/HEARTBEAT/FETCH/COMMIT on
  raw TCP connections through the real broker).
- **Duplicate commits are fine.** Committing the same offset twice is a no-op.
  A retried commit after a client timeout never corrupts state. A
  stale-generation replay can never rewind a newer offset
  (`TestDuplicateCommitIdempotent`).
- **Reconnects are cheap.** Re-joining before the session expires keeps your
  assignment and moves nothing (`TestReconnectBeforeTimeoutIsSeamless`).

One honest note: "never process the same partition simultaneously" is enforced
at the mechanism level — exclusive assignment plus fencing on fetch/commit. A
zombie can still finish the one batch it had already fetched when the
rebalance hit (at-least-once delivery means the new owner will reprocess those
records anyway). What it cannot do is fetch anything new or commit anything.
That's the strongest guarantee a fence can give, and it's what Kafka does too.
