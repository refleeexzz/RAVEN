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
2. **No retention/deletion.** Logs grow forever. Old segments are never
   compacted or removed.
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
