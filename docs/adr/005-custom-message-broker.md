# ADR 005: Build a custom message broker instead of using Kafka/NATS

Status: accepted (for this project's goals)

## Context

The jobs system needs a work queue with consumer groups, retries and a
dead-letter topic. The obvious move is to pull in Kafka, Redpanda or NATS
and get back to the services. This project exists to learn distributed
systems, and "configured Kafka" teaches you Kafka's config, not how a log
broker works.

## Decision

Write the broker from scratch in Go (`internal/broker/`): a framed binary
protocol over TCP, append-only segmented logs with CRC-32C and sparse
indexes, batched fsync, server-side consumer groups with rebalancing and
committed offsets, and a client library the jobs service and workers share.
The full internals are in [../broker-internals.md](../broker-internals.md).

## Alternatives

- **Kafka.** The real thing. Rejected because the goal is to understand the
  machinery — partition logs, the fsync window, group coordination,
  at-least-once semantics — and you understand them by building them. Also
  operationally: a local Kafka is a heavy way to run a demo.
- **NATS JetStream.** Closer in weight, excellent project. Same objection:
  using it teaches you NATS.
- **Redis streams.** Already have Redis in the stack. It would work, but
  the consumer-group semantics we wanted to study (generations, fencing,
  range assignment) are exactly the parts Redis streams abstract away.
- **Just Postgres** (`SELECT FOR UPDATE SKIP LOCKED` queue). A legitimately
  good design for job queues at small scale, and the fence update already
  proves we know the trick. Rejected because then there is no broker to
  learn from — and this project is the broker.

## Consequences

Positive:

- Total control over semantics. We chose at-least-once deliberately,
  picked the fsync window (100 ms / 256 records), decided how the fence
  interacts with offsets at dispatch, and can explain every one of those
  choices because we wrote them.
- The protocol is small enough to hold in your head: 9 opcodes, one frame
  format, JSON for control and binary for the hot path.
- Benchmarks answer real questions about our own code (batching buys ~2.8×
  on my machine), not about someone else's tuning guide.
- Debugging is civilized: `/topics` shows high-water marks and consumer
  lag, and the code you are reading is the code that is running.
- It is genuinely fun. This was the best part of the project to build.

Negative (the honest cost):

- **No replication.** Single node; disk dies, data is gone. This alone
  disqualifies it for any real workload.
- **No retention, no compaction.** Logs grow forever.
- **No idempotent produce.** Retrying an ambiguous produce can duplicate
  records.
- **Group membership is in memory.** Offsets survive a restart; members
  re-join. Simpler than Kafka's persisted group state, and it shows.
- **Static partition counts.** No repartitioning.
- **We maintain it.** Every bug in the broker is our bug. There is no
  vendor, no community, no Stack Overflow answer. The ~10.2k msg/s
  single-record throughput on a Ryzen 5 5600X is fine for a learning
  platform and orders of magnitude away from what Kafka does in anger.
- Features that real brokers get for free — auth, encryption, quotas,
  mirror making — are all missing. Port 9100 trusts the private network.

If this platform ever grew into something real, this ADR is the first one
you revisit.
