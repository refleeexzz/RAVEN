# ADR 009: Job leases and generation fencing (stranded job recovery)

Status: accepted

## Context

The worker commits the broker offset **at dispatch**, not when the job
finishes. That choice keeps the consumer loop hot, but it has a price we paid
in a live chaos test: `kill -9` a worker mid-job and the job sits in
`PROCESSING` forever. The broker rebalances the partitions, but the committed
offset means nobody ever re-reads that message. Same story for a worker that
dies after marking a job `RETRYING` but before its retry timer fires — the
timer was in-memory, so it died with the process.

So we had a silent stranding window, and the only fix was a human with
`psql`. Not great for a platform that sells reliability.

Constraints going in:

- Postgres is already the source of truth for job state. Workers trust the
  row, not the broker message.
- The jobs service runs 3 replicas in k8s. Anything that scans and repairs
  jobs must be a singleton, or the replicas would fight over the same rows.
- We do not want to touch the broker's storage/offset machinery for this —
  it is evolving in parallel, and "re-deliver un-acked messages after a
  timeout" is a big feature there, not a flag.

## Decision

**Leases on the job row, a sweeper in the jobs service, and a generation
token on every execution write.**

1. **Lease columns** (migration 000003): `heartbeat_at`, `lease_until`,
   `execution_generation`. Claiming a job stamps
   `lease_until = now() + lease` (`WORKER_JOB_LEASE_MS`, default 30 s). While
   the handler runs, the worker renews every `lease/3` — two renewals can
   fail back to back before the lease actually expires. A job going
   `RETRYING` gets `lease_until = now() + backoff + lease`, so the
   dead-retry-timer case is covered by the same machinery.

2. **Sweeper** (`services/jobs/sweeper.go`): every `JOBS_SWEEP_INTERVAL`
   (default 15 s) it finds `PROCESSING`/`RETRYING` rows with
   `lease_until < now()` and, per job in one transaction: bumps
   `execution_generation`, moves the job to `RETRYING` (or `DEAD` when
   attempts are exhausted), clears `worker_id`/lease, writes a
   `job_attempts` row with `error = 'lease expired (worker lost)'`, commits,
   and republishes to the `jobs` topic with the new generation. Attempts stay
   — the interrupted try is audited but not burned.

   Singleton across replicas with a Postgres advisory lock
   (`pg_try_advisory_lock`) on a fixed key: each pass, one replica wins, does
   the sweep, releases. The losers skip and retry next interval. If the
   winner dies, its session dies, the lock dies with it, and another replica
   wins the next pass. No extra infrastructure — the lock lives in the same
   database that already decides everything about jobs.

3. **Generation fencing**: every write that mutates an execution — the claim
   fence, lease renewals, success/failure finishes — carries
   `execution_generation = $n` in its `WHERE` clause. The sweeper's bump
   makes the old token worthless: a late write from a "dead" worker matches
   zero rows, gets logged and counted
   (`raven_worker_fenced_writes_total{op}`), and is treated as a no-op
   success. A worker that notices the takeover mid-run (its renewal updates
   0 rows) cancels the handler context and writes nothing at all.

   The broker message now carries the generation too. A stale-generation
   message is skipped at claim time — it was already requeued. Messages
   written before leases existed decode with generation 0, which the fence
   reads as "unknown, take the row's current token", so a rolling upgrade
   does not eat in-flight messages.

## Alternatives

- **Broker-side visibility timeout (SQS-style).** The textbook answer:
  messages are invisible while being processed and reappear if not acked in
  time. Rejected here because our broker is a log with committed offsets —
  there is no "in-flight" state to time out. Building it means ack tracking,
  redelivery deadlines and a redrive policy inside the broker, while
  `internal/broker` is being hardened by someone else. And we would still
  need the database fence: a visibility timeout decides *when* to redeliver,
  but only a fencing token on the row decides *whose write wins* when two
  workers both think they own the job. Since the fence has to live in
  Postgres anyway, the lease might as well live there too. If the broker
  grows visibility timeouts one day, the sweeper becomes a safety net instead
  of the mechanism — that is a fine endgame.
- **Ops runbook only.** "If a job strands, run this SQL." Rejected: silent
  stranding is the worst failure mode we have, and the whole point of the
  platform is that it heals itself.
- **Redis leases with TTL keys.** Tempting because TTLs are free there, but
  now job state splits across two stores with no transaction between them:
  the row says `PROCESSING`, the Redis lease says whatever it says, and every
  recovery decision needs both to agree. One writer, one clock
  (`now()` in Postgres), one transaction. Simpler to reason about.
- **Heartbeat table instead of columns.** One row per worker heartbeating,
  jobs pointing at it. More moving parts for the same result; a timestamp on
  the job row is the honest minimum.

## Consequences

Positive:

- `kill -9` self-heals in roughly `lease + 2 × sweep interval` (defaults: ~60 s
  worst case, usually much less). Nobody pages a human for a stuck job
  anymore.
- The at-least-once story is now complete: the fence dedups starts, the
  generation dedups finishes, the sweeper re-drives whatever dies in between.
- The audit trail got *better*: `job_attempts` shows the interrupted attempt
  (`lease expired (worker lost)`, the dead worker's id) right next to the
  winning one.
- The sweeper is ~200 lines, one query for candidates, one transaction per
  job, and exactly-one semantics from a primitive Postgres already gives us.

Negative (being straight about it):

- **Write amplification**: every running job renews every `lease/3` (default:
  one UPDATE per job per 10 s). At our scale this is noise; at thousands of
  concurrent long jobs it is a real line on the database's write bill, and
  the answer would be longer leases, not more renewals.
- **One clock to rule us**: all lease math is `now()` inside Postgres, so
  app-server clock skew cannot break it — but it also means the sweeper and
  the workers must talk to the *same* Postgres. True today, worth
  remembering.
- **Crash-loop blind spot**: the sweeper preserves `attempts`, so a worker
  that gets SIGKILLed mid-job every single time never burns attempts and the
  job loops forever instead of going DEAD. Deliberate (a kill is not a
  failure of the job), but an operator watching a job with many
  `lease expired` rows should kill the worker *image*, not the worker.
- **Singleton sweeper, single point of patience**: only one replica sweeps
  per pass, and a pass recovers at most 100 jobs, so a mass casualty
  (every worker dies at once) drains in batches over several intervals.
  Fine for a demo fleet; revisit the batch size before it matters.
- The sweeper publishes after committing, not inside the transaction. If the
  publish fails, the recovered row keeps a short fresh lease and the next
  pass retries the publish — eventually consistent on purpose, at the cost of
  one extra interval of delay in that failure mode.

What production would do on top of this, in order: (1) alerts on
`raven_jobs_sweeper_recovered_total` growing without a matching deploy or
incident — recoveries should be rare and explained; (2) a per-type lease
override for job types that legitimately run for minutes; (3) broker-side
visibility timeouts so the sweeper becomes the backup, not the mechanism.
