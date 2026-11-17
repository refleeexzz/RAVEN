# Scheduling: delayed jobs, cron and replay

The jobs service uses migration `000004_scheduling` for schedules,
`000008_scheduled_dispatch` for the execution outbox and
`000009_immediate_dispatch` to recover queued work across upgrades.

- **Delayed jobs** — create now, run later (`scheduled_at`).
- **Cron jobs** — recurring schedules that spawn jobs (`/api/crons`).
- **Replay** — clone any job with one call (`POST /api/jobs/{id}/replay`).
- **Priority topics** — how work reaches workers (`jobs.p1`..`jobs.p9`).

Everything here is owner-scoped like the rest of the jobs API: users only
see and manage their own stuff, admins (`admin:*`) see everything.

---

## Delayed jobs

Create a job with `scheduled_at` (Unix seconds) and it parks in the new
`SCHEDULED` status instead of `QUEUED`. It is stored, it shows up in
`GET /api/jobs/{id}` with its `scheduled_at`, and **nothing is published to
the broker yet**.

```json
POST /api/jobs
{
  "type": "send_email",
  "payload": { "to": "me@example.com", "subject": "later" },
  "scheduled_at": 1767225600
}
```

When the time comes, the dispatcher claims a batch of due jobs with
`FOR UPDATE SKIP LOCKED`. In one PostgreSQL transaction it marks each
job `QUEUED` and records its ID and execution generation in
`scheduled_job_outbox`. Once the transaction commits, the job is visible
to workers and its message is ready to publish. If the transaction rolls
back, the job stays `SCHEDULED` for another pass.

The interval controls how often the dispatcher looks for due jobs.
A backlog or a slow publish can delay a job across multiple passes.

### Rules for `scheduled_at`

| Value                        | Result                                   |
|------------------------------|------------------------------------------|
| `0` / absent                 | run now (normal `QUEUED` create)         |
| negative                     | `400 scheduled_at_invalid`               |
| more than 1 minute in past   | `400 scheduled_at_in_past`               |
| inside the 1-minute skew     | run now (no schedule recorded)           |
| future                       | `SCHEDULED`, eligible for dispatch at that time (UTC) |

The one-minute past tolerance exists because clocks skew — re-sending a
just-due time should run the job, not error. There is no upper bound on how
far ahead you can schedule.

A `SCHEDULED` job can still be cancelled (`POST /api/jobs/{id}/cancel`) —
the schedule is simply dropped and the dispatcher never touches it. It
holds no lease, so the stranded-job sweeper ignores it completely.

---

## Cron jobs

A cron schedule says "spawn this job every time the expression comes due".
Stored in `cron_schedules`, managed over REST:

```
POST   /api/crons        (jobs:create)
GET    /api/crons        (jobs:read)    — your schedules only
DELETE /api/crons/{id}   (jobs:cancel)
```

```json
POST /api/crons
{
  "name": "nightly-report",
  "cron_expr": "0 3 * * *",
  "type": "webhook",
  "payload": { "url": "https://example.com/hook", "event": "nightly" },
  "priority": 5,
  "enabled": true
}
```

The response carries `next_run_at` — the first fire time, computed at
create. Job fields follow the exact same rules as `POST /api/jobs` (known
types, 64 KiB payload cap, priority 1–9 default 5), because a cron can only
spawn jobs that `CreateJob` would accept. Every spawned job belongs to the
schedule's owner.

### The expression

Classic 5 fields: `minute hour day-of-month month day-of-week`. Numbers
plus `*`, `,`, `-`, `/`. Day-of-week is `0–6` (`7` also works for Sunday).
**No names** (`MON`, `JAN` are rejected) and no `@daily` shorthands. The
parser is hand-rolled on purpose (stdlib only) and pinned by a big table
suite.

Day matching follows the classic Vixie rule: when **both** day-of-month and
day-of-week are restricted, a day matches when **either** does;
otherwise both must match.

Expressions that can never fire (like `0 0 31 2 *`) are rejected at create
time instead of silently doing nothing forever.

### How a cron run is dispatched

The cron scheduler locks due schedules with `FOR UPDATE SKIP LOCKED`.
It inserts a `QUEUED` job, advances `next_run_at`, and records the
pending dispatch in one PostgreSQL transaction. A rollback leaves the
schedule due for another pass. After commit, a broker outage leaves
the same job pending for delivery.

**Missed runs do not catch up.** If the service was down for three hours
on a minutely schedule, it fires once when it comes back and
`next_run_at` jumps to the next future match. No backfill storm.

Deleting a schedule is a hard delete: it stops firing immediately and the
row is gone (job history it spawned stays, of course).

---

## Delivery and retries

Immediate creation, replay and manual requeue commit the job's `QUEUED`
state and its pending publication in a single PostgreSQL transaction.
Requeue advances the existing job's execution generation. Replay creates a
new job ID. A successful submission means PostgreSQL has accepted the work.
The jobs service attempts publication after commit. If the broker is
unavailable or the process dies at that boundary, another jobs replica can
publish the pending execution when the broker is available.

Every jobs replica scans pending publications on startup and then once a
second, including when scheduled releases are paused. A replica locks one
pending row at a time. If the job is
still `QUEUED` in the recorded generation, it publishes to the configured
execution topics and deletes the entry after broker acknowledgement. A
publish error leaves the entry available for a later pass. Another replica
can resume delivery after a restart. If the job has already been cancelled,
claimed or moved to another generation, the replica discards the stale entry.

Delivery is at least once. A crash after the broker accepts a message but
before PostgreSQL commits the deletion can result in another publish with
the same job ID and generation. The worker checks the job's current status
and generation when claiming it. A `QUEUED` job can still be waiting for
the broker. To inspect the number of pending dispatches, run
`SELECT count(*) FROM scheduled_job_outbox`.

Worker retries after a handler failure use the retry and lease recovery
mechanisms described in [ADR 009](adr/009-job-leases.md).

### Dead-letter delivery

When a job enters `DEAD`, PostgreSQL saves its terminal state in
`job_dead_letter_outbox` in the same transaction. The worker or sweeper
tries to send that snapshot to `jobs.dlq` after commit. If the broker is
unavailable or the publishing process exits, a jobs replica can deliver
it later. Recovery also runs when scheduled releases are paused. Pending
deliveries can be counted with `SELECT count(*) FROM job_dead_letter_outbox`.

A manual requeue starts a new generation. The pending dead letter still
describes the earlier failure, including its payload, attempt count and error.
The relay publishes only to `jobs.dlq`. If a publisher loses its connection
after broker acknowledgement but before committing the outbox deletion, the
same terminal generation can appear again. Consumers that need to deduplicate
can use the pair `(id, execution_generation)`.

Apply migration `000010_dead_letter_outbox` before upgrading jobs or worker
replicas. It captures transitions committed after the migration. Earlier
terminal jobs have no durable broker receipt, so the migration leaves their
delivery history alone. During a rolling upgrade an older replica can publish
directly in addition to the relay. Database roles that change job status to
`DEAD` need `INSERT` on the new table. Workers and the jobs service use
`SELECT`, `UPDATE` and `DELETE` to deliver pending messages.

To roll back, stop new terminal transitions and let upgraded jobs replicas
drain the outbox with a healthy broker. The down migration checks for pending
rows and refuses to drop them.

---

## Replay

`POST /api/jobs/{id}/replay` clones a job: same `type`, `payload`,
`priority` and `max_attempts`, but a **fresh id, zero attempts, no
schedule**. It queues and runs like any new job.

The clone carries `replayed_from` with the source job id, so every replay
stays auditable — you can always trace a replay back to its original.

Replays are owner-scoped: replaying somebody else's job answers
`404 job_not_found` (never an existence oracle), and admins replay via the
usual `admin:*` permission. You can replay a job in any state — a
`SUCCESS` job to run it again, a `DEAD` one instead of a DLQ requeue, even
a `SCHEDULED` one (the clone runs now, it does not inherit the schedule).

---

## Priority topics

The contract with the workers: execution messages go to
**`jobs.p<priority>`** — `jobs.p1` (most urgent) down to `jobs.p9`. The
priority you set at create time picks the topic, so `priority` is
validated as **1–9** now (the database still allows 1–10 for older rows).

While workers migrate to the new topics, a compatibility **fanout**
(`JOBS_LEGACY_TOPIC_FANOUT`, default `true`) mirrors every publish onto
the legacy `jobs` topic as well. Old workers keep working; new workers
that consume both simply ignore the duplicate through the claim fence.
Once every worker subscribes to the priority topics directly, set
`JOBS_LEGACY_TOPIC_FANOUT=false` to stop the double publish. The DLQ
(`jobs.dlq`) and the retry flow are unchanged.

All execution publishes route this way: creates, replays, cron spawns,
DLQ requeues and sweeper republishes.

---

## Configuration

| Env var                    | Default | What it does                                      |
|----------------------------|---------|---------------------------------------------------|
| `JOBS_SCHEDULER_INTERVAL`  | `1s`    | Tick for due delayed and cron jobs. At `0` these releases pause, and recovery continues publishing already queued work. |
| `JOBS_LEGACY_TOPIC_FANOUT` | `true`  | Also publish executions to the legacy `jobs` topic. |

## Metrics

| Metric                                  | Type    | Meaning                          |
|-----------------------------------------|---------|----------------------------------|
| `raven_jobs_scheduled`                  | gauge   | jobs waiting in `SCHEDULED`      |
| `raven_jobs_scheduled_dispatched_total` | counter | delayed jobs released to `QUEUED`|
| `raven_jobs_cron_created_total`         | counter | schedules accepted by CreateCron |
| `raven_jobs_cron_spawned_total`         | counter | job instances fired by cron      |

## Schema notes

Apply migrations through `000010_dead_letter_outbox` before rolling out the
jobs service and workers. Migration `000009` locks the jobs table against writes while
it registers every existing `QUEUED` job in the outbox and installs a
deferred trigger. Allow time for the queued backlog during this upgrade.
The trigger records the current execution generation when older replicas
commit newly queued work during the rollout. An old API replica that receives
a broker error can still return a publish error and set the job to `FAILED`
or revert a requeue to `DEAD`. Durable acceptance on broker errors takes
effect on upgraded replicas. Existing scheduled jobs enter the outbox when
released. A job queued before the migration may already have a broker
message, so the worker's claim fence also applies during backfill.
Custom PostgreSQL roles that queue jobs need `SELECT`, `INSERT` and
`UPDATE` on `scheduled_job_outbox`. The dispatcher's role also needs `DELETE`.

Rolling back `000009` removes the trigger and leaves pending publications for a
replica with outbox support to deliver. Before rolling back `000008`, pause
submissions and new scheduled releases, then let an outbox-capable jobs replica
drain the queue against a healthy broker. Migration `000008` refuses to drop
the table while it contains pending work.
