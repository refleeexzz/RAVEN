# Scheduling: delayed jobs, cron and replay

This doc covers everything time-related in the jobs service, all built on
migration `000004_scheduling`:

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

When the time comes, the **dispatcher** loop inside the jobs service wakes
up (every `JOBS_SCHEDULER_INTERVAL`, default **1s**), claims the due batch
with `SELECT ... FOR UPDATE SKIP LOCKED`, flips each job to `QUEUED` and
publishes it — **all in one transaction**. So:

- A job is only visible as `QUEUED` when its message is already on the
  broker. No "queued but unpublished" limbo.
- If the broker is down, the batch rolls back and stays `SCHEDULED` for the
  next pass. Nothing is lost.
- Replicas can all run the loop; `SKIP LOCKED` hands each row to exactly
  one of them.

Lateness is bounded: a due job fires at most one interval late (plus one
publish round-trip) when the broker is healthy.

### Rules for `scheduled_at`

| Value                        | Result                                   |
|------------------------------|------------------------------------------|
| `0` / absent                 | run now (normal `QUEUED` create)         |
| negative                     | `400 scheduled_at_invalid`               |
| more than 1 minute in past   | `400 scheduled_at_in_past`               |
| inside the 1-minute skew     | run now (no schedule recorded)           |
| future                       | `SCHEDULED`, fires at that time (UTC)    |

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

### Firing, exactly once

The cron scheduler loop (same `JOBS_SCHEDULER_INTERVAL`) locks due
schedules `FOR UPDATE SKIP LOCKED`, then **in one transaction**: inserts
the job, advances `next_run_at`, and publishes. That is the idempotency
story:

- Restart mid-fire → the transaction rolls back, the schedule is still
  due, the next pass fires it **once**.
- Multiple replicas → `SKIP LOCKED`, no duplicates.
- Broker down → everything rolls back, the run is retried next pass, not
  lost.

**Missed runs do not catch up.** If the service was down for three hours
on a minutely schedule, it fires once when it comes back and
`next_run_at` jumps to the next future match. No backfill storm.

Deleting a schedule is a hard delete: it stops firing immediately and the
row is gone (job history it spawned stays, of course).

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
| `JOBS_SCHEDULER_INTERVAL`  | `1s`    | Dispatcher + cron tick. `0` disables both loops.  |
| `JOBS_LEGACY_TOPIC_FANOUT` | `true`  | Also publish executions to the legacy `jobs` topic. |

## Metrics

| Metric                                  | Type    | Meaning                          |
|-----------------------------------------|---------|----------------------------------|
| `raven_jobs_scheduled`                  | gauge   | jobs waiting in `SCHEDULED`      |
| `raven_jobs_scheduled_dispatched_total` | counter | delayed jobs released to `QUEUED`|
| `raven_jobs_cron_created_total`         | counter | schedules accepted by CreateCron |
| `raven_jobs_cron_spawned_total`         | counter | job instances fired by cron      |

## Schema notes

Migration `000004_scheduling` adds `jobs.scheduled_at`,
`jobs.replayed_from`, the `SCHEDULED` status and the `cron_schedules`
table; the down migration restores the exact previous shape. The jobs
service **probes** for it at startup: against a pre-000004 schema the
binary behaves exactly like the old one and scheduling endpoints answer
`503 scheduling_unavailable` — rolling deploys stay safe in both
directions.
