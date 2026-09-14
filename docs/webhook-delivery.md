# Webhook delivery observability

Every time a worker tries to deliver a webhook, it writes down what happened.
Not just the wins — the 500s, the connection refusals, the timeouts, and the
targets the SSRF guard refused to touch. One attempt, one row. If you have
ever stared at a job that "should have called that URL" and had nothing to
look at, this table is for you.

## What gets recorded

The `webhook_deliveries` table (migration `000005_webhook_deliveries`):

| Column            | Meaning                                                            |
| ----------------- | ------------------------------------------------------------------ |
| `id`              | bigserial primary key                                               |
| `job_id`          | the job this attempt belongs to (FK to `jobs`, cascade on delete)   |
| `attempt`         | attempt number (`jobs.attempts` at finish time; restarts at 1 after a requeue, like `job_attempts`) |
| `url`             | the target URL from the payload                                     |
| `status_code`     | HTTP status when a response came back, `NULL` when it never did     |
| `latency_ms`      | round-trip to response headers, `NULL` when no request left the worker |
| `response_snippet`| first bytes of the response body, hard-capped at **1 KiB**           |
| `blocked`         | `true` when the egress guard refused the target (JOBS-01)            |
| `error`           | what went wrong, for blocked / transport / HTTP-error rows           |
| `ts`              | when the attempt was recorded (`now()`)                              |

Reads run through the `(job_id, ts)` index — a job's history comes back in
the order the attempts happened.

## What counts as an attempt

Everything, even the awkward cases:

- **success** — 2xx came back. Status, latency and a body snippet are stored.
- **HTTP error** — the target answered, but not 2xx (500, 404, ...). Status
  and snippet included, `error` says `got status N`.
- **transport error** — dial failure, DNS failure, timeout, context
  cancellation. `status_code` stays `NULL`, `error` carries the detail.
- **blocked** — the egress guard refused the target (private / loopback /
  link-local / reserved range, bad scheme, userinfo, too many redirects).
  `blocked = true`, no status code: the request never touched the wire.
- **invalid** — the payload itself was broken (no `url`, not a JSON object).
  Also recorded, so "why didn't this even try?" has an answer.

A delivery whose job lease is lost mid-flight is still recorded: the HTTP
call really happened, and the audit trail should say so. Retries produce one
row per attempt, so a job that died four times shows four rows.

## How it gets there (and why straight to Postgres)

The worker writes deliveries **directly into Postgres** instead of reporting
them over the jobs gRPC API:

- The worker already owns a Postgres pool for the lease/fencing machinery —
  no new dependency, no new failure mode.
- Delivery recording must **never** break execution. A gRPC hop would couple
  every webhook to jobs-service availability; a local insert keeps the blast
  radius at "one log line".

The write is best effort: if the insert fails (table missing on an old
schema, database hiccup), the worker logs a warning and moves on. The job
retries, succeeds or dies exactly as before — observability follows
execution, never the other way around. There is an integration test that
runs a webhook job against a schema without the table to prove it.

The snippet is truncated to 1 KiB in the handler before insert, clamped
again in the store function, and backed by a database `CHECK`. Three layers,
because payloads are creative.

## Reading deliveries

`Server.ListDeliveries(ctx, jobID, page, pageSize)` in
`services/jobs/deliveries.go` is the read path, owner-scoped exactly like
`GetJob` (JOBS-02):

- the **owner** sees their job's deliveries;
- `admin:*` bypasses;
- **no identity** at all means internal service-to-service traffic — full
  access;
- a **stranger** gets `NotFound`, so the endpoint is not an existence oracle
  for other people's jobs.

Pagination follows the platform rules: 1-based page, size defaults to 20,
caps at 100, page capped at `MaxListPage` (JOBS-04).

> **Wire-up status: DONE.** The rpc lives on its own proto service,
> `raven.jobs.v1.JobDeliveriesService` — a separate service because Go
> forbids two same-named methods on one type, and the plain-Go
> `Server.ListDeliveries` above is kept verbatim for direct internal
> callers. `DeliveriesServer` (services/jobs/server.go) is a thin adapter
> over that exact logic, registered next to `JobService` in
> services/jobs/service.go. REST: `GET /api/jobs/{id}/deliveries`
> (`jobs:read`) on the gateway.

## Metrics

```text
raven_worker_webhook_deliveries_total{outcome}
```

`outcome` is one of `success`, `http_error`, `transport_error`, `blocked`,
`invalid`. The counter ticks even when the database insert fails — it
measures attempts, not inserts. Handy alerts: a rising `blocked` rate means
someone is poking at internal targets; `transport_error` spikes mean the
network (or DNS) is having a day.

## Priority lanes (worker side)

Separate feature, same worker: the broker contract pins the topic family
`jobs.p1` … `jobs.p9` (`p1` = most urgent) plus the legacy `jobs` topic.

- **One consumer per topic**, all in the same `workers` group, all sharing
  the same lease / fencing / execution pipeline.
- The priority order is enforced at **dispatch**: execution slots are
  granted to the highest-rank queued message first. While urgent lanes
  saturate the worker, cheaper lanes wait in `Handle` (their offsets simply
  don't commit yet — the broker holds the work).
- **Anti-starvation budget:** a queued message is promoted one rank every
  2 seconds. Under a full p1 flood, legacy work still gets a slot in bounded
  time (worst case ~20 s with the default).
- **Retries keep their lane:** a retry republish goes back to the topic the
  message was consumed from instead of falling back to legacy.

Configuration via `WORKER_PRIORITY_TOPICS` (default `1..9+legacy`):

| Example        | Topics consumed                          |
| -------------- | ---------------------------------------- |
| `1..9+legacy`  | `jobs.p1` … `jobs.p9`, `jobs` (default)  |
| `1..3,legacy`  | `jobs.p1` … `jobs.p3`, `jobs`            |
| `legacy`       | only `jobs` (old behaviour)              |
| `1`            | only `jobs.p1`                           |

Items are separated by `,` or `+`; an item is `legacy`, a level `1`-`9`, or
a range like `1..3`. Unknown items and out-of-range levels fail the boot —
a typo must not silently drop a lane. During the migration window the jobs
service dual-publishes (priority topic + legacy), so a job can be delivered
twice; the claim fence skips the second copy, as with any redelivery.
