# ADR 007: Event-driven jobs and a Redis event bus for notifications

Status: accepted

## Context

Two related choices. First: how does submitted work get executed — does the
jobs service call workers directly, or is a job an event on a queue?
Second: how do clients find out a job changed state — polling, or pushed
events?

## Decision

- **Jobs are events.** `POST /api/jobs` writes the row to Postgres and
  publishes a message to the broker topic `jobs`. Workers consume that
  topic in a consumer group. The API call returns as soon as the job is
  durably queued; execution is fully asynchronous.
- **Notifications ride a Redis channel.** Every status transition (create,
  fence, success, retry, dead, cancel, requeue) publishes a small JSON
  event to `raven:events:jobs`. The websocket service subscribes and fans
  events out to the `jobs` room (and to `user:<owner_id>` when an owner is
  known). The console UI and any ws client see jobs move in real time.

## Alternatives

- **Synchronous execution.** The API call blocks until a worker finishes.
  Simpler mental model, terrible practice: slow jobs hold HTTP connections,
  retries block the caller, and you lose the whole point of a queue —
  decoupling submission from capacity.
- **Workers poll Postgres.** No broker needed for dispatch. Rejected: poll
  intervals trade latency against database load, SKIP LOCKED queues are
  their own rabbit hole, and again — the broker is the project.
- **Server-Sent Events or long polling** for notifications. SSE is
  one-directional (fine for events, useless for rooms/dm), long polling is
  a hack. WebSocket gives one connection for both directions, so the same
  pipe carries chat-style rooms and job events.
- **Workers call the websocket service directly.** Couples the job muscle
  to the push layer: websocket outages would backpressure workers, and
  scaling either side would drag the other.

## Consequences

Positive:

- Submission and execution are fully decoupled. Twenty workers or two, the
  API behaves the same; the queue absorbs bursts.
- Retries fall out naturally: a failed job is republished after a backoff
  timer, and at-least-once delivery turns "retry" from a feature we bolt on
  into the default behavior of the transport.
- The event bus makes the console feel alive with ~50 lines of publish code
  and one subscription loop.
- Failure isolation: publish failures are logged and dropped
  (`PublishJobEvent` is never fatal), so a Redis hiccup never blocks a job
  transition.

Negative:

- **Events are lossy.** Redis pub/sub has no history: a client that
  reconnects missed everything in between and must refetch state from the
  REST API. Acceptable because events are a nicety and Postgres is the
  truth — but you have to know that rule to not build on shaky ground.
- **Two sources of truth to keep consistent** for observers: the job row
  and the event stream. They can briefly disagree (event published, DB
  write visible a touch earlier/later). The UI treats events as hints and
  REST reads as truth.
- The `jobs.retry` topic exists but is unused today — retries are scheduled
  by in-process timers in the worker, not by a broker-side delay queue.
  The topic is there so that upgrade has a home later.
- Owner-targeted events (`user:<id>` rooms) depend on the gateway forwarding
  the caller's user id as `x-user-id` gRPC metadata — it does, on every
  upstream call — and anonymous/direct calls to the jobs service simply get
  no owner room. That is the intended behavior, not a gap.
