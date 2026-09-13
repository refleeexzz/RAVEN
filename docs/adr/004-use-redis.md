# ADR 004: Redis for ephemeral coordination

Status: accepted

## Context

The platform needs a place for data that is fast to read, fine to lose, and
shared across replicas: the token revocation denylist, the permission cache,
idempotency claims, the worker registry, presence, and three pub/sub
channels (job events, websocket fanout, presence transitions).

## Decision

One Redis 8 instance, accessed with `go-redis` v9. The rules: Redis holds
**ephemeral coordination only** — nothing lives there that Postgres or the
broker cannot rebuild or outlive, and every Redis failure degrades the
service instead of killing it.

Key map (TTLs from the code):

| Key / channel | Owner | TTL / nature |
|---------------|-------|--------------|
| `revoked:jti:<jti>` | auth | remaining token lifetime |
| `perms:<user_id>` | auth | 30 s |
| `idem:<key>` | jobs | 24 h |
| `worker:<id>` | worker | 15 s, refreshed every 5 s |
| `presence:user:<id>` | websocket | 90 s, refreshed every 30 s |
| `raven:events:jobs` | jobs/worker publish → websocket | pub/sub channel |
| `raven:ws:fanout`, `raven:ws:presence` | websocket inter-replica | pub/sub channels |

## Alternatives

- **etcd / Consul.** The "right" answer for coordination in a serious
  system: strong consistency, watches, leader election. Rejected for weight
  — running etcd to store "is this token revoked" and "which workers are
  alive" is like hiring an accountant to track your coffee budget. Redis is
  one container everyone can reason about.
- **Postgres for everything** (e.g. `LISTEN/NOTIFY` instead of pub/sub).
  Postgres already carries the durable state, and adding the hot
  heartbeat/fanout traffic to it would couple the database's health to
  features that should never take it down. Separation of concerns won.
- **NATS.** Would double as both cache-ish KV (JetStream KV) and pub/sub,
  but the broker is already the messaging learning project; adding a second
  message system would blur the story.

## Consequences

Positive:

- One small dependency covers caching, TTL state and pub/sub. The docker
  compose service is ten lines.
- TTLs do the cleanup work: dead workers and stale presence disappear on
  their own.
- Pub/sub decouples producers from consumers — the worker publishing a job
  event has no idea the websocket service exists.
- `SCAN worker:*` from the gateway turns "who is alive right now" into a
  cheap read with zero coordination code.

Negative:

- **Pub/sub has no memory.** Events published while the websocket service
  (or Redis) is down are gone. Acceptable because events are a UI nicety —
  the job row in Postgres is the truth — but it surprises people.
- Single instance, no failover. Redis down means degraded mode everywhere
  (documented in [../failure-scenarios.md](../failure-scenarios.md)).
- The fail-open denylist is a deliberate security trade-off that only works
  because access tokens are short (15 min). If token lifetime ever grows,
  this decision needs to be revisited.
- Ephemeral-by-policy is enforced by code review, not by Redis. Nothing
  stops a future contributor from putting durable data here except docs
  like this one.
