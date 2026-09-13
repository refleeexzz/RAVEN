# ADR 003: PostgreSQL as the source of truth

Status: accepted

## Context

Users, sessions, RBAC, jobs and job attempts all need durable storage. Jobs
in particular need atomic state transitions that multiple workers race over
— the database has to be the referee.

## Decision

One Postgres 17 instance, one schema, migrations as numbered SQL files
(`migrations/`, applied by `cmd/migrate` — no framework). Access through
`pgx` v5 connection pools. All state changes that must be atomic are single
SQL statements or explicit transactions.

## Alternatives

- **MongoDB.** Document-shaped payloads are tempting for jobs. Rejected
  because the job lifecycle is a state machine, and the fence trick — the
  dedup that makes at-least-once delivery safe — is one atomic statement:

  ```sql
  UPDATE jobs SET status='PROCESSING', started_at=now(), worker_id=$3
  WHERE id=$1 AND status IN ('QUEUED','RETRYING')
  RETURNING ...
  ```

  If that matches zero rows, the delivery is a duplicate or the job was
  cancelled, and the worker skips it. You can build compare-and-swap in
  Mongo, but in Postgres it is one line and `RETURNING` hands you the row.
- **MySQL/MariaDB.** Would have worked. Postgres won on JSONB (job payloads
  are stored as `jsonb`, queryable later), `citext` for case-insensitive
  emails, and pgx being the nicest Go driver of the bunch.
- **CockroachDB / distributed SQL.** Massive overkill for a learning
  platform with one database node.

## Consequences

Positive:

- Transactions make the hard parts small. Registration is user + role +
  profile + audit row in one transaction; refresh rotation with reuse
  detection is one `SELECT ... FOR UPDATE` transaction; finishing a job is
  attempt row + status update atomically.
- JSONB stores job payloads without a schema per job type, while CHECK
  constraints still pin the status enum at the database level.
- Partial unique index `WHERE idempotency_key IS NOT NULL` gives exactly
  the idempotency backstop the jobs service needs, small and cheap.
- Everyone already knows SQL. The whole schema is two migration files you
  can read in five minutes.

Negative:

- Single instance, no failover. Postgres down means most of the platform
  goes read-only-broken (see [../failure-scenarios.md](../failure-scenarios.md)).
- One shared schema for all services (the `public` schema) — deliberate for
  the demo, and documented in migration 000001 and ADR 008. A stricter
  setup would separate schemas per service.
- No migration framework means no lock management or out-of-order handling.
  Fine at two migrations; it will need revisiting if the schema grows.
