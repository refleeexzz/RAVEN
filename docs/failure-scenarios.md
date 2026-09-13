# Failure scenarios

What breaks, what the platform does about it, what you observe, and how to
reproduce it on the local Compose stack. Everything below is implemented
behavior — where the honest answer is "it strands and a human has to fix
it", that is what the section says.

Quick reference for the reproduction commands: the Compose project is named
`raven`, so containers are `raven-worker-1`, `raven-postgres-1`, etc.
`docker compose ps` shows the current names.

## Worker killed mid-job (graceful)

**What happens.** SIGTERM (this is what `docker compose stop worker` sends):
the consumer stops fetching, the worker drains in-flight jobs for up to
15 s, flushes every pending retry timer by republishing immediately, closes
the producer, and deletes its registry entry. Nothing strands. A job
mid-execution either finishes inside the drain window or is left running
past it — see the next section for what that costs.

**What you observe.** Log lines: `flushed pending retries at shutdown`. The
worker's `worker:<id>` key disappears from Redis immediately (clean delete,
not TTL expiry), so `GET /api/workers` stops listing it. Inflight jobs
complete and publish their final `job_status` events.

**Reproduce.**

```bash
curl -X POST http://localhost:8080/api/jobs -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"type":"send_email","payload":{"to":"a@b.c","subject":"s","body":"b"}}'
docker compose stop worker    # during the ~200-800ms of "sending"
docker compose logs worker --tail=20
```

## Worker `kill -9` (the stranding window)

**What happens.** This is the one to understand. The worker commits the
broker offset **at dispatch** — when the handler goroutine starts, not when
it finishes. That keeps the consumer loop hot (offset commits don't wait on
30 s jobs), but it means a hard kill can leave up to `concurrency` (8)
dispatched-but-unfinished jobs in `PROCESSING` (or a scheduled retry in
`RETRYING` whose timer died with the process) with no message in flight.
The broker notices the dead member after the 10 s session timeout,
rebalances, and its partitions move to the surviving workers — but the
committed offsets mean those messages are **not** redelivered.

The honest status: a sweeper that rescans for jobs stuck in
`PROCESSING`/`RETRYING` past a deadline and republishes them is a
**documented TODO** (`services/worker/worker.go`), not a built feature.
Postgres is the source of truth, so nothing is lost — the rows are right
there with `status='PROCESSING'` — but today an operator (or the DLQ
requeue endpoint, after marking them) has to nudge them.

Two sub-cases are handled:

- Died **before** the offset commit (fetch in flight, handler not yet
  dispatched): the rebalance moves the partition, the surviving worker
  refetches from the last committed offset, the message **is** redelivered,
  and the fence dedups it if the dead worker somehow already started it.
- Died **after** dispatch: stranded, as above.

**What you observe.** `raven_broker_messages_pending` for the dead worker's
partitions stops moving. Broker logs show the member being expelled and the
group rebalancing. `GET /api/workers` drops the entry after ~15 s (TTL).
Stuck jobs show up as rows in `PROCESSING` with an old `started_at`:

```bash
docker compose exec postgres psql -U raven -d raven \
  -c "select id, status, started_at from jobs where status='PROCESSING';"
```

**Reproduce.**

```bash
docker kill raven-worker-1          # SIGKILL — no drain, no flush
docker compose logs broker --tail=20 | grep -i rebalance
```

## Redis down

Redis is the platform's ephemeral-coordination layer, and every service
degrades differently — by design, nothing hard-crashes.

**auth**: the revocation denylist check **fails open**. With Redis gone,
`ValidateToken` accepts any cryptographically valid JWT rather than taking
the platform down with the cache. Exposure is bounded: revoked tokens live
at most 15 minutes (the access-token TTL). The permission cache falls back
to Postgres reads. Log line to grep: `revocation denylist unavailable,
failing open`.

**jobs**: idempotency loses its fast path — `claimIdempotencyKey` errors are
logged and the Postgres unique index becomes the only guard (which is fine;
it was always the backstop). Job status events are **dropped**: publish
failures are logged, never fatal. The event stream is a UI nicety, not the
truth.

**gateway**: `GET /api/workers` returns `503 worker_registry_unavailable`.
Everything else works (the rate limiter and auth cache are in-memory).

**worker**: execution continues. Registry heartbeats fail (logged, retried
every 5 s), so the worker vanishes from `GET /api/workers` while alive. Job
events drop.

**websocket**: presence stops (`presence:user:*` can't be written), and
cross-replica fanout degrades to **local only** — room messages still
deliver to clients on the same replica, other replicas miss them. Job event
push stops entirely (the subscription is down). The fanout loop
resubscribes with backoff (500 ms doubling to 5 s) and catches up when Redis
returns. Missed events are gone — pub/sub has no history.

**Readiness**: every service that uses Redis reports `503` on `/ready` while
it is down (each registers a Redis checker), so k8s/Compose healthchecks
show the blast radius immediately even though traffic still flows.

**Reproduce.**

```bash
docker compose stop redis
curl http://localhost:8080/ready          # 503, names redis
curl http://localhost:8080/api/workers -H "Authorization: Bearer $TOKEN"   # 503
docker compose logs auth --tail=20        # failing-open warnings
docker compose start redis                # everything heals by itself
```

## Postgres down

**What happens.** Postgres is the source of truth, so this one hurts the
most — and it is loud, not silent:

- auth/users/jobs/worker all report `/ready` 503 (each registers a Postgres
  checker).
- Gateway calls into those services start failing with `Unavailable`/
  `DeadlineExceeded`. Those count against the circuit breakers: after 5
  consecutive transport failures the breaker for that upstream opens, and
  for the next 30 s calls fail fast with `503 circuit_open` instead of
  waiting 5 s each. A single half-open probe after 30 s decides whether to
  close again.
- Logins and registrations fail (no user rows). Reads fail. Job creation
  fails.
- Workers mid-job can't run the fence: the fence update fails, the worker
  logs `fence update failed, requeueing message` and republishes the message
  after 2 s. It keeps doing that until the database is back — the offset was
  already committed, so this republish loop is the compensating action.
- Completion writes can fail too (`could not record success`). The job then
  sits in `PROCESSING` — same stranding story as the kill -9 section, same
  sweeper TODO.

**What you observe.** `raven_gateway_circuit_breaker_state{upstream="auth"}`
flips to `1`, then `2` during the probe, then `0` when Postgres is back.
Gateway logs show `circuit breaker state change` warnings. Readiness across
the platform goes red; the Grafana dashboard's error-rate panel spikes.

**Reproduce.**

```bash
docker compose stop postgres
curl http://localhost:8080/api/jobs -H "Authorization: Bearer $TOKEN"   # 503s
docker compose logs gateway --tail=30     # breaker transitions
docker compose logs worker --tail=30      # fence requeue loop
docker compose start postgres
```

## Broker down

**What happens.**

- **Job creation fails loudly.** The jobs service inserts the row, tries to
  produce, fails, marks the row `FAILED` with the error, and returns
  `503 broker_produce_failed`. No silent loss — you never get a `201` for a
  job that is not actually queued. (Idempotency keys are released so a retry
  with the same key can succeed later.)
- The jobs service and worker **refuse to boot** without the broker:
  `EnsureTopics` is fatal at startup with a 30 s budget. Better to crash
  loop than to accept jobs that can never run.
- Running workers lose their consumer connection and reconnect with
  exponential backoff (50 ms doubling to 2 s, bounded by context). Scheduled
  retry republishes fail and are logged loudly (`retry republish failed; job
  stays RETRYING until requeued`) — those jobs wait for the sweeper/requeue.
- jobs and worker `/ready` report 503 (`broker` checker).

**What you observe.** `curl http://localhost:8080/ready` stays green at the
gateway (it checks its gRPC upstreams and Redis, not the broker), but
`POST /api/jobs` 503s. jobs/worker logs show reconnect attempts. When the
broker returns, consumers re-join the group and resume from committed
offsets.

**Reproduce.**

```bash
docker compose stop broker
curl -X POST http://localhost:8080/api/jobs -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"type":"send_email","payload":{"to":"a@b.c","subject":"s","body":"b"}}'
# 503 broker_produce_failed; the row exists, marked FAILED
docker compose start broker
```

## A gateway replica dies

**What happens.** The gateway is stateless — rate-limit buckets and the auth
cache are in-memory and rebuild themselves — so other replicas just keep
serving. Clients on the dead replica see connection resets and retry against
the load balancer. In Compose there is exactly one gateway, so this scenario
is a k8s thing (3 replicas by default there): `kubectl -n raven delete pod
<gateway-xyz>` and the Deployment replaces it; readiness gates the new pod
until its upstreams connect.

Side effect worth knowing: per-process rate limits mean the *effective*
limit drops by one replica's share until the pod is back. Documented in the
README limitations.

## Duplicate delivery

**What happens.** The broker is at-least-once: a crash after processing but
before committing, or a rebalance at the wrong moment, delivers a message
twice. Two layers absorb this:

1. **The fence.** Before executing, the worker runs
   `UPDATE jobs SET status='PROCESSING' ... WHERE status IN ('QUEUED','RETRYING')`.
   The second delivery matches zero rows and is skipped
   (`job skipped by fence (cancelled, duplicate or finished)` in the logs).
2. **Idempotency keys on create.** Same `Idempotency-Key` header → same job
   returned, never a second row. Redis claim first (24 h), Postgres unique
   index as the backstop.

Net effect: a job's handler can still run more than once in the
crash-between-fence-and-finish window (the fence guards *starts*, not
*finishes*), so handlers are written to be side-effect-safe for the demo
types. Exactly-once side effects would need the handler itself to dedup —
that is a property of your handler, not something the platform can give you.

**Reproduce.** Kill a consumer *before* dispatch rather than after:

```bash
# publish a burst, then SIGKILL a worker while fetches are in flight
for i in $(seq 1 50); do curl -s -X POST http://localhost:8080/api/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"type":"resize_image","payload":{"n":'$i'}}' -o /dev/null; done
docker kill raven-worker-2
# watch the survivors: redelivered messages, fence skips in the logs
docker compose logs worker --tail=50 | grep -i "skipped by fence"
```

## Broker crash (bonus: what durability actually means)

The broker fsyncs every 100 ms or 256 records, whichever comes first. A
crash can therefore lose up to that window of **un-acked-by-disk** data —
but never anything the producer got an ack for *and* the fsync already
covered. On restart, the active segment is scanned record by record
(CRC-32C + contiguous offsets); the first bad record wins, the file is
truncated at the last good byte, and the index is rebuilt. The torn tail is
gone, the WARN log says exactly which byte range was dropped, and the
broker keeps serving. Older segments are trusted (corruption there is
detected on read, not repaired — known limitation).

```bash
docker kill raven-broker-1 && docker compose start broker
docker compose logs broker --tail=30   # look for truncation WARNs (usually none)
```
