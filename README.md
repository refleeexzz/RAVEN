# RAVEN — Distributed Systems Platform

RAVEN is a distributed systems learning platform built in Go. It is shaped
like a production system: an API gateway, gRPC services behind it, a message
broker written from scratch, a worker pool, Postgres, Redis, Kubernetes
manifests, and a real observability stack. But it is not pretending to be
production software. I built it to learn how these pieces fit together by
actually writing them — and then by breaking them on purpose.

The thing it does, end to end: you register, log in, and submit jobs
(`send_email`, `resize_image`, `webhook`). The jobs service stores the job in
Postgres and publishes it to the broker. A pool of workers consumes the topic
through a consumer group, executes with bounded concurrency, retries failures
with exponential backoff, and sends hopeless jobs to a dead-letter topic.
Every status change is pushed to your browser over WebSocket in real time,
and there is a console UI to watch it all. If a worker dies mid-job — even
SIGKILL on every worker at once — leases expire, a sweeper requeues the work,
and generation fencing stops the dead workers' late writes. That last part is
chaos-tested, not hoped for.

Repo: [github.com/refleeexzz/RAVEN](https://github.com/refleeexzz/RAVEN) —
Go module `github.com/refleeexzz/RAVEN`.

Everything in this README describes what exists and runs today. Where the
code makes a trade-off, I say so. Where something is missing, it is in
[Known limitations](#known-limitations) or [Roadmap](#roadmap) — not in the
feature list.

## What this project demonstrates

- Go backend architecture and concurrency
- Distributed systems design, fault tolerance and recovery
- Custom TCP protocol design and message broker internals
- PostgreSQL transactions, Redis coordination
- gRPC microservices, WebSocket realtime
- Kubernetes, observability (Prometheus/Grafana/Jaeger)
- Chaos engineering and security testing

## Architecture

```text
                        ┌────────────────────────────────────────────┐
                        │                clients                     │
                        │     curl · console (:7100) · scripts       │
                        └───────────────────┬────────────────────────┘
                                            │ HTTP / WebSocket
                                            ▼
                               ┌─────────────────────────┐
                               │   gateway  :8080        │  CORS, authN/authZ,
                               │   REST edge             │  rate limit, circuit
                               │   timeouts · retries ·  │  breakers, /ws proxy
                               │   circuit breakers      │
                               └───┬───────┬───────┬─────┘
                        gRPC 9081  │       │ gRPC 9082/9083
              ┌────────────────────┘       └───────┬───────────┐
              ▼                                    ▼           ▼
      ┌───────────────┐   gRPC 9082      ┌──────────────┐  ┌──────────────┐
      │  auth         │◄────users CRUD──►│  users       │  │  jobs        │
      │  :9081        │                  │  :9082       │  │  :9083       │
      │  tokens, RBAC │                  │  profiles    │  │  lifecycle + │
      └──────┬────────┘                  └──────┬───────┘  │  sweeper     │
             │                                  │          └───┬────┬─────┘
             │                Postgres 5432     │              │    │ publish
             │             ┌────────────────┐   │              │    ▼
             ├────────────►│  postgres      │◄──┴──────────────┤ ┌───────────┐
             │             │  users, jobs,  │                  │ │ broker    │
             │             │  sessions,     │                  │ │ TCP :9100 │
             │             │  leases, ...   │                  │ │ 12 parts/ │
             │             └────────────────┘                  │ │ topic     │
             │                                                 │ └─────┬─────┘
             │               Redis 6379                        │       │ consume
             │             ┌─────────────────┐                 │       │ (group "workers")
             ├────────────►│  redis          │◄────────────────┤ ┌───────────┐
             │ denylist,   │  cache, pub/sub │   job events    │ │ worker ×N │
             │ perms cache │  presence,      │                 │ │ leases +  │
             └─────────────┤  worker registry │◄───────────────┤ │ heartbeats│
                           └───────┬─────────┘   registry beats│ └─────┬─────┘
                                   │ fanout                    │       │ events
                                   ▼                           │       ▼
                          ┌─────────────────┐                  └── redis pub/sub
                          │ websocket :8084 │                       (raven:events:jobs)
                          │ rooms, presence │
                          └─────────────────┘
                                   ▲
            browser connects via gateway /ws proxy (anonymous read-only
            in the dev cluster, JWT in production-shaped setups)

   observability: prometheus :9090 scrapes /metrics on every service,
   grafana :3000 (dashboard preloaded), jaeger :16686 — one trace covers
   gateway → gRPC → broker publish → worker execute → Postgres commit
```

Every service serves `/health`, `/ready` and `/metrics` on an ops port. The
full port/env-var table and the request-lifecycle deep dive live in
[docs/contracts/ports-and-env.md](docs/contracts/ports-and-env.md) and
[docs/architecture.md](docs/architecture.md).

## Engineering challenges

The hard problems this project actually had to solve:

- **Crash recovery.** Workers can be killed after acquiring a job — even
  SIGKILL on every worker at once. Jobs carry `lease_until`, `heartbeat_at`
  and an `execution_generation`; a sweeper (single-elected via a Postgres
  advisory lock) finds expired leases and requeues them. Proven live: 12/12
  in-flight jobs recovered to `SUCCESS`.
- **Zombie execution.** When a new worker takes over a job, the generation
  bumps — any late write from the old (zombie) worker is rejected by
  generation fencing. Same mechanism fences consumer-group offset commits.
- **Broker durability.** Append-only segmented logs (64 MiB segments),
  CRC-32C per record, sparse index (one entry per 4 KiB), crash recovery that
  truncates the torn tail and rebuilds corrupt or missing indexes. fsync
  every 100 ms or 256 records — tunable, and the benchmarks below show
  exactly what that knob costs.
- **Consumer coordination.** Server-side consumer groups: range assignor,
  10 s session timeout, at-least-once delivery. War story: in k8s we hit a
  **rebalance storm** — generations moved without real membership changes,
  consumers kept re-joining and throughput collapsed. Fix: only bump the
  generation when membership actually changes
  ([docs/broker-internals.md](docs/broker-internals.md)).
- **Backpressure.** Bounded per-partition produce queues return
  `BROKER_BUSY` instead of eating RAM; connection caps and idle/write
  deadlines keep slow clients from taking the broker down.

## Results

- **65,357 msg/s** peak broker throughput (spec goal was 50k — beaten)
- ~380,000 msg/s fetch throughput
- **106 broker tests** (protocol, storage, recovery, groups, hardening)
- `go test -race ./...` green
- 10/10 integration tests (testcontainers, real Postgres + Redis)
- 6/6 chaos scenarios: **zero data loss, zero duplicate executions**
- 12/12 jobs recovered after SIGKILL on all workers
- 10–17 s recovery across chaos scenarios
- CRC-protected WAL with crash recovery, generation-fenced job execution

## What's in the box

- **API gateway** (`:8080`) — the single public entrypoint. REST in, gRPC
  out. Per-route permission checks, token bucket rate limiter (100 req/min,
  burst 20), circuit breaker per upstream (5 consecutive transport failures →
  open 30 s → half-open probe), bounded retries on idempotent calls (2×, only
  on `Unavailable`), 10 s request timeout. WebSocket proxy at `/ws`.
  Full REST reference: [docs/api.md](docs/api.md).
- **auth service** (gRPC `:9081`) — register/login/refresh/logout. HS256 JWT
  access tokens (15 min), opaque 256-bit refresh tokens (7 days, SHA-256
  hashed at rest) with rotation and reuse detection — reusing a rotated
  token revokes all of that user's sessions. bcrypt cost 12. RBAC with roles
  `ADMIN`, `USER`, `WORKER`, `SERVICE`.
- **jobs service** (gRPC `:9083`) — job lifecycle
  `QUEUED → PROCESSING → SUCCESS | FAILED → RETRYING → SUCCESS | DEAD`.
  Idempotent create via `Idempotency-Key` (Redis fast path + Postgres unique
  index backstop), manual DLQ requeue, and the recovery sweeper.
- **worker pool** — consumes topic `jobs` in consumer group `workers`.
  Bounded concurrency (semaphore of 8), 30 s per-job timeout, retry backoff
  100 ms → 5 s cap, dead-lettering after 4 attempts. Handlers: `send_email`,
  `resize_image`, `webhook` (real HTTP POST). Live registry in Redis (15 s
  TTL) powering `GET /api/workers`.
- **the broker, built from scratch** (`:9100`) — custom framed TCP binary
  protocol with correlation ids and pipelining; segmented storage with CRC
  and sparse index; consumer groups; 12 partitions per topic by default (3
  was the measured bottleneck for consumer parallelism). Full internals:
  [docs/broker-internals.md](docs/broker-internals.md).
- **websocket service** (`:8084`) — rooms, presence, multi-replica fanout
  over Redis pub/sub, per-connection rate limit (20 msg/s).
- **console** (`web/console`) — React operations dashboard with a **Test
  Lab** page: one-click end-to-end check, mini load test, fail-on-purpose
  DLQ demo and a break-it-yourself chaos guide. Falls back to a built-in
  simulator when no backend is reachable.
- **observability** — Prometheus metrics everywhere (including
  `raven_broker_consumer_lag`, `raven_worker_fenced_writes_total`,
  `raven_gateway_circuit_breaker_state`), preloaded Grafana dashboard,
  end-to-end Jaeger traces (traceparent travels in broker record headers),
  structured `slog` JSON logs, real readiness probes.
- **ops** — Docker Compose for the laptop; Kubernetes manifests with HPAs
  (capped at `maxReplicas: 8` — uncapped autoscaling once starved the
  single Docker Desktop node and Postgres got evicted), probes, secrets,
  rolling updates.

## Testing and chaos

```bash
go test ./...                                         # unit tests
go test -race ./...                                   # with race detector — green
go test -tags=integration ./tests/integration/...     # 10/10, testcontainers
go test -run=^$ -bench=. -benchmem ./internal/broker/ # broker benchmarks
```

The failure scenarios are codified in `tests/chaos/` (kill workers mid-job,
restart broker/Redis/Postgres, kill consumers, kill multiple workers).
Measured results in [docs/chaos-results.md](docs/chaos-results.md): across
all 6, **zero data loss, zero duplicate executions**, recovery in 10–17 s
per scenario. Full testing guide: [docs/testing.md](docs/testing.md).
(Windows note: `-race` needs gcc via MinGW/MSYS2.)

## Benchmarks

Measured on my machine (Ryzen 5 5600X, Windows, 2026-09-13) — full results
and methodology in [docs/benchmarks.md](docs/benchmarks.md):

| Scenario | Throughput |
|----------|-----------|
| Peak produce (100 producers × 128 B) | **65,357 msg/s** |
| Produce, 100 producers × 1 KB | ~60,000 msg/s |
| Single-producer sync (1 KB) | ~8,500 msg/s |
| Consume (fetch) | up to ~380,000 msg/s |

What the fsync knob costs (same produce path):

| fsync policy | Throughput |
|--------------|-----------|
| `always` | ~1,700 msg/s |
| periodic (default: 100 ms / 256 records) | ~8,600 msg/s |
| disabled | ~9,000 msg/s |

These are my numbers on my hardware, not a guarantee; the suite is
reproducible, so run it on yours.

## Quickstart

The full platform runs on Docker Desktop's Kubernetes — that's the only
prerequisite (to hack on the code: Go 1.27+, and Node for the console).

```bash
kubectl apply -f deployments/kubernetes/namespace.yaml   # namespace first
kubectl apply -f deployments/kubernetes/                 # the rest
# or simply: make k8s-up
```

Wait for the pods, then open the console — it runs in the cluster (nginx,
LoadBalancer), so there is no port-forward to babysit:

```text
http://localhost:7100     console
http://localhost:3000     Grafana (admin/admin)
http://localhost:16686    Jaeger
http://localhost:9090     Prometheus
```

Sign in with the demo account (dev cluster only, do not reuse this anywhere):
`e2e@raven.dev / supersecret123`. Then create a job and watch it run:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"e2e@raven.dev","password":"supersecret123"}' | jq -r .access_token)

curl -X POST http://localhost:8080/api/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-first-job-v1' \
  -d '{"type":"send_email","payload":{"to":"me@example.com","subject":"hi","body":"from raven"}}'
```

Within a second or two the job moves `QUEUED → PROCESSING → SUCCESS`, live
in the console. Tear down with `make k8s-down`. (A Docker Compose variant
also exists — `docker compose up -d` — but k8s is the tested path.)

## Known limitations

Being honest about what this is:

1. **The broker is a single node.** No replication. The disk dies, the data
   is gone. This is the biggest gap between RAVEN and a real system, and
   fixing it is milestone 3 on the roadmap (the whole P1 track).
2. **No broker retention.** Logs grow forever; nothing compacts or deletes
   old segments yet (P3).
3. **Rate limiting is per-process.** With N gateway replicas each one has
   its own buckets, so the effective limit is `N × 100 rpm`.
4. **The token revocation denylist fails open.** If Redis is down, auth
   accepts any cryptographically valid token. Exposure is bounded by the
   15-minute access token TTL, but it is a real trade-off.
5. **Crash-loop blind spot.** A job that keeps crashing workers (think OOM
   on a poison payload) never burns attempts — the worker dies before it can
   record a failure, the sweeper requeues the job, and the loop repeats.
   The reasoning and the candidate fixes are in
   [docs/adr/009-job-leases.md](docs/adr/009-job-leases.md).
6. **No auth or encryption on the broker port** (`:9100`). It never leaves
   the cluster network (P3 covers TLS).
7. **Delivery is at-least-once end to end.** Consumers see duplicates
   sometimes. The job fence and generation fencing absorb them; anything new
   you build on the broker has to do the same.

## Roadmap

The tracked plan lives in
[RAVEN_IMPLEMENTATION_ROADMAP.md](RAVEN_IMPLEMENTATION_ROADMAP.md), with
checked items marking what shipped. Where it stands:

- **P0 — correctness and reliability: DONE.** Protocol hardening, storage
  recovery, consumer groups, stranded-job recovery (leases + fencing +
  sweeper), worker failure tests, chaos suite.
- **P2 — engineering proof: DONE.** Benchmark suite with measured results,
  consumer-lag metrics, advanced observability, distributed tracing.
- **P1 — distributed broker: NEXT.** Replication, acknowledgement modes,
  leader election, membership, partition reassignment, network-partition
  testing. This is the next step toward making the broker resilient beyond
  a single-node teaching system.
- **P3 — hardening: after that.** TLS, broker auth, retention, compaction.
- **P4 — next ideas**: Redis-backed rate limiting, KEDA-style autoscaling on
  consumer lag, cron-style job scheduling, JSON Schema per job type,
  priority-aware fetches, OpenAPI + generated console client, and more — the
  list is at the end of the roadmap doc.

## Docs

| Doc | What it covers |
|-----|----------------|
| [docs/architecture.md](docs/architecture.md) | request lifecycle, who talks to whom and why, data ownership, scaling |
| [docs/api.md](docs/api.md) | every REST route, the error envelope, the WebSocket protocol |
| [docs/broker-internals.md](docs/broker-internals.md) | broker protocol, storage, consumer groups, the rebalance-storm fix |
| [docs/failure-scenarios.md](docs/failure-scenarios.md) | what breaks, what you observe, how to reproduce it |
| [docs/chaos-results.md](docs/chaos-results.md) | measured results of the 6 chaos scenarios |
| [docs/benchmarks.md](docs/benchmarks.md) | full benchmark methodology and numbers |
| [docs/testing.md](docs/testing.md) | unit, race, integration, benchmarks, chaos |
| [docs/contracts/ports-and-env.md](docs/contracts/ports-and-env.md) | the platform contract: ports, env vars, routes, topics |
| [RAVEN_IMPLEMENTATION_ROADMAP.md](RAVEN_IMPLEMENTATION_ROADMAP.md) | the milestone plan (P0–P4) with shipped items checked |
| [deployments/README.md](deployments/README.md) | running with Compose or Kubernetes |
| [web/console/README.md](web/console/README.md) | the console, Test Lab and demo mode |

### Architecture Decision Records

The "why" behind the big choices, each with honest downsides:

1. [ADR 001 — Go](docs/adr/001-use-go.md)
2. [ADR 002 — gRPC between services](docs/adr/002-use-grpc.md)
3. [ADR 003 — PostgreSQL](docs/adr/003-use-postgresql.md)
4. [ADR 004 — Redis for ephemeral coordination](docs/adr/004-use-redis.md)
5. [ADR 005 — custom message broker](docs/adr/005-custom-message-broker.md)
6. [ADR 006 — Kubernetes](docs/adr/006-kubernetes.md)
7. [ADR 007 — event-driven jobs](docs/adr/007-event-driven-architecture.md)
8. [ADR 008 — shared users table](docs/adr/008-shared-users-table.md)
9. [ADR 009 — job leases and fencing](docs/adr/009-job-leases.md)

## License

RAVEN is released under the [Apache License 2.0](LICENSE).
