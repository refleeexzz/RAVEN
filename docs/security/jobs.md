# Security audit — job system (services/jobs, services/worker)

Scope: the jobs gRPC service, the worker service, their Postgres store and
the `cmd/jobs` / `cmd/worker` wiring. Audited against the pre-release tree at
commit `9535fe4` (the vulnerable code is preserved in git history).

Method: threat-model the job lifecycle (create → queue → claim → execute →
finish/retry/DLQ), attack every trust boundary with regression tests in
`tests/security/jobs_test.go` (no infra needed) and
`tests/security/jobs_integration_test.go` (`integration` tag,
testcontainers). Each real finding went: failing test → minimal fix → green.

**TL;DR — 4 real vulnerabilities found and fixed, 1 hardening added, 8 hunt
items verified clean.**

| ID | Title | Severity | Status |
|----|-------|----------|--------|
| JOBS-01 | Webhook handler is a full SSRF proxy | **critical** | fixed |
| JOBS-02 | No owner scoping on the jobs API (BOLA/IDOR) | **high** | fixed |
| JOBS-03 | CreateJob accepted unbounded payloads | medium | fixed |
| JOBS-04 | ListJobs offset overflowed int32 on huge pages | low | fixed |
| JOBS-05 | Webhook response body drained without a cap | low | hardened |
| JOBS-06 | Goroutine/connection lifecycle | info | verified clean |

---

## JOBS-01 — SSRF in the webhook handler

**Severity: critical.**

### Impact

The worker's `webhook` handler POSTed the job payload to an arbitrary
user-supplied `payload.url`. Any authenticated user with `jobs:create` could
turn the cluster into a request proxy running from *inside* the network:

- `http://broker:9101/topics` — broker ops API (topic admin)
- `http://postgres:5432` — raw TCP probe (connection semantics leak)
- `http://169.254.169.254/latest/meta-data/` — cloud instance metadata
  (IAM credentials on AWS/GCP/Azure when deployed on a VM)
- any internal service's `/debug` or ops endpoints (8083/8085/9101...)

Response *content* was not returned to the caller, but the request itself is
the attack: internal POSTs carry the job payload as the body, and
side-effect endpoints need no read-back. This was server-side request
forgery by design.

### Reproduction (pre-fix)

```json
POST /api/jobs {"type":"webhook","payload":{"url":"http://169.254.169.254/latest/meta-data/"}}
```

The job ran and the cluster issued the request. `services/worker/handlers.go`
called `client.Do(req)` on the raw user URL with no validation at all.

### Fix

New egress guard (`services/worker/egress.go`), enforced at three layers:

1. **Pre-flight** (`EgressGuard.CheckURL`): scheme must be `http`/`https`,
   userinfo is rejected (`http://public@127.0.0.1/` trick), the hostname is
   resolved and **every** resolved IP is checked — one bad IP fails the
   target (fail-closed).
2. **Dial** (`dialContext` on the transport): the dialer resolves the
   hostname itself and dials only an IP that passed the range check, closing
   the DNS-rebinding TOCTOU between validation and connection.
3. **Redirect** (`CheckRedirect`): every hop is re-validated and the chain
   is capped at `MaxWebhookRedirects = 2`, so a public URL cannot bounce the
   worker into the internal network.

Blocked ranges: loopback (127/8, ::1), private (10/8, 172.16/12,
192.168/16, fc00::/7), link-local (169.254/16, fe80::/10 — this kills the
cloud-metadata path), unspecified, multicast, CGNAT (100.64/10),
192.0.0.0/24, 198.18/15, 240/4. IPv4-mapped IPv6 forms (`::ffff:127.0.0.1`)
are unmapped before classification.

Policy refusals are **PermanentError** (job goes DEAD, no retries — retrying
cannot make a private IP public); DNS failures stay retryable.

Escape hatch for dev/tests: `WORKER_WEBHOOK_ALLOW_PRIVATE=true` (env) or
`Params.AllowPrivateWebhooks` / `Config.WebhookAllowPrivate`. httptest
servers live on loopback, so the integration suites need it. **Run the
integration suites with the flag set:**

```sh
WORKER_WEBHOOK_ALLOW_PRIVATE=true go test -tags=integration ./tests/integration/ ./tests/security/
```

(The pre-existing `jobs_worker_test.go` / `job_recovery_test.go` create
webhook jobs against loopback httptest servers; they inherit the env flag —
they were not modified.)

### Regression tests

`tests/security/jobs_test.go`:

- `TestWebhookBlocksPrivateAndReservedTargets` — 17 literal targets
  (loopback/private/link-local/metadata/CGNAT/multicast/reserved/v4-mapped-v6/
  userinfo), all refused permanently.
- `TestWebhookBlocksNonHTTPSchemes` — file/ftp/gopher/scheme-less refused.
- `TestEgressGuardResolution` — DNS-level matrix via fake resolver: public
  allowed, internal/metadata/mixed-A-records/v6 blocked, DNS error retryable.
- `TestEgressGuardRedirectPolicy` — redirect to 169.254.169.254 blocked;
  chain capped (`ErrRedirectLimit`).
- `TestWebhookHandlerBlocksLoopbackServerByDefault` — real httptest server,
  zero requests leave the worker (hit counter == 0).
- `TestWebhookHandlerAllowsPrivateWhenEnabled` — escape hatch works.
- `TestWebhookRedirectsCapped` — infinite redirect chain dies permanently
  after the budget.

`tests/security/jobs_integration_test.go`:
`TestIntegrationWebhookSSRFEndToEnd` — full path (gRPC → broker → worker →
HTTP): default worker never hits the loopback target and the job goes DEAD
with an egress-refusal error; after `WORKER_WEBHOOK_ALLOW_PRIVATE=true` the
requeued job succeeds.

---

## JOBS-02 — BOLA / IDOR: no owner scoping on the jobs API

**Severity: high.**

### Impact

`owner_id` was stored but never enforced: any authenticated user could
`GetJob` / `ListJobs` / `CancelJob` / `RequeueJob` for **any** owner's jobs.
That leaks other tenants' payloads (GetJob returns `payload_json`, which for
`send_email` contains recipient + body) and lets anyone cancel or resurrect
anyone's work. Bonus leak: `CreateJob` idempotent replay returned the
existing job regardless of who owned the idempotency key.

### Product rule chosen (self-hosted platform)

Owner-scoped by default:

- The gateway terminates auth and forwards `x-user-id` (+ `x-user-perms`) as
  gRPC metadata. A call **with** an identity is owner-scoped: users see and
  manage only their own jobs; denials return **NotFound**, not
  PermissionDenied, so foreign jobs are indistinguishable from missing ones
  (no existence oracle).
- `admin:*` in `x-user-perms` bypasses scoping (CheckPermission-style, via
  `internal/auth.HasPermission`).
- A call with **no identity at all** is internal service-to-service traffic
  on the private gRPC port → full access. This keeps the worker/sweeper and
  the existing integration suite (which dials gRPC directly) working, and it
  is safe exactly because the gateway always forwards the identity for user
  requests.
- A **malformed** identity (`x-user-id` not a UUID) or permissions presented
  **without** an identity are rejected with Unauthenticated — never silently
  trusted, never a free upgrade.
- Ownerless jobs (created by system callers) are visible only to admins and
  system callers.

### Fix

`services/jobs/authz.go` (`Caller`, `CallerFromMetadata`/`CallerFromContext`,
`CanAccessJob`, `AuthorizeJob`, `OwnerScope`), wired into all five RPCs in
`server.go`. `ListJobs` pushes the scope into SQL (`store.go`: owner filter
on both the count and the page query). The idempotency fast path (Redis) and
the database backstop both return `AlreadyExists/idempotency_key_taken` when
the key belongs to a different owner — the job itself never crosses owners.

### Regression tests

- `tests/security/jobs_test.go`: `TestCallerFromMetadata` (system / user /
  admin / malformed-UUID / perms-without-id injection),
  `TestCanAccessJob` (full matrix), `TestAuthorizeJobHidesExistence`
  (KindNotFound on denial).
- `tests/security/jobs_integration_test.go`:
  `TestIntegrationJobOwnerIsolation` (B blocked from A's job on all four
  RPCs + listing, A OK, admin OK, system OK, ownerless hidden from users,
  malformed identity → Unauthenticated, forged perms → Unauthenticated) and
  `TestIntegrationIdempotencyAcrossOwners` (same-owner replay works,
  cross-owner key reuse → AlreadyExists, no job leak).

### Known gap (not in this service's hands)

The gateway forwards only `x-user-id` today, not `x-user-perms` — so until
the gateway forwards the permission list, the jobs service never sees the
admin bypass on gateway-routed traffic (safe direction: less privilege, not
more). The metadata contract addition is tracked for the gateway audit.

---

## JOBS-03 — Unbounded payload on CreateJob

**Severity: medium.**

`payload_json` had no size cap: it is stored as jsonb, copied into every
broker message and re-read on every attempt. A user could create
multi-megabyte jobs and burn storage, broker bandwidth and worker memory.

**Fix:** `MaxPayloadBytes = 64 KiB` enforced in `ValidateCreate` before the
JSON parse (cheap reject), mapped to InvalidArgument (`payload_too_large`).
The boundary is pinned: exactly 64 KiB accepted, one byte over rejected.

**Tests:** `TestValidateCreatePayloadCap` (unit),
`TestIntegrationPayloadCapOverGRPC` (wire).

---

## JOBS-04 — ListJobs pagination offset overflow

**Severity: low.**

`int((page-1)*size)` was computed in **int32** arithmetic: `page=MaxInt32,
page_size=100` wraps to a negative OFFSET and Postgres answers with an
error → 500 on a syntactically valid request, and deep pages are a database
scan DoS vector anyway.

**Fix:** `NormalizePage` clamps page to `[1, MaxListPage=1_000_000]`, size to
`[1, 100]` (defaults 1/20), and the offset is computed in 64-bit
(`pageOffset`). 

**Tests:** `TestNormalizePageClamps` (unit, incl. the MaxInt32 case),
`TestIntegrationPaginationNoOverflow` (wire).

---

## JOBS-05 — Webhook response body drained without a cap

**Severity: low (hardening).**

The handler never parsed the response body, so there was no direct memory
blow-up, but an early `Close` on an unread body leaves the transfer to
abort semantics; a hostile endpoint streaming forever would pin the handler
until the client timeout. The drain is now explicit and capped at
`MaxWebhookResponseBody = 1 MiB`, which also keeps connection reuse sane.

**Test:** `TestWebhookResponseBodyCapped` (4 MiB body drains fast, success).

---

## JOBS-06 — Lifecycle verification (goroutines, timers, connections)

**Severity: info — verified clean.**

- Retry republish timers: one `time.Timer` per job id in a mutex-guarded
  map, bounded by backoff ≤ 5 s, flushed on graceful `Shutdown`, dropped on
  `HardStop` (pinned by `TestHardStopDropsPendingRetries`).
- Lease-renewal goroutines exit on handler-ctx done *and* on aliveCtx
  (pinned by `TestLeaseRenewalStopsWithHandlerContext`,
  `TestHardStopStopsLeaseRenewal`).
- The webhook handler spawns nothing: `TestWebhookHandlerNoGoroutineLeak`
  runs 100 executions (allowed + blocked mix) and asserts the goroutine
  count settles back.

---

## Clean bills (verified, no bug)

- **SQL injection** — every query in `store.go` is parameterized
  (`$1`..`$n`); no ILIKE, no string-built filters; `ORDER BY` is a static
  whitelist-free literal; pagination values are ints clamped by
  `NormalizePage`. The owner filter binds as `$3::uuid`.
- **Command injection** — zero `os/exec` usage anywhere in
  `services/jobs`, `services/worker`, `cmd/jobs`, `cmd/worker` (grep-verified).
- **SSTI / template injection** — no template engine in the job path;
  `send_email` is a log stub and the subject only ever reaches slog.
- **Insecure deserialization** — `payload_json` is data-only: stored as
  jsonb, unmarshalled into fixed structs (`emailPayload`, `webhookPayload`,
  `map[string]any` probe). No gob, no eval, no reflection-driven decoding.
- **Log injection** — production logging is `slog` with the JSON handler
  (`pkg/logger`), which escapes control characters; user-controlled values
  (job type, email subject) travel as JSON string fields, not format strings.
- **Integer abuse** — `priority` must be 1–10 (0 → default 5),
  `max_attempts` 1–25 (0 → default 4; negative rejected), idempotency key
  ≤ 255 chars. All pinned by `TestValidateCreate` +
  `TestValidateCreatePayloadCap`.
- **Unknown job type (mass-assignment / DLQ-flood concern)** — rejected at
  CreateJob with InvalidArgument (`job_type_unknown`), so the public API
  cannot flood the DLQ. Hand-crafted broker messages with unknown types
  still go DEAD by design — the broker is an internal trust boundary.
- **TOCTOU on the claim fence** — `StartJobFence` is a single atomic
  `UPDATE ... WHERE status IN (...) AND generation matches`; no read-check-
  write gap.
- **Replay / stale generation** — broker messages carry
  `execution_generation`; stale messages match zero rows and are skipped
  (pinned by `TestJobGenerationFenceAndRenew`, `TestJobsWorkerLifecycle`).
- **Duplicate delivery** — same fence dedups at-least-once redelivery.
- **Timeouts** — webhook client: 5 s overall timeout (pinned by
  `TestWebhookRespectsClientTimeout`) + 5 s dial timeout on the guarded
  dialer; job handlers run under `WORKER_JOB_TIMEOUT`; every DB call in the
  worker carries a context deadline.

## Limitations of this audit

- The integration-tagged tests compile and skip cleanly on machines without
  Docker (verified); the full container run happens in CI.
- BOLA admin bypass depends on the gateway forwarding `x-user-perms`; it
  currently forwards only `x-user-id` (fails safe: admins get owner scope
  until the gateway ships the header).
- `WORKER_WEBHOOK_ALLOW_PRIVATE` is a documented dev/test escape hatch;
  deployments should leave it unset. The Makefile/CI integration targets
  need it in the environment for the pre-existing webhook tests.
