# RAVEN — Infrastructure & Supply Chain Security Audit

**Date:** 2026-09-14 · **Auditor:** infra audit subagent · **Scope:** `deployments/**`, `docker-compose.yml`, `.github/**`, `Makefile`, `.dockerignore`, `.gitignore`, secrets handling, dependency scan. App code (`services/`, `internal/`, …) is covered by parallel auditors and is out of scope here.

**Cluster used for verification:** Docker Desktop Kubernetes, namespace `raven` (live, running the full stack).

Every finding below was reproduced against real state, fixed in config, and verified again on the live cluster. Commands and observed outputs are quoted.

---

## Findings summary

| ID | Title | Severity | Status |
|----|-------|----------|--------|
| INFRA-01 | No NetworkPolicies — broker's unauthenticated TCP port open to every pod | **High** | ✅ Fixed & verified live |
| INFRA-02 | No pod/container `securityContext` on any workload | Medium | ✅ Fixed & verified live |
| INFRA-03 | Console container ran as root (nginx on :80) | Medium | ✅ Fixed & verified live |
| INFRA-04 | Redis container entered as root (su-exec drop later) | Medium | ✅ Fixed & verified live |
| INFRA-05 | Postgres container entered as root | Medium | ✅ Fixed & verified live |
| INFRA-06 | Default ServiceAccount with automounted API token on every pod | Medium | ✅ Fixed & verified live |
| INFRA-07 | `secrets.yaml` not covered by `.gitignore` | Low | ✅ Fixed |
| INFRA-08 | Root `secrets.yaml` not covered by `.dockerignore` | Low | ✅ Fixed |
| INFRA-09 | CI workflow had no `permissions:` block | Low | ✅ Fixed |
| INFRA-10 | govulncheck: 0 code vulns; 1 module-level note (x/crypto/openpgp, unused) | Info | 📄 Documented |
| INFRA-11 | LoadBalancer services expose UIs without auth (dev-only by design) | Info | 📄 Documented + comments |
| INFRA-12 | `/metrics`, `/ready`, `/debug/stats` disclosure on ops ports | Info | 📄 Documented + scoped by INFRA-01 |
| INFRA-13 | Images pinned by tag, not digest; Docker Desktop sticky-tag trap | Info | 📄 Documented |
| INFRA-14 | Dev credentials in the tree (postgres raven/raven, JWT dev default, grafana admin/admin) | Info | 📄 Documented as dev-only |
| INFRA-15 | busybox initContainer in migrate Job had no resource limits | Low | ✅ Fixed |
| INFRA-16 | No privileged containers / hostNetwork / hostPID / hostIPC anywhere | — | ✅ Verified clean (non-issue) |
| INFRA-17 | Declared worker replicas (5) exceed Docker Desktop node RAM | Info | 📄 Documented |

---

## INFRA-01 — No NetworkPolicies (broker 9100 open to all pods)

**Severity:** High (the only control between a compromised pod and the raw message log)

**Impact / scope.** The broker's data port `9100` has no application-level authentication (by design, app-level audit). Before this audit the namespace had **zero** NetworkPolicies, so *any* pod in the cluster — including a compromised third-party one — could produce/fetch messages freely. Same for postgres (5432), redis (6379) and every ops port.

**Evidence (before).**

```text
$ kubectl -n raven get networkpolicies
No resources found in raven namespace.
```

Enforcement sanity check (scratch namespace, deny-all applied, then probe):
`wget http://server:8080` from another pod → `wget: download timed out / BLOCKED` → **Docker Desktop's CNI does enforce NetworkPolicy**, so the fix below is a real control, not decoration.

**Fix.** New file `deployments/kubernetes/networkpolicy.yaml`:
- `default-deny-all`: deny all ingress + egress for every pod in `raven`.
- `allow-dns-egress`: UDP/TCP 53 to `kube-dns` in `kube-system` for all pods.
- One policy per workload with exact flows (from `docs/contracts/ports-and-env.md` + code-verified consumers):
  - broker: ingress `9100` from **jobs + worker only**; `9101` from gateway + prometheus.
  - postgres: `5432` from auth, users, jobs, worker, migrate only. No egress.
  - redis: `6379` from gateway, auth, jobs, worker, websocket. No egress.
  - gateway: `8080` from `0.0.0.0/0` (LoadBalancer) + prometheus; egress to its upstreams only (auth 9081, users 9082, jobs 9083, websocket 8084, redis 6379, broker **9101 only**, jaeger 4317).
  - console: ingress `8080` from anywhere; **zero egress**.
  - prometheus/grafana/jaeger: UI ingress from anywhere (dev), egress only to their datasources/scrape targets.
  - migrate: egress to postgres only; no ingress.

One flow was initially missed and caught by verification: the k8s ConfigMap injects `REDIS_ADDR` into **every** service, and `jobs` pings Redis at startup (compose never set it, so it was easy to overlook). Jobs crash-looped with `redis: connection pool: failed to dial ... i/o timeout` until the policy allowed jobs→redis:6379. Fixed in both directions.

**Verification (after, live).**

```text
rogue pod → broker:9100     BLOCKED     (nc -z -w3, timeout)
rogue pod → postgres:5432   BLOCKED
rogue pod → gateway:8080    BLOCKED     (pod-to-pod not in policy)
jobs pod  → broker:9100     ALLOWED
worker pod→ broker:9100     ALLOWED
gateway   → broker:9100     BLOCKED     (gateway gets ops 9101 only)
gateway   → broker:9101     ALLOWED
auth      → postgres:5432   ALLOWED
```

And the external path still works (the classic policy mistake — killing the LoadBalancer — was explicitly tested):

```text
$ curl localhost:8080/health → 200
$ curl localhost:8080/ready  → 200 {"checks":{"auth_grpc":"ok","jobs_grpc":"ok","redis":"ok","users_grpc":"ok"},"status":"ready"}
```

Full end-to-end proof: register (201) → login (200, JWT) → submit `send_email` job (201) → job reached `SUCCESS` in ~3 s. Prometheus reports all 8 scrape targets `up`.

---

## INFRA-02 — No securityContext on any workload

**Severity:** Medium

**Impact / scope.** None of the 14 workload manifests declared pod- or container-level `securityContext`. The raven/* images did run as uid 10001 via `USER` in the Dockerfile, but nothing enforced it at the orchestrator level, root filesystems were writable, and all default capabilities were available.

**Evidence (before).** `grep -rn securityContext deployments/kubernetes/` → no matches.

**Fix.** On every app pod (gateway, auth, users, jobs, websocket, worker, broker, migrate):

```yaml
# pod level
securityContext:
  runAsNonRoot: true
  runAsUser: 10001      # uid baked into every raven/* image (verified in all 8 Go Dockerfiles)
  runAsGroup: 10001
  fsGroup: 10001        # broker PVC stays writable
  seccompProfile: { type: RuntimeDefault }
# container level
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: { drop: ["ALL"] }
```

Broker keeps `/data` writable via its PVC (fsGroup 10001). Third-party images got the same treatment with their own expected uids (see INFRA-03/04/05 + observability pods: prometheus 65534, grafana 472, jaeger 10001), each with `emptyDir` scratch mounts where the software needs to write.

**Verification (after, live).**

```text
$ kubectl -n raven exec deploy/gateway -- id
uid=10001(raven) gid=10001(raven)
$ kubectl -n raven exec deploy/gateway -- touch /etc/x
touch: /etc/x: Read-only file system
$ kubectl -n raven exec broker-0 -- touch /data/.wtest   → WRITE-OK (PVC path works)
```

All pods `Ready` within a few minutes of the rolling update; full job flow succeeded (INFRA-01).

---

## INFRA-03 — Console container ran as root

**Severity:** Medium

**Evidence (before).**

```text
$ kubectl -n raven exec deploy/console -- id
uid=0(root) gid=0(root) ...
```

`Dockerfile.console` used stock `nginx:1.27-alpine`: master process as root, listening on :80 (needs `CAP_NET_BIND_SERVICE`).

**Fix.** Runtime stage switched to `nginxinc/nginx-unprivileged:1.27-alpine` (uid 101, no root at all). The shared `web/console/nginx.conf` still says `listen 80;` for the stock image, so the Dockerfile rewrites the directive at build time (`RUN sed -i 's/listen 80;/listen 8080;/'`) — the app file stays untouched. Pod runs uid 101, `readOnlyRootFilesystem: true`, drop ALL caps, with `emptyDir` mounts for `/var/cache/nginx`, `/var/run`, `/tmp`. k8s port moved 80 → 8080; Service still reaches it by name.

**Verification.**
- Local, exact pod flags (`--user 101:101 --cap-drop ALL --read-only --tmpfs …`): `HTTP 200`.
- Live: `uid=101(nginx)`, socket `0.0.0.0:8080`, `curl localhost:7100 → 200`, rollout completed, old root-based ReplicaSet terminated.

**Trap found while fixing (matters for contributors):** Docker Desktop k8s caches an image tag at first resolution under `IfNotPresent` — rebuilding `:latest` (or reusing any tag the node already saw) silently keeps running the STALE image. Evidence during this audit: `:latest`, `:v2`, `:v3` all resolved to older builds (`listen 80;` inside the pod) while a brand-new tag propagated instantly. The manifest now uses a unique tag (`raven/console:unpriv-nginx-20260914`) and `deployments/README.md` documents the bump-the-tag rule.

---

## INFRA-04 — Redis entered as root

**Evidence (before).** `kubectl -n raven exec deploy/redis -- id` → `uid=0(root)`. The image entrypoint starts as root and su-execs to `redis` — meaning the container must carry SETUID/SETGID capabilities.

**Fix.** `runAsUser: 999, runAsGroup: 1000` (the image's own user — verified `uid=999(redis) gid=1000(redis)`), so the entrypoint skips its root-only path entirely. Plus drop ALL caps, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, `emptyDir` at `/data` (redis writes `dump.rdb` there — default save points are on), seccomp RuntimeDefault.

**Verification.** Pod Running; `id` → `uid=999(redis)`; `redis-cli ping` probes green; gateway `/ready` reports `redis: ok`; worker heartbeats visible in `/api/workers`.

---

## INFRA-05 — Postgres entered as root

**Evidence (before).** Container started as root (image has no `USER`); the entrypoint chowns the data dir and su-execs to uid 70.

**Fix.** Run directly as the image's expected user: `runAsUser: 70, runAsGroup: 70, fsGroup: 70` (the initialized PVC is already owned by 70, so no data migration needed). `capabilities: drop ALL` is safe because the root-only chown path is skipped. `readOnlyRootFilesystem: true` + `emptyDir` for `/run/postgresql` (unix socket/lock files) and `/tmp`.

**Verification.** Pod `1/1 Running`; all DB clients Ready; live register+login+job flow exercised the database end to end.

---

## INFRA-06 — Default ServiceAccount with automounted token

**Severity:** Medium

**Impact / scope.** Every pod used the namespace `default` ServiceAccount with the API token automounted. A compromised pod could read the token and call the Kubernetes API with the default account's permissions.

**Fix.** New `serviceaccount.yaml`: dedicated `raven` SA with `automountServiceAccountToken: false`. Every pod template (including third-party and the migrate Job) sets `serviceAccountName: raven` + `automountServiceAccountToken: false`. **RBAC: intentionally zero Roles/Bindings** — no RAVEN pod calls the k8s API; documented in the file so nobody adds a binding casually.

**Verification (live).**

```text
$ kubectl -n raven exec deploy/gateway -- ls /var/run/secrets/kubernetes.io/serviceaccount
ls: ...: No such file or directory
```

All pods Ready; nothing broke (confirming no pod needs the API).

---

## INFRA-07 — `secrets.yaml` not gitignored

**Severity:** Low (pre-commit hazard)

**Evidence.** `secrets.example.yaml` tells users to "copy to secrets.yaml, which must stay gitignored" — but `.gitignore` only had `.env` and `*.local`. A user following the instructions inside the repo would get a committable file full of real secrets.

**Fix.** `.gitignore` (append-only): `secrets.yaml`, `*.secrets.yaml`, `deployments/kubernetes/secrets.yaml`.

**Verification.** `git check-ignore` semantics covered by the patterns; `git status` shows no secrets file.

---

## INFRA-08 — `.dockerignore` did not exclude root `secrets.yaml`

**Severity:** Low

**Evidence.** Go Dockerfiles do `COPY . .` in the build stage. `.dockerignore` excluded `.env*` and `deployments/kubernetes/` but not a root-level `secrets.yaml` / `*.secrets.yaml` — those would land in build-stage layers (never in the final image, but still on disk of every builder/CI worker).

**Fix.** Appended `secrets.yaml` and `*.secrets.yaml` to `.dockerignore`.

---

## INFRA-09 — CI workflow: no `permissions:` block

**Severity:** Low

**Evidence.** `.github/workflows/ci.yml` declared no top-level `permissions`, leaving the `GITHUB_TOKEN` at the repo default (potentially read-write). Positives already present and verified: `govulncheck` runs on every push/PR (`go run golang.org/x/vuln/cmd/govulncheck@latest ./...`), no secrets are echoed, no third-party actions beyond the well-known official ones.

**Fix.** Added:

```yaml
permissions:
  contents: read
```

**Remaining hardening idea (not applied, noted):** actions are pinned by major tag (`@v5`, `@v6`, `@v9`), not by commit SHA. SHA-pinning is the next step if the threat model ever includes action-tag hijack.

---

## INFRA-10 — govulncheck (supply chain)

**Command.** `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` (Go 1.27.1, from repo root).

**Result.**

```text
=== Symbol Results ===
No vulnerabilities found.
Your code is affected by 0 vulnerabilities.
... 1 vulnerability in modules you require, but your code doesn't appear to call these
```

Verbose mode identifies the module-level item:

```text
Vulnerability #1: GO-2026-5932
  golang.org/x/crypto/openpgp is unmaintained, unsafe by design
  Module: golang.org/x/crypto  Found in: v0.57.0  Fixed in: N/A
```

**Assessment.** `x/crypto` is a direct dependency (bcrypt for password hashing), but RAVEN never imports the `openpgp` subpackage — symbol-level analysis confirms zero reachable vulns. No fixed version exists; nothing to bump. **Action for maintainers: none.** Keep `x/crypto` current as usual.

---

## INFRA-11 — LoadBalancer exposure (dev-only by design)

**Evidence.** Six services are `type: LoadBalancer` on localhost: gateway 8080, console 7100, websocket 8084, grafana 3000 (`admin`/`admin`), prometheus 9090, jaeger 16686 (+4317 OTLP). Only the gateway's API has any auth; the UIs are wide open.

**Assessment.** Fine for Docker Desktop dev (localhost-only binding). Not fine to carry into production unknowingly. **Done:** `DEV ONLY` comments added to each LoadBalancer Service (incl. the observability generator template), a Security section in `deployments/README.md`, and the production checklist in `SECURITY.md` (authenticated Ingress, change grafana login, etc.). Also flagged: `WS_ALLOW_ANONYMOUS: "true"` in `configmaps.yaml` is a dev default — the checklist mandates flipping it.

---

## INFRA-12 — `/metrics`, `/ready`, `/debug/stats` disclosures

**Evidence.** Every ops port serves `/metrics`; `/ready` returns per-dependency states (`{"auth_grpc":"ok",...}`); the websocket service exposes `/debug/stats` (used by the gateway's health aggregation).

**Assessment.** Info-level disclosure. After INFRA-01 these ports are reachable in-cluster only from Prometheus (and the gateway where needed) — verified by the rogue-pod tests. Note: the gateway serves `/metrics` on its public port 8080 (only one port exists); acceptable on localhost dev, listed in the SECURITY.md checklist for production.

---

## INFRA-13 — Image pinning & local registry hygiene

**Evidence.** Base images pinned by tag only: `alpine:3.21`, `golang:1.27-alpine`, `postgres:17-alpine`, `redis:8-alpine`, `nginxinc/nginx-unprivileged:1.27-alpine`, `prom/prometheus:v3.4.1`, `grafana/grafana:11.6.0`, `jaegertracing/all-in-one:1.67.0`, `busybox:1.37`. No digest pins. Local app images use `:latest` + `IfNotPresent` (with the sticky-tag trap documented in INFRA-03).

**Assessment.** Acceptable for a dev-focused open-source repo today. **Documented as future work:** pin base images by digest for reproducible builds; publish signed release images (cosign) once a registry exists. Noted in SECURITY.md checklist.

---

## INFRA-14 — Dev credentials in the tree

**Evidence.** postgres `raven`/`raven` (compose + `postgres.yaml` env), JWT `dev-only-secret-change-me` (`.env.example`, code fallback, `secrets.example.yaml` base64), grafana `admin`/`admin` (compose + `observability.yaml`).

**Verification that nothing worse exists:**
- `secrets.example.yaml` decodes to exactly the two documented dev defaults — placeholders only. ✅
- Git history scan (`git log -p --all -S` for `PRIVATE KEY`, `AKIA`, `password`, `secret` across yaml/yml/env; `git grep` over the tree for credential literals): **only the documented dev defaults and test fixtures** (`test-secret-do-not-use-in-prod`, `integration-test-secret`, `password-123` in tests) have ever been committed. No real secrets, no private keys, no cloud tokens. ✅
- `JWT_SECRET` appears only in the Secret template, never in the ConfigMap. ✅

**Assessment.** These are intentional, documented dev defaults — required so a fresh clone works. `SECURITY.md` marks them public/dev-only and the deployment checklist mandates changing them.

---

## INFRA-15 — migrate initContainer without resources

**Evidence.** The `wait-for-postgres` busybox initContainer had no `resources` (every other container had them — verified `grep -c resources:` across all manifests).

**Fix.** Added `requests: 10m/16Mi, limits: 50m/32Mi` plus the full hardened securityContext (uid 65534, read-only, drop ALL). Job re-ran to `Completed` after the change.

---

## INFRA-16 — Non-issues verified

- **No `privileged: true`, no `hostNetwork`/`hostPID`/`hostIPC`** anywhere in `deployments/` or `docker-compose.yml` (grep over all files). No container escape surface.
- **Resource limits present on all 14 workloads** (and now the initContainer too).
- **ConfigMap contains no secrets** — endpoints, log level, and the dev anonymous-WS flag only.
- **CI runs govulncheck** on every push/PR; no secrets echoed; official actions only.

---

## INFRA-17 — Declared worker replicas exceed node RAM

**Evidence.** `worker.yaml` declares 5 replicas; a single-node Docker Desktop at ~4 GiB cannot fit the full fleet at 5 (`FailedScheduling: Insufficient memory` observed during the audit rollout; node at 96% memory requests). The pre-audit live cluster was already running 2 workers.

**Assessment.** Not a security bug — a dev-ergonomics note. The live cluster was left at the pre-audit count (2). Consider lowering the dev default or raising Docker Desktop's VM memory; the HPA comment in `hpa.yaml` already covers scaling intent.

---

## Verification log (final state, 2026-09-14)

```text
$ kubectl apply --dry-run=client -f deployments/kubernetes/   → all 17 files valid (16 netpols + SA created, rest configured)
$ kubectl -n raven get pods
auth ×2, users ×2, jobs ×3, gateway ×2 (HPA owns the count: metrics-server absent, it holds last-set 2),
websocket ×3, worker ×2, broker-0, console ×2,
postgres, redis, prometheus, grafana, jaeger — all 1/1 Running;  migrate — Completed

$ curl localhost:8080/health  → 200
$ curl localhost:8080/ready   → 200 (all dependency checks ok)
$ curl localhost:7100/        → 200 (console, uid 101, read-only fs)
$ curl localhost:3000/api/health → 200;  :16686 → 200;  :9090/-/ready → 200;  :8084/health → 200

NetworkPolicy probes: rogue→{broker 9100, postgres 5432, gateway 8080} BLOCKED;
                      jobs/worker→broker 9100 ALLOWED; gateway→broker 9100 BLOCKED / 9101 ALLOWED
E2E: register 201 → login 200 → job 201 → status SUCCESS (~3 s)
Prometheus: 8/8 scrape targets up
govulncheck: 0 reachable vulnerabilities (module-level note: GO-2026-5932, unused openpgp)
Secret scan: history + tree clean beyond documented dev defaults
```

*Left intentionally unchanged for maintainers/owners:* no dependency bumps needed (govulncheck clean); `README.md` root and owner files untouched; `go.mod`/`go.sum` untouched.
