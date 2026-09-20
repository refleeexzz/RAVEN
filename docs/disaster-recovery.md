# Disaster Recovery

This is the "bad day" document. It tells you what breaks, how much you can
lose, how long you are down, and exactly which commands bring RAVEN back.
Every runbook here was exercised against the local Docker Desktop cluster
while this document was written.

Read it once *before* the bad day. The matrix on a bad day.

---

## Targets (RTO / RPO)

**RTO** — how long the platform (or piece of it) can be down.
**RPO** — how much data you accept losing, measured in time.

| Component | State it owns | RPO target | RTO target | How |
|---|---|---|---|---|
| Postgres | jobs, users, audit, API keys | **1 h** (backup cadence) | **15 min** | `restore.sh` pg section: ~1–2 min measured on dev, plus triage |
| Broker PVC | commit logs = the queue itself | **1 h** (backup cadence) | **15 min** | `restore.sh` broker section: ~1 min measured on dev |
| Redis | worker registry, pub/sub | **0 — lossless by design** | **~30 s** | nothing to do: workers re-register, events re-flow |
| k8s objects | Secrets, ConfigMaps | ConfigMaps: in git. Secrets: **inventory only** (redacted) | 5 min | ConfigMaps re-apply from chart; Secrets re-created by a human |
| Whole namespace | everything above | 1 h | **30 min** | helm install + `restore.sh`, measured ~5–8 min on dev + DNS/LB settle time |
| Whole cluster | everything, plus the cluster | 1 h | **1 h** | new cluster + same as namespace rebuild |

Two honest caveats:

1. **RPO for Postgres and the broker is "time since last backup".** The
   1 h target assumes you run `backup.sh` hourly in any environment where
   losing an hour of jobs would hurt. On a laptop, run it when you care.
2. The broker is a **single replica**. Its PVC *is* the queue. There is no
   replication to fail over to today — the backup *is* the HA plan.

---

## Scenario matrix

| # | Scenario | Steps (runbook below) | Expected time | Max data loss |
|---|---|---|---|---|
| 1 | Postgres PVC lost/corrupt | R1 | ~15 min | since last backup (≤1 h) |
| 2 | Broker data dir lost/corrupt | R2 | ~15 min | since last backup (≤1 h) |
| 3 | Redis pod wiped | none | ~30 s | none (registry rebuilds) |
| 4 | Whole namespace deleted | R3 | ~30 min | since last backup (≤1 h) |
| 5 | Cluster gone / moving clusters | R4 | ~1 h | since last backup (≤1 h) |
| 6 | Secrets lost but data intact | R5 | ~10 min | none (but tokens die → re-login) |

"Expected time" assumes backups exist, are reachable, and someone who has
read this page is driving. Add time for paging, thinking, and coffee.

---

## Before any runbook: triage (2 minutes)

```bash
export PATH="$HOME/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"  # Windows/Docker Desktop
kubectl -n raven get pods
kubectl -n raven get pvc
ls -t backups/ | head -5        # newest backup first — verify it exists BEFORE you destroy anything
```

Golden rule: **locate a good backup before deleting broken things.** A
broken PVC you still have beats a clean slate with no backup.

---

## R1 — Postgres PVC lost or corrupt

Symptoms: `postgres` pod CrashLoops or Pending, events mention the
volume; `auth`, `users`, `jobs`, `worker` start failing right after.

1. Confirm the backup you will use:
   `ls -t backups/ | head` → pick the newest `raven-backup-*`, check its
   `manifest.json`.
2. Delete the broken volume and let the chart recreate it empty:

   ```bash
   kubectl -n raven scale deploy postgres --replicas=0
   kubectl -n raven delete pvc pgdata
   helm upgrade raven deployments/helm/raven -n raven --reuse-values   # recreates pgdata
   kubectl -n raven scale deploy postgres --replicas=1
   kubectl -n raven wait --for=condition=ready pod -l app=postgres --timeout=120s
   ```

3. Restore data:

   ```bash
   scripts/restore.sh -b backups/raven-backup-raven-<ts> -n raven \
     --skip-broker --skip-redis
   ```

   The script quiets jobs+worker, `pg_restore --clean --if-exists`, and
   prints `users=` / `jobs=` counts as proof.
4. Re-run migrations to catch any schema newer than the backup:
   `kubectl -n raven delete job migrate --ignore-not-found && helm upgrade raven deployments/helm/raven -n raven --reuse-values`
   (the post-upgrade hook re-runs migrate; it is idempotent).
5. Verify: `curl -s localhost:8080/health`, then log in through the
   gateway and list a few jobs.

Done. Measured on dev: **~5 min of commands**, 15 min with triage.

---

## R2 — Broker data dir lost or corrupt

Symptoms: `broker-0` CrashLoops, or starts but `jobs` creations fail and
workers idle; `/data` empty or garbage.

1. Pick the backup (see R1 step 1).
2. Restore:

   ```bash
   scripts/restore.sh -b backups/raven-backup-raven-<ts> -n raven \
     --skip-postgres --skip-redis
   ```

   The script quiets writers, wipes `/data`, streams the tarball back,
   restarts the pod, and waits for readiness.
3. Verify: `kubectl -n raven exec broker-0 -- ls /data/topics` shows
   `jobs`, `jobs.dlq`, `jobs.retry`; create one test job through the
   gateway and watch it go `QUEUED → SUCCESS`.

Note the trade: messages produced *after* the backup are gone, and
messages consumed-but-not-acked before it may re-run. RAVEN's producers
and consumers are idempotent, so this is safe — just noisy in the logs.

If the **PVC itself** (not just the data) is gone: delete
`brokerdata-broker-0` + the StatefulSet, `helm upgrade` to recreate both,
then run step 2.

---

## R3 — Whole namespace deleted

This is the full rebuild. It is also the **DR drill** — run it every
quarter so the bad day is boring.

1. Reinstall the platform (empty state):

   ```bash
   helm install raven deployments/helm/raven -n raven --create-namespace \
     -f deployments/helm/raven/values-dev.yaml     # or your prod values
   kubectl -n raven wait --for=condition=ready pod -l app=postgres --timeout=180s
   ```

2. Re-create Secrets with **real** values (the backup only stores a
   redacted inventory — check `k8s/secrets.redacted.json` for what must
   exist):

   ```bash
   helm upgrade raven deployments/helm/raven -n raven --reuse-values \
     --set secrets.create=true \
     --set secrets.jwtSecret="<from your vault>" \
     --set secrets.databaseUrl="<from your vault>"
   kubectl -n raven rollout restart deploy   # pods pick up the new env
   ```

   Heads-up: a **new JWT secret invalidates every live token** — users
   log in again. That is acceptable in DR; announce it.
3. Restore data:

   ```bash
   scripts/restore.sh -b backups/raven-backup-raven-<ts> -n raven --yes
   ```

4. Smoke test: gateway `/health`, register/login, create a job, watch it
   succeed; open Grafana and confirm all seven scrape targets are up.

Measured on dev, end to end: **under 10 minutes** of hands-on time.

---

## R4 — Cluster gone (or moving to a new one)

Same as R3 with two extra steps in front:

1. Stand up / select the new cluster, point `kubectl` at it
   (`kubectl config use-context ...`).
2. Make the container images available there: push your pinned tags to a
   registry and set `global.imageRegistry`, or load them into the nodes
   (`kind load`, `minikube image load`, ...). On Docker Desktop the
   shared image store means nothing to do.
3. Copy your `backups/` directory to a machine that can reach the new
   cluster. (You keep backups off-cluster, right?)
4. Follow R3 from step 1.

Expect **~1 h** the first time you ever do it, ~30 min once practiced.

---

## R5 — Secrets lost, data intact

Symptoms: pods up, but logins fail (JWT_SECRET changed/missing) or DB
connections refused (DATABASE_URL gone).

1. Check what *should* exist:
   `cat backups/raven-backup-*/k8s/secrets.redacted.json` — names, keys,
   sizes, sha256 prefixes of the values you lost.
2. Recreate from your vault (the backup cannot help with values — that is
   the point of redaction):

   ```bash
   helm upgrade raven deployments/helm/raven -n raven --reuse-values \
     --set secrets.create=true \
     --set secrets.jwtSecret="..." --set secrets.databaseUrl="..."
   kubectl -n raven rollout restart deploy
   ```

3. If the DATABASE_URL password itself is gone forever: connect as a
   superuser, `ALTER USER raven WITH PASSWORD '<new>';`, then set the new
   URL. Data is untouched; only live tokens/sessions die.

---

## Redis: the non-runbook

Redis losing everything is a **non-event**: the worker registry has
seconds-long TTLs and rebuilds as workers heartbeat; pub/sub has no
backlog. Pods restart, workers re-register, done. We restore its RDB for
completeness and for any future persistent keys — but if you skip it,
nobody notices.

---

## Keeping this honest

- **Drills**: run R3 quarterly in a scratch namespace
  (`-n raven-drill`). It took the author ~10 minutes on a laptop; if it
  takes you an hour, the runbook or the backups are wrong — fix them, not
  the clock.
- **Backup monitoring**: a backup you have never restored is a rumor.
  Every drill ends by restoring the newest one.
- **After every DR event**: take a fresh backup immediately, then write
  down what surprised you and patch this file.
