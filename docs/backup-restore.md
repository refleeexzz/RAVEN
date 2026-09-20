# Backup & Restore

RAVEN keeps state in three places: **Postgres** (jobs, users, audit, API
keys), the **broker PVC** (commit logs — the queue itself), and **Redis**
(worker registry + pub/sub). `scripts/backup.sh` snapshots all three plus
a redacted copy of the namespace's k8s objects; `scripts/restore.sh` puts
them back.

Both scripts were run for real against the local cluster while being
built — what you read here is what actually happened, not theory.

---

## Quick reference

```bash
# Backup the raven namespace into ./backups/raven-backup-raven-<timestamp>/
scripts/backup.sh -n raven -o ./backups

# See what a restore would do, without touching anything:
scripts/restore.sh -b backups/raven-backup-raven-<ts> -n raven --dry-run

# Restore (asks you to type the namespace name as confirmation):
scripts/restore.sh -b backups/raven-backup-raven-<ts> -n raven

# Restore into a different namespace (DR drills, staging rebuilds):
scripts/restore.sh -b backups/raven-backup-raven-<ts> -n raven-restore --yes
```

Flags worth knowing: `--skip-postgres/--skip-broker/--skip-redis` (both
scripts), `--skip-k8s` (backup), `--no-scale` (restore — do NOT pause
writers), `--yes` (restore — skip the interactive confirmation, for
automation).

---

## What a backup contains

```
raven-backup-raven-20260920-193928/
├── manifest.json                 # what/when/where + sha256 of every file
├── SHA256SUMS                    # machine-checkable checksums
├── postgres/
│   ├── raven-db.dump             # pg_dump custom format (compressed)
│   └── raven-db-schema.sql       # schema-only, for eyeballing
├── broker/
│   └── broker-data.tar.gz        # tar of broker-0:/data (topics, segments, offsets)
├── redis/
│   └── dump.rdb                  # BGSAVE snapshot
└── k8s/
    ├── configmaps.yaml           # full export
    └── secrets.redacted.json     # inventory with values REDACTED
```

A full backup of the seeded dev stack is ~90 KB. Sizes in real life scale
with job history and queue depth.

### What each piece really is

- **Postgres** — `pg_dump -Fc` of the whole `raven` database, streamed out
  of the running pod. That is jobs, job_attempts, users, sessions, RBAC,
  audit_logs — everything, not a hand-picked table list. The dump's magic
  bytes (`PGDMP`) are checked before it is accepted.
- **Broker** — an online `tar.gz` of `/data` from `broker-0`. The broker
  keeps serving while we copy; segments are append-only and fsynced, so a
  tar is a slightly-fuzzy point-in-time. That is fine for RAVEN: producers
  and consumers are idempotent, and a few duplicated or lost tail messages
  beat a stopped queue. (For a clean cut, scale jobs+worker to 0 first —
  the restore does this for you; the backup deliberately does not.)
- **Redis** — `BGSAVE`, wait for `LASTSAVE` to move, copy `dump.rdb`.
  Honest expectation: **most keys here have short TTLs** (worker
  heartbeats), so restoring an hour-old RDB loads them already expired —
  Redis just drops them on load. That is by design; workers re-register
  in seconds. Persistent keys restore fine (proven in the e2e test).
- **k8s objects** — ConfigMaps in full; Secrets as an **inventory with
  values replaced by `REDACTED (N bytes, sha256:…)`**. You keep proof of
  what existed and can detect drift, without a backup directory full of
  live credentials. Restoring secrets is a manual, deliberate act — see
  the DR runbook.

### Integrity

Every file gets a sha256 in both `SHA256SUMS` and `manifest.json`.
`restore.sh` refuses to run unless `sha256sum -c` passes on all of them.
The manifest also records the source namespace, UTC timestamp, and cluster
context, so you can tell six months from now where a backup came from.

---

## How the restore works (and why in that order)

1. **Preflight** — checksums, namespace exists, target pods found.
2. **Confirmation** — type the target namespace (or `--yes`).
3. **Quiet the writers** — `jobs` and `worker` scale to 0 so nothing
   produces/consumes mid-restore. Original replica counts are restored at
   the end, even on failure.
4. **Postgres** — dump copied into the pod, `pg_restore --clean
   --if-exists --no-owner --no-privileges`, then verified by counting
   `users` and `jobs` rows.
5. **Broker** — `/data` wiped, tarball streamed back in, pod deleted so
   the broker reloads segments from disk, readiness checked.
6. **Redis** — `dump.rdb` copied in, `SHUTDOWN NOSAVE`. Kubernetes
   restarts the *same container* (emptyDir survives), Redis loads the RDB
   on boot. `DBSIZE` is printed as verification.

A full restore of the dev stack takes **about 2–3 minutes**.

---

## Proven during development (2026-09-20, Docker Desktop k8s v1.36)

- Backup of live `raven` ns: 8 files, 89K, all checksums OK; dump TOC
  lists all 11 tables.
- Restore into a fresh namespace (`raven-restore-e2e`, infra installed
  from the Helm chart): Postgres came back with the seeded `users=1,
  jobs=3`; broker came back Ready with all three topics (`jobs`,
  `jobs.dlq`, `jobs.retry`) and 176K of segments; Redis came back with
  the persistent canary key (`GET restore-canary` → OK).
- TTL'd worker-registry keys restored as expired and were dropped by
  Redis on load — expected, see above.

---

## Scheduling backups

The scripts are cron-friendly. Example, every 6 hours with 14-day
retention:

```cron
17 */6 * * *  /path/to/repo/scripts/backup.sh -n raven -o /backups && find /backups -maxdepth 1 -name 'raven-backup-*' -mtime +14 -exec rm -rf {} +
```

(The odd minute is deliberate — top-of-hour jobs pile up.) Keep backups
**off the cluster's own disk**: the whole point is surviving its loss.
Copy `backups/` somewhere durable — object storage, another machine, both.

## Known limits

- Single-replica broker: its PVC is *the* queue. Back it up often; RPO
  for the broker is "time since last backup" (see the DR doc).
- Online tar of the broker is slightly fuzzy — acceptable for RAVEN's
  idempotent design, documented above.
- Secrets are redacted, so a full rebuild needs someone to re-create them
  (step 2 of every DR runbook).
- Windows/Git Bash: the scripts handle the kubectl path mangling
  (`MSYS2_ARG_CONV_EXCL` + `cygpath`), but run them from Git Bash, not
  PowerShell.
