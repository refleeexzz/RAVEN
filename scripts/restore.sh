#!/usr/bin/env bash
# restore.sh — bring a RAVEN namespace back from a scripts/backup.sh snapshot.
#
# What it does, in order:
#   1. Preflight — backup dir, manifest, sha256 verification, target pods.
#   2. Confirm  — interactive (type the namespace), or --yes for automation.
#   3. Quiets   — scales jobs + worker to 0 so nothing writes mid-restore.
#   4. Postgres — pg_restore --clean --if-exists of the full raven DB
#                 (jobs, auth/users, audit, api_keys, RBAC, ...).
#   5. Broker   — wipes broker-0:/data, streams the tar back in, restarts
#                 the pod so segments reload from disk.
#   6. Redis    — copies dump.rdb in, SHUTDOWN NOSAVE (container restarts,
#                 emptyDir survives, RDB loads on boot).
#   7. Restores the original replica counts and prints verification numbers.
#
# Usage:
#   scripts/restore.sh -b BACKUP_DIR [-n NAMESPACE] [--yes] [--dry-run]
#                      [--skip-postgres] [--skip-broker] [--skip-redis]
#                      [--no-scale] [-h]
set -euo pipefail

NAMESPACE="raven"
BACKUP_DIR=""
ASSUME_YES=0
DRY_RUN=0
NO_SCALE=0
SKIP_PG=0
SKIP_BROKER=0
SKIP_REDIS=0

log()  { printf '[restore] %s\n' "$*"; }
warn() { printf '[restore] WARN: %s\n' "$*" >&2; }
die()  { printf '[restore] ERROR: %s\n' "$*" >&2; exit 1; }

usage() { sed -n '2,23p' "$0"; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -b|--backup)     BACKUP_DIR="${2:?missing value}"; shift 2 ;;
    -n|--namespace)  NAMESPACE="${2:?missing value}"; shift 2 ;;
    --yes|-y)        ASSUME_YES=1; shift ;;
    --dry-run)       DRY_RUN=1; shift ;;
    --no-scale)      NO_SCALE=1; shift ;;
    --skip-postgres) SKIP_PG=1; shift ;;
    --skip-broker)   SKIP_BROKER=1; shift ;;
    --skip-redis)    SKIP_REDIS=1; shift ;;
    -h|--help)       usage 0 ;;
    *) die "unknown flag: $1 (try -h)" ;;
  esac
done

[ -n "$BACKUP_DIR" ] || die "no backup dir given — use -b (try -h)"
[ -d "$BACKUP_DIR" ] || die "backup dir not found: $BACKUP_DIR"
[ -f "$BACKUP_DIR/manifest.json" ] || die "$BACKUP_DIR has no manifest.json — not a raven backup?"
[ -f "$BACKUP_DIR/SHA256SUMS" ] || die "$BACKUP_DIR has no SHA256SUMS"

if ! command -v kubectl >/dev/null 2>&1; then
  export PATH="$HOME/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"
fi
command -v kubectl >/dev/null 2>&1 || die "kubectl not found in PATH"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum not found"

# kubectl cp wrapper that survives Git Bash/MSYS path mangling on Windows:
# pod:/path args must reach kubectl.exe untouched, local paths go through
# cygpath when available.
KCP() {
  local args=()
  local a
  for a in "$@"; do
    case "$a" in
      *:*) args+=("$a") ;;
      *) if command -v cygpath >/dev/null 2>&1; then args+=("$(cygpath -w "$a")"); else args+=("$a"); fi ;;
    esac
  done
  MSYS2_ARG_CONV_EXCL='*' kubectl cp "${args[@]}"
}

# --- preflight -------------------------------------------------------------------
log "preflight: verifying checksums in $BACKUP_DIR ..."
( cd "$BACKUP_DIR" && sha256sum -c SHA256SUMS >/dev/null ) \
  || die "checksum verification FAILED — backup is corrupt or tampered; refusing to restore"
log "preflight: checksums OK"

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 \
  || die "namespace '$NAMESPACE' does not exist — create it first (helm install / kubectl create ns)"

pod_for() {
  kubectl get pods -n "$NAMESPACE" -l "app=$1" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

PG_POD=$(pod_for postgres)
BROKER_POD=$(pod_for broker)
REDIS_POD=$(pod_for redis)
[ "$SKIP_PG" -eq 1 ]     || [ -n "$PG_POD" ]     || die "no Running postgres pod in $NAMESPACE"
[ "$SKIP_BROKER" -eq 1 ] || [ -n "$BROKER_POD" ] || die "no Running broker pod in $NAMESPACE"
[ "$SKIP_REDIS" -eq 1 ]  || [ -n "$REDIS_POD" ]  || warn "no Running redis pod — redis section will be skipped"

BACKUP_NS=$(grep -o '"namespace": *"[^"]*"' "$BACKUP_DIR/manifest.json" | head -1 | sed 's/.*: *"//; s/"$//')
BACKUP_TS=$(grep -o '"created_utc": *"[^"]*"' "$BACKUP_DIR/manifest.json" | head -1 | sed 's/.*: *"//; s/"$//')
BACKUP_NS=${BACKUP_NS:-?}
BACKUP_TS=${BACKUP_TS:-?}

# --- plan + confirm ----------------------------------------------------------------
SECTIONS=""
[ "$SKIP_PG" -eq 1 ]     && SECTIONS="$SECTIONS postgres(SKIP)"     || SECTIONS="$SECTIONS postgres"
[ "$SKIP_BROKER" -eq 1 ] && SECTIONS="$SECTIONS broker(SKIP)" || SECTIONS="$SECTIONS broker"
[ "$SKIP_REDIS" -eq 1 ]  && SECTIONS="$SECTIONS redis(SKIP)"  || SECTIONS="$SECTIONS redis"
SECTIONS=$(echo "$SECTIONS" | xargs)

cat <<EOF
[restore] plan
  backup:     $BACKUP_DIR
  taken:      $BACKUP_TS (from namespace '$BACKUP_NS')
  target ns:  $NAMESPACE
  sections:   $SECTIONS
  scale down: $([ "$NO_SCALE" -eq 1 ] && echo "no (--no-scale)" || echo "jobs + worker -> 0 during restore")
EOF

if [ "$DRY_RUN" -eq 1 ]; then
  log "dry-run: stopping here. Nothing was changed."
  exit 0
fi

if [ "$ASSUME_YES" -eq 0 ]; then
  printf '[restore] type the target namespace (%s) to proceed: ' "$NAMESPACE"
  read -r ANSWER
  [ "$ANSWER" = "$NAMESPACE" ] || die "confirmation did not match — aborting, nothing was changed"
fi

# --- scale down writers ------------------------------------------------------------
SCALE_STATE=$(mktemp)
restore_scales() {
  if [ -s "$SCALE_STATE" ]; then
    log "restoring original replica counts ..."
    while read -r dep count; do
      kubectl scale deployment "$dep" -n "$NAMESPACE" --replicas="$count" >/dev/null 2>&1 || true
    done < "$SCALE_STATE"
  fi
}
trap restore_scales EXIT

if [ "$NO_SCALE" -eq 0 ]; then
  for dep in jobs worker; do
    if kubectl get deployment "$dep" -n "$NAMESPACE" >/dev/null 2>&1; then
      CUR=$(kubectl get deployment "$dep" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}')
      echo "$dep $CUR" >> "$SCALE_STATE"
      log "scaling $dep $CUR -> 0 (writers quiet during restore)"
      kubectl scale deployment "$dep" -n "$NAMESPACE" --replicas=0 >/dev/null
    fi
  done
  # give in-flight work a moment to drain
  sleep 5
fi

# --- 4. postgres ---------------------------------------------------------------------
if [ "$SKIP_PG" -eq 0 ]; then
  [ -f "$BACKUP_DIR/postgres/raven-db.dump" ] || die "missing postgres/raven-db.dump in backup"
  log "postgres: copying dump into $PG_POD ..."
  KCP "$BACKUP_DIR/postgres/raven-db.dump" "$NAMESPACE/$PG_POD:/tmp/raven-restore.dump"
  log "postgres: pg_restore --clean --if-exists into database 'raven' ..."
  set +e
  # sh -c wrapper: a bare /tmp/... argument would be rewritten to a Windows
  # path by Git Bash before reaching kubectl.
  kubectl exec -n "$NAMESPACE" "$PG_POD" -- \
    sh -c 'pg_restore -U raven -d raven --clean --if-exists --no-owner --no-privileges /tmp/raven-restore.dump'
  RC=$?
  set -e
  kubectl exec -n "$NAMESPACE" "$PG_POD" -- sh -c 'rm -f /tmp/raven-restore.dump' || true
  [ "$RC" -eq 0 ] || warn "pg_restore exited $RC (usually benign with --clean; verifying data below)"
  JOBS=$(kubectl exec -n "$NAMESPACE" "$PG_POD" -- psql -U raven -d raven -tAc 'SELECT count(*) FROM jobs' 2>/dev/null | tr -d '\r' || echo ERR)
  USERS=$(kubectl exec -n "$NAMESPACE" "$PG_POD" -- psql -U raven -d raven -tAc 'SELECT count(*) FROM users' 2>/dev/null | tr -d '\r' || echo ERR)
  [ "$JOBS" = "ERR" ] || [ "$USERS" = "ERR" ] && die "postgres verification failed (cannot count rows)"
  log "postgres: verified — users=$USERS jobs=$JOBS"
fi

# --- 5. broker ------------------------------------------------------------------------
if [ "$SKIP_BROKER" -eq 0 ]; then
  [ -f "$BACKUP_DIR/broker/broker-data.tar.gz" ] || die "missing broker/broker-data.tar.gz in backup"
  log "broker: wiping /data on $BROKER_POD and streaming snapshot back ..."
  kubectl exec -n "$NAMESPACE" "$BROKER_POD" -- sh -c 'rm -rf /data/* /data/.[!.]* 2>/dev/null || true'
  kubectl exec -i -n "$NAMESPACE" "$BROKER_POD" -- sh -c 'cd /data && tar -xzf -' \
    < "$BACKUP_DIR/broker/broker-data.tar.gz"
  log "broker: restarting pod so commit logs reload from disk ..."
  kubectl delete pod "$BROKER_POD" -n "$NAMESPACE" >/dev/null
  kubectl wait --for=condition=ready pod -l app=broker -n "$NAMESPACE" --timeout=120s >/dev/null \
    || die "broker did not become ready after restore"
  log "broker: ready — $(kubectl exec -n "$NAMESPACE" "$(pod_for broker)" -- sh -c 'du -sh /data' | cut -f1) on disk"
fi

# --- 6. redis ---------------------------------------------------------------------------
if [ "$SKIP_REDIS" -eq 0 ] && [ -n "$REDIS_POD" ]; then
  [ -f "$BACKUP_DIR/redis/dump.rdb" ] || die "missing redis/dump.rdb in backup"
  log "redis: copying dump.rdb into $REDIS_POD and restarting the process ..."
  KCP "$BACKUP_DIR/redis/dump.rdb" "$NAMESPACE/$REDIS_POD:/data/dump.rdb"
  # SHUTDOWN NOSAVE kills the process; kubelet restarts the SAME container,
  # the emptyDir survives, and redis loads our dump.rdb on boot.
  kubectl exec -n "$NAMESPACE" "$REDIS_POD" -- redis-cli SHUTDOWN NOSAVE >/dev/null 2>&1 || true
  # Poll instead of kubectl wait: during the container restart the
  # container briefly does not exist and `kubectl wait` errors out.
  READY=""
  for _ in $(seq 1 45); do
    sleep 2
    READY=$(kubectl get pod "$REDIS_POD" -n "$NAMESPACE" \
      -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
    [ "$READY" = "true" ] && break
  done
  [ "$READY" = "true" ] || die "redis did not come back after RDB restore"
  KEYS=$(kubectl exec -n "$NAMESPACE" "$REDIS_POD" -- redis-cli DBSIZE | tr -d '\r')
  log "redis: ready — DBSIZE=$KEYS"
fi

log "OK — restore complete into namespace '$NAMESPACE'"
log "next: watch pods settle (kubectl -n $NAMESPACE get pods -w), then re-run"
log "scripts/backup.sh for a fresh post-restore snapshot."
