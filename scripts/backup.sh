#!/usr/bin/env bash
# backup.sh — snapshot a running RAVEN namespace.
#
# What gets captured:
#   1. Postgres    — pg_dump (custom format, compressed) of the whole raven
#                    database: jobs, auth/users, audit, api_keys, everything.
#   2. Broker      — tar.gz of broker-0's /data PVC (commit logs + offsets).
#   3. Redis       — RDB dump (BGSAVE + copy). Registry/pubsub state.
#   4. k8s objects — Secrets (REDACTED) + ConfigMaps of the namespace.
#
# Everything lands in a timestamped directory with a manifest.json holding
# sha256 checksums. Restore path: scripts/restore.sh.
#
# Usage:
#   scripts/backup.sh [-n NAMESPACE] [-o OUTPUT_ROOT] [--skip-redis]
#                     [--skip-broker] [--skip-k8s] [-h]
#
# Defaults: namespace=raven, output=./backups
set -euo pipefail

NAMESPACE="raven"
OUTPUT_ROOT="./backups"
SKIP_REDIS=0
SKIP_BROKER=0
SKIP_K8S=0

log()  { printf '[backup] %s\n' "$*"; }
warn() { printf '[backup] WARN: %s\n' "$*" >&2; }
die()  { printf '[backup] ERROR: %s\n' "$*" >&2; exit 1; }

usage() { sed -n '2,22p' "$0"; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -n|--namespace) NAMESPACE="${2:?missing value}"; shift 2 ;;
    -o|--output)    OUTPUT_ROOT="${2:?missing value}"; shift 2 ;;
    --skip-redis)   SKIP_REDIS=1; shift ;;
    --skip-broker)  SKIP_BROKER=1; shift ;;
    --skip-k8s)     SKIP_K8S=1; shift ;;
    -h|--help)      usage 0 ;;
    *) die "unknown flag: $1 (try -h)" ;;
  esac
done

# --- toolchain -------------------------------------------------------------
# Docker Desktop ships kubectl outside PATH on Windows; fall back to it.
if ! command -v kubectl >/dev/null 2>&1; then
  export PATH="$HOME/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"
fi
command -v kubectl >/dev/null 2>&1 || die "kubectl not found in PATH"
command -v python3 >/dev/null 2>&1 || command -v python >/dev/null 2>&1 \
  || die "python3 not found (needed for manifest + secret redaction)"
PY=$(command -v python3 || command -v python)
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

# --- preflight: namespace + pods -------------------------------------------
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 \
  || die "namespace '$NAMESPACE' does not exist"

pod_for() { # $1 = app label -> first Running pod name
  kubectl get pods -n "$NAMESPACE" -l "app=$1" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

PG_POD=$(pod_for postgres)
[ -n "$PG_POD" ] || die "no Running postgres pod in $NAMESPACE"
BROKER_POD=$(pod_for broker)
REDIS_POD=$(pod_for redis)
[ "$SKIP_BROKER" -eq 1 ] || [ -n "$BROKER_POD" ] || die "no Running broker pod in $NAMESPACE"
[ "$SKIP_REDIS" -eq 1 ]  || [ -n "$REDIS_POD" ]  || warn "no Running redis pod — redis section will be skipped"

# --- output dir --------------------------------------------------------------
TS=$(date -u +%Y%m%d-%H%M%S)
DEST="$OUTPUT_ROOT/raven-backup-$NAMESPACE-$TS"
mkdir -p "$DEST"/{postgres,broker,redis,k8s}
log "destination: $DEST"

cleanup_partial() {
  [ -f "$DEST/manifest.json" ] || warn "backup incomplete — partial data kept at $DEST"
}
trap cleanup_partial EXIT

# --- 1. Postgres -------------------------------------------------------------
log "postgres: pg_dump of database 'raven' from pod $PG_POD ..."
kubectl exec -n "$NAMESPACE" "$PG_POD" -- \
  pg_dump -U raven -d raven --format=custom --compress=6 \
  > "$DEST/postgres/raven-db.dump"
# Cheap integrity gate: custom-format dumps start with the PGDMP magic.
head -c 5 "$DEST/postgres/raven-db.dump" | grep -q PGDMP \
  || die "postgres dump failed magic check (empty or wrong format)"
log "postgres: schema-only copy for quick inspection ..."
kubectl exec -n "$NAMESPACE" "$PG_POD" -- \
  pg_dump -U raven -d raven --schema-only \
  > "$DEST/postgres/raven-db-schema.sql"
log "postgres: $(du -h "$DEST/postgres/raven-db.dump" | cut -f1) dumped"

# --- 2. Broker PVC -------------------------------------------------------------
if [ "$SKIP_BROKER" -eq 0 ]; then
  log "broker: tar of /data from $BROKER_POD (commit logs stay online; RAVEN replays idempotently) ..."
  kubectl exec -n "$NAMESPACE" "$BROKER_POD" -- \
    sh -c 'cd /data && tar -czf - .' > "$DEST/broker/broker-data.tar.gz"
  # A valid gzip stream is the minimum we can assert without a broker binary.
  gzip -t "$DEST/broker/broker-data.tar.gz" 2>/dev/null \
    || die "broker tarball failed gzip integrity check"
  log "broker: $(du -h "$DEST/broker/broker-data.tar.gz" | cut -f1) archived"
fi

# --- 3. Redis --------------------------------------------------------------------
if [ "$SKIP_REDIS" -eq 0 ] && [ -n "$REDIS_POD" ]; then
  log "redis: BGSAVE on $REDIS_POD ..."
  BEFORE=$(kubectl exec -n "$NAMESPACE" "$REDIS_POD" -- redis-cli LASTSAVE | tr -d '\r')
  kubectl exec -n "$NAMESPACE" "$REDIS_POD" -- redis-cli BGSAVE >/dev/null
  for i in $(seq 1 30); do
    sleep 1
    NOW=$(kubectl exec -n "$NAMESPACE" "$REDIS_POD" -- redis-cli LASTSAVE | tr -d '\r')
    [ "$NOW" != "$BEFORE" ] && break
    [ "$i" -eq 30 ] && warn "BGSAVE did not finish in 30s — copying whatever dump.rdb exists"
  done
  KCP "$NAMESPACE/$REDIS_POD:/data/dump.rdb" "$DEST/redis/dump.rdb" 2>/dev/null || true
  # kubectl cp can exit 0 even when the in-pod tar fails — verify bytes,
  # then fall back to a plain streamed cat.
  if [ ! -s "$DEST/redis/dump.rdb" ]; then
    kubectl exec -n "$NAMESPACE" "$REDIS_POD" -- sh -c 'cat /data/dump.rdb' > "$DEST/redis/dump.rdb"
  fi
  [ -s "$DEST/redis/dump.rdb" ] || die "redis dump.rdb is empty"
  log "redis: $(du -h "$DEST/redis/dump.rdb" | cut -f1) copied"
fi

# --- 4. k8s Secrets (redacted) + ConfigMaps --------------------------------------
if [ "$SKIP_K8S" -eq 0 ]; then
  log "k8s: exporting ConfigMaps (full) and Secrets (values REDACTED) ..."
  kubectl get configmaps -n "$NAMESPACE" -o yaml > "$DEST/k8s/configmaps.yaml"
  kubectl get secrets -n "$NAMESPACE" -o json \
    | "$PY" -c '
import json, sys, hashlib, base64
data = json.load(sys.stdin)
for item in data.get("items", []):
    if item.get("type") == "helm.sh/release.v1":
        item["data"] = {"release": "REDACTED (helm release record — reinstall from the chart)"}
        continue
    red = {}
    for k, v in (item.get("data") or {}).items():
        raw = base64.b64decode(v + "==", validate=False)
        red[k] = "REDACTED (%d bytes, sha256:%s)" % (len(raw), hashlib.sha256(raw).hexdigest()[:12])
    item["data"] = red
    item.pop("stringData", None)
json.dump(data, sys.stdout, indent=2)
' > "$DEST/k8s/secrets.redacted.json"
  log "k8s: $(grep -c 'kind: ConfigMap' "$DEST/k8s/configmaps.yaml" || true) ConfigMaps, secrets redacted to secrets.redacted.json"
fi

# --- manifest.json ----------------------------------------------------------------
log "manifest: checksums ..."
(
  cd "$DEST"
  FILES=$(find . -type f ! -name manifest.json | sort)
  echo "$FILES" | while read -r f; do
    printf '%s  %s\n' "$(sha256sum "$f" | cut -d' ' -f1)" "${f#./}"
  done > SHA256SUMS
  "$PY" - "$NAMESPACE" "$TS" <<'EOF'
import json, sys, os, hashlib, time, subprocess
ns, ts = sys.argv[1], sys.argv[2]
files = []
for root, _, names in os.walk("."):
    for n in sorted(names):
        if n in ("manifest.json",):
            continue
        p = os.path.join(root, n)
        rel = os.path.relpath(p, ".").replace(os.sep, "/")
        if rel.startswith("./"):
            rel = rel[2:]
        h = hashlib.sha256()
        with open(p, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        files.append({"path": rel, "bytes": os.path.getsize(p), "sha256": h.hexdigest()})
try:
    ctx = subprocess.check_output(["kubectl", "config", "current-context"], text=True).strip()
except Exception:
    ctx = "unknown"
manifest = {
    "tool": "raven scripts/backup.sh",
    "version": 1,
    "namespace": ns,
    "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "label": ts,
    "cluster_context": ctx,
    "contents": {
        "postgres": "postgres/raven-db.dump (pg_dump custom format) + raven-db-schema.sql",
        "broker": "broker/broker-data.tar.gz (tar of broker-0 /data PVC)",
        "redis": "redis/dump.rdb (BGSAVE snapshot)",
        "k8s": "k8s/configmaps.yaml + k8s/secrets.redacted.json (values redacted)",
    },
    "restore": "scripts/restore.sh -n <namespace> -b <this directory>",
    "files": files,
}
with open("manifest.json", "w") as fh:
    json.dump(manifest, fh, indent=2)
EOF
)

log "OK — backup complete:"
log "  $DEST"
log "  files: $(find "$DEST" -type f | wc -l | tr -d ' ') · total: $(du -sh "$DEST" | cut -f1)"
log "restore with: scripts/restore.sh -n $NAMESPACE -b $DEST"
trap - EXIT
