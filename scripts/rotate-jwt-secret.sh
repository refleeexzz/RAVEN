#!/usr/bin/env bash
# rotate-jwt-secret.sh — zero-downtime JWT_SECRET rotation for RAVEN on Kubernetes.
#
# How it works (the dual-secret window, see docs/security/rotation.md):
#   1. JWT_SECRET_PREVIOUS is set to the CURRENT secret,
#   2. JWT_SECRET is set to a freshly generated one,
#   3. the deployments that read JWT_SECRET are restarted so they pick up
#      both values — verification accepts primary OR previous, signing uses
#      only the primary, so no live session is dropped,
#   4. you watch raven_auth_jwt_previous_secret_used_total fall to ~0
#      (old tokens expire within 15 minutes — AccessTokenTTL),
#   5. you close the window with --finish (removes JWT_SECRET_PREVIOUS).
#
# Only the services that actually READ JWT_SECRET are restarted by default:
# auth (mints/validates), gateway (validates for audit attribution) and
# websocket (validates the upgrade token). users/jobs/worker mount the same
# Secret object but never read this key, so they are left alone.
#
# Usage:
#   scripts/rotate-jwt-secret.sh [options]           # start a rotation
#   scripts/rotate-jwt-secret.sh --finish [options]  # close the window
#
# Options:
#   -n, --dry-run               print every step without changing the cluster
#   -y, --yes                   skip the confirmation prompt
#       --finish                remove JWT_SECRET_PREVIOUS and restart again
#       --namespace NAME        k8s namespace        (default: raven)
#       --secret-name NAME      k8s Secret name      (default: raven-secrets)
#       --deployments "a b c"   workloads to restart (default: "auth gateway websocket")
#       --previous VALUE        override the detected current secret
#       --timeout DURATION      rollout status timeout (default: 180s)
#   -h, --help                  this help
#
# Env overrides: RAVEN_NAMESPACE, RAVEN_SECRET_NAME, RAVEN_JWT_DEPLOYMENTS.
#
# Requires: bash, kubectl (configured context), openssl, base64.
# Secret values are never printed — only lengths and sha256 fingerprints.

set -euo pipefail

# ---------------------------------------------------------------- defaults
NAMESPACE="${RAVEN_NAMESPACE:-raven}"
SECRET_NAME="${RAVEN_SECRET_NAME:-raven-secrets}"
DEPLOYMENTS="${RAVEN_JWT_DEPLOYMENTS:-auth gateway websocket}"
TIMEOUT="180s"
DRY_RUN=0
ASSUME_YES=0
FINISH=0
PREVIOUS_OVERRIDE=""
# Documented dev fallback used by the code when the Secret has no JWT_SECRET
# (cmd/auth, docker-compose). Only used if the live key is missing.
DEV_FALLBACK="dev-only-secret-change-me"

# ---------------------------------------------------------------- helpers
log()  { printf '%s\n' "$*"; }
step() { printf '\n==> %s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# run executes "$@" or, in dry-run mode, just prints it.
run() {
	if [ "$DRY_RUN" -eq 1 ]; then
		printf '[dry-run] would run:'
		printf ' %q' "$@"
		printf '\n'
	else
		"$@"
	fi
}

fingerprint() { printf '%s' "$1" | openssl dgst -sha256 | awk '{print $NF}'; }

# json_escape makes an arbitrary secret value safe to embed in a JSON string
# (secrets are expected to be single-line ASCII; control chars are stripped).
json_escape() {
	printf '%s' "$1" | tr -d '\r\n' | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

need_cmd() {
	command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

usage() { sed -n '2,41p' "$0"; }

# ---------------------------------------------------------------- args
while [ $# -gt 0 ]; do
	case "$1" in
		-n|--dry-run)   DRY_RUN=1 ;;
		-y|--yes)       ASSUME_YES=1 ;;
		--finish)       FINISH=1 ;;
		--namespace)    NAMESPACE="${2:?--namespace needs a value}"; shift ;;
		--secret-name)  SECRET_NAME="${2:?--secret-name needs a value}"; shift ;;
		--deployments)  DEPLOYMENTS="${2:?--deployments needs a value}"; shift ;;
		--previous)     PREVIOUS_OVERRIDE="${2:?--previous needs a value}"; shift ;;
		--timeout)      TIMEOUT="${2:?--timeout needs a value}"; shift ;;
		-h|--help)      usage; exit 0 ;;
		*) die "unknown flag: $1 (try --help)" ;;
	esac
	shift
done

need_cmd openssl
need_cmd base64

KUBECTL_OK=1
command -v kubectl >/dev/null 2>&1 || KUBECTL_OK=0
if [ "$KUBECTL_OK" -eq 0 ]; then
	if [ "$DRY_RUN" -eq 1 ]; then
		warn "kubectl not found — dry-run will show the plan with placeholders."
	else
		die "kubectl not found in PATH."
	fi
fi

# get_secret_key <key> — prints the decoded value of a key in the Secret
# (empty string if the key or the Secret does not exist).
get_secret_key() {
	kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" \
		-o "jsonpath={.data.$1}" 2>/dev/null | base64 -d 2>/dev/null || true
}

secret_has_key() {
	[ -n "$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" \
		-o "jsonpath={.data.$1}" 2>/dev/null)" ]
}

confirm() {
	[ "$ASSUME_YES" -eq 1 ] && return 0
	[ "$DRY_RUN" -eq 1 ] && return 0
	printf 'Type "rotate" to proceed: '
	read -r answer
	[ "$answer" = "rotate" ] || die "aborted by operator."
}

restart_deployments() {
	for d in $DEPLOYMENTS; do
		run kubectl rollout restart "deployment/$d" -n "$NAMESPACE"
	done
	for d in $DEPLOYMENTS; do
		run kubectl rollout status "deployment/$d" -n "$NAMESPACE" --timeout="$TIMEOUT"
	done
}

# ---------------------------------------------------------------- banner
step "RAVEN JWT secret rotation"
if [ "$KUBECTL_OK" -eq 1 ]; then
	log "context:    $(kubectl config current-context 2>/dev/null || echo '?')"
else
	log "context:    (kubectl unavailable)"
fi
log "namespace:  $NAMESPACE"
log "secret:     $SECRET_NAME"
log "mode:       $([ "$FINISH" -eq 1 ] && echo 'finish (close window)' || echo 'start rotation')"
log "deployments: $DEPLOYMENTS"
[ "$DRY_RUN" -eq 1 ] && log "dry-run:    ON — nothing will change"

# ================================================================= FINISH
if [ "$FINISH" -eq 1 ]; then
	step "Closing the rotation window (remove JWT_SECRET_PREVIOUS)"
	if [ "$KUBECTL_OK" -eq 1 ]; then
		if secret_has_key JWT_SECRET_PREVIOUS; then
			log "JWT_SECRET_PREVIOUS is present in $SECRET_NAME."
		elif [ "$DRY_RUN" -eq 0 ]; then
			die "JWT_SECRET_PREVIOUS not found in $SECRET_NAME — no open window? Nothing to do."
		else
			warn "cannot confirm JWT_SECRET_PREVIOUS (dry-run) — showing the plan anyway."
		fi
	fi
	log "Make sure raven_auth_jwt_previous_secret_used_total has been ~0 for"
	log "at least 15 minutes (AccessTokenTTL) before doing this."
	confirm
	run kubectl patch secret "$SECRET_NAME" -n "$NAMESPACE" --type=json \
		-p '[{"op":"remove","path":"/data/JWT_SECRET_PREVIOUS"}]'
	restart_deployments
	step "Window closed"
	log "Pods now run with the new JWT_SECRET only. Tokens signed with the"
	log "retired secret get 401 and clients simply refresh."
	exit 0
fi

# ================================================================= START
step "Reading current JWT_SECRET"
CURRENT=""
if [ -n "$PREVIOUS_OVERRIDE" ]; then
	CURRENT="$PREVIOUS_OVERRIDE"
	log "using --previous override (len=${#CURRENT})"
elif [ "$KUBECTL_OK" -eq 1 ]; then
	CURRENT="$(get_secret_key JWT_SECRET)"
	if [ -z "$CURRENT" ]; then
		warn "JWT_SECRET is not set in $SECRET_NAME."
		warn "Pods are probably running on the documented code fallback"
		warn "('$DEV_FALLBACK'). Using that as JWT_SECRET_PREVIOUS."
		warn "Override with --previous if your image changed the fallback."
		CURRENT="$DEV_FALLBACK"
	fi
	log "current secret: len=${#CURRENT} sha256=$(fingerprint "$CURRENT")"
else
	CURRENT="<current-secret-unavailable-in-dry-run>"
	log "current secret: (placeholder — kubectl unavailable)"
fi

step "Generating the new secret"
NEW_SECRET="$(openssl rand -base64 48 | tr -d '\r\n')"
[ "${#NEW_SECRET}" -ge 40 ] || die "generated secret looks too short — refusing to continue."
log "new secret: len=${#NEW_SECRET} sha256=$(fingerprint "$NEW_SECRET")"
log "(the value itself is never printed)"

step "Plan"
log "1. patch $SECRET_NAME: JWT_SECRET_PREVIOUS=<current>, JWT_SECRET=<new>"
log "2. rollout restart: $DEPLOYMENTS"
log "3. rollout status (timeout $TIMEOUT)"
confirm

step "Patching the Secret"
PATCH_FILE="$(mktemp)" && chmod 600 "$PATCH_FILE"
trap 'rm -f "$PATCH_FILE"' EXIT
cat > "$PATCH_FILE" <<EOF
{"stringData":{"JWT_SECRET_PREVIOUS":"$(json_escape "$CURRENT")","JWT_SECRET":"$(json_escape "$NEW_SECRET")"}}
EOF
# One atomic merge patch: pods never see one key without the other.
# In dry-run mode the patch content is REDACTED — secret values must never
# land in scrollback, tickets or CI logs, not even a freshly generated one.
if [ "$DRY_RUN" -eq 1 ]; then
	log "[dry-run] would run: kubectl patch secret $SECRET_NAME -n $NAMESPACE --type=merge --patch-file=<tmp> (contents redacted)"
elif kubectl patch --help 2>/dev/null | grep -q -- '--patch-file'; then
	kubectl patch secret "$SECRET_NAME" -n "$NAMESPACE" --type=merge --patch-file="$PATCH_FILE"
else
	kubectl patch secret "$SECRET_NAME" -n "$NAMESPACE" --type=merge -p "$(cat "$PATCH_FILE")"
fi

step "Restarting JWT consumers"
restart_deployments

step "Rotation window is OPEN"
log "Signing now uses the new secret; verification accepts new + previous."
log ""
log "Next steps:"
log "  1. Watch the metric fall to ~0 for >= 15 min:"
log "       raven_auth_jwt_previous_secret_used_total"
log "       e.g. sum(increase(raven_auth_jwt_previous_secret_used_total[15m]))"
log "  2. Close the window:"
log "       $0 --finish --namespace $NAMESPACE --secret-name $SECRET_NAME"
log ""
log "Rollback (only valid while the window is open): patch JWT_SECRET back"
log "to the previous value (sha256 $(fingerprint "$CURRENT")) and restart."
