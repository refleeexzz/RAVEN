# Secret & certificate rotation runbook

Secrets age. People leave, laptops get lost, CI logs leak. Rotating the
values RAVEN trusts is how you make an old leak worthless — and with the
right procedure, nobody online notices it happened.

This runbook covers every rotating credential in the platform, from "run
this one script" to "here is the exact order so pods don't crash". Read the
JWT section fully before your first rotation; the rest are shorter.

## The map

| Secret | Where it lives | What breaks when you rotate it | Zero-downtime? |
|---|---|---|---|
| `JWT_SECRET` (HS256 signing key) | k8s Secret `raven-secrets`, env `JWT_SECRET` — read by **auth**, **gateway**, **websocket** | Every access token signed with the old value | **Yes** — dual-secret window (below) |
| `JWT_SECRET_PREVIOUS` | same Secret, only exists during a rotation window | Nothing on its own — it *is* the rotation mechanism | n/a |
| Postgres password | `raven-secrets` `DATABASE_URL` (auth, users, jobs, worker, migrate) + the role inside Postgres itself | Every service's connection pool | **Almost** — seconds-long risk window with the simple path; fully zero-downtime with the dual-role path |
| Refresh tokens | Postgres `sessions` table (sha256 hashes only) | One login session per rotation | **Yes, automatic** — they rotate themselves on every use |
| Broker TLS certs | broker Secret / cert files, `BROKER_TLS_*` envs | Client connections to the broker | **Yes** — hot reload (landing in parallel, see below) |
| Redis | no credential today (`REDIS_ADDR` only) | — | — |

## JWT signing secret

### How the zero-downtime trick works

RAVEN access tokens are HS256 JWTs, valid for 15 minutes
(`AccessTokenTTL` in `internal/auth/jwt.go`). A naive rotation — swap the
secret, restart everything — instantly 401s every token minted in the last
15 minutes. Users would not lose their sessions (refresh tokens are not
JWTs, see below), but they would feel a weird blip.

So verification supports a **dual-secret window**:

- **Signing always uses the primary secret** (`JWT_SECRET`). From the
  moment you rotate, no new token depends on the old value.
- **Verification tries primary, then previous** (`JWT_SECRET_PREVIOUS`).
  Tokens minted just before the rotation keep working until they expire —
  at most 15 minutes later.
- Every time a token validates *only* because of the previous secret, the
  counter **`raven_auth_jwt_previous_secret_used_total`** goes up. While
  that rate is above zero, the window must stay open. When it sits at ~0
  for 15+ minutes, the old secret is dead weight and you close the window.

The mechanism lives in `internal/auth/rotation.go` (`Verifier` +
`NewPreviousSecretUsedCounter`), pinned by unit tests that simulate the
full rotation lifecycle.

> **Status — read before rotating.** The dual-secret `Verifier` ships in
> `internal/auth`. Each service opts in by reading `JWT_SECRET_PREVIOUS`
> and building a `Verifier` instead of calling the single-secret
> `ParseAccessToken` — that wiring lands per service (auth, gateway,
> websocket) with their owners. **Do not run the procedure below against
> services that still verify single-secret**: patching `JWT_SECRET` alone
> there is the naive rotation and *will* invalidate live tokens. To check
> where your deployment stands, look at `/metrics` once the rollout is
> done: if `raven_auth_jwt_previous_secret_used_total` doesn't exist on a
> service, that service is not in the window yet. For emergencies on
> unwired services see "Hard rotation" at the end of this section.

### The procedure (automated)

`scripts/rotate-jwt-secret.sh` does the whole dance. It never prints
secret values — only lengths and sha256 fingerprints.

```bash
# 0. Look before you leap — prints the plan, changes nothing:
scripts/rotate-jwt-secret.sh --dry-run

# 1. Open the window: set JWT_SECRET_PREVIOUS=<current>, JWT_SECRET=<new>,
#    then roll the JWT consumers:
scripts/rotate-jwt-secret.sh

# 2. Wait for the metric to go quiet (>= 15 min at ~0):
#    sum(increase(raven_auth_jwt_previous_secret_used_total[15m]))

# 3. Close the window: drop JWT_SECRET_PREVIOUS, roll again:
scripts/rotate-jwt-secret.sh --finish
```

Defaults: namespace `raven`, Secret `raven-secrets`, deployments
**`auth gateway websocket`** — the three services that actually read
`JWT_SECRET` (verified in `cmd/auth`, `cmd/gateway`, `cmd/websocket`).
`users`, `jobs` and `worker` mount the same Secret but never read this
key, so restarting them would be pointless churn. Override anything with
flags (`--namespace`, `--secret-name`, `--deployments`, `--previous`,
`--timeout`) or the `RAVEN_*` env vars listed in `--help`.

### The procedure (manual, same steps)

```bash
# (a) capture the current value as PREVIOUS and mint a new primary —
#     ONE atomic patch, so no pod ever sees one key without the other:
CURRENT=$(kubectl get secret raven-secrets -n raven \
  -o jsonpath='{.data.JWT_SECRET}' | base64 -d)
NEW=$(openssl rand -base64 48)
kubectl patch secret raven-secrets -n raven --type=merge -p \
  "{\"stringData\":{\"JWT_SECRET_PREVIOUS\":\"$CURRENT\",\"JWT_SECRET\":\"$NEW\"}}"

# (b)+(c) restart the deployments that read JWT_SECRET — env vars are only
#     read at container start, so the patch alone changes nothing:
kubectl rollout restart deployment/auth deployment/gateway deployment/websocket -n raven
kubectl rollout status  deployment/auth      -n raven
kubectl rollout status  deployment/gateway   -n raven
kubectl rollout status  deployment/websocket -n raven

# (d) watch the previous-secret metric fall to ~0 for >= 15 minutes, then
# (e) remove PREVIOUS and restart once more:
kubectl patch secret raven-secrets -n raven --type=json \
  -p '[{"op":"remove","path":"/data/JWT_SECRET_PREVIOUS"}]'
kubectl rollout restart deployment/auth deployment/gateway deployment/websocket -n raven
```

### Rollback

While the window is open, rollback is trivial: patch `JWT_SECRET` back to
the previous value (the script prints its sha256 fingerprint so you can
confirm) and restart the three deployments. Once the window is closed and
the old secret is gone, there is nothing to roll back to — you would just
rotate again.

### Hard rotation (emergency, sessions dropped)

If the secret is *compromised right now*, waiting 15 minutes may not be
acceptable. Patch `JWT_SECRET` to the new value **without** setting
`JWT_SECRET_PREVIOUS`, restart the three deployments. Every live access
token dies instantly; clients hit 401, refresh (refresh sessions are
unaffected), and get tokens signed with the new secret. Worst case users
see one retry. That is the correct trade when the old value is public.

### What happens to refresh tokens

Nothing. Refresh tokens are 256-bit random strings stored as sha256 hashes
in Postgres — not JWTs, not signed with `JWT_SECRET`. They already rotate
themselves: every `Refresh` call returns a brand-new token and revokes the
presented one, with reuse detection that nukes the whole session family if
an old token ever shows up again (`services/auth/tokens.go` for minting,
`services/auth/server.go` `Refresh` for the rotation + `SELECT ... FOR
UPDATE` race handling, `security_refresh_reuse` audit rows for the alarm).
Operationally: a JWT secret rotation never logs anyone out.

## Postgres password

The password lives in two places that must agree: the role inside Postgres
(`ALTER USER`) and the `DATABASE_URL` key in `raven-secrets` (read by
**auth, users, jobs, worker** and the **migrate** job). The manifest's
`POSTGRES_PASSWORD` env only matters when the PVC is first initialized —
editing it later changes nothing, which is why `ALTER USER` is the real
lever.

Order matters. Do it so no pod ever *restarts into* a mismatch:

```bash
# 1. Update the Secret FIRST. Running pods keep their env (read at start),
#    so this alone changes nothing and is always safe:
kubectl patch secret raven-secrets -n raven --type=merge \
  -p '{"stringData":{"DATABASE_URL":"postgres://raven:NEW_PASSWORD@postgres:5432/raven?sslmode=disable"}}'

# 2. Change the role inside Postgres. Existing pooled connections stay
#    alive (Postgres does not kill sessions on password change):
kubectl exec -n raven deploy/postgres -- \
  psql -U raven -d raven -c "ALTER USER raven WITH PASSWORD 'NEW_PASSWORD';"

# 3. Restart the consumers immediately so new connections use the new
#    password:
kubectl rollout restart deployment/auth deployment/users deployment/jobs deployment/worker -n raven
kubectl rollout status deployment/auth -n raven   # ...repeat per deployment
```

The honest caveat: between steps 2 and 3 an old pod that opens a *fresh*
connection (pool churn, health-check recycling) still presents the old
password and gets one failed attempt. Keep the gap to seconds and it is a
non-event; the retry logic absorbs it. If your SLO cannot stomach even
that, use the **dual-role path** instead: create a second role
(`CREATE ROLE raven_v2 LOGIN PASSWORD ...` plus the same grants — or simply
`GRANT raven TO raven_v2` so it inherits everything), point the new
`DATABASE_URL` at `raven_v2`, and roll the deployments. Old pods keep
connecting as `raven`, new pods as `raven_v2`, both valid the whole time.
Once nothing references the old role, retire it (`DROP ROLE raven`, or
`ALTER ROLE raven NOLOGIN` if you want an easy undo).

Also update `POSTGRES_PASSWORD` in `deployments/kubernetes/postgres.yaml`
(or your overlay) so a *fresh* cluster initialized from the manifest
matches reality — remember it only applies at first boot of the PVC.

## Broker TLS certificates

> **Landing in parallel.** Broker TLS is being implemented by another
> workstream right now (`internal/broker` is out of this runbook's write
> scope). This section is written against its design — `BROKER_TLS_*`
> environment variables and file-based certs — so confirm the exact env
> names against the broker docs/manifests once that work merges, and treat
> this as the shape of the procedure rather than gospel.

Design points that make cert rotation boring (the good kind of boring):

- The broker loads its certificate/key/CA from the files pointed to by the
  `BROKER_TLS_*` envs — typically a mounted k8s Secret.
- **Hot reload**: the broker picks up replaced cert files without a
  process restart, so rotating certs does not drop broker connections.

Procedure, once it lands:

```bash
# 1. Issue the new cert/key (your CA of choice; cert-manager, step-ca, ...).
# 2. Update the Secret the broker mounts:
kubectl create secret tls broker-tls -n raven \
  --cert=new.crt --key=new.key --dry-run=client -o yaml | kubectl apply -f -
# 3. Mounted Secrets update in-place (kubelet sync, ~1 min) and the broker
#    hot-reloads — NO rollout restart needed.
# 4. Verify the handshake from a client pod:
kubectl exec -n raven deploy/worker -- \
  openssl s_client -connect broker:9100 -CAfile /path/to/ca.crt </dev/null
```

Watch the broker logs for the reload line and failed handshakes after the
swap; keep the old cert until every client presents the new chain happily.

## Cheatsheet

| You want to… | Do this |
|---|---|
| Rotate `JWT_SECRET`, nobody notices | `scripts/rotate-jwt-secret.sh` → wait for metric ~0 ≥ 15 min → `--finish` |
| `JWT_SECRET` just leaked publicly | Hard rotation: patch without PREVIOUS + restart auth/gateway/websocket |
| Rotate the Postgres password | Patch `DATABASE_URL` → `ALTER USER` → rollout restart DB consumers (seconds gap) |
| Rotate broker TLS certs | Swap the mounted Secret → hot reload picks it up (landing in parallel) |
| Rotate a user's refresh token | Already done — it rotates on every refresh call |
