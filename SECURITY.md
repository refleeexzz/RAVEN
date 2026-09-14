# Security Policy

Thanks for caring about security. RAVEN is a learning-grade distributed
systems platform — we take real-world security habits seriously, even
though the project runs locally by default.

## Supported versions

We patch the latest `main` branch only. There are no stable release
lines yet.

| Version        | Supported |
|----------------|-----------|
| `main`         | ✅        |
| anything older | ❌        |

## Reporting a vulnerability

**Please do not open a public issue for security bugs.**

Use **GitHub Security Advisories** instead: go to the repository's
*Security* tab → *Report a vulnerability*. That gives us a private space
to reproduce, fix, and credit you before anything goes public.

What to include: the affected component, steps to reproduce, and what an
attacker could do with it. A short proof of concept helps a lot.

You can expect a first answer within a few days. If the report is
accepted, we will ship a fix on `main` and mention you in the changelog
(unless you'd rather stay anonymous).

## Deployment security checklist

RAVEN ships with **dev defaults** so a fresh clone just works. They are
documented, public, and **not safe for any real deployment**. Before you
put RAVEN anywhere near real users:

- [ ] **Change `JWT_SECRET`.** The default is `dev-only-secret-change-me`
      (in `.env.example`, `secrets.example.yaml`, and the code fallback).
      Generate a long random one and inject it via your secrets manager.
- [ ] **Change the database credentials.** Dev uses `raven`/`raven`.
      Never reuse it outside Docker Desktop.
- [ ] **Set `WS_ALLOW_ANONYMOUS=false`** (it is `true` in the dev
      ConfigMap so the console can show events without a login).
- [ ] **Put the gateway behind TLS** (an Ingress with certificates).
      The plain LoadBalancer services in `deployments/kubernetes/` are a
      Docker Desktop convenience — Grafana (`admin`/`admin`), Prometheus,
      Jaeger and the console have no authentication at all.
- [ ] **Keep the NetworkPolicies on.** `networkpolicy.yaml` is what stops
      a random pod from talking to the broker's unauthenticated TCP port.
      Only `jobs` and `worker` pods may reach it.
- [ ] **Review CORS origins** and any other `*_ALLOW_*` knobs for your
      domains.
- [ ] **Don't expose `/metrics`, `/ready`, `/debug/*`** publicly. They
      leak dependency states and internals. In-cluster + Prometheus only.
- [ ] **Run the containers as they are defined here**: non-root,
      read-only root filesystem, all capabilities dropped. If you fork
      the manifests, keep those flags.
- [ ] **Pin images by digest** before production (the repo pins tags like
      `alpine:3.21` today; digest pinning is on the roadmap).

## Audit reports

Internal security audits live in [`docs/security/`](docs/security/):

- [`infra.md`](docs/security/infra.md) — infrastructure & supply chain
  (Kubernetes, Docker, CI, dependencies, secrets)

## Secrets hygiene

- `secrets.example.yaml` and `.env.example` contain **placeholders only**.
  Copy them, fill in real values, and keep the copies out of git —
  `.gitignore` already covers `.env`, `secrets.yaml`, `*.secrets.yaml`.
- If you ever commit a real secret by accident: rotate it immediately,
  then rewrite history. Treat it as compromised the moment it lands.

## Rotation

Every secret RAVEN trusts has a rotation procedure — most of them
zero-downtime. The full runbook lives in
[`docs/security/rotation.md`](docs/security/rotation.md); the short
version:

- **`JWT_SECRET`** rotates through a dual-secret window: verification
  accepts the previous secret while signing always uses the new one
  (`internal/auth` `Verifier`), so no live session is dropped. The metric
  `raven_auth_jwt_previous_secret_used_total` tells you when the window
  can close, and `scripts/rotate-jwt-secret.sh` automates the whole
  procedure (with `--dry-run`).
- **Refresh tokens** rotate themselves on every use, with reuse
  detection — a JWT rotation never logs anyone out.
- **Postgres password** and **broker TLS certs** have their own
  step-by-step procedures in the runbook, ordered so pods never restart
  into a mismatch.

Rotate on a schedule, and always rotate on suspicion: a secret everyone
has forgotten about is a secret someone else may remember.
