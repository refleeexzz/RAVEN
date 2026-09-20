# RAVEN on Helm

One chart, whole platform. `deployments/helm/raven` installs everything the
flat manifests in `deployments/kubernetes/` install — same images, same
replica counts, same security posture — but parameterized, repeatable, and
upgradeable with one command.

This is now the **recommended way to deploy RAVEN to Kubernetes**. The flat
manifests stay in the repo as readable reference material; the chart is the
living thing.

---

## TL;DR

```bash
# Docker Desktop, zero manual steps (chart creates dev Secrets for you):
helm install raven deployments/helm/raven -n raven --create-namespace \
  -f deployments/helm/raven/values-dev.yaml

kubectl -n raven get pods -w
# gateway on http://localhost:8080 · console :7100 · grafana :3000 · jaeger :16686
```

Upgrade after changing values or the chart:

```bash
helm upgrade raven deployments/helm/raven -n raven -f deployments/helm/raven/values-dev.yaml
```

Tear down:

```bash
helm uninstall raven -n raven
kubectl delete job migrate -n raven   # hook jobs are not tracked by the release
kubectl delete pvc -n raven --all     # ONLY if you really want the data gone
```

---

## What the chart installs

| Piece | Kind | Notes |
|---|---|---|
| gateway, auth, users, jobs, worker, websocket, console | Deployment + Service | non-root uid 10001, read-only root fs |
| broker | StatefulSet + Service + PVC | exactly 1 replica, commit logs on `brokerdata` |
| postgres | Deployment + Service + PVC | `PGDATA` fix baked in (see gotchas) |
| redis | Deployment + Service | stateless by design (emptyDir) |
| migrate | Job (Helm hook) | runs post-install/post-upgrade, idempotent |
| raven-config | ConfigMap | service DNS contract — same values as flat |
| raven-secrets | Secret | created by the chart **or** expected to exist |
| 16 NetworkPolicies | NetworkPolicy | default-deny + least privilege, can be switched off |
| prometheus, grafana, jaeger | Deployment + Service + ConfigMaps | gated by `observability.enabled` |
| HPAs for gateway + worker | HorizontalPodAutoscaler | gated by `autoscaling.enabled` |

Grafana dashboards ship inside the chart (`files/dashboards/`, copies of
`deployments/grafana/dashboards/`). Refresh them with:

```bash
cp deployments/grafana/dashboards/*.json deployments/helm/raven/files/dashboards/
```

---

## Values that matter

Everything is in `values.yaml` with comments. The knobs you will actually touch:

| Knob | Default | Why you would change it |
|---|---|---|
| `global.imageRegistry` | `""` | point at your private registry mirror |
| `*.image.tag` | pinned (`sec-20260913`) | bump on every rebuild — never `:latest` |
| `gateway.service.type` | `LoadBalancer` | `ClusterIP` + `gateway.ingress.enabled=true` in prod |
| `worker.replicas` | `5` | the scaling muscle (or turn the HPA on) |
| `postgres.persistence.size` | `5Gi` | prod wants more, and a real `storageClass` |
| `broker.persistence.size` | `5Gi` | commit logs grow with traffic |
| `secrets.create` | `false` | `true` = chart renders the Secret from values |
| `secrets.jwtSecret` / `secrets.databaseUrl` | `""` | inject with `--set`, never commit |
| `networkPolicy.enabled` | `true` | keep it on unless your CNI ignores policies |
| `networkPolicy.externalAccess` | `true` | `false` drops every `0.0.0.0/0` ingress rule |
| `observability.enabled` | `true` | saves ~400Mi of requests when off |
| `autoscaling.enabled` | `false` | needs metrics-server on the cluster |
| `config.wsAllowAnonymous` | `true` | `false` anywhere real |

### Profiles

- `values-dev.yaml` — Docker Desktop. Chart creates the Secret with the
  documented DEV credentials, everything is a LoadBalancer on localhost.
- `values-prod.yaml` — hardened **example**: Ingress instead of raw
  LoadBalancers, `externalAccess: false`, HPA on, bigger PVCs, no anonymous
  websockets. Still needs your real secrets and hostnames injected.

---

## Secrets, two ways

1. **Bring your own** (default, `secrets.create: false`): create
   `raven-secrets` yourself from
   `deployments/kubernetes/secrets.example.yaml`. Pods start with dev
   defaults if it is missing — fine for a laptop, never for real.
2. **Chart renders it** (`secrets.create: true` + inject values):

   ```bash
   helm upgrade raven deployments/helm/raven -n raven \
     --set secrets.create=true \
     --set secrets.jwtSecret="$(openssl rand -base64 48)" \
     --set secrets.databaseUrl="postgres://raven:...@postgres:5432/raven?sslmode=disable"
   ```

`secrets.jwtSecretPrevious` exists for rotation windows only — see
`docs/security/rotation.md`.

---

## The migrate hook (and why it is post-, not pre-)

`migrate` runs as a `post-install,post-upgrade` Helm hook. The obvious
choice — pre-install — **deadlocks on first install**: the Job's
initContainer waits for postgres, but postgres is deployed by the same
chart and only exists after the hooks finish. We hit this for real while
building the chart; that is why it is post-.

Consequence: on a cold install the app pods crash-loop two or three times
until the schema lands, then go Ready — same behavior as the flat
manifests. This is normal. The hook re-runs on every upgrade
(`before-hook-creation` deletes the old immutable Job first) and is
idempotent. Set `migrate.hook: false` if you want a plain Job instead.

After `helm uninstall`, hook Jobs are **not** removed by Helm — delete
`job/migrate` manually (or delete the whole namespace).

---

## Validate before you ship a change

```bash
helm lint deployments/helm/raven
helm lint deployments/helm/raven -f deployments/helm/raven/values-prod.yaml
helm template raven deployments/helm/raven -f deployments/helm/raven/values-dev.yaml \
  | kubectl apply --dry-run=server -f -
```

Default render: 49 objects. Dev: 50 (adds the Secret). Prod: 53 (adds
Secret, Ingress, 2 HPAs).

---

## Gotchas we already stepped on (so you don't have to)

- **`.helmignore` has no negation support.** A `!README.md` rule makes the
  loader ignore `Chart.yaml` itself and every command fails with
  "Chart.yaml file is missing". Keep the ignore file dumb.
- **Fresh PVC + uid 70 postgres = initdb chmod failure** on the mountpoint.
  The chart sets `PGDATA=/var/lib/postgresql/data/pgdata` (a subdirectory
  postgres can own). The flat manifests needed a live patch for this; the
  chart bakes the fix in.
- **Docker Desktop LoadBalancer port conflicts.** Two namespaces both
  asking for `localhost:8080` deadlock the second Service's cleanup
  finalizer. One `raven` namespace with LB services per machine, or use
  `ClusterIP` + port-forward in extra namespaces.
- **One release per namespace.** Service names (`gateway`, `broker`,
  `postgres`...) are a DNS contract with the Go services and the
  NetworkPolicies, so the chart deliberately uses fixed names instead of
  release-prefixed ones.
- **Windows + Git Bash + helm.exe**: run helm from a path without spaces
  or em-dashes (the repo path `F:\RAVEN — ...` breaks the Windows binary's
  chart loader). Copy the chart to `C:\tmp` / `%TEMP%` for local helm runs.
