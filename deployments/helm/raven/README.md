# raven — Helm chart

One chart for the whole RAVEN platform: gateway, auth, users, jobs, worker,
websocket, console, broker (StatefulSet), Postgres, Redis, the migrate Job,
lockdown NetworkPolicies and Prometheus/Grafana/Jaeger.

Full docs: [docs/helm.md](../../../docs/helm.md).

## Quick start (Docker Desktop)

```bash
# build the images first (see deployments/kubernetes/namespace.yaml), then:
helm install raven deployments/helm/raven -n raven --create-namespace \
  -f deployments/helm/raven/values-dev.yaml
kubectl -n raven get pods -w
```

## Layout

| File | What it is |
|---|---|
| `values.yaml` | every knob, with defaults matching the flat manifests |
| `values-dev.yaml` | local profile: chart creates dev Secrets, LoadBalancers on localhost |
| `values-prod.yaml` | hardened example: Ingress, no external access, HPA on, bigger PVCs |
| `templates/` | all workloads, ported from `deployments/kubernetes/` |
| `files/dashboards/` | Grafana dashboards (copies of `deployments/grafana/dashboards/`) |

## Refresh the dashboards

```bash
cp deployments/grafana/dashboards/*.json deployments/helm/raven/files/dashboards/
```

## Validate

```bash
helm lint deployments/helm/raven
helm template raven deployments/helm/raven -f deployments/helm/raven/values-dev.yaml
```
