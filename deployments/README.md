# RAVEN — running the platform

This folder has everything you need to run RAVEN on your machine.
Two ways: Docker Compose (easiest) or Kubernetes on Docker Desktop
(closer to "real"). Pick one. Both give you the same stack.

What's in the stack: gateway, auth, users, jobs, websocket, worker,
broker, postgres, redis, plus Prometheus, Grafana and Jaeger for
observability.

## Option 1: Docker Compose (start here)

From the repo root:

```bash
cp .env.example .env     # optional — every var has a dev default
docker compose up -d
```

That's it. The first run builds 8 images, so go get a coffee.
After that, builds are cached and fast.

Check that everything is up:

```bash
docker compose ps
curl http://localhost:8080/health
```

Useful day-to-day commands:

```bash
docker compose logs -f gateway     # follow one service
docker compose up -d --build jobs  # rebuild one service after a code change
docker compose down                # stop everything (data survives)
docker compose down -v             # stop and DELETE the data volumes
```

Only port 8080 is exposed for the apps. The gateway is the single entry
point — every API call goes through it. The other services talk to each
other inside the compose network and are not reachable from your host.
That's on purpose.

## Option 2: Kubernetes on Docker Desktop

First enable Kubernetes in Docker Desktop (Settings → Kubernetes →
Enable). Wait for the little green light.

Then build the images. Docker Desktop's cluster shares your local Docker
image store, so there is nothing to push:

```bash
for s in gateway auth users jobs websocket broker worker migrate; do
  docker build -f deployments/docker/Dockerfile.$s -t raven/$s:latest .
done
```

Create the secrets (the example file is a template — the values in it
are the dev defaults, fine for a laptop):

```bash
cp deployments/kubernetes/secrets.example.yaml /tmp/secrets.yaml
kubectl apply -f /tmp/secrets.yaml
```

Apply everything:

```bash
kubectl apply -f deployments/kubernetes/
kubectl -n raven get pods     # wait until all are Running
```

The services are ClusterIP only (nothing is exposed to the host
automatically). Use port-forward to reach them:

```bash
kubectl -n raven port-forward svc/gateway 8080:8080
```

Heads-up: the HPAs for gateway and worker need `metrics-server`, which
Docker Desktop does not install. The HPAs won't break anything without
it, they just won't scale. If you want autoscaling:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

If you edit the Grafana dashboard JSON, regenerate the k8s copy with
`python deployments/gen-observability-k8s.py` and re-apply.

## Observability

Both setups give you the same three UIs:

| Tool       | Compose                    | Kubernetes (port-forward)                     |
|------------|----------------------------|-----------------------------------------------|
| Grafana    | http://localhost:3000      | `kubectl -n raven port-forward svc/grafana 3000:3000`     |
| Prometheus | http://localhost:9090      | `kubectl -n raven port-forward svc/prometheus 9090:9090`  |
| Jaeger     | http://localhost:16686     | `kubectl -n raven port-forward svc/jaeger 16686:16686`    |

Grafana login is `admin` / `admin`. Open the "RAVEN" folder — the
"RAVEN Platform Status" dashboard is already loaded. Prometheus and
Jaeger datasources are pre-wired, no clicks needed.

Jaeger receives traces from every service over OTLP gRPC (`jaeger:4317`
inside the network). Pick a service in the Jaeger UI and hit Search to
see traces flowing.

## Where things live

```
deployments/
  docker/          one multi-stage Dockerfile per service
  prometheus/      scrape config used by compose
  grafana/         provisioning + the platform dashboard JSON
  kubernetes/      all manifests for Docker Desktop k8s
.github/workflows/ CI: lint, test, build, docker builds, integration
docker-compose.yml the full local stack
.env.example       documented dev defaults (copy to .env)
```

One rule: ports and env var names are fixed by
`docs/contracts/ports-and-env.md`. If you change something there, change
it everywhere.
