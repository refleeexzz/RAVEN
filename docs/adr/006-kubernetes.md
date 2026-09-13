# ADR 006: Kubernetes as the target environment (Compose for the laptop)

Status: accepted

## Context

The platform needs a local dev story and a "shaped like production" story.
Docker Compose is the easiest local answer; Kubernetes is what the target
environment looks like in the jobs this project is practice for.

## Decision

Support both, deliberately:

- **Docker Compose** (`docker-compose.yml`) is the day-to-day dev stack:
  one command, named healthchecks, ordered startup (`depends_on:
  service_healthy` / `service_completed_successfully` for migrations).
- **Kubernetes manifests** (`deployments/kubernetes/`) target Docker
  Desktop's built-in cluster: one Deployment per service, a StatefulSet for
  the broker with a persistent volume, ClusterIP services, HPAs for gateway
  and worker (2–20 replicas at 70% CPU), readiness/liveness probes wired to
  the real `/ready` and `/health` endpoints, rolling updates, and secrets in
  a separate manifest you create from the example file.

The images are built locally (`raven/<service>:latest`) and never pushed —
Docker Desktop's cluster shares the local image store.

## Alternatives

- **Compose only.** Less YAML, and Compose already does healthchecks and
  replicas. Rejected because the point is practicing the k8s primitives —
  probes, HPAs, rolling updates — that Compose only approximates.
- **k3s/kind/minikube.** All fine; Docker Desktop k8s won because it is one
  checkbox for anyone already running Docker, and the image-store sharing
  removes the registry step entirely.
- **Helm/kustomize.** More machinery than eight services justify. Plain
  manifests you can read top to bottom were the goal.

## Consequences

Positive:

- The k8s manifests exercise the things the services were built for:
  readiness actually gates traffic, so a pod whose Postgres check fails
  never receives requests; HPAs scale the two stateless scaling muscles
  (gateway, worker); rolling updates plus graceful shutdown (15 s HTTP
  drain, 10 s gRPC drain) mean deploys don't drop requests.
- One command each way: `make k8s-up` / `make k8s-down`.
- The observability stack runs in-cluster too (Prometheus scrape config is
  the same one Compose uses, kept in sync by hand + a generator script for
  the Grafana dashboard ConfigMap).

Negative:

- **Two deployment systems to keep in sync.** The Compose file and the
  manifests both encode ports, env vars and images. The contract doc
  (`docs/contracts/ports-and-env.md`) is the mitigation, but drift is on
  us.
- Docker Desktop's k8s ships without `metrics-server`, so the HPAs are
  inert until you install it — the manifests ship with that caveat written
  in the file header and in `deployments/README.md`.
- Compose `deploy.replicas: 3` for the worker and k8s `replicas: 5`
  disagree on purpose (local watching vs. target-env shape). Anyone
  comparing the two files should know that.
- Everything is ClusterIP, so reaching the stack means `kubectl
  port-forward`. Fine for dev, not a pattern for anything real.
