# Worker Autoscaling by Consumer Lag

How RAVEN scales its worker fleet: the design, the math, the knobs, and what
actually works on the local cluster today.

## The one-paragraph design

Workers pull jobs from the broker in the consumer group `workers`. When
producers outpace consumers, the gap shows up immediately as
`raven_broker_consumer_lag` (partition end offset − committed offset, per
topic/partition/group). The HPA in
`deployments/kubernetes/hpa-worker.yaml` reads that lag through
prometheus-adapter and scales the `worker` Deployment so the fleet always
owns roughly the same share of the queue. CPU stays in the manifest as a
fallback seatbelt.

## Why lag and not CPU

CPU is a *lagging* indicator for a queue consumer: it only spikes after the
jobs have already arrived and started executing. By the time a CPU-based HPA
reacts, the queue has been growing for minutes and users already feel the
delay. Consumer lag is the *leading* indicator — it rises the instant
arrival rate beats service rate, even if every worker is idle-bored because
the jobs are tiny. Bonus: lag-based scaling also scales **down** correctly.
A CPU-based HPA keeps pods alive while they chew through a backlog at full
tilt; a lag-based one sees the backlog draining and releases pods as it
empties.

There is one honest caveat: lag says nothing about *why* consumers are slow.
If workers are stuck on a dead downstream, adding pods adds stuck workers.
That's why the ceiling exists.

## The formula

The HPA `External` metric uses `AverageValue`, which divides the total
metric value by the current replica count before comparing with the target.
Net effect:

```
desiredReplicas = ceil( totalLag / lagPerPod )

totalLag   = sum(raven_broker_consumer_lag{group="workers"})
             across every topic/partition the group consumes
lagPerPod  = 2000        # target queue share per pod (averageValue)
```

Examples: 10k lag → 5 pods. 25k lag → 13 pods → capped at `maxReplicas`.

Picking 2000: one worker pod handles the observed throughput with headroom
at ~2k queued messages; the number is a starting point to tune against the
p95 handler duration, not physics. Watch `raven:jobs_duration_p95:5m` and
the drain rate after a scaling event and adjust.

## Floor, ceiling and cooldown

| Knob | Value | Why |
|------|-------|-----|
| `minReplicas` | 2 | one dead worker must never stall the queue |
| `maxReplicas` | 12 | past this, broker connections and DB pool slots become the real bottleneck; raise deliberately |
| scale-up stabilization | 60 s | react to sustained lag, ignore 15-second spikes |
| scale-up policy | +2 pods/min or double, max wins | fast enough for a flood, bounded enough to observe |
| scale-down stabilization | 300 s | a queue that took minutes to build doesn't drain in seconds; premature scale-down re-queues in-flight leases |
| scale-down policy | 1 pod/min, never >25%/min | flapping workers thrash leases and the Redis registry for nothing |

## Prerequisites (this is the honest part)

**1. metrics-server — NOT installed on the local cluster (checked today).**
`kubectl top pods` answers `error: Metrics API not available` and there is
no `v1beta1.metrics.k8s.io` APIService. Without it even the CPU fallback
metric does nothing — the HPA sits idle and harmless. Install:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

**2. prometheus-adapter — NOT installed, and its manifest is owned by
another workstream.** The `External` metric needs the adapter to translate
Prometheus into the k8s metrics API. Once installed, give it a rule like
this (helm value `rules:` / configMap, depending on how you deploy it):

```yaml
rules:
  - seriesQuery: 'raven_broker_consumer_lag{group="workers"}'
    resources:
      overrides:
        namespace: {resource: "namespace"}
        pod: {resource: "pod"}
    name:
      matches: "^(.*)$"
      as: "${1}"
    metricsQuery: 'sum(<<.Series>>) by (<<.GroupBy>>)'
```

The `metricsQuery` collapses all topic/partition series of the `workers`
group into one total, which is exactly the `totalLag` in the formula. The
HPA selector `group: workers` then picks that series.

**3. Resolve the HPA conflict.** `deployments/kubernetes/hpa.yaml` already
defines a CPU-only HPA named `worker` on the same Deployment. Two HPAs on
one target fight (each overrides the other's replica count). Choose one:

```bash
kubectl -n raven delete hpa worker
kubectl apply -f deployments/kubernetes/hpa-worker.yaml
```

## What works today vs what needs the adapter

| Piece | Status on the local cluster |
|-------|------------------------------|
| `raven_broker_consumer_lag` exported by the broker, scraped by Prometheus | **Live** (confirmed: `group="workers"`, topic `jobs`, partitions 0/1/7) |
| Broker Overview dashboard lag panel | **Live** |
| `hpa-worker.yaml` passes API validation | **Live** (`kubectl apply --dry-run=server` accepted) |
| CPU fallback metric | **Blocked on metrics-server** (not installed) |
| Lag-based scaling itself | **Blocked on prometheus-adapter** (manifest owned elsewhere) |
| Alerting when lag > 10k for 5 min | Ships in `deployments/prometheus/rules.yaml`, needs the `rule_files` wiring described there |

So today the HPA is real, valid and inert; it comes alive piece by piece as
metrics-server and the adapter land.

## Alternative: KEDA

If the platform ever adopts [KEDA](https://keda.sh), the same design becomes
a `ScaledObject` and the behavior knobs move into KEDA's hands. Kept here as
a reference — **not applied**, KEDA is not installed:

```yaml
# apiVersion: keda.sh/v1alpha1
# kind: ScaledObject
# metadata:
#   name: worker-lag
#   namespace: raven
# spec:
#   scaleTargetRef:
#     name: worker
#   minReplicaCount: 2
#   maxReplicaCount: 12
#   pollingInterval: 15        # seconds between Prometheus queries
#   cooldownPeriod: 300        # matches the HPA scale-down window
#   triggers:
#     - type: prometheus
#       metadata:
#         serverAddress: http://prometheus:9090
#         metricName: raven_broker_consumer_lag_workers_total
#         query: sum(raven_broker_consumer_lag{group="workers"})
#         threshold: "2000"    # same lag-per-pod target; KEDA does the
#                              # desired = ceil(lag/threshold) math itself
```

KEDA's advantage over HPA+adapter: no metrics API translation layer, and
scale-to-zero (`minReplicaCount: 0`) if we ever want the fleet to sleep at
night. The disadvantage: another controller to run and secure. For now the
HPA file is the production answer; this block is the escape hatch.

## Verifying a scaling event

Once the adapter is in:

```bash
kubectl -n raven get hpa worker-lag --watch   # TARGETS column shows lag/2k
kubectl -n raven describe hpa worker-lag      # which metric drove the decision
```

Flood the queue (e.g. run a benchmark produce burst against topic `jobs`),
watch lag climb on the Broker Overview dashboard, and watch the replica
count follow it up the `ceil(lag/2000)` curve — then fall back, one pod per
minute, once the queue drains.
