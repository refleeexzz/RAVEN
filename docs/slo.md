# RAVEN SLOs, SLIs and Error Budgets

This doc defines what "reliable" means for RAVEN in numbers: the indicators
we measure (SLIs), the targets we hold ourselves to (SLOs), and what we do
when the numbers turn red. It ships with executable alerting rules in
`deployments/prometheus/rules.yaml`.

## SLI vs SLO vs SLA — 30 seconds

- **SLI (indicator)**: the thing you measure. Example: share of gateway
  requests that do not return 5xx.
- **SLO (objective)**: the target for that measurement, over a time window.
  Example: 99.9% of gateway requests are non-5xx, over rolling 30 days.
- **SLA (agreement)**: a contract with a customer, built on top of SLOs,
  with money attached (credits, penalties) when you miss. We don't have one
  yet — see [What a formal SLA still needs](#what-a-formal-sla-still-needs).

SLOs are internal and honest; SLAs are external and conservative. You always
want the SLO tighter than the SLA, so you burn your own budget before the
customer's.

## The indicators (SLIs) and objectives (SLOs)

| # | SLI | PromQL | SLO target | When it burns |
|---|-----|--------|-----------|----------------|
| 1 | **Gateway availability** — non-5xx share of all gateway requests | `1 - (sum(rate(raven_gateway_http_requests_total{status=~"5.."}[w])) / sum(rate(raven_gateway_http_requests_total[w])))` | **99.9%** per rolling 30d | `GatewayErrorBudgetBurnFast/Slow` alerts fire; page on-call, check upstreams (`raven_gateway_circuit_breaker_state`), Postgres, broker |
| 2 | **Job success rate** — 1 − terminal failures / all executions. Terminal = `dead` (exhausted retries) or `poison`. Retries are normal operation, not failures | `1 - (sum(rate(raven_worker_jobs_processed_total{result=~"dead\|poison"}[1h])) / sum(rate(raven_worker_jobs_processed_total[1h])))` | **99.5%** over 1h windows | `JobSuccessErrorBudgetBurnFast/Slow`; find the dying job type on the Workers dashboard, check the DLQ (`raven_jobs_dead`), suspect a bad deploy |
| 3 | **Job latency (proxy)** — p95 of worker handler execution time | `histogram_quantile(0.95, sum by (le) (rate(raven_worker_job_duration_seconds_bucket[5m])))` | **p95 < 10s** sustained | `JobLatencyP95High`; look for slow handlers, saturated workers, or slow downstreams (webhooks, DB) |
| 4 | **Consumer lag** — worst lag across all topic/partition/group | `max(raven_broker_consumer_lag)` | **< 10,000 msgs** for 5m | `BrokerConsumerLagHigh`; consumers are losing to producers — scale workers (see [autoscaling.md](autoscaling.md)) or fix the slow consumer |

Notes on the choices:

- **4xx never burns budget.** A client sending garbage is the client's
  problem; only 5xx means we failed. Same reason `retry` results don't count
  as job failures — the retry machinery doing its job is the system working,
  not failing.
- **The latency SLI is a proxy.** We measure handler execution inside the
  worker, not true submit-to-finish end-to-end latency. Closing that gap
  needs a creation→completion duration histogram in the jobs service. Until
  then, queue wait time shows up indirectly in SLI #4 (lag).
- **Job success is measured on 1h windows**, tighter than the classic 30d
  window, because a jobs platform can destroy trust in an afternoon. The
  burn-rate alerts still use 1h/6h long windows, which fits both.

## Error budgets

The error budget is the allowed amount of "bad" before the SLO is broken.
It exists to be spent — on deploys, experiments and honest accidents — and
it tells you when to stop shipping and start stabilizing.

| SLO | Budget | What the budget buys per 30 days |
|-----|--------|----------------------------------|
| Gateway 99.9% | 0.1% of requests may 5xx | ~43m 50s of total outage (or a proportional trickle of errors) |
| Jobs 99.5% | 0.5% of executions may die | 1 in 200 job executions may end `dead`/`poison` |

**Burn rate** = observed error ratio ÷ budget ratio. Burning at 1x eats the
budget exactly in 30 days. Our alerts (Google SRE Workbook multiwindow
pattern):

| Alert | Condition | Meaning | Response |
|-------|-----------|---------|----------|
| Fast burn | ratio > **14.4x** budget over **1h** *and* 5m | Budget gone in ~2 days | **Page**. This is an incident now. |
| Slow burn | ratio > **6x** budget over **6h** *and* 30m | Budget gone in ~5 days | **Ticket**. Investigate in business hours. |

Requiring the long *and* short window at once filters out blips that already
recovered: you only get paged for damage that is still happening.

Recording rules (`raven:gateway_error_ratio:*`, `raven:jobs_failure_ratio:*`,
`raven:jobs_duration_p95:5m`, `raven:broker_consumer_lag_max`) and the six
alerts live in `deployments/prometheus/rules.yaml`. The file header explains
how to wire it into the compose and k8s Prometheus configs (a `rule_files`
line + a mount — neither is applied automatically yet).

## What a formal SLA still needs

We're not selling promises yet. To turn these SLOs into a customer-facing
SLA we'd still need:

1. **External measurement.** All SLIs are self-reported by our own
   Prometheus. An SLA needs an outside probe (synthetic checks from a
   different network) so "everything is down including monitoring" doesn't
   count as 100% uptime.
2. **Looser numbers.** Standard practice: SLA = SLO minus margin (e.g. SLO
   99.9% → SLA 99.5%), so internal alerts fire before the contract breaks.
3. **Defined exclusions.** Planned maintenance windows, customer-caused
   failures, and force majeure must be written down, or every restart costs
   money.
4. **Longer retention.** Dev Prometheus keeps data on an `emptyDir` — a pod
   restart wipes the evidence. SLA compliance reporting needs durable,
   long-term metrics storage (remote write, Thanos/Mimir, or a managed
   service).
5. **Legal & billing plumbing.** Service credits, claim process, and someone
   who owns the number.

Until those exist, treat the SLOs here as engineering targets: real enough
to page on, informal enough to tune.

## Dashboards

The SLIs above are the top-row stats on the production dashboards
(`deployments/grafana/dashboards/`): gateway 5xx ratio on **Edge & Security**,
job success ratio on **Workers**, lag on **Broker Overview**. Same numbers,
same PromQL — the dashboards and the alerts can't disagree.
