# Chaos test results

Last run: 2026-09-13 19:54:07 -03 · target: local k8s cluster, namespace `raven` · last `go test` process: 15s

We broke RAVEN on purpose and wrote down what happened. Each scenario below
kills or restarts a real piece of the platform on the live local cluster, then
measures what the system does about it: what we broke, what we expected, what
actually happened, how long recovery took, and whether any data was lost or
any job ran twice. Everything here is measured, not guessed.

## Summary

| Scenario | Verdict | Recovery time | Data loss | Duplicates |
|---|---|---|---|---|
| KillWorkerGraceful | PASS | 11.8s until all jobs terminal (replacement pod ready 10s after the kill) | no (0 lost) | 0 jobs ran more than once |
| KillWorkerForced | PASS | deployment ready 10.7s after the kill; job states settled in 10.8s (deadline 4m) | no (0 lost) | 0 jobs ran more than once |
| KillMultipleWorkers | PASS | deployment back at full strength 11.2s after the kills; job states settled in 11.5s (deadline 4m) | no (0 lost) | 0 jobs ran more than once |
| RestartBroker | PASS | broker-0 Ready + ops API answering 17.1s after the kill; accepted jobs settled in 18.6s | no — 0 lost; high watermarks only moved forward (pre sum=42020, post sum=42031) | 0 jobs ran more than once |
| RestartRedis | PASS | 11.6s from kill to redis Ready (sampled the API every 700ms during the window) | no — Postgres is the source of truth; redis holds only ephemeral state. Job status events published during the window were dropped by design (pub/sub has no history). | n/a (infra restart; no duplicate execution measured) |
| RestartPostgres | PASS | 12.2s from kill to first successful API read; sampled every 1s during the outage | no — 4/4 pre-created jobs intact and still SUCCESS after the restart | n/a (infra restart; no duplicate execution measured) |

## Scenarios

### KillWorkerGraceful

- **Failure:** SIGTERM to one worker pod (kubectl delete pod, default grace period) while 20 resize_image jobs are flowing.
- **Expected behavior:** The worker stops fetching, drains in-flight jobs inside its 15s window and exits; the Deployment replaces the pod; all 20 jobs reach SUCCESS; no data loss; redeliveries are deduped by the fence; no job row ever shows attempts > max_attempts.
- **Actual behavior:** All 20 jobs reached SUCCESS. The pod drained and the Deployment replaced it; the burst kept flowing the whole time. 0 jobs ran more than once.
- **Recovery time:** 11.8s until all jobs terminal (replacement pod ready 10s after the kill)
- **Data loss:** no (0 lost)
- **Duplicate execution:** 0 jobs ran more than once
- **Final state:** worker deployment healthy at 2/2 replicas; 20/20 jobs SUCCESS

### KillWorkerForced

- **Failure:** SIGKILL to one worker pod (kubectl delete pod --force --grace-period=0) while 20 resize_image jobs are flowing.
- **Expected behavior:** The Deployment replaces the pod; the broker rebalances after the session timeout; queued messages are redelivered to survivors and everything eventually finishes. Known gap: jobs already dispatched to the dead worker can strand in PROCESSING until the milestone-2 sweeper exists.
- **Actual behavior:** All 20 jobs reached a terminal state (20 SUCCESS, 0 FAILED) after the rebalance; nothing stranded, nothing lost. 0 jobs ran more than once.
- **Recovery time:** deployment ready 10.7s after the kill; job states settled in 10.8s (deadline 4m)
- **Data loss:** no (0 lost)
- **Duplicate execution:** 0 jobs ran more than once
- **Final state:** worker deployment healthy at 2/2 replicas

### KillMultipleWorkers

- **Failure:** SIGKILL to 2 of 3 worker pods (force delete, grace 0) while 30 send_email jobs are flowing.
- **Expected behavior:** The surviving worker keeps consuming; the Deployment replaces both pods; everything still queued is drained by the survivor. Known gap (same as KillWorkerForced): jobs dispatched to the dead workers can strand until the milestone-2 sweeper exists.
- **Actual behavior:** The surviving worker drained everything: 30/30 SUCCESS, 0 FAILED, nothing stranded, nothing lost. 0 jobs ran more than once.
- **Recovery time:** deployment back at full strength 11.2s after the kills; job states settled in 11.5s (deadline 4m)
- **Data loss:** no (0 lost)
- **Duplicate execution:** 0 jobs ran more than once
- **Final state:** worker deployment healthy (replica count restored in cleanup)

### RestartBroker

- **Failure:** Full broker restart: kubectl delete pod broker-0 while resize_image jobs are flowing.
- **Expected behavior:** Creates fail loudly with 503 broker_produce_failed while the broker is down (rows marked FAILED, never silently lost); consumers reconnect with backoff; once broker-0 is back the queue resumes and every committed message survives — per-partition high watermarks never move backwards.
- **Actual behavior:** broker-0 restarted cleanly. 3 creates were rejected with 503 during the outage (documented fail-loud path; those rows exist marked FAILED, keys released). All 13 accepted jobs reached SUCCESS after the restart and a fresh job flowed end to end.
- **Recovery time:** broker-0 Ready + ops API answering 17.1s after the kill; accepted jobs settled in 18.6s
- **Data loss:** no — 0 lost; high watermarks only moved forward (pre sum=42020, post sum=42031)
- **Duplicate execution:** 0 jobs ran more than once
- **Final state:** broker statefulset 1/1 Ready; queue flowing; watermarks intact

### RestartRedis

- **Failure:** Redis pod deleted (SIGTERM) while traffic is flowing.
- **Expected behavior:** Degraded but alive: auth keeps validating tokens (fail-open denylist), job CRUD keeps working off Postgres, /ready reports 503 naming redis, job events are dropped (pub/sub has no history). Full recovery once the pod is back.
- **Actual behavior:** sampled 4 times during the outage: authenticated reads kept answering 200=true, /ready went 503=true, any request failed=false. After the pod came back, job creation and reads worked immediately. Presence flapped and job events published mid-window are gone — both by design.
- **Recovery time:** 11.6s from kill to redis Ready (sampled the API every 700ms during the window)
- **Data loss:** no — Postgres is the source of truth; redis holds only ephemeral state. Job status events published during the window were dropped by design (pub/sub has no history).
- **Duplicate execution:** n/a (infra restart; no duplicate execution measured)
- **Final state:** redis 1/1 Ready; jobs/auth/gateway fully functional

### RestartPostgres

- **Failure:** Postgres pod deleted (SIGTERM) right after 4 jobs were created and completed.
- **Expected behavior:** During the window: logins, reads and creates fail — gateway returns 503 and its circuit breaker opens (fail-fast after 5 consecutive upstream failures). After postgres is back the breaker closes and the API recovers. No phantom job loss: jobs created before the restart still exist afterwards.
- **Actual behavior:** sampled 8 times: 5xx responses=8 (code histogram map[500:7], transport errors=1), occasional 200 mid-window=false, mid-outage create rejected=true. Failure was loud (5xx, never silent), reads recovered 12.2s after the kill; all 4 pre-jobs still SUCCESS; a fresh job flowed end to end. NOTE: docs say the gateway breaker opens and fails fast with 503 circuit_open; observed plain 500s instead (breaker never visibly tripped — upstream errors may not count as transport failures).
- **Recovery time:** 12.2s from kill to first successful API read; sampled every 1s during the outage
- **Data loss:** no — 4/4 pre-created jobs intact and still SUCCESS after the restart
- **Duplicate execution:** n/a (infra restart; no duplicate execution measured)
- **Final state:** postgres 1/1 Ready; jobs API fully functional

## How to re-run

```bash
RAVEN_CHAOS=1 go test -tags=chaos ./tests/chaos/ -v -timeout 20m
```

Double-gated on purpose (build tag `chaos` + env `RAVEN_CHAOS=1`): these tests kill
pods. Every scenario restores replica counts and waits for readiness in
cleanup, so the suite is safe to run repeatedly. Results are rewritten on
every run.
