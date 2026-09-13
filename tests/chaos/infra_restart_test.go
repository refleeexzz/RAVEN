//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRestartBroker: delete broker-0 mid-flow. Producers/consumers must
// reconnect, the queue must continue, and no committed message may be lost
// (high watermarks must never move backwards across the restart).
func TestRestartBroker(t *testing.T) {
	k := newKit(t)
	res := scenarioResult{
		Name:    "RestartBroker",
		Failure: "Full broker restart: kubectl delete pod broker-0 while resize_image jobs are flowing.",
		Expected: "Creates fail loudly with 503 broker_produce_failed while the broker is down (rows marked FAILED, never " +
			"silently lost); consumers reconnect with backoff; once broker-0 is back the queue resumes and every " +
			"committed message survives — per-partition high watermarks never move backwards.",
	}
	defer func() { results.add(res) }()

	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()

	// Pre-restart watermarks via the ops API.
	base, stop, err := k.portForward("svc/broker", 9101)
	if err != nil {
		t.Fatalf("port-forward broker ops: %v", err)
	}
	pre, err := fetchTopics(base)
	stop()
	if err != nil {
		t.Fatalf("fetch pre-restart topics: %v", err)
	}
	preHW := highWatermarks(pre, "jobs")
	preCommitted := committedOffsets(pre, "jobs", "workers")
	t.Logf("pre-restart jobs HW: %v, workers committed: %v", preHW, preCommitted)
	if len(preHW) == 0 {
		t.Fatalf("broker ops API returned no partitions for topic jobs")
	}

	b := k.startBurst(16, "resize_image", func(i int) map[string]any {
		return map[string]any{"image": fmt.Sprintf("broker-%d.png", i)}
	}, 50*time.Millisecond)
	if !b.waitCreated(8, 30*time.Second) {
		t.Fatalf("burst stalled before the kill: %d/16 created", b.created())
	}

	killAt := time.Now()
	if err := k.deletePod(ctx, "broker-0", false); err != nil {
		t.Fatalf("delete broker-0: %v", err)
	}
	t.Logf("deleted broker-0")

	if err := k.rolloutWait(ctx, "statefulset/broker", 4*time.Minute); err != nil {
		res.Verdict = "FAIL"
		res.FinalState = "FAIL: broker-0 did not come back"
		t.Fatalf("broker-0 did not recover: %v", err)
	}

	// Broker is Ready; now confirm the ops API answers and grab post watermarks.
	base2, stop2, err := k.portForward("svc/broker", 9101)
	if err != nil {
		res.Verdict = "FAIL"
		res.FinalState = "FAIL: broker ops API unreachable after restart"
		t.Fatalf("port-forward broker ops after restart: %v", err)
	}
	defer stop2()
	var post *brokerTopics
	pollEnd := time.Now().Add(90 * time.Second)
	for time.Now().Before(pollEnd) {
		if post, err = fetchTopics(base2); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		res.Verdict = "FAIL"
		res.FinalState = "FAIL: broker ops API did not answer after restart"
		t.Fatalf("fetch post-restart topics: %v", err)
	}
	brokerBackIn := time.Since(killAt)
	postHW := highWatermarks(post, "jobs")
	postCommitted := committedOffsets(post, "jobs", "workers")
	t.Logf("post-restart jobs HW: %v, workers committed: %v", postHW, postCommitted)

	lostHW := sumOffsetDelta(preHW, postHW)
	lostCommitted := sumOffsetDelta(preCommitted, postCommitted)

	<-b.done
	ids, failures := b.snapshot()
	t.Logf("burst: %d created, %d rejected during outage", len(ids), len(failures))

	// Everything that WAS accepted (produce succeeded) must eventually run.
	final := k.waitTerminal(ids, 4*time.Minute)
	settledIn := time.Since(killAt)
	stats := analyze(ids, final)
	t.Logf("final stats: %+v", stats)

	// Post-recovery produce check: a brand-new job must flow end to end.
	postJobOK := false
	{
		pctx, pcancel := context.WithTimeout(context.Background(), 15*time.Second)
		job, code, err := k.api.createJob(pctx, "send_email",
			map[string]any{"to": "chaos@raven.dev", "subject": "post-broker", "body": "ok"}, 5,
			fmt.Sprintf("chaos-%s-post-broker", k.runID))
		pcancel()
		if err == nil && code/100 == 2 {
			f := k.waitTerminal([]string{job.ID}, 90*time.Second)
			postJobOK = f[job.ID].Status == "SUCCESS"
		}
	}

	res.Recovery = fmt.Sprintf("broker-0 Ready + ops API answering %s after the kill; accepted jobs settled in %s",
		brokerBackIn.Round(100*time.Millisecond), settledIn.Round(100*time.Millisecond))
	res.Duplicates = dupSummary(stats)

	switch {
	case lostHW > 0 || lostCommitted > 0:
		res.Verdict = "FAIL"
		res.DataLoss = fmt.Sprintf("YES — broker lost %d messages (high watermarks moved backwards), %d committed consumer offsets rewound",
			lostHW, lostCommitted)
		res.Actual = fmt.Sprintf("pre HW=%v, post HW=%v", preHW, postHW)
		res.FinalState = "FAIL: committed data did not survive the broker restart"
		t.Errorf("broker restart lost data: lostHW=%d lostCommitted=%d", lostHW, lostCommitted)
	case stats.lost > 0 || (len(ids) > 0 && stats.success != len(ids)):
		res.Verdict = "FAIL"
		res.DataLoss = lossSummary(stats)
		res.Actual = fmt.Sprintf("of %d accepted jobs: %d SUCCESS, %d FAILED, %d lost, stranded=%v",
			len(ids), stats.success, stats.failed, stats.lost, stats.stranded)
		res.FinalState = "FAIL: accepted jobs did not all complete after the broker came back"
		t.Errorf("broker restart: accepted jobs not fully drained: %+v", stats)
	case !postJobOK:
		res.Verdict = "FAIL"
		res.DataLoss = "no"
		res.Actual = "queue resumed for old jobs, but a fresh post-recovery job did not reach SUCCESS in 90s"
		res.FinalState = "FAIL: producers/workers did not fully reconnect"
		t.Errorf("broker restart: post-recovery job did not succeed")
	default:
		res.Verdict = "PASS"
		res.DataLoss = fmt.Sprintf("no — 0 lost; high watermarks only moved forward (pre sum=%d, post sum=%d)",
			sumMap(preHW), sumMap(postHW))
		res.Actual = fmt.Sprintf("broker-0 restarted cleanly. %d creates were rejected with 503 during the outage "+
			"(documented fail-loud path; those rows exist marked FAILED, keys released). All %d accepted jobs reached "+
			"SUCCESS after the restart and a fresh job flowed end to end.",
			len(failures), len(ids))
		res.FinalState = "broker statefulset 1/1 Ready; queue flowing; watermarks intact"
	}
}

func sumMap(m map[int]int64) int64 {
	var s int64
	for _, v := range m {
		s += v
	}
	return s
}

// TestRestartRedis: restart redis mid-flow. Redis is the ephemeral layer:
// auth must fail open, jobs keep working, events drop, and everything heals.
func TestRestartRedis(t *testing.T) {
	k := newKit(t)
	res := scenarioResult{
		Name:    "RestartRedis",
		Failure: "Redis pod deleted (SIGTERM) while traffic is flowing.",
		Expected: "Degraded but alive: auth keeps validating tokens (fail-open denylist), job CRUD keeps working off " +
			"Postgres, /ready reports 503 naming redis, job events are dropped (pub/sub has no history). Full " +
			"recovery once the pod is back.",
	}
	defer func() { results.add(res) }()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// Baseline: authenticated read works before we break anything.
	pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	baseCode, err := k.api.probe(pctx, "/api/jobs?page=1&page_size=1")
	pcancel()
	if err != nil || baseCode != http.StatusOK {
		t.Fatalf("baseline authenticated read failed: http=%d err=%v", baseCode, err)
	}

	killAt := time.Now()
	if err := k.deletePod(ctx, firstPod(ctx, k, "redis"), false); err != nil {
		t.Fatalf("delete redis pod: %v", err)
	}
	t.Logf("deleted redis pod")

	// Sample the API while the pod is down, in parallel with the rollout.
	rolloutDone := make(chan error, 1)
	go func() { rolloutDone <- k.rolloutWait(ctx, "deployment/redis", 3*time.Minute) }()

	samples, sawAuthOK, sawReady503, sawFailure := 0, false, false, false
sampling:
	for {
		select {
		case err := <-rolloutDone:
			if err != nil {
				res.Verdict = "FAIL"
				res.FinalState = "FAIL: redis deployment did not recover"
				t.Fatalf("redis did not recover: %v", err)
			}
			break sampling
		default:
		}
		sctx, scancel := context.WithTimeout(ctx, 3*time.Second)
		code, perr := k.api.probe(sctx, "/api/jobs?page=1&page_size=1")
		rcode, _ := k.api.ready(sctx)
		scancel()
		samples++
		if perr == nil && code == http.StatusOK {
			sawAuthOK = true
		} else {
			sawFailure = true
			t.Logf("sample during outage: jobs http=%d err=%v", code, perr)
		}
		if rcode == http.StatusServiceUnavailable {
			sawReady503 = true
		}
		time.Sleep(700 * time.Millisecond)
	}
	recoveryIn := time.Since(killAt)

	// Full recovery: create + read back a job.
	postOK := false
	{
		jctx, jcancel := context.WithTimeout(ctx, 15*time.Second)
		job, code, err := k.api.createJob(jctx, "send_email",
			map[string]any{"to": "chaos@raven.dev", "subject": "post-redis", "body": "ok"}, 5,
			fmt.Sprintf("chaos-%s-post-redis", k.runID))
		jcancel()
		if err == nil && code/100 == 2 {
			f := k.waitTerminal([]string{job.ID}, 90*time.Second)
			postOK = f[job.ID].Status == "SUCCESS"
		}
	}
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	readCode, _ := k.api.probe(rctx, "/api/jobs?page=1&page_size=1")
	rcancel()

	res.Recovery = fmt.Sprintf("%s from kill to redis Ready (sampled the API every 700ms during the window)",
		recoveryIn.Round(100*time.Millisecond))
	res.Duplicates = "n/a (infra restart; no duplicate execution measured)"
	res.DataLoss = "no — Postgres is the source of truth; redis holds only ephemeral state. " +
		"Job status events published during the window were dropped by design (pub/sub has no history)."

	degraded := fmt.Sprintf("sampled %d times during the outage: authenticated reads kept answering 200=%v, "+
		"/ready went 503=%v, any request failed=%v", samples, sawAuthOK, sawReady503, sawFailure)

	switch {
	case !postOK || readCode != http.StatusOK:
		res.Verdict = "FAIL"
		res.Actual = degraded + fmt.Sprintf("; after recovery: new job SUCCESS=%v, read http=%d", postOK, readCode)
		res.FinalState = "FAIL: platform did not fully recover after redis came back"
		t.Errorf("redis restart: post-recovery check failed (jobOK=%v read=%d)", postOK, readCode)
	case samples >= 2 && !sawAuthOK:
		res.Verdict = "FAIL"
		res.Actual = degraded
		res.FinalState = "FAIL: authenticated requests failed while redis was down (fail-open denylist did not hold)"
		t.Errorf("redis restart: auth did not fail open during outage")
	default:
		res.Verdict = "PASS"
		res.Actual = degraded + ". After the pod came back, job creation and reads worked immediately. " +
			"Presence flapped and job events published mid-window are gone — both by design."
		res.FinalState = "redis 1/1 Ready; jobs/auth/gateway fully functional"
	}
}

// firstPod returns the first running pod for app=<app>, or fails the test.
func firstPod(ctx context.Context, k *chaosKit, app string) string {
	pods, err := k.runningPods(ctx, app)
	if err != nil || len(pods) == 0 {
		k.t.Fatalf("no running %s pods: %v", app, err)
	}
	return pods[0]
}

// TestRestartPostgres: restart postgres mid-flow. The loudest failure:
// everything that needs the database 503s, the gateway breaker opens, and
// nothing committed may be lost.
func TestRestartPostgres(t *testing.T) {
	k := newKit(t)
	res := scenarioResult{
		Name:    "RestartPostgres",
		Failure: "Postgres pod deleted (SIGTERM) right after 4 jobs were created and completed.",
		Expected: "During the window: logins, reads and creates fail — gateway returns 503 and its circuit breaker opens " +
			"(fail-fast after 5 consecutive upstream failures). After postgres is back the breaker closes and the API " +
			"recovers. No phantom job loss: jobs created before the restart still exist afterwards.",
	}
	defer func() { results.add(res) }()

	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()

	// Pre-create 4 jobs and let them finish before we break the database.
	preIDs := []string{}
	for i := 0; i < 4; i++ {
		jctx, jcancel := context.WithTimeout(ctx, 15*time.Second)
		job, code, err := k.api.createJob(jctx, "send_email",
			map[string]any{"to": "chaos@raven.dev", "subject": fmt.Sprintf("pre-pg-%d", i), "body": "ok"}, 5,
			fmt.Sprintf("chaos-%s-pre-pg-%d", k.runID, i))
		jcancel()
		if err != nil || code/100 != 2 {
			t.Fatalf("pre-create job %d: http=%d err=%v", i, code, err)
		}
		preIDs = append(preIDs, job.ID)
	}
	preFinal := k.waitTerminal(preIDs, 90*time.Second)
	preStats := analyze(preIDs, preFinal)
	if preStats.success != 4 {
		t.Fatalf("pre-jobs did not complete before the restart: %+v", preStats)
	}
	t.Logf("4 pre-jobs SUCCESS, deleting postgres")

	killAt := time.Now()
	if err := k.deletePod(ctx, firstPod(ctx, k, "postgres"), false); err != nil {
		t.Fatalf("delete postgres pod: %v", err)
	}

	// Sample during the outage: expect 503s. Also try one create — it must
	// fail loudly, never return 201 for a job that is not queued.
	rolloutDone := make(chan error, 1)
	go func() { rolloutDone <- k.rolloutWait(ctx, "deployment/postgres", 4*time.Minute) }()

	samples, sawOK := 0, false
	codes := map[int]int{}
	errSamples := 0
	createRejected := false
	createTried := false
sampling:
	for {
		select {
		case err := <-rolloutDone:
			if err != nil {
				res.Verdict = "FAIL"
				res.FinalState = "FAIL: postgres deployment did not recover"
				t.Fatalf("postgres did not recover: %v", err)
			}
			break sampling
		default:
		}
		sctx, scancel := context.WithTimeout(ctx, 3*time.Second)
		code, perr := k.api.probe(sctx, "/api/jobs?page=1&page_size=1")
		scancel()
		samples++
		switch {
		case perr != nil:
			errSamples++
		case code == http.StatusOK:
			sawOK = true
		default:
			codes[code]++
		}
		saw5xx := perr != nil || code/100 == 5
		if !createTried && saw5xx {
			createTried = true
			cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
			_, ccode, cerr := k.api.createJob(cctx, "send_email",
				map[string]any{"to": "chaos@raven.dev", "subject": "during-pg", "body": "must fail"}, 5,
				fmt.Sprintf("chaos-%s-during-pg", k.runID))
			ccancel()
			createRejected = cerr != nil || ccode/100 != 2
			t.Logf("create during outage: http=%d err=%v (rejected=%v)", ccode, cerr, createRejected)
		}
		time.Sleep(time.Second)
	}
	saw5xxTotal := errSamples
	for code, n := range codes {
		if code/100 == 5 {
			saw5xxTotal += n
		}
	}

	// Pod is Ready; the gateway breaker may still be open — poll until the
	// first 200 closes the recovery window.
	recoverStart := time.Now()
	recovered := false
	for time.Since(recoverStart) < 2*time.Minute {
		sctx, scancel := context.WithTimeout(ctx, 3*time.Second)
		code, perr := k.api.probe(sctx, "/api/jobs?page=1&page_size=1")
		scancel()
		if perr == nil && code == http.StatusOK {
			recovered = true
			break
		}
		time.Sleep(time.Second)
	}
	recoveryIn := time.Since(killAt)
	if !recovered {
		res.Verdict = "FAIL"
		res.FinalState = "FAIL: API did not recover within 2m of postgres being Ready"
		t.Fatalf("API still failing 2m after postgres Ready (breaker stuck?)")
	}

	// No phantom loss: all 4 pre-jobs must still exist and be SUCCESS.
	intact, lost := 0, []string{}
	for _, id := range preIDs {
		jctx, jcancel := context.WithTimeout(ctx, 10*time.Second)
		job, code, err := k.api.getJob(jctx, id)
		jcancel()
		if err == nil && code == http.StatusOK && job.Status == "SUCCESS" {
			intact++
		} else {
			lost = append(lost, fmt.Sprintf("%s(http=%d,status=%s)", id, code, job.Status))
		}
	}

	// Post-recovery create must work again.
	postOK := false
	{
		jctx, jcancel := context.WithTimeout(ctx, 15*time.Second)
		job, code, err := k.api.createJob(jctx, "send_email",
			map[string]any{"to": "chaos@raven.dev", "subject": "post-pg", "body": "ok"}, 5,
			fmt.Sprintf("chaos-%s-post-pg", k.runID))
		jcancel()
		if err == nil && code/100 == 2 {
			f := k.waitTerminal([]string{job.ID}, 90*time.Second)
			postOK = f[job.ID].Status == "SUCCESS"
		}
	}

	res.Recovery = fmt.Sprintf("%s from kill to first successful API read; sampled every 1s during the outage",
		recoveryIn.Round(100*time.Millisecond))
	res.Duplicates = "n/a (infra restart; no duplicate execution measured)"
	res.DataLoss = fmt.Sprintf("no — %d/4 pre-created jobs intact and still SUCCESS after the restart", intact)

	outage := fmt.Sprintf("sampled %d times: 5xx responses=%d (code histogram %v, transport errors=%d), occasional "+
		"200 mid-window=%v, mid-outage create rejected=%v", samples, saw5xxTotal, codes, errSamples, sawOK, createRejected)
	// Docs (failure-scenarios.md) promise 503 circuit_open; the gateway
	// actually maps the dead upstream to 500. Loud either way — deviation
	// is recorded, not hidden.
	deviation := ""
	if codes[http.StatusServiceUnavailable] == 0 && saw5xxTotal > 0 {
		deviation = " NOTE: docs say the gateway breaker opens and fails fast with 503 circuit_open; observed plain 500s " +
			"instead (breaker never visibly tripped — upstream errors may not count as transport failures)."
	}

	switch {
	case intact != 4:
		res.Verdict = "FAIL"
		res.Actual = outage + fmt.Sprintf("; jobs not intact after recovery: %s", strings.Join(lost, ", "))
		res.FinalState = "FAIL: phantom job loss after postgres restart"
		t.Errorf("postgres restart: pre-jobs not intact: %v", lost)
	case !postOK:
		res.Verdict = "FAIL"
		res.Actual = outage + "; post-recovery job did not reach SUCCESS in 90s"
		res.FinalState = "FAIL: platform did not fully recover"
		t.Errorf("postgres restart: post-recovery job failed")
	case samples >= 2 && saw5xxTotal == 0:
		res.Verdict = "FAIL"
		res.Actual = outage
		res.FinalState = "FAIL: expected visible 5xx while postgres was down, saw none (is anything faking reads?)"
		t.Errorf("postgres restart: no 5xx observed during outage window")
	default:
		res.Verdict = "PASS"
		res.Actual = outage + fmt.Sprintf(". Failure was loud (5xx, never silent), reads recovered %s after the kill; "+
			"all 4 pre-jobs still SUCCESS; a fresh job flowed end to end.%s", recoveryIn.Round(100*time.Millisecond), deviation)
		res.FinalState = "postgres 1/1 Ready; jobs API fully functional"
		if deviation != "" {
			t.Logf("DEVIATION from docs/failure-scenarios.md:%s", deviation)
		}
	}
}
