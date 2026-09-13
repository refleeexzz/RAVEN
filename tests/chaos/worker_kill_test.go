//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestKillWorkerGraceful: SIGTERM one worker pod while 20 resize_image jobs
// are flowing. The worker must drain, everything must finish, the fence must
// keep redeliveries from double-executing.
func TestKillWorkerGraceful(t *testing.T) {
	k := newKit(t)
	res := scenarioResult{
		Name:    "KillWorkerGraceful",
		Failure: "SIGTERM to one worker pod (kubectl delete pod, default grace period) while 20 resize_image jobs are flowing.",
		Expected: "The worker stops fetching, drains in-flight jobs inside its 15s window and exits; the Deployment " +
			"replaces the pod; all 20 jobs reach SUCCESS; no data loss; redeliveries are deduped by the fence; " +
			"no job row ever shows attempts > max_attempts.",
	}
	defer func() { results.add(res) }()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	orig, err := k.deployReplicas(ctx, "worker")
	if err != nil {
		t.Fatalf("read worker replicas: %v", err)
	}
	k.restoreReplicas("worker", orig)

	pods, err := k.runningPods(ctx, "worker")
	if err != nil || len(pods) == 0 {
		t.Fatalf("list worker pods: %v (pods=%v)", err, pods)
	}
	victim := pods[0]
	t.Logf("worker replicas=%d, SIGTERM victim=%s", orig, victim)

	b := k.startBurst(20, "resize_image", func(i int) map[string]any {
		return map[string]any{"image": fmt.Sprintf("graceful-%d.png", i)}
	}, 25*time.Millisecond)
	if !b.waitCreated(10, 30*time.Second) {
		t.Fatalf("burst stalled before the kill: %d/20 created", b.created())
	}

	killAt := time.Now()
	if err := k.deletePod(ctx, victim, false); err != nil {
		t.Fatalf("SIGTERM delete %s: %v", victim, err)
	}
	t.Logf("SIGTERM sent to %s", victim)

	readyStart := time.Now()
	if err := k.rolloutWait(ctx, "deployment/worker", 3*time.Minute); err != nil {
		res.FinalState = "FAIL: worker deployment did not recover"
		res.Verdict = "FAIL"
		t.Fatalf("worker deployment did not recover after SIGTERM: %v", err)
	}
	deployReadyIn := time.Since(readyStart)

	<-b.done
	ids, failures := b.snapshot()
	if len(failures) > 0 {
		t.Logf("burst create failures: %s", strings.Join(failures, "; "))
	}
	if len(ids) != 20 {
		res.Verdict = "FAIL"
		res.FinalState = fmt.Sprintf("FAIL: only %d/20 jobs were created", len(ids))
		t.Fatalf("expected 20 created jobs, got %d", len(ids))
	}

	final := k.waitTerminal(ids, 3*time.Minute)
	settledIn := time.Since(killAt)
	stats := analyze(ids, final)
	t.Logf("final stats: %+v", stats)

	res.Recovery = fmt.Sprintf("%s until all jobs terminal (replacement pod ready %s after the kill)",
		settledIn.Round(100*time.Millisecond), deployReadyIn.Round(100*time.Millisecond))
	res.Duplicates = dupSummary(stats)
	res.DataLoss = lossSummary(stats)

	if stats.lost > 0 || stats.success != 20 || len(stats.overAttempted) > 0 {
		res.Verdict = "FAIL"
		res.Actual = fmt.Sprintf("%d/20 SUCCESS, %d FAILED, %d lost, %d stranded (%s), %d pending",
			stats.success, stats.failed, stats.lost, len(stats.stranded), strings.Join(stats.stranded, ", "), stats.pending)
		res.FinalState = "FAIL: jobs missing, not all SUCCESS, or attempts overflowed max_attempts"
		t.Errorf("graceful kill: success=%d/20 failed=%d lost=%d stranded=%v overAttempted=%v",
			stats.success, stats.failed, stats.lost, stats.stranded, stats.overAttempted)
		return
	}
	res.Verdict = "PASS"
	res.Actual = fmt.Sprintf("All 20 jobs reached SUCCESS. The pod drained and the Deployment replaced it; "+
		"the burst kept flowing the whole time. %s.", dupSummary(stats))
	res.FinalState = fmt.Sprintf("worker deployment healthy at %d/%d replicas; 20/20 jobs SUCCESS", orig, orig)
}

// TestKillWorkerForced: SIGKILL one worker pod mid-flow. The worker commits
// offsets at dispatch, so hard kills can strand jobs in PROCESSING — the
// sweeper that rescues them is milestone 2 work. We assert the cluster
// recovers and record honestly whatever the jobs do.
func TestKillWorkerForced(t *testing.T) {
	k := newKit(t)
	res := scenarioResult{
		Name:    "KillWorkerForced",
		Failure: "SIGKILL to one worker pod (kubectl delete pod --force --grace-period=0) while 20 resize_image jobs are flowing.",
		Expected: "The Deployment replaces the pod; the broker rebalances after the session timeout; queued messages are " +
			"redelivered to survivors and everything eventually finishes. Known gap: jobs already dispatched to the dead " +
			"worker can strand in PROCESSING until the milestone-2 sweeper exists.",
	}
	defer func() { results.add(res) }()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	orig, err := k.deployReplicas(ctx, "worker")
	if err != nil {
		t.Fatalf("read worker replicas: %v", err)
	}
	k.restoreReplicas("worker", orig)

	pods, err := k.runningPods(ctx, "worker")
	if err != nil || len(pods) == 0 {
		t.Fatalf("list worker pods: %v (pods=%v)", err, pods)
	}
	victim := pods[0]
	t.Logf("worker replicas=%d, SIGKILL victim=%s", orig, victim)

	b := k.startBurst(20, "resize_image", func(i int) map[string]any {
		return map[string]any{"image": fmt.Sprintf("forced-%d.png", i)}
	}, 25*time.Millisecond)
	if !b.waitCreated(10, 30*time.Second) {
		t.Fatalf("burst stalled before the kill: %d/20 created", b.created())
	}

	killAt := time.Now()
	if err := k.deletePod(ctx, victim, true); err != nil {
		t.Fatalf("SIGKILL delete %s: %v", victim, err)
	}
	t.Logf("SIGKILL sent to %s", victim)

	readyStart := time.Now()
	if err := k.rolloutWait(ctx, "deployment/worker", 3*time.Minute); err != nil {
		res.Verdict = "FAIL"
		res.FinalState = "FAIL: worker deployment did not recover"
		t.Fatalf("worker deployment did not recover after SIGKILL: %v", err)
	}
	deployReadyIn := time.Since(readyStart)

	<-b.done
	ids, failures := b.snapshot()
	if len(failures) > 0 {
		t.Logf("burst create failures: %s", strings.Join(failures, "; "))
	}
	if len(ids) != 20 {
		res.Verdict = "FAIL"
		res.FinalState = fmt.Sprintf("FAIL: only %d/20 jobs were created", len(ids))
		t.Fatalf("expected 20 created jobs, got %d", len(ids))
	}

	// Generous deadline: rebalance needs the 10s session timeout, then the
	// survivor refetches and runs everything that was still queued.
	final := k.waitTerminal(ids, 4*time.Minute)
	settledIn := time.Since(killAt)
	stats := analyze(ids, final)
	t.Logf("final stats: %+v", stats)

	// Cleanup: nudge stranded jobs back through the requeue endpoint
	// (best-effort) so the database is left tidy.
	if len(stats.stranded) > 0 {
		strandedIDs := make([]string, 0, len(stats.stranded))
		for _, s := range stats.stranded {
			if idx := strings.Index(s, "("); idx > 0 {
				strandedIDs = append(strandedIDs, s[:idx])
			}
		}
		t.Cleanup(func() {
			k.requeueBestEffort(strandedIDs)
		})
	}

	res.Recovery = fmt.Sprintf("deployment ready %s after the kill; job states settled in %s (deadline 4m)",
		deployReadyIn.Round(100*time.Millisecond), settledIn.Round(100*time.Millisecond))
	res.Duplicates = dupSummary(stats)
	res.DataLoss = lossSummary(stats)

	// Hard failures: lost rows, attempts overflow, or the cluster not
	// recovering. Stranded jobs are the documented known gap.
	if stats.lost > 0 || len(stats.overAttempted) > 0 {
		res.Verdict = "FAIL"
		res.Actual = fmt.Sprintf("lost=%d, overAttempted=%v, success=%d/20", stats.lost, stats.overAttempted, stats.success)
		res.FinalState = "FAIL: real data loss or fence violation"
		t.Errorf("forced kill: lost=%d overAttempted=%v", stats.lost, stats.overAttempted)
		return
	}
	if len(stats.stranded) > 0 {
		res.Verdict = "PASS (known gap)"
		res.Actual = fmt.Sprintf("%d/20 jobs reached SUCCESS, but %d stranded in PROCESSING/RETRYING on the dead worker "+
			"(%s). Offsets are committed at dispatch, so the broker never redelivers them — this is the documented "+
			"stranding window, fixed by the milestone-2 sweeper. No row was lost; Postgres has every job.",
			stats.success, len(stats.stranded), strings.Join(stats.stranded, ", "))
		res.FinalState = fmt.Sprintf("cluster healthy at %d/%d workers; %d stranded jobs requeued in cleanup",
			orig, orig, len(stats.stranded))
		t.Logf("KNOWN GAP (milestone 2 sweeper): %d stranded jobs: %v", len(stats.stranded), stats.stranded)
		return
	}
	res.Verdict = "PASS"
	res.Actual = fmt.Sprintf("All 20 jobs reached a terminal state (%d SUCCESS, %d FAILED) after the rebalance; "+
		"nothing stranded, nothing lost. %s.", stats.success, stats.failed, dupSummary(stats))
	res.FinalState = fmt.Sprintf("worker deployment healthy at %d/%d replicas", orig, orig)
}

// requeueBestEffort requeues stranded jobs so they can finish; purely
// cosmetic cleanup, all errors are logged and ignored.
func (k *chaosKit) requeueBestEffort(ids []string) {
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		code, err := k.api.requeueJob(ctx, id)
		cancel()
		if err != nil || code/100 != 2 {
			k.t.Logf("CLEANUP: requeue %s: http=%d err=%v (ignored)", id, code, err)
		} else {
			k.t.Logf("CLEANUP: requeued stranded job %s", id)
		}
	}
	if len(ids) == 0 {
		return
	}
	final := k.waitTerminal(ids, 90*time.Second)
	s := analyze(ids, final)
	k.t.Logf("CLEANUP: after requeue: success=%d failed=%d still-stranded=%v", s.success, s.failed, s.stranded)
}

// TestKillMultipleWorkers: SIGKILL 2 of 3 workers mid-flow; the remaining
// worker must keep draining. Scales up to 3 first if the cluster has fewer,
// and always restores the original replica count.
func TestKillMultipleWorkers(t *testing.T) {
	k := newKit(t)
	res := scenarioResult{
		Name:    "KillMultipleWorkers",
		Failure: "SIGKILL to 2 of 3 worker pods (force delete, grace 0) while 30 send_email jobs are flowing.",
		Expected: "The surviving worker keeps consuming; the Deployment replaces both pods; everything still queued is " +
			"drained by the survivor. Known gap (same as KillWorkerForced): jobs dispatched to the dead workers can " +
			"strand until the milestone-2 sweeper exists.",
	}
	defer func() { results.add(res) }()

	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()

	orig, err := k.deployReplicas(ctx, "worker")
	if err != nil {
		t.Fatalf("read worker replicas: %v", err)
	}
	k.restoreReplicas("worker", orig)
	if orig < 3 {
		t.Logf("worker replicas=%d < 3, scaling up for this scenario", orig)
		if err := k.scale(ctx, "worker", 3); err != nil {
			t.Fatalf("scale worker to 3: %v", err)
		}
		if err := k.rolloutWait(ctx, "deployment/worker", 3*time.Minute); err != nil {
			t.Fatalf("worker scale-up did not become ready: %v", err)
		}
	}

	pods, err := k.runningPods(ctx, "worker")
	if err != nil || len(pods) < 3 {
		t.Fatalf("need 3 running workers, got %v (err=%v)", pods, err)
	}
	victims := pods[:2]
	t.Logf("SIGKILL victims=%v", victims)

	// send_email takes 200-800ms, so more jobs are genuinely in flight.
	b := k.startBurst(30, "send_email", func(i int) map[string]any {
		return map[string]any{"to": "chaos@raven.dev", "subject": fmt.Sprintf("multi-%d", i), "body": "chaos"}
	}, 30*time.Millisecond)
	if !b.waitCreated(15, 45*time.Second) {
		t.Fatalf("burst stalled before the kill: %d/30 created", b.created())
	}

	killAt := time.Now()
	for _, v := range victims {
		if err := k.deletePod(ctx, v, true); err != nil {
			t.Fatalf("SIGKILL delete %s: %v", v, err)
		}
	}
	t.Logf("SIGKILL sent to %v", victims)

	readyStart := time.Now()
	if err := k.rolloutWait(ctx, "deployment/worker", 3*time.Minute); err != nil {
		res.Verdict = "FAIL"
		res.FinalState = "FAIL: worker deployment did not recover"
		t.Fatalf("worker deployment did not recover after killing 2 pods: %v", err)
	}
	deployReadyIn := time.Since(readyStart)

	<-b.done
	ids, failures := b.snapshot()
	if len(failures) > 0 {
		t.Logf("burst create failures: %s", strings.Join(failures, "; "))
	}
	if len(ids) != 30 {
		res.Verdict = "FAIL"
		res.FinalState = fmt.Sprintf("FAIL: only %d/30 jobs were created", len(ids))
		t.Fatalf("expected 30 created jobs, got %d", len(ids))
	}

	final := k.waitTerminal(ids, 4*time.Minute)
	settledIn := time.Since(killAt)
	stats := analyze(ids, final)
	t.Logf("final stats: %+v", stats)

	if len(stats.stranded) > 0 {
		strandedIDs := make([]string, 0, len(stats.stranded))
		for _, s := range stats.stranded {
			if idx := strings.Index(s, "("); idx > 0 {
				strandedIDs = append(strandedIDs, s[:idx])
			}
		}
		t.Cleanup(func() {
			k.requeueBestEffort(strandedIDs)
		})
	}

	res.Recovery = fmt.Sprintf("deployment back at full strength %s after the kills; job states settled in %s (deadline 4m)",
		deployReadyIn.Round(100*time.Millisecond), settledIn.Round(100*time.Millisecond))
	res.Duplicates = dupSummary(stats)
	res.DataLoss = lossSummary(stats)

	switch {
	case stats.lost > 0 || len(stats.overAttempted) > 0:
		res.Verdict = "FAIL"
		res.Actual = fmt.Sprintf("lost=%d, overAttempted=%v, success=%d/30", stats.lost, stats.overAttempted, stats.success)
		res.FinalState = "FAIL: real data loss or fence violation"
		t.Errorf("multi kill: lost=%d overAttempted=%v", stats.lost, stats.overAttempted)
	case stats.success == 0:
		res.Verdict = "FAIL"
		res.Actual = "no job completed after the kills — the surviving worker made no progress"
		res.FinalState = "FAIL: queue stopped draining"
		t.Errorf("multi kill: surviving worker made no progress")
	case len(stats.stranded) > 0:
		res.Verdict = "PASS (known gap)"
		res.Actual = fmt.Sprintf("The surviving worker kept draining: %d/30 jobs reached SUCCESS. %d jobs stranded on the "+
			"dead workers (%s) — same documented stranding window as KillWorkerForced, fixed by the milestone-2 sweeper.",
			stats.success, len(stats.stranded), strings.Join(stats.stranded, ", "))
		res.FinalState = fmt.Sprintf("cluster healthy; %d stranded jobs requeued in cleanup", len(stats.stranded))
		t.Logf("KNOWN GAP (milestone 2 sweeper): %d stranded jobs: %v", len(stats.stranded), stats.stranded)
	default:
		res.Verdict = "PASS"
		res.Actual = fmt.Sprintf("The surviving worker drained everything: %d/30 SUCCESS, %d FAILED, nothing stranded, "+
			"nothing lost. %s.", stats.success, stats.failed, dupSummary(stats))
		res.FinalState = "worker deployment healthy (replica count restored in cleanup)"
	}
}
