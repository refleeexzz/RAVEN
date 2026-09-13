//go:build chaos

// Package chaos contains RAVEN's destructive, reproducible chaos tests.
//
// These tests kill pods and restart infrastructure against the live local
// k8s cluster (namespace "raven"). They are double-gated on purpose:
//
//	go test -tags=chaos ./tests/chaos/   (build tag)
//	RAVEN_CHAOS=1                         (env opt-in — these tests kill things)
//
// Every scenario restores the cluster to its pre-test replica counts and
// waits for readiness in cleanup, so the suite is safe to run repeatedly.
// TestMain writes the measured results to docs/chaos-results.md.
package chaos

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// resultsDocPath is relative to the package dir (go test sets cwd to it).
var resultsDocPath = filepath.Join("..", "..", "docs", "chaos-results.md")

// sidecarPath keeps results as JSON so scenario runs executed one by one
// (each a separate `go test` process) merge into one document.
var sidecarPath = ".chaos-results.json"

// scenarioResult is one roadmap result block plus a verdict.
type scenarioResult struct {
	Name       string
	Failure    string
	Expected   string
	Actual     string
	Recovery   string
	DataLoss   string
	Duplicates string
	FinalState string
	Verdict    string // PASS / PASS (known gap) / FAIL
}

// recorder accumulates per-scenario results; TestMain renders them.
type recorder struct {
	mu      sync.Mutex
	results []scenarioResult
}

var results = &recorder{}

func (r *recorder) add(res scenarioResult) {
	r.mu.Lock()
	r.results = append(r.results, res)
	r.mu.Unlock()
}

func TestMain(m *testing.M) {
	if os.Getenv("RAVEN_CHAOS") != "1" {
		fmt.Println("chaos suite skipped: destructive tests are double-gated.")
		fmt.Println("opt in with: RAVEN_CHAOS=1 go test -tags=chaos ./tests/chaos/ -v -timeout 20m")
		os.Exit(0)
	}
	start := time.Now()
	results.loadSidecar(sidecarPath)
	code := m.Run()
	if err := results.write(resultsDocPath, time.Since(start)); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: could not write results doc %s: %v\n", resultsDocPath, err)
		if code == 0 {
			code = 1
		}
	} else {
		fmt.Printf("chaos: results written to %s\n", resultsDocPath)
	}
	if err := results.writeSidecar(sidecarPath); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: could not write sidecar %s: %v\n", sidecarPath, err)
	}
	os.Exit(code)
}

// loadSidecar merges results from earlier `go test` processes into the
// recorder (this process's own results take precedence by name at write).
func (r *recorder) loadSidecar(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var prior []scenarioResult
	if err := json.Unmarshal(b, &prior); err != nil {
		return
	}
	r.mu.Lock()
	r.results = append(r.results, prior...)
	r.mu.Unlock()
}

func (r *recorder) writeSidecar(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, err := json.MarshalIndent(r.mergedLocked(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// mergedLocked overlays newer results onto older ones, keyed by name,
// preserving first-seen order. Call with r.mu held.
func (r *recorder) mergedLocked() []scenarioResult {
	idx := map[string]int{}
	var out []scenarioResult
	for _, res := range r.results {
		if i, ok := idx[res.Name]; ok {
			out[i] = res // newer occurrence wins
			continue
		}
		idx[res.Name] = len(out)
		out = append(out, res)
	}
	return out
}

// mdCell keeps a string safe inside a markdown table cell.
func mdCell(s string) string {
	s = strings.ReplaceAll(s, "|", "/")
	s = strings.ReplaceAll(s, "\n", "; ")
	return s
}

func (r *recorder) write(path string, dur time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	merged := r.mergedLocked()

	var b strings.Builder
	b.WriteString("# Chaos test results\n\n")
	fmt.Fprintf(&b, "Last run: %s · target: local k8s cluster, namespace `raven` · last `go test` process: %s\n\n",
		time.Now().Format("2006-01-02 15:04:05 MST"), dur.Round(time.Second))

	b.WriteString(`We broke RAVEN on purpose and wrote down what happened. Each scenario below
kills or restarts a real piece of the platform on the live local cluster, then
measures what the system does about it: what we broke, what we expected, what
actually happened, how long recovery took, and whether any data was lost or
any job ran twice. Everything here is measured, not guessed.

`)

	if len(merged) == 0 {
		b.WriteString("No scenarios ran (the suite was skipped before any test started).\n")
		return os.WriteFile(path, []byte(b.String()), 0o644)
	}

	b.WriteString("## Summary\n\n")
	b.WriteString("| Scenario | Verdict | Recovery time | Data loss | Duplicates |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, res := range merged {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n",
			res.Name, mdCell(res.Verdict), mdCell(res.Recovery), mdCell(res.DataLoss), mdCell(res.Duplicates))
	}

	b.WriteString("\n## Scenarios\n\n")
	for _, res := range merged {
		fmt.Fprintf(&b, "### %s\n\n", res.Name)
		fmt.Fprintf(&b, "- **Failure:** %s\n", res.Failure)
		fmt.Fprintf(&b, "- **Expected behavior:** %s\n", res.Expected)
		fmt.Fprintf(&b, "- **Actual behavior:** %s\n", res.Actual)
		fmt.Fprintf(&b, "- **Recovery time:** %s\n", res.Recovery)
		fmt.Fprintf(&b, "- **Data loss:** %s\n", res.DataLoss)
		fmt.Fprintf(&b, "- **Duplicate execution:** %s\n", res.Duplicates)
		fmt.Fprintf(&b, "- **Final state:** %s\n\n", res.FinalState)
	}

	b.WriteString(`## How to re-run

` + "```bash" + `
RAVEN_CHAOS=1 go test -tags=chaos ./tests/chaos/ -v -timeout 20m
` + "```" + `

Double-gated on purpose (build tag ` + "`chaos`" + ` + env ` + "`RAVEN_CHAOS=1`" + `): these tests kill
pods. Every scenario restores replica counts and waits for readiness in
cleanup, so the suite is safe to run repeatedly. Results are rewritten on
every run.
`)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
