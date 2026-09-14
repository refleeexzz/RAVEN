// Command benchrun runs the RAVEN broker benchmark scenarios and prints
// the results as a Markdown table (the one docs/benchmarks.md ships).
// Unlike go test -bench it measures whole-scenario latency percentiles.
//
// Usage:
//
//	go run ./benchmarks/cmd/benchrun [-suite=produce|durability|consume|all] [-out results.md]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/refleeexzz/RAVEN/benchmarks"
)

func main() {
	suite := flag.String("suite", "all", "produce | durability | consume | all")
	out := flag.String("out", "", "also write the Markdown table to this file")
	flag.Parse()

	fmt.Printf("benchrun: go=%s %s/%s GOMAXPROCS=%d\n\n",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))

	ctx := context.Background()
	var results []benchmarks.Result
	run := func(name string, fn func() (benchmarks.Result, error)) {
		fmt.Printf("%-34s ... ", name)
		t0 := time.Now()
		r, err := fn()
		if err != nil {
			fmt.Printf("FAILED: %v\n", err)
			return
		}
		fmt.Printf("%.0f msg/s  p50=%.0fus p95=%.0fus p99=%.0fus  (%d msgs in %.1fs, wall %.1fs)\n",
			r.MsgPerSec, r.P50Us, r.P95Us, r.P99Us, r.Messages, r.Duration.Seconds(), time.Since(t0).Seconds())
		results = append(results, r)
	}

	if *suite == "produce" || *suite == "all" {
		for _, sc := range benchmarks.ProduceScenarios() {
			sc := sc
			run(sc.Name, func() (benchmarks.Result, error) {
				return benchmarks.RunProduce(ctx, mustTempDir(), sc)
			})
		}
	}
	if *suite == "durability" || *suite == "all" {
		for _, sc := range benchmarks.DurabilityScenarios() {
			sc := sc
			run(sc.Name, func() (benchmarks.Result, error) {
				return benchmarks.RunProduce(ctx, mustTempDir(), sc)
			})
		}
	}
	if *suite == "consume" || *suite == "all" {
		for _, sc := range benchmarks.ConsumeScenarios() {
			sc := sc
			run(sc.Name, func() (benchmarks.Result, error) {
				return benchmarks.RunConsume(ctx, mustTempDir(), sc)
			})
		}
	}

	table := benchmarks.Markdown(results)
	fmt.Printf("\n%s", table)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(table), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", *out, err)
			os.Exit(1)
		}
		fmt.Printf("table written to %s\n", *out)
	}
}

func mustTempDir() string {
	d, err := os.MkdirTemp("", "raven-bench-*")
	if err != nil {
		panic(err)
	}
	return d
}
