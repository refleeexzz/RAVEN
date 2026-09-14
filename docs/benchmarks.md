# RAVEN Benchmarks

Reproducible benchmark suite for the broker (roadmap P2). Two entry points:

- **Go benchmarks** — throughput + allocations (`-benchmem`) per sub-benchmark:
  `go test -run=^$ -bench=. -benchmem ./internal/broker/ ./benchmarks/`
- **Scenario runner** — whole-scenario throughput + P50/P95/P99 latency:
  `go run ./benchmarks/cmd/benchrun -suite=all` (`produce` / `durability` / `consume`)

Both run a **real in-process broker over loopback TCP** (fresh temp data dir
per scenario), so numbers cross the full production path: frame codec →
dispatch → per-partition writer → segment append → (optional) fsync →
fetch → group commit.

## Environment of this run

| | |
|---|---|
| CPU | AMD Ryzen 5 5600X (6 cores / 12 threads) |
| OS / arch | Windows amd64 |
| Go | go1.27.1 (GOMAXPROCS=12) |
| Date | 2026-09-14 |
| Clock | QueryPerformanceCounter (10 MHz, 100 ns quantum) — `time.Now()` on this Windows build ticks at ~0.5 ms, which would quantize sub-ms latency percentiles to zero; `benchmarks/hires_windows.go` uses QPC instead (fallback: `time.Since` elsewhere) |

## Results (this machine, trimmed laptop matrix)

Producer matrix — fsync=periodic (production default: 100 ms / 256 records),
measured with the scenario runner:

| Scenario | Messages | Msg/s | P50 (µs) | P95 (µs) | P99 (µs) | CPU | ~Alloc B/op |
|----------|----------|-------|----------|----------|----------|-----|-------------|
| 1 producer × 128 B | 20,000 | 8,829 | 103 | 161 | 275 | n/a | 5,875 |
| 10 producers × 128 B | 40,000 | 55,168 | 111 | 559 | 1,337 | n/a | 5,855 |
| 100 producers × 128 B | 60,000 | 65,357 | 846 | 4,437 | 6,153 | n/a | 5,901 |
| 1 producer × 1 KB | 20,000 | 8,488 | 106 | 176 | 316 | n/a | 9,735 |
| 10 producers × 1 KB | 40,000 | 46,627 | 124 | 731 | 1,397 | n/a | 9,729 |
| 100 producers × 1 KB | 60,000 | 60,276 | 1,028 | 4,664 | 6,761 | n/a | 9,764 |
| 1 producer × 10 KB | 10,000 | 6,230 | 135 | 255 | 468 | n/a | 59,090 |
| 10 producers × 10 KB | 20,000 | 17,923 | 468 | 1,232 | 2,681 | n/a | 59,227 |
| 100 producers × 10 KB | 30,000 | 32,852 | 2,661 | 7,212 | 9,649 | n/a | 59,175 |
| 1 producer × 100 KB | 2,000 | 709 | 268 | 12,791 | 15,677 | n/a | 538,396 |
| 10 producers × 100 KB | 4,000 | 5,806 | 1,101 | 4,398 | 13,508 | n/a | 538,316 |
| 100 producers × 100 KB | 6,000 | 8,518 | 8,421 | 36,580 | 43,203 | n/a | 538,448 |

Durability — 1 KB messages, 1 producer:

| Scenario | Messages | Msg/s | P50 (µs) | P95 (µs) | P99 (µs) | CPU | ~Alloc B/op |
|----------|----------|-------|----------|----------|----------|-----|-------------|
| fsync=always | 2,000 | 1,663 | 557 | 1,340 | 2,411 | n/a | 9,779 |
| fsync=periodic | 20,000 | 8,571 | 105 | 167 | 285 | n/a | 9,737 |
| fsync=disabled | 20,000 | 9,001 | 104 | 162 | 234 | n/a | 9,735 |

Consumers — 1 KB messages, topic pre-filled (producers benchmarked separately),
consumer group with one partition per member:

| Scenario | Messages delivered | Drain time | Msg/s | P50 | P95 | P99 | CPU |
|----------|--------------------|-----------|-------|-----|-----|-----|-----|
| 1 consumer | 20,000 | 0.38 s | 53,005 | n/a | n/a | n/a | n/a |
| 10 consumers | 30,000 | 0.08 s | 379,803 | n/a | n/a | n/a | n/a |
| 100 consumers | 30,900 | 0.09 s | 354,932 | n/a | n/a | n/a | n/a |

## Honest limitations

- **CPU: n/a.** Per-scenario CPU% is not measurable from inside the
  benchmark process in a portable way; the column is kept (the roadmap
  asks for it) and filled honestly. Use an external profiler
  (`perfmon`, `pprof` under load) for CPU.
- **Memory: approximation.** `~Alloc B/op` is the process-wide heap
  allocation delta divided by messages — it includes background broker
  work. The precise per-op allocation numbers come from
  `go test -benchmem` (`BenchmarkProduce` in `internal/broker` and
  `./benchmarks/`).
- **Consumer percentiles: n/a.** Consumption is batch-driven (fetch up to
  500 records, then per-message handler), so per-message latency is not
  meaningful; delivery throughput is what the runner measures.
- **100-consumer row delivered 30,900 of 30,000 pre-filled**: delivery is
  at-least-once, and in-flight batches redelivered during the group
  shutdown rebalance are counted. Throughput = deliveries / drain time.
- **Trimmed matrix.** The roadmap's full matrix would take hours on a
  laptop; message counts scale down for large payloads (e.g. 100 KB ×
  100 producers = 6,000 messages, not 60,000). Sizes, producer/consumer
  counts and fsync modes themselves are untrimmed. Scenario definitions:
  `benchmarks.ProduceScenarios` / `DurabilityScenarios` /
  `ConsumeScenarios` in `benchmarks/run.go`.
- Loopback TCP, single NVMe machine, in-process broker: numbers are a
  relative baseline for regression checks, not a production SLA.

## What the numbers say

- Single-producer throughput is latency-bound (~100 µs loopback RTT):
  ~8.8k msg/s at any small size. Parallelism scales ~7× at 10 producers.
- 100 producers saturate the partition writers: throughput flattens
  (~60–65k msg/s small, ~8.5k msg/s at 100 KB ≈ 850 MB/s on disk) and
  P99 climbs to ms — classic backpressure at the bounded writer queues.
- fsync=always costs ~5× throughput (1.7k vs 8.6k msg/s): one fsync per
  batch on the write path. periodic ≈ disabled, confirming the 100 ms /
  256-record policy hides fsync cost at this scale.
- Consumption is far cheaper than production (fetch batches): a single
  consumer drains 53k msg/s; group delivery peaks ~355–380k msg/s.

## Reproduce

```bash
export PATH="/c/Program Files/Go/bin:$PATH"   # Windows Git Bash

# full scenario runner (~2 min), prints the tables above:
go run ./benchmarks/cmd/benchrun -suite=all

# Go benchmarks (throughput + allocs):
go test -run=^$ -bench=. -benchmem ./internal/broker/ ./benchmarks/

# single scenario family:
go test -run=^$ -bench='BenchmarkProduce/1KB' -benchmem ./benchmarks/
```

Fsync modes map to broker env vars (`docs/contracts/ports-and-env.md`):
`always` = `BROKER_FSYNC_RECORDS=1`; `periodic` = `BROKER_FSYNC_MS=100`,
`BROKER_FSYNC_RECORDS=256`; `disabled` = both effectively infinite.
