# alosmap benchmarks

Reproducible, head-to-head benchmarks so you can measure `alosmap` against the
other Go concurrent maps **on your own hardware** instead of trusting numbers
from the main README. Every benchmark in the root README's comparison tables
lives here.

This is a **separate Go module** (`github.com/guno1928/alosmap/benchmarks`) with
its own `go.mod`. That keeps the competitor dependencies (xsync, cmap) out of the
library itself — importing `alosmap` pulls in **none** of them. A `replace`
directive points the module at the parent so you always benchmark the local
checkout.

## Maps compared

| Label in output | Implementation |
|---|---|
| `alosmap_any`   | `alosmap.Map` (the `any`-valued map) |
| `alosmap_typed` | `alosmap.TypedMap[string,int64]` (inline typed values) |
| `sync.Map`      | standard library `sync.Map` |
| `map+RWMutex`   | `map[string]int64` guarded by `sync.RWMutex` |
| `xsync`         | [`puzpuzpuz/xsync/v3`](https://github.com/puzpuzpuz/xsync) `MapOf` |
| `cmap`          | [`orcaman/concurrent-map/v2`](https://github.com/orcaman/concurrent-map) |

All cross-map benchmarks use `string` keys and `int64` values, 8 192 keys, and
run parallel across `GOMAXPROCS` goroutines.

## Quick start

Run the full suite (works the same on Windows, macOS, and Linux — `go test`
already prints your CPU, OS, and Go version in the header):

```bash
go test -run '^$' -bench . -benchmem -benchtime=150ms -count=3
```

Run a subset, or change the budget, with the standard `go test` flags:

```bash
go test -run '^$' -bench BenchmarkX_ParallelRead -benchmem -benchtime=500ms -count=5
go test -run '^$' -bench BenchmarkX_              -benchmem -benchtime=150ms   # only the cross-map suite
```

Want a saved log to compare later? Just redirect:

```bash
go test -run '^$' -bench . -benchmem -benchtime=150ms -count=3 | tee results/bench.txt
```

## What each benchmark covers

`BenchmarkX_*` — cross-map comparisons (each map is a labeled sub-benchmark):

| Benchmark | Workload |
|---|---|
| `BenchmarkX_Insert`            | single-goroutine fill of 8 192 fresh keys (incl. `*_prealloc`) |
| `BenchmarkX_ParallelRead`      | all-read, parallel |
| `BenchmarkX_ParallelStore`     | all-write, parallel |
| `BenchmarkX_Mixed90Read`       | 90% read / 10% write, parallel |
| `BenchmarkX_Mixed50Read`       | 50% read / 50% write, parallel |
| `BenchmarkX_Delete`            | 50% delete / 50% write, parallel |
| `BenchmarkX_Range`             | full iteration of 8 192 entries |
| `BenchmarkX_RangeWhileWriting` | iteration with a concurrent writer |
| `BenchmarkX_HotKeyRead`        | every goroutine reads one shared key |

`BenchmarkAlos_*` — alosmap-only characteristics:

| Benchmark | Shows |
|---|---|
| `BenchmarkAlos_AnyVsTyped`      | `any` map vs `TypedMap` store/load |
| `BenchmarkAlos_TTL`             | `StoreWithTTL` / `StoreWithHits` / `StoreWithTTLAndHits` |
| `BenchmarkAlos_ShardScaling`    | parallel read at 1 → 1024 shards |
| `BenchmarkAlos_CapacityScaling` | parallel read at 64 → 262 144 entries |
| `BenchmarkAlos_LoadedPointer`   | `atomic.Int64.Add` / `atomic.Bool.Store` / `atomic.Value.Store` / `StringSet` via a loaded pointer |

## Reading the results

`go test` reports `ns/op` (lower is faster), `B/op`, and `allocs/op`. Within one
sub-benchmark group the maps are directly comparable because they share the same
keys, value type, and parallelism. Absolute numbers depend on your CPU, core
count, and Go version — that is the point: run it yourself and compare.

For statistically clean comparisons, feed the saved log to
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
go install golang.org/x/perf/cmd/benchstat@latest
benchstat results/bench.txt
```
