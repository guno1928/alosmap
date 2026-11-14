# alosmap

A high-performance concurrent map for Go with string and int64 keys, lock-free reads,
per-entry TTL, hit-limited entries, and automatic or manual cleanup — plus a
zero-allocation generic `TypedMap[K, V]` and opt-in chunked `Prealloc`.

```go
go get github.com/guno1928/alosmap
```

## Test Suite

`testall.go` holds the large registered case list for the package. `typedmap_testall.go` adds
300 `TypedMap[K, V]` cases (IDs 301–600) covering round-trips across key/value types,
delete/probe-chain integrity, `Range`/`Len`, pointer identity, `Prealloc`, table growth, tombstone
reuse, and concurrent torn-read stress, and `typedmap_exttestall.go` adds 200 more (IDs 601–800)
for the full lifecycle/atomic API — `LoadOrStore`, `Swap`, `CompareAndSwap`/`CompareAndDelete`,
`Has`/`Peek`/`Get`, TTL- and hit-limited stores, `Snapshot`, `RangePar`, `Clear`, `Stats`, and
background cleanup. Run the full registry through the `TestAllCases` harness with:

```bash
go test -run TestAllCases -count=1 -v
```

Run the full repository test suite with:

```bash
go test -count=1 ./...
```

Latest run (Linux, Go 1.26.0, Intel Xeon E3-1270 v6):

- `TestAllCases`: `775/775` cases passed (275 core + 500 `TypedMap` cases)
- Full package suite (148 top-level tests): `ok github.com/guno1928/alosmap`, on amd64
  and on `GOARCH=386`
- Race detector: the **whole package** passes under `go test -race`, and so does a
  public-API stress that drives every exported method of `Map` and `TypedMap`
  concurrently at 2, 4, 8 and 16 CPUs. No source file is selected by a `race` build
  tag, so the detector checks the same code a normal build ships
- Both benchmark modules (`benchmarks/`, `fairbenchmark/`) pass under `-race`
- Memory-integrity runs, all passing:
  - the full suite with `GODEBUG=clobberfree=1` (the garbage collector overwrites freed
    memory, so a value the map lost track of would read back as garbage), with `GOGC=1`
    (near-continuous collection), and with `-gcflags=all=-d=checkptr` (unsafe pointer
    arithmetic checked at run time)
  - the typed map tests under `GODEBUG=gccheckmark=1` and AddressSanitizer (`-asan`),
    and the memory tests under MemorySanitizer (`-msan`)
  - 4 minutes of native fuzzing (`FuzzTypedMem`) that replays random operation
    sequences against a plain Go map as the model: no failures
- tmap's conformance tests: `TypedMap` passes all 8 it is wired into — see
  [tmap's conformance tests](#tmaps-conformance-tests)

## Key Types

All map methods accept a `Key` value. Use `S()` for string keys and `I()` for int64 keys.

```go
store := alosmap.New()
defer store.Close()

store.Store(alosmap.S("name"), "alice")
store.Store(alosmap.I(42), "answer")

name, _ := store.Load(alosmap.S("name"))
answer, _ := store.Load(alosmap.I(42))

fmt.Println(name, answer) // alice answer
```

`S()` and `I()` are zero-allocation value constructors passed on the stack.

## Typed Map — `TypedMap[K, V]`

The `Map` above stores values as `any`, which boxes every value onto the heap. When the
value type is fixed, `TypedMap[K, V]` stores `V` **inline** — no interface boxing, no
per-store allocation, and `Load` returns `V` directly with no type assertion. Reads and
updates of existing keys are lock-free; inserting a new key or deleting one takes a short
per-shard lock. It is a separate type, so the existing `Map`/`New`/`Store`/`Load` API is
untouched.

```go
users := alosmap.NewTyped[string, int64]()
users.Store("u:42", 1000)       // 0 allocations
n, ok := users.Load("u:42")     // n is int64, no assertion
users.Delete("u:42")
```

`TypedMap` mirrors the full `Map` API — beyond the basics above it has `LoadOrStore`,
`Swap`, `CompareAndSwap`/`CompareAndDelete`, `Compute` (atomic read-modify-write),
`Has`/`Peek`/`Get`, TTL- and hit-limited
stores (`StoreWithTTL`, `StoreWithHits`, `StoreWithTTLAndHits`, …), `Snapshot`,
`RangePar`, `Clear`, `CleanupNow`, and `Stats`. Plain `Store`/`Load` stay zero-alloc;
only TTL/hit entries allocate a small metadata word, so the hot path keeps the speeds
below. See [TypedMap Methods](#typedmap-methods) for the full list.

Measured head-to-head, same keys and machine (median of 5, `-benchtime=200ms`;
throughput in brackets):

| Operation | `any` Map | `TypedMap[K,V]` | Speedup |
|---|---|---|---|
| Store (update), 1 goroutine | 43.8 ns (22.8 M/s) | **16.7 ns (59.9 M/s)** | 2.62× |
| Load, 1 goroutine | 22.7 ns (44.0 M/s) | **11.9 ns (83.7 M/s)** | 1.90× |
| Store (update), 8 goroutines | 12.3 ns (81.0 M/s) | **3.89 ns (257 M/s)** | 3.17× |
| Load, 8 goroutines | 4.82 ns (207 M/s) | **2.78 ns (360 M/s)** | 1.74× |

When `K` is an 8-byte integer type (`int64`, `uint64`, and `int`/`uint`/`uintptr` on
64-bit platforms) and `V` holds no pointers, keys and values are stored directly in the
table: inserts allocate nothing per key, and deletes normally take no lock. A deleted key's slot is reclaimed
the next time its shard's table is rebuilt (on growth, or once enough deletes pile up),
so a map with heavy delete/insert turnover of distinct keys keeps extra table headroom.
Shards that hold TTL- or hit-limited entries use the pointer-per-entry layout instead.

`V` must be **≤ 8 bytes** — `int`, `int64`, `float64`, any pointer, or a small struct.
For larger structs store a pointer (itself 8 bytes), which stays zero-alloc and tear-free:

```go
type User struct { Name string; Age int }
m := alosmap.NewTyped[string, *User]()
m.Store("u:42", &User{Name: "Ann", Age: 30})
u, _ := m.Load("u:42")          // u is *User
```

A `V` that holds a pointer must be exactly one pointer wide. That is automatic on 64-bit
platforms; on 32-bit platforms `NewTyped` panics for 8-byte pointer-holding types such as
`string`, an interface, or a struct with a pointer and a second field.

Because each `TypedMap` is bound to one value type, "a different pool per struct type" is
simply one map per type, each with its own prealloc depth:

```go
users     := alosmap.NewTyped[string, *User]().Prealloc(256)
companies := alosmap.NewTyped[string, *Company]().Prealloc(128)
cars      := alosmap.NewTyped[string, *Car]()        // no pool
```

## Prealloc — chunked node allocation

`Prealloc(chunk)` grabs entry storage in **chunks** instead of one node per insert: each
shard hands out entries from its current chunk and allocates the next one only when it is
used up — turning one `mallocgc` per insert into one per chunk. A shard's first chunk
covers its share of the map's capacity, and later chunks grow with the shard up to
`chunk` entries, so a large `chunk` never reserves memory a small map won't use.

```go
// Typed map — fluent:
m := alosmap.NewTyped[string, int64]().Prealloc(256)

// Any map — Builder or option:
m := alosmap.NewBuilder().Capacity(10_000).Prealloc(256).Build()
m := alosmap.New(alosmap.WithCapacity(10_000), alosmap.WithPrealloc(256))
```

Insert throughput vs every other map (single-goroutine fill of 8 192 fresh keys,
`BenchmarkX_Insert`):

| Map | ns/op | inserts/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| map + RWMutex | **41.5** | **24.1 M/s** | 0 | 0 |
| tmap | 51.4 | 19.5 M/s | 0 | 0 |
| xsync | 68.9 | 14.5 M/s | 24 | 1 |
| alosmap Typed + `Prealloc(1024)` | 71.3 | 14.0 M/s | 31 | 0 |
| alosmap **Typed** | 74.5 | 13.4 M/s | 24 | 0 |
| cmap | 110 | 9.12 M/s | 105 | 0 |
| alosmap (any) + `WithPrealloc(8192)` | 138 | 7.24 M/s | 140 | 1 |
| alosmap (any) | 147 | 6.81 M/s | 103 | 1 |
| sync.Map | 176 | 5.68 M/s | 134 | 3 |

On a raw single-thread fill `map + RWMutex` (which has zero contention here) is
fastest, then tmap and xsync; `Typed` is fourth. `Typed` reports 0 allocs/op even
without `Prealloc` because it batches 8 entries per allocation by default (one
allocation per 8 inserts rounds down to 0); `Prealloc(1024)` makes the batches larger
and brings the insert to 71.3 ns. allocs/op is as `go test` reports it (rounded down).

`map + sync.RWMutex` wins *uncontended single-thread* insert, but it has no
sharding and **collapses under concurrent writes** (106 ns vs the typed map's
3.89 ns in the parallel write benchmark). The lock-free maps trade a little
single-thread insert speed for surviving real concurrency.

**Choosing the chunk size** — you pick it (10, 128, 256, 1000, …):

- Bigger chunk → fewer allocations once a shard has grown; best for insert-heavy /
  long-lived maps. The first chunk is capped at the shard's share of capacity either way.
- Smaller chunk → less memory per chunk, allocates more often.
- A chunk's memory is held until **every** entry in it is gone, so for heavy
  insert+delete churn of distinct keys keep it moderate. **256 is a good default.**

`TypedMap` batches 8 entries per allocation by default, so a plain `NewTyped` map
already avoids one `mallocgc` per insert. `Prealloc(n)` raises the batch size; `Prealloc(1)` goes back to one allocation
per entry, which lets each deleted entry be freed on its own.

Prealloc speeds up the **insert** path; reads and in-place updates are unaffected (the
typed map updates with zero allocations regardless). On the `any` map, prealloc removes
the node allocation, but value boxing remains, and string keys longer than 16 bytes are
still cloned separately — so the win is largest with `int64` or short keys and especially
with `TypedMap`.

## Performance

Head-to-head against the maps Go developers actually reach for:

- **`sync.Map`** — the standard library's concurrent map.
- **`map[K]V` + `sync.RWMutex`** — the canonical "just lock it" pattern.
- **[`puzpuzpuz/xsync/v4`](https://github.com/puzpuzpuz/xsync) `Map`** (v4.5.0) —
  the modern lock-free-leaning generic concurrent map.
- **[`tumult1337/tmap`](https://gitlab.skybyte.cloud/tumult1337/tmap) `Map`** (v0.5.0) —
  a generic concurrent map with lock-free readers.
- **[`orcaman/concurrent-map/v2`](https://github.com/orcaman/concurrent-map)** (v2.0.1) —
  the classic sharded concurrent map (`cmap`).

**Machine:** Intel Xeon E3-1270 v6 @ 3.80 GHz (4 cores / 8 threads, 8 MB L3), Linux,
Go 1.26.0. Every number in this section comes from that one machine, measured on the
current code, **median of 5 runs** (3 runs for the single-goroutine pass of tmap's suite).
Parallel benchmarks run 8 goroutines (`GOMAXPROCS=8`).

**How to read the tables**

- **ns/op** — nanoseconds per operation, lower is better. In a parallel benchmark this is
  wall-clock time divided by the operations of *all* goroutines together.
- **ops/sec** — `1 000 000 000 / ns/op`, higher is better. In a parallel benchmark it is
  the combined throughput of all 8 goroutines; `368 M/s` means 368 million operations
  per second across the machine.
- **B/op, allocs/op** — heap bytes and allocations per operation, as `go test` reports
  them (rounded down).
- **Bold** — fastest in the row (or the fastest row in a per-map table).
- `alosmap` / `alosmap (any)` is the `any`-valued `Map`; **Typed** is `TypedMap[K, V]`.

All of these maps are benchmarked **in this repo**, head-to-head, in the
[`benchmarks/`](benchmarks/) and [`fairbenchmark/`](fairbenchmark/) folders (separate
modules, so the competitor dependencies never touch the library). Reproduce on your own
hardware with:

```bash
cd benchmarks
go test -run='^$' -bench=. -benchmem -benchtime=200ms -count=5
```

```bash
cd fairbenchmark
go test -run='^$' -bench='BenchmarkFair_|BenchmarkInt_' -benchmem -benchtime=100ms -cpu=1,8 -count=5
go test -run='TestMemoryFootprint|TestChurnFootprint' -v
```

### Headline scorecard

String keys, `int64` values, 8 192 keys, 8 goroutines (`benchmarks/`, `BenchmarkX_*`).

**Time per operation — ns/op, lower is better:**

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| Read parallel | **2.78** | 4.82 | 5.15 | 3.40 | 6.64 | 13.8 | 47.5 |
| Write parallel | **3.89** | 12.3 | 15.2 | 20.6 | 51.6 | 36.5 | 106 |
| Mixed 90% read / 10% write | **3.26** | 5.89 | 6.18 | 5.71 | 11.7 | 41.7 | 62.5 |
| Mixed 50% read / 50% write | **3.76** | 8.95 | 10.2 | 12.4 | 29.1 | 52.9 | 64.9 |
| Delete 50% / Store 50% | **3.36** | 11.1 | 12.4 | 12.2 | 29.4 | 34.1 | 101 |
| HotKey read (one key) | **2.01** | 3.50 | 3.44 | 2.48 | 3.39 | 41.5 | 42.7 |
| Range 8 192 entries (µs) | **47.3** | 101 | 63.2 | 104 | 196 | 87.5 | 87.0 |
| Insert, 1 goroutine | 74.5 | 147 | 51.4 | 68.9 | 176 | 110 | **41.5** |

**Throughput — operations per second, higher is better** (the same runs, shown as
`1e9 / ns/op`; the Range row counts entries visited per second):

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| Read parallel | **360 M** | 207 M | 194 M | 294 M | 151 M | 72.4 M | 21.1 M |
| Write parallel | **257 M** | 81.0 M | 65.9 M | 48.4 M | 19.4 M | 27.4 M | 9.48 M |
| Mixed 90% read / 10% write | **307 M** | 170 M | 162 M | 175 M | 85.8 M | 24.0 M | 16.0 M |
| Mixed 50% read / 50% write | **266 M** | 112 M | 98.2 M | 80.3 M | 34.3 M | 18.9 M | 15.4 M |
| Delete 50% / Store 50% | **298 M** | 89.8 M | 80.6 M | 82.0 M | 34.0 M | 29.3 M | 9.90 M |
| HotKey read (one key) | **497 M** | 286 M | 290 M | 403 M | 295 M | 24.1 M | 23.4 M |
| Range (entries scanned) | **173 M** | 80.7 M | 130 M | 79.0 M | 41.8 M | 93.6 M | 94.1 M |
| Insert, 1 goroutine | 13.4 M | 6.81 M | 19.5 M | 14.5 M | 5.68 M | 9.12 M | **24.1 M** |

Honest reading of the numbers:

- **`Typed` is fastest in 7 of the 8 rows.**
- **Writes are its strongest category:** 3.89 ns per `Store`, about 257 million writes
  per second with zero allocations — 3.9× faster than tmap, 5.3× faster than xsync and
  13× faster than `sync.Map`.
- **Mixed read/write and Delete** go to `Typed` by 1.7×–3.7× over xsync and tmap.
- **Reads:** 2.78 ns (360 million reads per second) against 3.40 ns for xsync and
  5.15 ns for tmap. On a single hot key it reaches 2.01 ns — about 497 million reads
  per second.
- **Range:** 8 192 entries in 47.3 µs, about 173 million entries per second; tmap is
  second at 63.2 µs.
- **The `any` map** is second in the write, 50/50 and delete rows, and third in the
  parallel-read row (behind `Typed` and xsync, ahead of tmap).
- **Single-goroutine insert is the row `Typed` does not win.** With no contention
  `map + RWMutex` is fastest (41.5 ns), then tmap (51.4 ns) and xsync (68.9 ns);
  `Typed` is fourth at 74.5 ns (71.3 ns with `Prealloc(1024)`). Publishing a new key
  takes three atomic stores, so that lock-free readers never race with an insert; that
  is most of the gap. The full insert table is in the
  [Prealloc](#prealloc--chunked-node-allocation) section.
- **`map + sync.RWMutex`** — the "just lock it" default — is **17×–30× slower** than
  `Typed` once 8 goroutines contend for it.

---

### Read benchmarks — parallel, string keys, 8 goroutines

| Map | ns/op | ops/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| alosmap **Typed** | **2.78** | **360 M/s** | 0 | 0 |
| xsync | 3.40 | 294 M/s | 0 | 0 |
| alosmap (any) | 4.82 | 207 M/s | 0 | 0 |
| tmap | 5.15 | 194 M/s | 0 | 0 |
| sync.Map | 6.64 | 151 M/s | 0 | 0 |
| cmap | 13.8 | 72.4 M/s | 0 | 0 |
| map + RWMutex | 47.5 | 21.1 M/s | 0 | 0 |

#### HotKey read — every goroutine reads one shared key

| Map | ns/op | ops/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| alosmap **Typed** | **2.01** | **497 M/s** | 0 | 0 |
| xsync | 2.48 | 403 M/s | 0 | 0 |
| sync.Map | 3.39 | 295 M/s | 0 | 0 |
| tmap | 3.44 | 290 M/s | 0 | 0 |
| alosmap (any) | 3.50 | 286 M/s | 0 | 0 |
| cmap | 41.5 | 24.1 M/s | 0 | 0 |
| map + RWMutex | 42.7 | 23.4 M/s | 0 | 0 |

The lock-free readers cluster at the top; every map that takes a lock per read (cmap,
`map + RWMutex`) is an order of magnitude behind.

---

### Write benchmarks — parallel, string keys, 8 goroutines

Every operation overwrites an existing key.

| Map | ns/op | ops/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| alosmap **Typed** | **3.89** | **257 M/s** | 0 | 0 |
| alosmap (any) | 12.3 | 81.0 M/s | 7 | 0 |
| tmap | 15.2 | 65.9 M/s | 0 | 0 |
| xsync | 20.6 | 48.4 M/s | 24 | 1 |
| cmap | 36.5 | 27.4 M/s | 0 | 0 |
| sync.Map | 51.6 | 19.4 M/s | 71 | 2 |
| map + RWMutex | 106 | 9.48 M/s | 0 | 0 |

The typed map stores the value in an atomic word with **zero allocations** and no lock.
The `any` map is second.

---

### Mixed workload — 90% reads / 10% writes, parallel

| Map | ns/op | ops/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| alosmap **Typed** | **3.26** | **307 M/s** | 0 | 0 |
| xsync | 5.71 | 175 M/s | 2 | 0 |
| alosmap (any) | 5.89 | 170 M/s | 0 | 0 |
| tmap | 6.18 | 162 M/s | 0 | 0 |
| sync.Map | 11.7 | 85.8 M/s | 7 | 0 |
| cmap | 41.7 | 24.0 M/s | 0 | 0 |
| map + RWMutex | 62.5 | 16.0 M/s | 0 | 0 |

### Mixed workload — 50% reads / 50% writes, parallel

| Map | ns/op | ops/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| alosmap **Typed** | **3.76** | **266 M/s** | 0 | 0 |
| alosmap (any) | 8.95 | 112 M/s | 3 | 0 |
| tmap | 10.2 | 98.2 M/s | 0 | 0 |
| xsync | 12.4 | 80.3 M/s | 11 | 0 |
| sync.Map | 29.1 | 34.3 M/s | 35 | 1 |
| cmap | 52.9 | 18.9 M/s | 0 | 0 |
| map + RWMutex | 64.9 | 15.4 M/s | 0 | 0 |

### Delete — parallel, 50% delete / 50% store

| Map | ns/op | ops/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| alosmap **Typed** | **3.36** | **298 M/s** | 0 | 0 |
| alosmap (any) | 11.1 | 89.8 M/s | 3 | 0 |
| xsync | 12.2 | 82.0 M/s | 12 | 0 |
| tmap | 12.4 | 80.6 M/s | 0 | 0 |
| sync.Map | 29.4 | 34.0 M/s | 35 | 1 |
| cmap | 34.1 | 29.3 M/s | 0 | 0 |
| map + RWMutex | 101 | 9.90 M/s | 0 | 0 |

---

### Range / iteration

The `any` map's Range distributes shard scanning across up to GOMAXPROCS×4 goroutines (one per
2 048 live entries, so small maps scan sequentially) and pipelines the visitor
pass over completed slabs. ctrl bytes are read in
uint64 chunks so runs of empty slots are skipped, and the next live
entry's heap struct is PREFETCHT0'd before dereference.

#### Range — 8 192 entries (full iteration)

| Map | µs per full scan | entries/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| alosmap **Typed** | **47.3** | **173 M/s** | 0 | 0 |
| tmap | 63.2 | 130 M/s | 0 | 0 |
| map + RWMutex | 87.0 | 94.1 M/s | 0 | 0 |
| cmap | 87.5 | 93.6 M/s | 0 | 0 |
| alosmap (any) | 101 | 80.7 M/s | 516 | 7 |
| xsync | 104 | 79.0 M/s | 0 | 0 |
| sync.Map | 196 | 41.8 M/s | 0 | 0 |

`Typed` scans fastest and allocates nothing. The `any` map carries a small allocation
(the bounded worker fan-out) and iterates **without ever blocking a writer** —
`map + RWMutex` holds its read lock for the whole scan.

#### Range while writing — one full Range with a concurrent writer

`TypedMap` is not included in this benchmark.

| Map | µs per full scan | entries/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| tmap | **64.2** | **128 M/s** | 0 | 0 |
| map + RWMutex | 89.9 | 91.1 M/s | 0 | 0 |
| cmap | 96.2 | 85.1 M/s | 0 | 0 |
| alosmap (any) | 115 | 70.9 M/s | 13121 | 1570 |
| xsync | 130 | 63.0 M/s | 42530 | 1772 |
| sync.Map | 226 | 36.2 M/s | 90416 | 3767 |

tmap is fastest here. `map + RWMutex` and cmap get their place by making the writer
wait; the `any` map keeps writes flowing and is ahead of xsync and `sync.Map`. The
allocations in this table come mostly from the concurrent writer boxing its values.

#### Parallel-visitor Range (`RangePar`)

For workloads where the visitor is safe to call concurrently, alosmap
exposes `RangePar(visitor)` — visitor is invoked from up to GOMAXPROCS×4
goroutines simultaneously, skipping the slab and the sequential visitor
pass entirely. This is an alosmap-only API; not benchmarked head-to-head
because the others don't offer a parallel-visitor Range.

---

### Sequential operation latency

Single goroutine, 8 192 pre-loaded entries, `any` Map (`BenchmarkAlos_SequentialLatency`):

| Operation | String keys | ops/sec | Int64 keys | ops/sec |
|---|---:|---:|---:|---:|
| Load | 21.8 ns | 45.9 M/s | 16.2 ns | 61.5 M/s |
| Store (update) | 28.1 ns | 35.5 M/s | 22.2 ns | 45.0 M/s |
| Peek | 21.3 ns | 47.0 M/s | 15.4 ns | 64.7 M/s |
| Has | 20.8 ns | 48.2 M/s | 13.9 ns | 71.8 M/s |
| Swap | 27.5 ns | 36.3 M/s | 21.9 ns | 45.7 M/s |
| CompareAndSwap | 41.0 ns | 24.4 M/s | 34.2 ns | 29.2 M/s |
| Delete + re-Store | 178 ns | 5.62 M/s | 159 ns | 6.29 M/s |
| LoadOrStore (hit) | 22.6 ns | 44.3 M/s | 15.7 ns | 63.5 M/s |

Single goroutine, 8 192 pre-loaded entries, `TypedMap[string, int64]`
(`BenchmarkTypedExt_*`). `CompareAndDelete` includes re-storing the key; `Snapshot`,
`RangePar` and `Stats` cover all 8 192 entries per call; `Clear` excludes the refill:

| `TypedMap` operation | time/op | ops/sec | B/op | allocs/op |
|---|---:|---:|---:|---:|
| `LoadOrStore (hit)` | 15.0 ns | 66.5 M/s | 0 | 0 |
| `Swap` | 21.2 ns | 47.2 M/s | 0 | 0 |
| `CompareAndSwap` | 19.6 ns | 50.9 M/s | 0 | 0 |
| `CompareAndDelete` | 141 ns | 7.08 M/s | 23 | 0 |
| `Has` | 11.8 ns | 84.4 M/s | 0 | 0 |
| `Peek` | 11.7 ns | 85.3 M/s | 0 | 0 |
| `Get` | 12.4 ns | 80.6 M/s | 0 | 0 |
| `StoreWithTTL` | 173 ns | 5.78 M/s | 48 | 1 |
| `StoreWithHits` | 95.5 ns | 10.5 M/s | 48 | 1 |
| `StoreWithTTLAndHits` | 175 ns | 5.71 M/s | 48 | 1 |
| `Snapshot` | 92.8 µs | 10.8 K/s | 196608 | 1 |
| `RangePar` | 39.7 µs | 25.2 K/s | 4663 | 131 |
| `Stats` | 51.1 µs | 19.6 K/s | 0 | 0 |
| `CleanupNow` | 1 522 ns | 657 K/s | 0 | 0 |
| `Clear` | 16.2 µs | 61.6 K/s | 147456 | 64 |

---

### alosmap-only features

`any` Map write paths that carry TTL or hit-limit metadata, 8 goroutines
(`BenchmarkAlos_TTL`):

| Operation (`any` Map) | ns/op | ops/sec | B/op / allocs |
|---|---:|---:|---:|
| `StoreWithTTL` | 65.9 | 15.2 M/s | 64 B / 3 |
| `StoreWithHits` | 51.2 | 19.5 M/s | 64 B / 3 |
| `StoreWithTTLAndHits` | 65.7 | 15.2 M/s | 64 B / 3 |

These are roughly 4–5× slower than a plain parallel `Store` (12.3 ns) because they
allocate a `valueBox` plus a `valueMeta` record to carry the expiration / hit-counter
state — in the range of a plain `sync.Map.Store` (51.6 ns), for an entry with TTL
semantics that `sync.Map` doesn't offer.

---

### alosmap scaling

`any` Map, parallel reads, 8 goroutines.

#### Read parallel — varying shard count (8 192 keys)

| Shards | ns/op | ops/sec |
|---:|---:|---:|
| 1 | 4.73 | 212 M/s |
| 16 | 4.70 | 213 M/s |
| 64 | 5.06 | 198 M/s |
| 256 | 5.54 | 180 M/s |
| 1 024 | 6.58 | 152 M/s |

Read throughput is nearly flat up to a few hundred shards. More shards help
concurrent inserts, deletes, and resizes, so by default the map uses 4 shards
per GOMAXPROCS (rounded up to a power of two, between 32 and 1 024).

#### Read parallel — varying cardinality

| Entries | ns/op | ops/sec |
|---:|---:|---:|
| 64 | 3.28 | 305 M/s |
| 1 024 | 3.67 | 272 M/s |
| 16 384 | 4.28 | 234 M/s |
| 262 144 | 5.73 | 174 M/s |

Reads cost about 1.7× more at 262 144 entries than at 64 — cache-miss cost as the
map outgrows the CPU caches.

---

### Fair head-to-head (realistic Zipfian / churn / footprint)

A second, stricter suite in the separate `fairbenchmark/` module drives every map through one identical interface on realistic workloads — Zipfian hot-key skew, contended grow-from-empty, cache-style churn of distinct keys, and pointer values — and measures retained memory, the dimension throughput hides. String keys, 100 000 live entries for the read / update / mixed rows, a 10 000-entry working set for churn, 64 contended keys for LoadOrStore.

#### 8 goroutines — ns/op (lower is better)

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| Read Zipf, int64 | **3.85** | 6.50 | 6.66 | 4.20 | 9.85 | 19.5 | 37.5 |
| Read Uniform, int64 | **10.3** | 26.2 | 15.7 | 10.9 | 35.4 | 18.7 | 37.7 |
| Read Zipf, \*ptr | **4.03** | 6.30 | 6.67 | 4.17 | 9.76 | 21.0 | 37.6 |
| Update Zipf, int64 | **9.31** | 13.3 | 36.0 | 58.8 | 94.8 | 74.6 | 106 |
| Update Zipf, \*ptr | **26.6** | 28.8 | 52.5 | 75.1 | 120 | 86.5 | 159 |
| Mixed 95% read | **4.52** | 7.19 | 7.36 | 6.46 | 14.2 | 58.1 | 71.1 |
| Mixed 50% read | **6.97** | 10.3 | 19.2 | 32.0 | 52.2 | 65.7 | 64.5 |
| LoadOrStore, int64 | **5.06** | 10.2 | 5.74 | 5.20 | 22.1 | 61.5 | 92.2 |
| Churn, int64 | 92.0 | 204 | 122 | **82.2** | 129 | 144 | 452 |
| Churn, \*ptr | 122 | 214 | 147 | **106** | 143 | 169 | 495 |

#### 8 goroutines — operations per second (higher is better)

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| Read Zipf, int64 | **260 M** | 154 M | 150 M | 238 M | 102 M | 51.4 M | 26.6 M |
| Read Uniform, int64 | **97.1 M** | 38.2 M | 63.7 M | 91.9 M | 28.2 M | 53.4 M | 26.5 M |
| Read Zipf, \*ptr | **248 M** | 159 M | 150 M | 240 M | 102 M | 47.6 M | 26.6 M |
| Update Zipf, int64 | **107 M** | 75.2 M | 27.8 M | 17.0 M | 10.5 M | 13.4 M | 9.43 M |
| Update Zipf, \*ptr | **37.7 M** | 34.8 M | 19.0 M | 13.3 M | 8.30 M | 11.6 M | 6.28 M |
| Mixed 95% read | **221 M** | 139 M | 136 M | 155 M | 70.4 M | 17.2 M | 14.1 M |
| Mixed 50% read | **144 M** | 96.7 M | 52.0 M | 31.2 M | 19.2 M | 15.2 M | 15.5 M |
| LoadOrStore, int64 | **198 M** | 97.8 M | 174 M | 192 M | 45.2 M | 16.3 M | 10.8 M |
| Churn, int64 | 10.9 M | 4.91 M | 8.21 M | **12.2 M** | 7.77 M | 6.94 M | 2.21 M |
| Churn, \*ptr | 8.21 M | 4.66 M | 6.79 M | **9.45 M** | 7.01 M | 5.92 M | 2.02 M |

#### 8 goroutines — growth and iteration

InsertGrow fills an empty, unsized map with 100 000 distinct keys; Range scans 16 384 entries.

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| InsertGrow, int64 (ns/key) | **52.7** | 135 | 99.2 | 58.0 | 90.6 | 84.7 | 163 |
| InsertGrow, int64 (keys/sec) | **19.0 M** | 7.41 M | 10.1 M | 17.2 M | 11.0 M | 11.8 M | 6.15 M |
| InsertGrow, \*ptr (ns/key) | **74.9** | 157 | 122 | 84.3 | 104 | 106 | 232 |
| InsertGrow, \*ptr (keys/sec) | **13.3 M** | 6.37 M | 8.17 M | 11.9 M | 9.59 M | 9.41 M | 4.31 M |
| Range, int64 (µs / scan) | **101** | 256 | 133 | 218 | 419 | 213 | 177 |
| Range, int64 (entries/sec) | **162 M** | 63.9 M | 123 M | 75.0 M | 39.1 M | 76.9 M | 92.4 M |
| Range, \*ptr (µs / scan) | **100** | 244 | 132 | 219 | 416 | 213 | 178 |
| Range, \*ptr (entries/sec) | **164 M** | 67.2 M | 124 M | 74.9 M | 39.4 M | 76.8 M | 92.3 M |

#### 1 goroutine — ns/op (lower is better)

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| Read Zipf, int64 | **19.4** | 31.5 | 32.7 | 20.4 | 61.5 | 42.5 | 29.0 |
| Read Uniform, int64 | 57.5 | 148 | 87.3 | 58.9 | 263 | 70.7 | **52.6** |
| Read Zipf, \*ptr | **19.2** | 31.1 | 36.5 | 20.2 | 53.0 | 44.4 | 28.9 |
| Update Zipf, int64 | **25.5** | 46.4 | 53.1 | 89.9 | 274 | 54.4 | 42.1 |
| Update Zipf, \*ptr | **110** | 122 | 157 | 176 | 372 | 143 | 128 |
| Mixed 95% read | **20.8** | 33.9 | 34.8 | 24.2 | 78.1 | 44.3 | 30.5 |
| Mixed 50% read | **23.0** | 39.4 | 44.9 | 54.4 | 159 | 49.9 | 37.4 |
| LoadOrStore, int64 | **22.6** | 42.4 | 28.0 | 24.2 | 82.8 | 76.2 | 42.6 |
| Churn, int64 | 286 | 513 | 387 | 278 | 474 | 288 | **260** |
| Churn, \*ptr | 359 | 555 | 510 | 350 | 520 | 369 | **340** |

#### 1 goroutine — operations per second (higher is better)

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| Read Zipf, int64 | **51.6 M** | 31.7 M | 30.6 M | 49.1 M | 16.3 M | 23.5 M | 34.5 M |
| Read Uniform, int64 | 17.4 M | 6.76 M | 11.4 M | 17.0 M | 3.81 M | 14.1 M | **19.0 M** |
| Read Zipf, \*ptr | **52.0 M** | 32.2 M | 27.4 M | 49.6 M | 18.9 M | 22.5 M | 34.6 M |
| Update Zipf, int64 | **39.3 M** | 21.6 M | 18.8 M | 11.1 M | 3.65 M | 18.4 M | 23.8 M |
| Update Zipf, \*ptr | **9.06 M** | 8.18 M | 6.37 M | 5.69 M | 2.69 M | 6.97 M | 7.79 M |
| Mixed 95% read | **48.0 M** | 29.5 M | 28.7 M | 41.3 M | 12.8 M | 22.6 M | 32.8 M |
| Mixed 50% read | **43.5 M** | 25.4 M | 22.3 M | 18.4 M | 6.27 M | 20.0 M | 26.8 M |
| LoadOrStore, int64 | **44.3 M** | 23.6 M | 35.8 M | 41.3 M | 12.1 M | 13.1 M | 23.5 M |
| Churn, int64 | 3.50 M | 1.95 M | 2.59 M | 3.60 M | 2.11 M | 3.47 M | **3.84 M** |
| Churn, \*ptr | 2.79 M | 1.80 M | 1.96 M | 2.85 M | 1.92 M | 2.71 M | **2.94 M** |

#### 1 goroutine — growth and iteration

| Workload | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| InsertGrow, int64 (ns/key) | 132 | 331 | 188 | 172 | 365 | 126 | **112** |
| InsertGrow, int64 (keys/sec) | 7.59 M | 3.02 M | 5.31 M | 5.83 M | 2.74 M | 7.91 M | **8.91 M** |
| InsertGrow, \*ptr (ns/key) | **226** | 361 | 274 | 260 | 433 | 255 | 237 |
| InsertGrow, \*ptr (keys/sec) | **4.43 M** | 2.77 M | 3.65 M | 3.84 M | 2.31 M | 3.92 M | 4.22 M |
| Range, int64 (µs / scan) | **100** | 334 | 133 | 219 | 428 | 212 | 175 |
| Range, int64 (entries/sec) | **163 M** | 49.0 M | 123 M | 74.9 M | 38.3 M | 77.3 M | 93.4 M |
| Range, \*ptr (µs / scan) | **99.9** | 316 | 132 | 220 | 421 | 212 | 177 |
| Range, \*ptr (entries/sec) | **164 M** | 51.8 M | 124 M | 74.3 M | 38.9 M | 77.4 M | 92.3 M |

Reading the fair suite:

- **8 goroutines:** `Typed` is first in 12 of the 14 workloads — every read, update,
  mixed, LoadOrStore, InsertGrow and Range row. The two churn rows go to xsync
  (82.2 vs 92.0 ns with `int64` values, 106 vs 122 ns with pointer values).
- **1 goroutine:** `Typed` is first in 10 of the 14 workloads. With no contention
  `map + RWMutex` takes uniform reads (52.6 vs 57.5 ns), both churn rows (260 vs
  286 ns and 340 vs 359 ns, with xsync also ahead of `Typed`) and the `int64`
  InsertGrow (112 vs 132 ns per key, with cmap second at 126 ns).
- **The `any` map** is second on the update rows and the 50/50 mix at 8 goroutines,
  and trails every map except `map + RWMutex` on churn and InsertGrow.

#### Integer keys

`int` keys, `int64` values (`BenchmarkInt_*`) — the layout where `TypedMap` stores keys
and values directly in its table. Each cell is time per operation · throughput. The
CPUs column is the `-cpu` setting; the InsertGrow rows always insert from 8 goroutines
and report the time for the whole fill.

| Workload | CPUs | **Typed** | xsync | tmap |
|---|---:|---:|---:|---:|
| Read Uniform (100k keys) | 1 | **12.4 ns · 80.4 M/s** | 15.3 ns · 65.4 M/s | 27.6 ns · 36.3 M/s |
| Read Uniform (100k keys) | 8 | **3.07 ns · 325 M/s** | 3.32 ns · 301 M/s | 6.55 ns · 153 M/s |
| Read Zipf (100k keys) | 1 | **9.62 ns · 104 M/s** | 10.3 ns · 97.2 M/s | 19.5 ns · 51.3 M/s |
| Read Zipf (100k keys) | 8 | **2.16 ns · 462 M/s** | 2.18 ns · 459 M/s | 4.07 ns · 246 M/s |
| Update Zipf (100k keys) | 1 | **14.5 ns · 68.8 M/s** | 59.9 ns · 16.7 M/s | 39.5 ns · 25.3 M/s |
| Update Zipf (100k keys) | 8 | **16.1 ns · 62.1 M/s** | 48.6 ns · 20.6 M/s | 33.4 ns · 29.9 M/s |
| Churn (insert + delete) | 1 | **108 ns · 9.29 M/s** | 127 ns · 7.90 M/s | 172 ns · 5.82 M/s |
| Churn (insert + delete) | 8 | 51.9 ns · 19.3 M/s | **46.2 ns · 21.6 M/s** | 69.9 ns · 14.3 M/s |
| InsertGrow 100k keys | 1 | 9.24 ms · 10.8 M keys/s | 14.0 ms · 7.13 M keys/s | **8.85 ms · 11.3 M keys/s** |
| InsertGrow 100k keys | 8 | **5.00 ms · 20.0 M keys/s** | 5.38 ms · 18.6 M keys/s | 6.33 ms · 15.8 M keys/s |
| InsertGrow 448k keys | 1 | 52.3 ms · 8.57 M keys/s | 75.0 ms · 5.97 M keys/s | **39.4 ms · 11.4 M keys/s** |
| InsertGrow 448k keys | 8 | 24.2 ms · 18.5 M keys/s | **20.3 ms · 22.0 M keys/s** | 25.5 ms · 17.6 M keys/s |
| Range 16 384 entries | 1 | **109 µs · 151 M entries/s** | 214 µs · 76.4 M entries/s | 131 µs · 125 M entries/s |
| Range 16 384 entries | 8 | **108 µs · 151 M entries/s** | 215 µs · 76.3 M entries/s | 130 µs · 126 M entries/s |

`Typed` is first in 10 of the 14 rows: every read, update and Range row, the
single-CPU churn row and the 8-CPU 100k InsertGrow. It is second in the other four:

- **Churn on 8 CPUs** goes to xsync, 46.2 vs 51.9 ns.
- **InsertGrow on one CPU** goes to tmap: 8.85 vs 9.24 ms for 100k keys and 39.4 vs
  52.3 ms for 448k keys.
- **InsertGrow of 448k keys on 8 CPUs** goes to xsync, 20.3 vs 24.2 ms.

#### Memory footprint

Lower is leaner. **Bold = leanest.** String keys.

Retained heap, 500 000 live entries (B/entry):

| Value type | **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---|---:|---:|---:|---:|---:|---:|---:|
| int64 values | **40.8** | 129.2 | 59.7 | 58.0 | 124.8 | 55.8 | 55.9 |
| \*Session ptr values | **104.8** | 185.2 | 126.4 | 122.0 | 180.8 | 119.9 | 119.9 |

Cache churn, held heap after 2 000 000 distinct insert+delete ops over a 10 000-entry working set (B/live):

| **Typed** | alosmap | tmap | xsync | sync.Map | cmap | map+RWMutex |
|---:|---:|---:|---:|---:|---:|---:|
| 73.7 | 229.5 | 127.1 | 94.4 | 179.0 | **59.8** | 73.4 |

`Typed` holds the least memory per live entry of all seven maps at 500 000 entries, and
is level with `map + RWMutex` after heavy churn (cmap is leanest there). The `any` map
holds the most memory under churn because it keeps deleted slots around longer rather
than resizing repeatedly.

---

### tmap's own benchmark suite

tmap ships its own benchmark module (`bench/`, `BenchmarkSuite`). These are **tmap's
workloads and tmap's harness**, run through tmap's own alosmap adapter (pointed at this
repo's code) against `TypedMap`, tmap and xsync:
`int` keys, 524 288 distinct keys (large enough that reads are cache-miss-bound), with
the `-cold` rows starting from a presized but empty map. ns/op unless marked µs; the
last column is `TypedMap`'s throughput.

#### `int64` values, 8 goroutines

| tmap suite workload | **Typed** | tmap | xsync | Typed ops/sec |
|---|---:|---:|---:|---:|
| ReadUniform | 10.3 | 14.4 | **7.89** | 96.9 M/s |
| ReadZipf | **3.50** | 5.49 | 3.59 | 285 M/s |
| NegativeLookup | **3.31** | 3.71 | 6.17 | 302 M/s |
| Update | **8.54** | 35.2 | 50.0 | 117 M/s |
| Mixed90 | **4.42** | 6.97 | 8.00 | 226 M/s |
| Mixed50 | **6.37** | 18.7 | 28.4 | 157 M/s |
| Insert | **45.2** | 58.6 | 64.4 | 22.1 M/s |
| LoadOrStore | 10.3 | 14.2 | **8.89** | 97.3 M/s |
| Churn | 41.6 | 170 | **35.8** | 24.0 M/s |
| Range (65 536 entries) | **454 µs** | 525 µs | 882 µs | 144 M entries/s |
| Mixed90-cold | **4.34** | 7.01 | 7.53 | 230 M/s |
| Mixed50-cold | **6.36** | 18.8 | 27.2 | 157 M/s |
| LoadOrStore-cold | 10.4 | 14.1 | **8.94** | 95.7 M/s |

#### `*SessionSmall` pointer values, 8 goroutines

| tmap suite workload | **Typed** | tmap | xsync | Typed ops/sec |
|---|---:|---:|---:|---:|
| ReadUniform | **6.94** | 14.4 | 7.99 | 144 M/s |
| ReadZipf | 3.83 | 5.42 | **3.58** | 261 M/s |
| NegativeLookup | 4.82 | **3.73** | 6.10 | 208 M/s |
| Update | **9.38** | 36.1 | 55.4 | 107 M/s |
| Mixed90 | **4.93** | 7.17 | 8.31 | 203 M/s |
| Mixed50 | **7.20** | 19.4 | 30.5 | 139 M/s |
| Insert | **74.0** | 74.1 | 76.6 | 13.5 M/s |
| LoadOrStore | **7.14** | 14.1 | 8.75 | 140 M/s |
| Churn | 51.6 | 180 | **37.4** | 19.4 M/s |
| Range (65 536 entries) | **394 µs** | 529 µs | 877 µs | 166 M entries/s |
| Mixed90-cold | **4.59** | 7.31 | 7.71 | 218 M/s |
| Mixed50-cold | **6.84** | 19.5 | 29.1 | 146 M/s |
| LoadOrStore-cold | 10.1 | 14.1 | **9.08** | 99.5 M/s |

#### `int64` values, 1 goroutine

| tmap suite workload | **Typed** | tmap | xsync | Typed ops/sec |
|---|---:|---:|---:|---:|
| ReadUniform | 43.9 | 62.5 | **38.2** | 22.8 M/s |
| ReadZipf | **19.6** | 30.0 | 20.3 | 51.0 M/s |
| NegativeLookup | **15.2** | 17.4 | 28.9 | 65.9 M/s |
| Update | **24.2** | 45.5 | 75.7 | 41.3 M/s |
| Mixed90 | **21.6** | 32.7 | 31.6 | 46.3 M/s |
| Mixed50 | **23.5** | 39.1 | 49.3 | 42.5 M/s |
| Insert | 131 | **109** | 344 | 7.62 M/s |
| LoadOrStore | 45.8 | 62.4 | **43.1** | 21.8 M/s |
| Churn | **90.9** | 187 | 113 | 11.0 M/s |
| Range (65 536 entries) | **450 µs** | 523 µs | 875 µs | 146 M entries/s |
| Mixed90-cold | **21.3** | 30.8 | 27.1 | 47.0 M/s |
| Mixed50-cold | **23.4** | 37.9 | 46.5 | 42.8 M/s |
| LoadOrStore-cold | **48.9** | 63.3 | 52.6 | 20.5 M/s |

#### `*SessionSmall` pointer values, 1 goroutine

| tmap suite workload | **Typed** | tmap | xsync | Typed ops/sec |
|---|---:|---:|---:|---:|
| ReadUniform | **34.7** | 68.2 | 38.1 | 28.8 M/s |
| ReadZipf | 22.8 | 32.5 | **20.9** | 43.8 M/s |
| NegativeLookup | 25.9 | **19.6** | 30.4 | 38.6 M/s |
| Update | **28.2** | 52.1 | 89.7 | 35.4 M/s |
| Mixed90 | **24.2** | 35.8 | 30.0 | 41.2 M/s |
| Mixed50 | **31.6** | 44.5 | 50.0 | 31.6 M/s |
| Insert | 208 | **160** | 391 | 4.81 M/s |
| LoadOrStore | **36.1** | 68.3 | 42.0 | 27.7 M/s |
| Churn | 135 | 214 | **119** | 7.40 M/s |
| Range (65 536 entries) | **398 µs** | 527 µs | 879 µs | 165 M entries/s |
| Mixed90-cold | **20.4** | 31.1 | 27.8 | 49.1 M/s |
| Mixed50-cold | **22.8** | 39.2 | 50.5 | 43.9 M/s |
| LoadOrStore-cold | **43.0** | 62.8 | 54.2 | 23.2 M/s |

Across the four tables (52 rows) `TypedMap` is ahead of tmap on 48 and ahead of xsync
on 41. Where it is behind:

- **ReadUniform and LoadOrStore with `int64` values go to xsync** (for example 7.89 vs
  10.3 ns for ReadUniform at 8 goroutines), as does LoadOrStore-cold at 8 goroutines
  with either value type. Those workloads walk the keys in exactly the order they were
  inserted, which suits maps that keep each entry in its own heap object.
- **Churn goes to xsync** at 8 goroutines (35.8 vs 41.6 ns with `int64` values, 37.4 vs
  51.6 ns with pointer values) and, with pointer values, at 1 goroutine (119 vs 135 ns).
- **Insert at 1 goroutine goes to tmap** (109 vs 131 ns with `int64` values, 160 vs
  208 ns with pointer values). At 8 goroutines `TypedMap` leads it.
- **NegativeLookup with pointer values goes to tmap** (3.73 vs 4.82 ns at 8 goroutines,
  19.6 vs 25.9 ns at 1).
- **ReadZipf with pointer values goes to xsync** by a small margin (3.58 vs 3.83 ns at
  8 goroutines, 20.9 vs 22.8 ns at 1).

### tmap's conformance tests

tmap's `bench/conformance_test.go` is a correctness battery that runs every map in its
harness through the same checks. `TypedMap` is wired into 8 of its 10 tests (the other
two have no alosmap adapter in tmap's harness) and **passes all 8**, including when run
with the Go runtime overwriting freed memory (`GODEBUG=clobberfree=1`):

| tmap conformance test | What it checks | `TypedMap` |
|---|---|---|
| `TestConfDifferential` (`int/int`, `string/int`, `int32/int`) | results compared against a plain `map[K]V` | pass |
| `TestConfProvenanceIntKey` | concurrent readers never see one key's value tagged for another key | pass |
| `TestConfProvenanceStringKey` | the same, with string keys | pass |
| `TestConfLoadAndDeleteVsStore` | `LoadAndDelete` racing a `Store` yields only linearizable outcomes | pass |
| `TestConfGCRetention` | pointer values held only by the map survive garbage collection | pass |
| `TestConfNoDroppedWriteUnderGrow` | no insert is lost while goroutines force repeated table growth | pass |
| `TestConfNoDroppedUpdateUnderGrow` | no update to an existing key is lost while the table migrates | pass |
| `TestConfChurnGrow` | concurrent Store + Delete churn with table growth, contents verified afterwards | pass |

---

### Why it's fast

The core design exploits three principles:

1. **Zero-allocation type-stable writes.** The common `m.Store(k, sameTypeValue)`
   path mutates the live `valueBox`'s data word in place — a single
   `atomic.Pointer.Store` with zero allocations. The box's type word is immutable
   for its lifetime, so readers reconstruct the value from a consistent
   `(type, data)` pair and never observe a torn write. A fresh `valueBox` is
   allocated only for TTL, hit limits, custom cloners, or type changes.

2. **Inlined hot paths.** Load, Store, Peek, Has, Swap, CompareAndSwap, and
   LoadOrStore all inline the hash computation and probe loop directly in the
   exported method. No function-call overhead, no interface boxing on the read
   path, and the compiler can see the full data flow for register allocation.

3. **Single-allocation inserts.** Fresh key inserts use one bundle struct that
   combines the entry metadata and value box in one heap allocation (instead of
   two), and string keys up to 48 bytes (16 with `Prealloc`) are copied into
   that same allocation instead of a separate `strings.Clone`. Combined with
   deferred allocation (don't allocate until the probe confirms the key is
   absent), inserts are 2.8× faster than before.

4. **Prefetched Range.** Sequential Range reads ctrl bytes 8 at a time as a
   uint64, skips zero chunks, and issues PREFETCHT0 on the next chunk's slot
   pointers before processing the current chunk. This hides ~100 cycles of
   memory latency per cache line.

### Direct pointer mutation (the recommended pattern for hot state)

Once a pointer is in the map, you don't have to write through the map again
to mutate the pointed-to value — `Load` the pointer once and use atomics on
the struct directly:

Single goroutine (`BenchmarkAlos_LoadedPointer`):

| Mutation through a loaded pointer | ns/op | ops/sec | allocs/op |
|---|---:|---:|---:|
| `atomic.Int64.Add` | 4.52 | 221 M/s | 0 |
| `atomic.Bool.Store` | 6.24 | 160 M/s | 0 |
| `atomic.Value.Store` | 8.00 | 125 M/s | 0 |
| `StringSet` | 46.2 | 21.7 M/s | 2 |

### Value and key semantics

- **Values are stored by reference.** `m.Store(k, ptr)` keeps `ptr`; if you
  mutate the pointed-to struct after Store, the mutation is visible on the
  next `Load`. This is the basis of the direct-pointer mutation pattern.
- **String keys are defensively copied** at insertion (into the entry's own
  allocation for short keys, via `strings.Clone` for longer ones)
  so callers cannot corrupt lookup invariants by mutating the underlying
  bytes (via `unsafe`) after Store. One-time cost paid only on the first
  Store of a unique key — never on the hot re-Store path.

## Examples

These examples are intentionally larger than a minimal snippet. Each one combines
multiple APIs so you can see how the pieces fit together in real code.

Imports are omitted for brevity. Add `fmt`, `strings`, `sync`, `sync/atomic`, and
`time` as needed.

### 1. Basic string-key lifecycle with `Store`, `Get`, `Has`, `Delete`, and `Len`

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("cfg:theme"), "solarized")
m.Store(alosmap.S("cfg:locale"), "en-AU")

theme, _ := m.Get(alosmap.S("cfg:theme"))
fmt.Println(theme) // solarized
fmt.Println(m.Has(alosmap.S("cfg:locale"))) // true
fmt.Println(m.Len()) // 2

m.Store(alosmap.S("cfg:theme"), "tokyo-night")
updated, _ := m.Load(alosmap.S("cfg:theme"))
fmt.Println(updated) // tokyo-night

old, deleted := m.Delete(alosmap.S("cfg:locale"))
fmt.Println(old, deleted) // en-AU true
fmt.Println(m.Has(alosmap.S("cfg:locale"))) // false
fmt.Println(m.Len()) // 1
```

### 2. Direct pointer mutation with atomic add, subtract, `StringSet`, and `Range`

```go
type Session struct {
	Hits   atomic.Int64
	Bytes  atomic.Int64
	Name   atomic.Value
	Status atomic.Value
	Active atomic.Bool
}

m := alosmap.New()
defer m.Close()

sess := &Session{}
sess.Name.Store("alice")
sess.Status.Store("warm")
sess.Active.Store(true)
m.Store(alosmap.S("sess:1"), sess)

ptr, _ := m.Load(alosmap.S("sess:1"))
live := ptr.(*Session)
live.Hits.Add(1)
live.Bytes.Add(512)
live.Bytes.Add(-128)
alosmap.StringSet(&live.Name, "alice-prod")
alosmap.StringSet(&live.Status, "ready")
live.Active.Store(false)

ptr2, _ := m.Load(alosmap.S("sess:1"))
same := ptr2.(*Session)
fmt.Println(live == same) // true

m.Range(func(key alosmap.Key, value any) bool {
	v := value.(*Session)
	fmt.Println(key.String(), v.Hits.Load(), v.Bytes.Load(), v.Name.Load(), v.Status.Load(), v.Active.Load())
	return true
})
```

### 3. Int64-key account ledger with transfer, subtraction, and `Snapshot`

```go
type Account struct {
	Balance atomic.Int64
	Label   atomic.Value
}

m := alosmap.New(alosmap.WithCapacity(100_000))
defer m.Close()

for _, id := range []int64{1001, 1002, 1003} {
	acc := &Account{}
	acc.Balance.Store(1_000)
	acc.Label.Store(fmt.Sprintf("acct-%d", id))
	m.Store(alosmap.I(id), acc)
}

fromValue, _ := m.Load(alosmap.I(1001))
toValue, _ := m.Load(alosmap.I(1002))
from := fromValue.(*Account)
to := toValue.(*Account)

from.Balance.Add(-250)
to.Balance.Add(250)
alosmap.StringSet(&from.Label, "acct-1001-debited")

pairs := m.Snapshot()
for _, pair := range pairs {
	acc := pair.Value.(*Account)
	fmt.Println(pair.Key.Int64Val(), acc.Balance.Load(), acc.Label.Load())
}
```

### 4. TTL cache entry with manual cleanup, `Peek`, `Has`, and `Stats`

```go
m := alosmap.New(alosmap.WithoutCleanup())
defer m.Close()

m.StoreWithTTL(alosmap.S("page:/home"), "<html>cached</html>", 20*time.Millisecond)

cached, ok := m.Peek(alosmap.S("page:/home"))
fmt.Println(cached, ok) // <html>cached</html> true
fmt.Println(m.Has(alosmap.S("page:/home"))) // true

time.Sleep(30 * time.Millisecond)

_, ok = m.Load(alosmap.S("page:/home"))
fmt.Println(ok) // false

m.CleanupNow()
stats := m.Stats()
fmt.Println(stats.ExpiredDeletes >= 1) // true
fmt.Println(stats.LiveEntries)          // 0
```

### 5. Hit-limited OTP flow with `Peek`, `Load`, `Has`, and `Range`

```go
m := alosmap.New()
defer m.Close()

m.StoreWithHits(alosmap.S("otp:12345"), "secret-code", 2)

peeked, _ := m.Peek(alosmap.S("otp:12345"))
fmt.Println(peeked) // secret-code

first, _ := m.Load(alosmap.S("otp:12345"))
second, _ := m.Load(alosmap.S("otp:12345"))
_, ok := m.Load(alosmap.S("otp:12345"))

liveCount := 0
m.Range(func(key alosmap.Key, value any) bool {
	liveCount++
	return true
})

fmt.Println(first, second, ok) // secret-code secret-code false
fmt.Println(m.Has(alosmap.S("otp:12345"))) // false
fmt.Println(liveCount) // 0
```

### 6. TTL and hits together, including the `SetWithTTLAndHits` alias

```go
m := alosmap.New()
defer m.Close()

m.SetWithTTLAndHits(alosmap.S("promo:summer"), "20%-off", 30*time.Second, 2)

first, _ := m.Load(alosmap.S("promo:summer"))
second, _ := m.Load(alosmap.S("promo:summer"))
_, ok := m.Load(alosmap.S("promo:summer"))
fmt.Println(first, second, ok) // 20%-off 20%-off false

m.StoreWithTTLAndHits(alosmap.S("promo:summer"), "25%-off", time.Minute, 3)
current, _ := m.Peek(alosmap.S("promo:summer"))
fmt.Println(current) // 25%-off
```

### 7. `LoadOrStore` for once-only config initialization with `Len` and `Stats`

```go
m := alosmap.New()
defer m.Close()

dsn, loaded := m.LoadOrStore(alosmap.S("config:db"), "postgres://localhost/app")
timeout, timeoutLoaded := m.LoadOrStore(alosmap.S("config:timeout"), 5*time.Second)
dsn2, loaded2 := m.LoadOrStore(alosmap.S("config:db"), "mysql://shadow-db")

fmt.Println(dsn, loaded) // postgres://localhost/app false
fmt.Println(timeout, timeoutLoaded) // 5s false
fmt.Println(dsn2, loaded2) // postgres://localhost/app true

stats := m.Stats()
fmt.Println(m.Len()) // 2
fmt.Println(stats.LiveEntries) // 2
```

### 8. `LoadOrStoreWithOptions` for a TTL-bound pointer singleton

```go
type SessionBucket struct {
	Requests atomic.Int64
	Region   atomic.Value
}

m := alosmap.New()
defer m.Close()

opts := alosmap.EntryOptions{TTL: 5 * time.Minute, Hits: 1000}
actual, loaded := m.LoadOrStoreWithOptions(alosmap.S("bucket:eu-west"), &SessionBucket{}, opts)
bucket := actual.(*SessionBucket)
bucket.Requests.Add(1)
bucket.Region.Store("eu-west-1")

again, loaded2 := m.LoadOrStoreWithOptions(alosmap.S("bucket:eu-west"), &SessionBucket{}, opts)
same := again.(*SessionBucket)

fmt.Println(loaded, loaded2) // false true
fmt.Println(bucket == same) // true
fmt.Println(m.Has(alosmap.S("bucket:eu-west"))) // true
```

### 9. `Swap` and `SwapWithOptions` for leader handoff and cache refresh

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("leader"), "node-a")
m.Store(alosmap.S("cache:/feed"), "v1")

oldLeader, leaderOK := m.Swap(alosmap.S("leader"), "node-b")
oldPage, pageOK := m.SwapWithOptions(
	alosmap.S("cache:/feed"),
	"v2",
	alosmap.EntryOptions{TTL: time.Hour},
)

leader, _ := m.Get(alosmap.S("leader"))
page, _ := m.Load(alosmap.S("cache:/feed"))

fmt.Println(oldLeader, leaderOK, leader) // node-a true node-b
fmt.Println(oldPage, pageOK, page) // v1 true v2
```

### 10. `CompareAndSwap` and `CompareAndSwapWithOptions` for guarded upgrades

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("schema"), "v1")
m.StoreWithTTL(alosmap.S("lock"), "holder-a", time.Minute)

schemaMoved := m.CompareAndSwap(alosmap.S("schema"), "v1", "v2")
schemaFailed := m.CompareAndSwap(alosmap.S("schema"), "v1", "v3")
leaseMoved := m.CompareAndSwapWithOptions(
	alosmap.S("lock"),
	"holder-a",
	"holder-b",
	alosmap.EntryOptions{TTL: 2 * time.Minute},
)

schema, _ := m.Load(alosmap.S("schema"))
lease, _ := m.Load(alosmap.S("lock"))

fmt.Println(schemaMoved, schemaFailed, leaseMoved) // true false true
fmt.Println(schema, lease) // v2 holder-b
```

### 11. `CompareAndDelete` for pending-work cleanup plus normal `Delete`

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("job:99"), "pending")

fmt.Println(m.CompareAndDelete(alosmap.S("job:99"), "running")) // false
fmt.Println(m.CompareAndDelete(alosmap.S("job:99"), "pending")) // true
fmt.Println(m.Has(alosmap.S("job:99"))) // false

m.Store(alosmap.S("job:99"), "done")
old, deleted := m.Delete(alosmap.S("job:99"))
fmt.Println(old, deleted) // done true
```

### 12. `StoreWithOptions` with reusable `EntryOptions` and the `Get` alias

```go
m := alosmap.New()
defer m.Close()

opts := alosmap.EntryOptions{TTL: 30 * time.Second, Hits: 3}
m.StoreWithOptions(alosmap.S("report:weekly"), []string{"a", "b", "c"}, opts)

report, _ := m.Get(alosmap.S("report:weekly"))
fmt.Println(report.([]string)[0]) // a

_, _ = m.Load(alosmap.S("report:weekly"))
_, _ = m.Load(alosmap.S("report:weekly"))
stillThere := m.Has(alosmap.S("report:weekly"))
fmt.Println(stillThere) // true, one hit remains
```

### 13. `Peek`, `Get`, and `Has` on a hit-limited entry

```go
m := alosmap.New()
defer m.Close()

m.StoreWithHits(alosmap.S("coupon:A"), "50%-off", 1)

peeked, _ := m.Peek(alosmap.S("coupon:A"))
fmt.Println(peeked) // 50%-off

got, _ := m.Get(alosmap.S("coupon:A"))
fmt.Println(got) // 50%-off

fmt.Println(m.Has(alosmap.S("coupon:A"))) // false
_, ok := m.Load(alosmap.S("coupon:A"))
fmt.Println(ok) // false
```

### 14. `Range` across mixed key types with key inspection and early stop

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("name"), "alice")
m.Store(alosmap.S("env"), "prod")
m.Store(alosmap.I(42), "answer")
m.Store(alosmap.I(-1), "negative")

visited := make([]string, 0, 2)
m.Range(func(key alosmap.Key, value any) bool {
	if key.IsString() {
		visited = append(visited, fmt.Sprintf("string:%s=%v", key.StringVal(), value))
	} else {
		visited = append(visited, fmt.Sprintf("int:%d=%v", key.Int64Val(), value))
	}
	return len(visited) < 2
})

fmt.Println(visited)
```

### 15. `Snapshot` as a point-in-time view while the live map keeps changing

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("a"), 1)
m.Store(alosmap.S("b"), 2)

snap := m.Snapshot()

m.Store(alosmap.S("b"), 22)
m.Delete(alosmap.S("a"))
m.Store(alosmap.S("c"), 3)

for _, pair := range snap {
	fmt.Println(pair.Key.String(), pair.Value)
}

fmt.Println(m.Len()) // 2 in the live map, even though the snapshot still shows a + b
```

### 16. `Clear` plus `Stats` reset and reuse

```go
m := alosmap.New(alosmap.WithCapacity(10_000), alosmap.WithShardCount(32))
defer m.Close()

for i := 0; i < 2_000; i++ {
	m.Store(alosmap.S(fmt.Sprintf("item:%d", i)), i)
}

before := m.Stats()
fmt.Println(before.LiveEntries > 0, before.SlotCapacity > 0) // true true

m.Clear()
after := m.Stats()

fmt.Println(m.Len()) // 0
fmt.Println(after.LiveEntries) // 0
fmt.Println(after.Tombstones) // 0 or near-zero after rebuild

m.Store(alosmap.S("fresh"), 1)
fmt.Println(m.Len()) // 1
```

### 17. Builder pattern with `RecommendedShardCount`

```go
recommended := alosmap.RecommendedShardCount(250_000)

m := alosmap.NewBuilder().
	Capacity(250_000).
	Shards(recommended).
	LoadFactor(0.75).
	CleanupInterval(2 * time.Second).
	Build()
defer m.Close()

m.Store(alosmap.S("config:region"), "ap-southeast-2")
value, _ := m.Load(alosmap.S("config:region"))
stats := m.Stats()

fmt.Println(recommended)
fmt.Println(stats.Shards) // matches the builder input after normalization
fmt.Println(value) // ap-southeast-2
```

### 18. Explicit sizing with `WithCapacity`, `WithShardCount`, and `WithLoadFactor`

```go
m := alosmap.New(
	alosmap.WithCapacity(1_000_000),
	alosmap.WithShardCount(128),
	alosmap.WithLoadFactor(0.80),
	alosmap.WithCleanupInterval(time.Second),
)
defer m.Close()

for i := int64(0); i < 100_000; i++ {
	m.Store(alosmap.I(i), i*2)
}

stats := m.Stats()
fmt.Println(m.Len()) // 100000
fmt.Println(stats.Shards) // 128
fmt.Println(stats.LoadFactor > 0) // true
fmt.Println(stats.MaxShardLive > 0) // true
```

### 19. `Close` stops background cleanup but the map remains usable

```go
m := alosmap.New(alosmap.WithCleanupInterval(10 * time.Millisecond))

m.StoreWithTTL(alosmap.S("ephemeral"), "value", 20*time.Millisecond)
time.Sleep(30 * time.Millisecond)

_, ok := m.Load(alosmap.S("ephemeral"))
fmt.Println(ok) // false

m.Close()
m.Close() // idempotent

m.Store(alosmap.S("still-usable"), "after-close")
value, _ := m.Load(alosmap.S("still-usable"))
fmt.Println(value) // after-close

m.CleanupNow() // manual cleanup still works after Close
```

### 20. Concurrent readers and writers with a final `Range` summary

```go
m := alosmap.New(alosmap.WithCapacity(65_536))
defer m.Close()

var wg sync.WaitGroup
for worker := 0; worker < 8; worker++ {
	worker := worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10_000; i++ {
			key := alosmap.S(fmt.Sprintf("worker:%d:item:%d", worker, i))
			m.Store(key, i)
			_, _ = m.Load(key)
		}
	}()
}
wg.Wait()

count := 0
m.Range(func(key alosmap.Key, value any) bool {
	count++
	return true
})

fmt.Println(m.Len()) // 80000
fmt.Println(count) // about 80000
```

### 21. Pointer values with an internal mutex-protected map and slice

```go
type ConnTracker struct {
	mu     sync.Mutex
	Counts map[string]int
	Recent []string
}

m := alosmap.New()
defer m.Close()

tracker := &ConnTracker{Counts: make(map[string]int)}
m.Store(alosmap.S("tracker"), tracker)

value, _ := m.Load(alosmap.S("tracker"))
t := value.(*ConnTracker)

t.mu.Lock()
t.Counts["10.0.0.1"]++
t.Counts["10.0.0.2"] += 2
t.Recent = append(t.Recent, "10.0.0.1", "10.0.0.2")
t.mu.Unlock()

m.Range(func(key alosmap.Key, value any) bool {
	tracker := value.(*ConnTracker)
	tracker.mu.Lock()
	fmt.Println(key.String(), tracker.Counts, tracker.Recent)
	tracker.mu.Unlock()
	return true
})
```

### 22. Producer-consumer metrics with direct pointer mutation and `StringSet`

```go
type Metrics struct {
	Total  atomic.Int64
	Errors atomic.Int64
	Bytes  atomic.Int64
	Status atomic.Value
}

m := alosmap.New()
defer m.Close()

metrics := &Metrics{}
metrics.Status.Store("warm")
m.Store(alosmap.S("api:/v1/users"), metrics)

value, _ := m.Load(alosmap.S("api:/v1/users"))
live := value.(*Metrics)
live.Total.Add(1)
live.Bytes.Add(1024)
live.Bytes.Add(-128)
live.Errors.Add(1)
alosmap.StringSet(&live.Status, "hot")

consumer, _ := m.Load(alosmap.S("api:/v1/users"))
current := consumer.(*Metrics)
fmt.Println(current.Total.Load(), current.Errors.Load(), current.Bytes.Load(), current.Status.Load())
```

### 23. Mixed string and int64 keys with key helper methods

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("name"), "alice")
m.Store(alosmap.I(100), "hundred")
m.Store(alosmap.I(-1), "negative")

for _, key := range []alosmap.Key{alosmap.S("name"), alosmap.I(100), alosmap.I(-1)} {
	value, _ := m.Load(key)
	if key.IsString() {
		fmt.Println(key.StringVal(), key.Raw(), value)
	} else {
		fmt.Println(key.Int64Val(), key.String(), key.Raw(), value)
	}
}

m.Store(alosmap.S("100"), "string-hundred")
stringHundred, _ := m.Load(alosmap.S("100"))
intHundred, _ := m.Load(alosmap.I(100))
fmt.Println(stringHundred, intHundred) // string-hundred hundred
```

### 24. `WithValueCloner` with `MapCloner` and `SizedMapCloner`

```go
type UserConfig struct {
	Flags []string
}

func (c UserConfig) CloneForMapWithSize() (any, int64) {
	clone := UserConfig{Flags: append([]string(nil), c.Flags...)}
	return clone, int64(len(clone.Flags) * 16)
}

cloneValue := func(value any) (any, int64) {
	if sized, ok := value.(alosmap.SizedMapCloner); ok {
		return sized.CloneForMapWithSize()
	}
	if plain, ok := value.(alosmap.MapCloner); ok {
		return plain.CloneForMap(), 0
	}
	return value, 0
}

m := alosmap.New(alosmap.WithValueCloner(cloneValue))
defer m.Close()

cfg := UserConfig{Flags: []string{"blue", "beta"}}
m.Store(alosmap.S("cfg:user:1"), cfg)
cfg.Flags[0] = "mutated-outside"

stored, _ := m.Load(alosmap.S("cfg:user:1"))
copy := stored.(UserConfig)

fmt.Println(copy.Flags[0]) // blue
fmt.Println(m.Stats().TrackedValueBytes) // > 0 because the cloner returned a size
```

### 25. `MapEqualer` for semantic equality in `CompareAndSwap` and `CompareAndDelete`

```go
type Version struct {
	Generation int
	UpdatedAt  time.Time
}

func (v Version) EqualForMap(other any) bool {
	candidate, ok := other.(Version)
	return ok && candidate.Generation == v.Generation
}

m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("schema"), Version{Generation: 1, UpdatedAt: time.Now()})

swapped := m.CompareAndSwap(
	alosmap.S("schema"),
	Version{Generation: 1},
	Version{Generation: 2, UpdatedAt: time.Now()},
)

deleted := m.CompareAndDelete(alosmap.S("schema"), Version{Generation: 2})

fmt.Println(swapped) // true
fmt.Println(deleted) // true
```

### 26. Heterogeneous values: nil, bool, func, and channel

```go
m := alosmap.New()
defer m.Close()

m.Store(alosmap.S("feature:dark-mode"), true)
m.Store(alosmap.S("handler"), func(v int) int { return v * 2 })
m.Store(alosmap.S("notify"), make(chan string, 1))
m.Store(alosmap.S("optional"), nil)

flag, _ := m.Load(alosmap.S("feature:dark-mode"))
handlerValue, _ := m.Load(alosmap.S("handler"))
notifyValue, _ := m.Load(alosmap.S("notify"))
optional, ok := m.Load(alosmap.S("optional"))

fn := handlerValue.(func(int) int)
ch := notifyValue.(chan string)
ch <- "ready"

fmt.Println(flag, fn(21), <-ch, optional == nil, ok) // true 42 ready true true
```

### 27. Concurrent `LoadOrStore` of a singleton pointer

```go
type WorkerPool struct {
	Started atomic.Int64
	Name    atomic.Value
}

m := alosmap.New()
defer m.Close()

var wg sync.WaitGroup
for worker := 0; worker < 8; worker++ {
	worker := worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		actual, _ := m.LoadOrStore(alosmap.S("pool:email"), &WorkerPool{})
		pool := actual.(*WorkerPool)
		pool.Started.Add(1)
		alosmap.StringSet(&pool.Name, fmt.Sprintf("email-%d", worker%2))
	}()
}
wg.Wait()

value, _ := m.Load(alosmap.S("pool:email"))
pool := value.(*WorkerPool)
fmt.Println(pool.Started.Load()) // 8
fmt.Println(pool.Name.Load()) // last stored name wins
```

### 28. Background cleanup, `CleanupNow`, and continued use after `Close`

```go
m := alosmap.New(alosmap.WithCleanupInterval(10 * time.Millisecond))

m.StoreWithTTL(alosmap.S("temp"), "data", 20*time.Millisecond)
time.Sleep(30 * time.Millisecond)

_, ok := m.Load(alosmap.S("temp"))
fmt.Println(ok) // false

statsBeforeClose := m.Stats()
fmt.Println(statsBeforeClose.CleanupRuns, statsBeforeClose.ExpiredDeletes)

m.Close()
m.Store(alosmap.S("after-close"), "still-works")
m.CleanupNow()

value, _ := m.Load(alosmap.S("after-close"))
fmt.Println(value) // still-works
```

### 29. `StringSet` for short-lived substrings stored inside pointer fields

```go
type Profile struct {
	Name atomic.Value
}

m := alosmap.New()
defer m.Close()

source := strings.Repeat("x", 1024) + "alice"
short := source[len(source)-5:]

profile := &Profile{}
m.Store(alosmap.S("user:1"), profile)

value, _ := m.Load(alosmap.S("user:1"))
live := value.(*Profile)
alosmap.StringSet(&live.Name, short)
source = ""

m.Range(func(key alosmap.Key, value any) bool {
	p := value.(*Profile)
	fmt.Println(key.String(), p.Name.Load())
	return true
})
```

### 30. Full lifecycle: store, mutate, `Range`, `Snapshot`, CAS, delete, cleanup, and stats

```go
type LedgerAccount struct {
	Balance atomic.Int64
	Label   atomic.Value
	Frozen  atomic.Int64
}

m := alosmap.New(alosmap.WithCapacity(1024), alosmap.WithShardCount(32))
defer m.Close()

for id := int64(1); id <= 3; id++ {
	acc := &LedgerAccount{}
	acc.Balance.Store(1_000 * id)
	acc.Label.Store(fmt.Sprintf("account-%d", id))
	m.Store(alosmap.I(id), acc)
}
m.Store(alosmap.S("batch:status"), "open")

fromValue, _ := m.Load(alosmap.I(1))
toValue, _ := m.Load(alosmap.I(2))
from := fromValue.(*LedgerAccount)
to := toValue.(*LedgerAccount)
from.Balance.Add(-500)
to.Balance.Add(500)
alosmap.StringSet(&from.Label, "account-1-debited")

_ = m.CompareAndSwap(alosmap.S("batch:status"), "open", "settled")
snapshot := m.Snapshot()

acct3, _ := m.Load(alosmap.I(3))
acct3.(*LedgerAccount).Frozen.Store(1)
m.Delete(alosmap.I(3))
m.CleanupNow()

total := int64(0)
m.Range(func(key alosmap.Key, value any) bool {
	if key.IsInt64() {
		total += value.(*LedgerAccount).Balance.Load()
	}
	return true
})

stats := m.Stats()
fmt.Println(len(snapshot)) // 4 entries: 3 accounts + batch status
fmt.Println(total) // 3000 after deleting account 3
fmt.Println(m.Len()) // 3 live entries after deleting account 3
fmt.Println(stats.LiveEntries) // 3
fmt.Println(m.Has(alosmap.I(3))) // false
```

### 31. `Compute` for atomic read-modify-write: counters, conditional updates, and refcounts

`Compute(key, fn)` reads the current value, runs `fn(old, loaded)`, and applies the
returned op — `UpdateOp` writes `new`, `DeleteOp` removes the key, `CancelOp` leaves it
untouched. The whole read-modify-write is atomic, so it is the right tool whenever the new
value depends on the old one (a plain `Store` can lose concurrent updates). `fn` must be
**pure / side-effect-free**: under contention it may be invoked more than once before an
update commits.

```go
hits := alosmap.NewTyped[string, int64]()

hits.Compute("/home", func(old int64, loaded bool) (int64, alosmap.ComputeOp) {
	return old + 1, alosmap.UpdateOp // loaded is false on first touch, old is 0
})
hits.Compute("/home", func(old int64, loaded bool) (int64, alosmap.ComputeOp) {
	return old + 1, alosmap.UpdateOp
})
n, _ := hits.Load("/home")
fmt.Println(n) // 2

version := alosmap.NewTyped[string, int64]()
version.Store("schema", 5)
incoming := int64(4)
version.Compute("schema", func(old int64, loaded bool) (int64, alosmap.ComputeOp) {
	if loaded && incoming <= old {
		return old, alosmap.CancelOp // stale write, skip without touching the map
	}
	return incoming, alosmap.UpdateOp
})
v, _ := version.Load("schema")
fmt.Println(v) // 5, the stale update was cancelled

refs := alosmap.NewTyped[string, int64]()
refs.Store("conn:1", 1)
post, ok := refs.Compute("conn:1", func(old int64, loaded bool) (int64, alosmap.ComputeOp) {
	if old <= 1 {
		return 0, alosmap.DeleteOp // last reference released, drop the entry
	}
	return old - 1, alosmap.UpdateOp
})
fmt.Println(post, ok, refs.Has("conn:1")) // 1 false false
```

## API Reference

### Construction

| Function | Description |
|---|---|
| `New(options ...Option) *Map` | Construct an any-value map with functional options |
| `NewBuilder() *Builder` | Construct an any-value map with the fluent builder |
| `RecommendedShardCount(n int) int` | Reuse the package's shard heuristic for explicit sizing |
| `NewTyped[K comparable, V any](opts ...Option) *TypedMap[K,V]` | Construct a typed map (V ≤ 8 bytes; use `*T` for larger) |
| `NewTypedSized[K, V](capacity, shards int, opts ...Option) *TypedMap[K,V]` | Typed map with explicit sizing |

`TypedMap` accepts the same functional options as `New` — most usefully
`WithCleanupInterval(d)` and `WithoutCleanup()`. Like the `any` map it runs a
**background cleaner by default** (5s) that reclaims expired / hit-exhausted
entries; the goroutine only starts once the first TTL/hit entry is stored, so a
typed map used purely as a plain map never spawns one. Pass `WithoutCleanup()`
to disable it, and call `Close()` to stop it.

### Options

| Option | Default | Description |
|---|---|---|
| `WithCapacity(n)` | 1024 | Expected number of live entries |
| `WithShardCount(n)` | 4 per GOMAXPROCS (32–1 024) | Number of shards, rounded to a power of two |
| `WithLoadFactor(f)` | 0.72 | Target occupancy before growth |
| `WithCleanupInterval(d)` | 5s | Background cleanup cadence |
| `WithoutCleanup()` | - | Disable the background cleaner |
| `WithValueCloner(fn)` | pass-through | Install a write-time clone hook |
| `WithPrealloc(chunk)` / `Builder.Prealloc(chunk)` | off | Allocate entry nodes in chunks |

### TypedMap Methods

`TypedMap` now mirrors the `any` map's API. Plain `Store`/`Load` stay
allocation-free and lock-free on reads; entries carry no lifecycle metadata unless
you use a TTL/hits variant (an optional, lazily-allocated `meta` word), so the hot
path keeps the typed map's speed.

| Item | Description |
|---|---|
| `tm.Store(k, v)` | Insert or replace; 0-alloc in-place update |
| `tm.Load(k) (V, bool)` | Lock-free read, returns `V` directly (consumes a hit) |
| `tm.Get(k) (V, bool)` | Alias of `Load` (consumes a hit) |
| `tm.Peek(k) (V, bool)` | Read without consuming a hit |
| `tm.Has(k) bool` | Liveness check, no hit consumed |
| `tm.Delete(k) (V, bool)` | Remove and return previous value |
| `tm.LoadOrStore(k, v) (V, bool)` | Return existing or insert |
| `tm.Swap(k, v) (V, bool)` | Replace, return previous |
| `tm.CompareAndSwap(k, old, new) bool` | Conditional replace |
| `tm.CompareAndDelete(k, old) bool` | Conditional delete |
| `tm.Compute(k, fn) (V, bool)` | Atomic read-modify-write; `fn(old, loaded)` returns `(new, op)` where `op` is `UpdateOp`/`DeleteOp`/`CancelOp`. `fn` must be pure — it may be invoked more than once |
| `tm.StoreWithTTL(k, v, ttl)` | Insert with time-to-live |
| `tm.StoreWithHits(k, v, hits)` | Insert with a read budget |
| `tm.StoreWithTTLAndHits(k, v, ttl, hits)` | Insert with both limits |
| `tm.StoreWithOptions(k, v, EntryOptions)` | Insert applying `EntryOptions` |
| `tm.SetWithTTLAndHits(...)` | Alias of `StoreWithTTLAndHits` |
| `tm.LoadOrStoreWithOptions` / `SwapWithOptions` / `CompareAndSwapWithOptions` | Option-carrying variants |
| `tm.Range(func(K, V) bool)` | Lock-free iteration; return false to stop |
| `tm.RangePar(func(K, V) bool)` | Parallel iteration, one goroutine per shard |
| `tm.Snapshot() []TypedPair[K,V]` | Point-in-time copy of live pairs |
| `tm.Len() int` | Live entry count |
| `tm.Stats() TypedStats` | Occupancy snapshot |
| `tm.Clear()` | Remove all entries |
| `tm.CleanupNow()` | Reclaim expired / hit-exhausted / tombstoned slots now |
| `tm.Close()` | Stop the background cleaner (idempotent; map stays usable) |
| `tm.Prealloc(chunk) *TypedMap[K,V]` | Turn on chunked allocation (fluent) |

### Key Constructors and Methods

| Item | Description |
|---|---|
| `S(key string) Key` | Create a string key |
| `I(key int64) Key` | Create an int64 key |
| `key.String()` | Render a key as text |
| `key.IsString()` | Report whether the key is string-backed |
| `key.IsInt64()` | Report whether the key is int64-backed |
| `key.StringVal()` | Return the string value or `""` |
| `key.Int64Val()` | Return the int64 value or `0` |
| `key.Raw()` | Return the underlying `string` or `int64` as `any` |

### Entry and Clone Hooks

| Item | Description |
|---|---|
| `EntryOptions{TTL, Hits}` | Per-entry lifetime rules used by the options-based APIs |
| `ValueCloneFunc` | Write-time clone callback used by `WithValueCloner` |
| `MapCloner` | Optional interface you can honor inside your own clone function |
| `SizedMapCloner` | Clone interface that can also report tracked bytes |
| `MapEqualer` | Semantic equality hook used by `CompareAndSwap` and `CompareAndDelete` |

### Map Methods

| Method | Description |
|---|---|
| `Store(key, value)` | Write or overwrite a value |
| `StoreWithTTL(key, value, ttl)` | Write with a time-to-live |
| `StoreWithHits(key, value, hits)` | Write with a hit limit |
| `StoreWithTTLAndHits(key, value, ttl, hits)` | Write with both TTL and hit limit |
| `SetWithTTLAndHits(key, value, ttl, hits)` | Alias for `StoreWithTTLAndHits` |
| `StoreWithOptions(key, value, opts)` | General write API with `EntryOptions` |
| `Load(key) (any, bool)` | Read, consuming hits |
| `Get(key) (any, bool)` | Alias for `Load` |
| `Peek(key) (any, bool)` | Read without consuming hits |
| `Has(key) bool` | Existence check without consuming hits |
| `Delete(key) (any, bool)` | Remove a key and return the old value |
| `LoadOrStore(key, value) (any, bool)` | Read an existing value or install a new one |
| `LoadOrStoreWithOptions(key, value, opts)` | `LoadOrStore` plus `EntryOptions` |
| `Swap(key, value) (any, bool)` | Replace and return the old value |
| `SwapWithOptions(key, value, opts)` | `Swap` plus `EntryOptions` |
| `CompareAndSwap(key, old, new) bool` | Replace only when the current value matches |
| `CompareAndSwapWithOptions(key, old, new, opts)` | CAS plus `EntryOptions` |
| `CompareAndDelete(key, old) bool` | Delete only when the current value matches |
| `Range(func(Key, any) bool)` | Visit all live entries |
| `Snapshot() []Pair` | Build a flat point-in-time slice |
| `Len() int` | Count live entries |
| `Clear()` | Remove all entries and rebuild shard tables |
| `CleanupNow()` | Force a maintenance pass |
| `Stats() Stats` | Inspect current counters and capacity estimates |
| `Close()` | Stop the background cleanup goroutine; the map remains usable |

### Returned Types

| Type | Description |
|---|---|
| `Pair` | One `Snapshot` item with `Key` and `Value` fields |
| `Stats` | Point-in-time counters and memory estimates |

### Helper Functions

| Function | Description |
|---|---|
| `StringSet(target *atomic.Value, value string)` | Clone a string and store it in an `atomic.Value` |

## Recommended Pattern: Store a Pointer Once, Then Mutate the Fields

If your values are naturally mutable state, the fastest pattern is:

1. Build a struct with `atomic` fields or an internal `sync.Mutex`.
2. Store a pointer to that struct in the map once.
3. `Load` the pointer and mutate the fields directly.

```go
type Stats struct {
	Count  atomic.Int64
	Bytes  atomic.Int64
	Status atomic.Value
	Live   atomic.Bool
}

m := alosmap.New()
defer m.Close()

stats := &Stats{}
stats.Status.Store("initial")
stats.Live.Store(true)
m.Store(alosmap.S("stats:api"), stats)

value, _ := m.Load(alosmap.S("stats:api"))
live := value.(*Stats)
live.Count.Add(1)
live.Bytes.Add(1024)
live.Bytes.Add(-256)
alosmap.StringSet(&live.Status, "updated")
live.Live.Store(false)

fmt.Println(live.Count.Load(), live.Bytes.Load(), live.Status.Load(), live.Live.Load())
```

That pattern is what the direct-pointer benchmarks measure. It avoids map-level write
overhead after the initial insert and lets your hot mutations ride on atomic or mutex
operations inside the pointed-to value.

## Requirements

- Go 1.26+
- No cgo
- No dependencies outside the standard library