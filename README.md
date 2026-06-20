# alosmap

A high-performance concurrent map for Go with string and int64 keys, lock-free reads,
per-entry TTL, hit-limited entries, and automatic or manual cleanup — plus a
zero-allocation generic `TypedMap[K, V]` and opt-in chunked `Prealloc`.

```go
go get github.com/guno1928/alosmap
```

## Test Suite

`testall.go` holds the large registered case list for the package, and `typedmap_testall.go` adds
300 dedicated `TypedMap[K, V]` cases (IDs 301–600) covering round-trips across key/value types,
delete/probe-chain integrity, `Range`/`Len`, pointer identity, `Prealloc`, table growth, tombstone
reuse, and concurrent torn-read stress. Run the full registry through the `TestAllCases` harness with:

```bash
go test -run TestAllCases -count=1 -v
```

Run the full repository test suite with:

```bash
go test -count=1 ./...
```

Latest local run in this repo:

- `TestAllCases`: `575/575` cases passed (275 core + 300 `TypedMap` cases)
- Typed map, prealloc, and concurrency/race suites pass (`-race` clean)
- Full package suite: `ok github.com/guno1928/alosmap`

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
per-store allocation, and `Load` returns `V` directly with no type assertion. Reads stay
lock-free; writes take one short per-shard lock. It is a separate type, so the existing
`Map`/`New`/`Store`/`Load` API is untouched.

```go
users := alosmap.NewTyped[string, int64]()
users.Store("u:42", 1000)       // 0 allocations
n, ok := users.Load("u:42")     // n is int64, no assertion
users.Delete("u:42")
```

`TypedMap` mirrors the full `Map` API — beyond the basics above it has `LoadOrStore`,
`Swap`, `CompareAndSwap`/`CompareAndDelete`, `Has`/`Peek`/`Get`, TTL- and hit-limited
stores (`StoreWithTTL`, `StoreWithHits`, `StoreWithTTLAndHits`, …), `Snapshot`,
`RangePar`, `Clear`, `CleanupNow`, and `Stats`. Plain `Store`/`Load` stay zero-alloc;
only TTL/hit entries allocate a small metadata word, so the hot path keeps the speeds
below. See [TypedMap Methods](#typedmap-methods) for the full list.

Measured head-to-head, same keys and machine (median of 3, `-benchtime=150ms`):

| Operation             | `any` Map      | `TypedMap[K,V]`     | Speedup  |
|-----------------------|----------------|---------------------|----------|
| Store (update)        | 30 ns, 1 box   | **18 ns, 0 alloc**  | **1.7×** |
| Store, parallel (16T) | 6.3 ns         | **3.4 ns, 0 alloc** | **1.9×** |
| Load                  | 16 ns          | 16 ns               | ~1×      |
| Load, parallel (16T)  | 2.05 ns        | **1.94 ns**         | 1.06×    |

`V` must be **≤ 8 bytes** — `int`, `int64`, `float64`, any pointer, or a small struct.
For larger structs store a pointer (itself 8 bytes), which stays zero-alloc and tear-free:

```go
type User struct { Name string; Age int }
m := alosmap.NewTyped[string, *User]()
m.Store("u:42", &User{Name: "Ann", Age: 30})
u, _ := m.Load("u:42")          // u is *User
```

Because each `TypedMap` is bound to one value type, "a different pool per struct type" is
simply one map per type, each with its own prealloc depth:

```go
users     := alosmap.NewTyped[string, *User]().Prealloc(256)
companies := alosmap.NewTyped[string, *Company]().Prealloc(128)
cars      := alosmap.NewTyped[string, *Car]()        // no pool
```

## Prealloc — chunked node allocation

`Prealloc(chunk)` grabs entry storage in **chunks** instead of one node per insert: it
allocates `chunk` entries up front, hands them out, and allocates the next chunk only
when the current one is used up — turning one `mallocgc` per insert into one per `chunk`
inserts.

```go
// Typed map — fluent:
m := alosmap.NewTyped[string, int64]().Prealloc(256)

// Any map — Builder or option:
m := alosmap.NewBuilder().Capacity(10_000).Prealloc(256).Build()
m := alosmap.New(alosmap.WithCapacity(10_000), alosmap.WithPrealloc(256))
```

Insert throughput vs every other map (single-goroutine fill, 8 192 keys):

| Map               |   ns/op | allocs |
|-------------------|--------:|-------:|
| map + RWMutex\*   |  27.1   | 0      |
| xsync             |  40.5   | 1      |
| alosmap Typed     |  47.2   | 1      |
| alosmap (any)     |  94.0   | 2      |
| cmap              |  95.2   | 0      |
| sync.Map          | 124.5   | 3      |

On a raw single-thread fill the lock-free maps cluster behind `map + RWMutex`
(which has zero contention here). Turning on `Prealloc` takes the **typed map's
inserts to 0 allocations** (the any map keeps 2) by handing out entry nodes in
chunks instead of one `mallocgc` per key.

\* `map + sync.RWMutex` wins *uncontended single-thread* insert, but it has no
sharding and **collapses under concurrent writes** (81.6 ns vs the typed map's
3.4 ns in the parallel write benchmark). The lock-free maps trade a little
single-thread insert speed for surviving real concurrency.

**Choosing the chunk size** — you pick it (10, 128, 256, 1000, …):

- Bigger chunk → fewer allocations; best for insert-heavy / long-lived maps, but grabs
  more memory up front.
- Smaller chunk → less memory up front, allocates more often.
- A chunk's memory is held until **every** entry in it is gone, so for heavy
  insert+delete churn of distinct keys keep it moderate. **256 is a good default.**

Prealloc speeds up the **insert** path; reads and in-place updates are unaffected (the
typed map updates with zero allocations regardless). On the `any` map, prealloc removes
the node allocation, but the defensive key-clone and value-boxing remain — so the win is
largest with `int64` keys and especially with `TypedMap`.

## Performance

Head-to-head against the four maps Go developers actually reach for:

- **`sync.Map`** — the standard library's concurrent map.
- **`map[K]V` + `sync.RWMutex`** — the canonical "just lock it" pattern.
- **[`puzpuzpuz/xsync/v3`](https://github.com/puzpuzpuz/xsync) `MapOf`** —
  the modern lock-free-leaning generic concurrent map.
- **[`orcaman/concurrent-map/v2`](https://github.com/orcaman/concurrent-map)** —
  the classic sharded concurrent map (`cmap`).

Machine: AMD Ryzen 7 5700X, 16 threads, Windows, Go 1.26. Parallel benches
unless noted. Lower is better. **Bold = fastest in the row.**

All six maps are benchmarked **in this repo**, head-to-head, in the
[`benchmarks/`](benchmarks/) folder (a separate module so the competitor deps
never touch the library). Reproduce on your own hardware with:

```bash
cd benchmarks
go test -run='^$' -bench='BenchmarkX_' -benchmem -benchtime=150ms -count=3
```

### Headline scorecard

ns/op, lower is better, string keys + `int64` values, 16 goroutines.
**Bold = fastest in the row.** `alosmap` is the `any` map; `Typed` is
`TypedMap[string,int64]`.

| Workload          | alosmap | **Typed** | sync.Map | map+RWMutex | xsync   | cmap   |
|-------------------|--------:|----------:|---------:|------------:|--------:|-------:|
| Read parallel     |  2.05   |   1.94    |  2.66    |   34.7      | **1.48**| 4.88   |
| **Write parallel**|  6.29   | **3.39**  | 25.8     |   81.6      | 11.7    | 14.2   |
| **Mixed 90/10**   |  2.80   | **2.26**  |  5.89    |   96.9      | 3.30    | 17.2   |
| Delete 50/50      | **5.13**|   5.18    | 15.4     |   74.7      | 8.16    | 13.9   |
| **Range 16k (µs)**|  65     |  **25**   | 130      |  62         | 59      | 66     |
| HotKey read       |  1.53   |   1.19    |  1.47    |   33.6      | **1.12**| 33.5   |

Honest reading of the numbers:

- **Writes are alosmap's strongest category.** `Typed` is **the fastest map
  here by far** — ~3.5× faster than xsync, ~7.6× faster than `sync.Map`, with
  zero allocations. The `any` map is second (6.3 ns), still ~1.9× faster than
  xsync and ~4× faster than `sync.Map`.
- **Range is alosmap's other clear win.** `Typed` iterates 16k entries fastest
  of all (25 µs) — its compact inline entries are a cache-friendly sequential
  scan. The `any` map's parallel-collect pipeline clusters with xsync/cmap and
  iterates *without blocking writers*, unlike builtin+RWMutex.
- **Reads** are a near-tie at the top: xsync edges it (1.48 ns), `Typed` second
  (1.94 ns), `any` third (2.05 ns) — all lock-free, all far ahead of `sync.Map`,
  cmap, and RWMutex.
- **Delete leads the field** under a 50/50 delete/store mix: both alosmap maps
  (~5.1 ns) beat xsync (8.2 ns) and cmap (13.9 ns) — tombstone-only deletes keep
  it close to a plain store. RWMutex is an order of magnitude behind on every
  contended op.
- **`map + sync.RWMutex`** — the "just lock it" default — is **10–30× slower**
  than the lock-free maps under contention. The numbers are why this library
  exists.

For visitors that are safe to call concurrently, the `any` map also exposes
`RangePar`, fanning the visitor out across GOMAXPROCS×4 goroutines.

---

### Read benchmarks — parallel, string keys, 16 goroutines

| Map               |   ns/op |    ops/s | allocs |
|-------------------|--------:|---------:|-------:|
| **xsync**         | **1.48**| **678 M/s** | 0   |
| alosmap **Typed** |   1.94  |  514 M/s | 0      |
| alosmap (any)     |   2.05  |  489 M/s | 0      |
| sync.Map          |   2.66  |  376 M/s | 0      |
| cmap              |   4.88  |  205 M/s | 0      |
| map + RWMutex     |  34.7   | 29.0 M/s | 0      |

The three lock-free maps (xsync, Typed, any) cluster at the top; every map that
takes a lock per read falls off a cliff.

#### HotKey read — single contended key

| Map               |   ns/op |    ops/s | allocs |
|-------------------|--------:|---------:|-------:|
| **xsync**         | **1.12**| **890 M/s** | 0   |
| alosmap Typed     |   1.19  |  844 M/s | 0      |
| sync.Map          |   1.47  |  681 M/s | 0      |
| alosmap (any)     |   1.53  |  656 M/s | 0      |
| cmap              |  33.5   | 30.0 M/s | 0      |
| map + RWMutex     |  33.6   | 29.7 M/s | 0      |

---

### Write benchmarks — parallel, string keys, 16 goroutines

| Map               |   ns/op |    ops/s | allocs/op |
|-------------------|--------:|---------:|----------:|
| **alosmap Typed** | **3.39**| **295 M/s** | **0**  |
| alosmap (any)     |   6.29  |  159 M/s | 0         |
| xsync             |  11.7   | 85.0 M/s | 1         |
| cmap              |  14.2   | 70.0 M/s | 0         |
| sync.Map          |  25.8   | 38.8 M/s | 2         |
| map + RWMutex     |  81.6   | 12.3 M/s | 0         |

Writes are where alosmap pulls away. The typed map stores the value in an atomic
word with **zero allocations** — ~3.5× faster than xsync and ~7.6× faster than
`sync.Map`. The `any` map is second, still ~1.9× faster than xsync.

---

### Mixed workload — 90% reads / 10% writes, parallel

| Map               |   ns/op |    ops/s |
|-------------------|--------:|---------:|
| **alosmap Typed** | **2.26**| **442 M/s** |
| alosmap (any)     |   2.80  |  357 M/s |
| xsync             |   3.30  |  303 M/s |
| sync.Map          |   5.89  |  170 M/s |
| cmap              |  17.2   | 58.0 M/s |
| map + RWMutex     |  96.9   | 10.3 M/s |

### Delete — parallel, 50% delete / 50% store

| Map               |   ns/op |    ops/s |
|-------------------|--------:|---------:|
| **alosmap (any)** | **5.13**| **195 M/s** |
| alosmap Typed     |   5.18  |  193 M/s |
| xsync             |   8.16  |  123 M/s |
| cmap              |  13.9   | 72.0 M/s |
| sync.Map          |  15.4   | 65.0 M/s |
| map + RWMutex     |  74.7   | 13.4 M/s |

Under a 50/50 delete/store mix both alosmap maps lead the field — their delete
just stamps a tombstone with no synchronous compaction, so it stays close to a
plain store. `xsync` and `cmap` follow; `map + RWMutex` is ~10–15× slower.

---

### Range / iteration

Range distributes shard scanning across GOMAXPROCS×4 goroutines and
pipelines the visitor pass over completed slabs. ctrl bytes are read in
uint64 chunks so runs of empty slots are skipped, and the next live
entry's heap struct is PREFETCHT0'd before dereference.

#### Range — 16 384 entries (full iteration)

| Map               |   ns/op  | allocs |
|-------------------|---------:|--------|
| **alosmap Typed** | **25 µs**| 0      |
| xsync             |    59 µs | 0      |
| map + RWMutex     |    62 µs | 0 (but blocks all writers) |
| alosmap (any)     |    65 µs | ~0.5 KB / 7 |
| cmap              |    66 µs | 0      |
| sync.Map          |   130 µs | 0      |

`Typed`'s compact inline entries (hash + key + value word, no `valueBox` chase)
make a plain sequential scan the fastest of all. The `any` map clusters with
xsync/cmap; it carries a small alloc (~7, the bounded worker fan-out) but
iterates **without ever blocking a writer** — `map+RWMutex` only matches it by
RLock-freezing every writer for the whole scan.

#### Range while writing — Range with a concurrent writer

| Map           | ns/op (full Range) | allocs (driven by writer's int boxing) |
|---------------|-------------------:|---------------------------------------|
| **alosmap**   |     **69 700 ns**  | 11 KB / 1 358                       |
| builtin + RW  |           64 600 ns | 0 B / 0 — writer is blocked          |
| cmap          |           74 500 ns | 0 B / 0                             |
| xsync         |           85 100 ns | 33 KB / 1 397                       |
| sync.Map      |          149 200 ns | 70 KB / 2 931                       |

alosmap delivers the fastest iteration *while keeping writes flowing*.
builtin+RW only edges it here by **blocking every concurrent writer** behind
the iteration's RLock — it isn't iterating a live map. Among the maps that
don't freeze writers, alosmap is the clear winner (~1.2× faster than xsync,
~1.1× faster than cmap, ~2.1× faster than sync.Map).

#### Parallel-visitor Range (`RangePar`)

For workloads where the visitor is safe to call concurrently, alosmap
exposes `RangePar(visitor)` — visitor is invoked from up to GOMAXPROCS×4
goroutines simultaneously, skipping the slab and the sequential visitor
pass entirely. This is an alosmap-only API; not benchmarked head-to-head
because the others don't offer a parallel-visitor Range.

---

### Sequential operation latency

Single-goroutine performance with 10 000 pre-loaded entries:

| Operation           | String keys | Int64 keys |
|---------------------|------------:|-----------:|
| Load                |    20.1 ns  |   16.7 ns  |
| Store (update)      |    37.9 ns  |   36.1 ns  |
| Peek                |    20.9 ns  |   13.8 ns  |
| Has                 |    20.8 ns  |       —    |
| Swap                |    37.7 ns  |       —    |
| CompareAndSwap      |    59.2 ns  |       —    |
| Delete + re-Store   |   142.6 ns  |  132.3 ns  |
| LoadOrStore (hit)   |    32.4 ns  |       —    |

---

### alosmap-only features

Numbers for the write paths that require the slower `valueBox` (because
they carry TTL or hit-limit metadata):

| Operation                      |   ns/op | ops/s    | allocs       |
|--------------------------------|--------:|---------:|--------------|
| `StoreWithTTL` (parallel)      |   29.79 |  33.6 M/s | 56 B / 3     |
| `StoreWithHits` (parallel)     |   24.36 |  41.1 M/s | 56 B / 3     |
| `StoreWithTTLAndHits` (par.)   |   27.13 |  36.9 M/s | 56 B / 3     |

These are ~4× slower than a plain `Store` because they allocate a `valueBox`
plus a `valueMeta` record to carry the expiration / hit-counter state — but
still faster than `sync.Map.Store` (31 ns) for an entry with TTL semantics
that sync.Map doesn't even offer.

---

### alosmap scaling

#### Read parallel — varying shard count (16 384 keys)

| Shards |    ns/op | ops/s    |
|-------:|---------:|---------:|
|      1 |    2.057 |  486 M/s |
|     16 |    1.962 |  510 M/s |
|     64 |    2.058 |  486 M/s |
|    256 |    2.351 |  425 M/s |
|  1 024 |    2.581 |  387 M/s |

Read throughput is essentially flat across shard counts; the map
auto-selects a reasonable shard count for you.

#### Read parallel — varying cardinality

| Entries  |    ns/op | ops/s    |
|---------:|---------:|---------:|
|       64 |    1.424 |  702 M/s |
|    1 024 |    1.601 |  625 M/s |
|   16 384 |    2.400 |  417 M/s |
|  262 144 |    3.103 |  322 M/s |

Reads grow ~2× from 64 to 256 K entries — pure cache-miss cost, not a
data-structure tax.

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

3. **Single-allocation inserts.** Fresh key inserts use an `entryBundle` struct
   that combines the entry metadata and value box in one heap allocation
   (instead of two). Combined with deferred allocation (don't allocate until
   the probe confirms the key is absent), inserts are 2.8× faster than before.

4. **Prefetched Range.** Sequential Range reads ctrl bytes 8 at a time as a
   uint64, skips zero chunks, and issues PREFETCHT0 on the next chunk's slot
   pointers before processing the current chunk. This hides ~100 cycles of
   memory latency per cache line.

### Direct pointer mutation (the recommended pattern for hot state)

Once a pointer is in the map, you don't have to write through the map again
to mutate the pointed-to value — `Load` the pointer once and use atomics on
the struct directly:

| Pattern                                  |       ops/s |  ns/op | allocs |
|------------------------------------------|------------:|-------:|:-------|
| `atomic.Int64.Add` via loaded pointer    |     608 M/s |  1.645 | 0      |
| `atomic.Bool.Store` via loaded pointer   |     584 M/s |  1.711 | 0      |
| `atomic.Value.Store` via loaded pointer  |     271 M/s |  3.689 | 0      |
| `StringSet` via loaded pointer           |    33.9 M/s | 29.49  | 2      |

### Value and key semantics

- **Values are stored by reference.** `m.Store(k, ptr)` keeps `ptr`; if you
  mutate the pointed-to struct after Store, the mutation is visible on the
  next `Load`. This is the basis of the direct-pointer mutation pattern.
- **String keys are defensively copied** at insertion via `strings.Clone`
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

## API Reference

### Construction

| Function | Description |
|---|---|
| `New(options ...Option) *Map` | Construct an any-value map with functional options |
| `NewBuilder() *Builder` | Construct an any-value map with the fluent builder |
| `RecommendedShardCount(n int) int` | Reuse the package's shard heuristic for explicit sizing |
| `NewTyped[K comparable, V any]() *TypedMap[K,V]` | Construct a typed map (V ≤ 8 bytes; use `*T` for larger) |
| `NewTypedSized[K, V](capacity, shards int) *TypedMap[K,V]` | Typed map with explicit sizing |

### Options

| Option | Default | Description |
|---|---|---|
| `WithCapacity(n)` | 1024 | Expected number of live entries |
| `WithShardCount(n)` | auto | Number of shards, rounded to a power of two |
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
| `tm.CleanupNow()` | Reclaim expired / hit-exhausted / tombstoned slots |
| `tm.Close()` | No-op (TypedMap has no background goroutine) |
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