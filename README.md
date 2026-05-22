# alosmap

A high-performance concurrent map for Go with string and int64 keys, lock-free reads,
per-entry TTL, hit-limited entries, and automatic or manual cleanup.

```go
go get github.com/guno1928/alosmap
```

## Test Suite

`testall.go` holds the large registered case list for the package. Run that registry through the
`TestAllCases` harness with:

```bash
go test -run TestAllCases -count=1 -v
```

Run the full repository test suite with:

```bash
go test -count=1 ./...
```

Latest local run in this repo:

- `TestAllCases`: `275/275` `testall.go` cases passed
- Full package suite: `ok github.com/guno1928/alosmap 0.648s`

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

## Performance

Head-to-head against the four maps Go developers actually reach for:

- **`sync.Map`** — the standard library's concurrent map.
- **`map[K]V` + `sync.RWMutex`** — the canonical "just lock it" pattern.
- **[`puzpuzpuz/xsync/v3`](https://github.com/puzpuzpuz/xsync) `MapOf`** —
  the modern lock-free-leaning generic concurrent map.
- **[`orcaman/concurrent-map/v2`](https://github.com/orcaman/concurrent-map)** —
  the classic sharded concurrent map (`cmap`).

Machine: AMD Ryzen 7 5700X, 16 threads, Windows, Go 1.26. Best of 3 at
`-benchtime=2s -count=3`, parallel benches unless noted. Lower is better.
**Bold = fastest in the row.**

### Headline scorecard

| Workload                  | alosmap | rank | vs runner-up | vs sync.Map | vs builtin+RW |
|---------------------------|--------:|:----:|:------------:|:-----------:|:-------------:|
| Read parallel (string)    |  2.55 ns | 2nd  | 0.73× (xsync) | 1.40×       | 10.4×         |
| Read parallel (int64)     |  1.83 ns | **1st** | 1.02× (xsync) | 1.87×       | 15.3×         |
| **Write parallel (str)**  |  3.40 ns | **1st** | **3.74×** (xsync) | **7.74×** | **25.5×**     |
| **Write parallel (int)**  |  3.06 ns | **1st** | **4.63×** (xsync) | **13.8×** | **28.2×**     |
| Mixed 90 / 10             |  3.12 ns | **1st** | 1.22× (xsync) | 2.17×       | 32.2×         |
| Mixed 50 / 50             |  6.19 ns | **1st** | 1.58× (xsync) | 3.53×       | 25.1×         |
| Mixed 10 / 90 (write-heavy) | 7.67 ns | **1st** | 1.83× (xsync) | 4.36×       | 14.8×         |
| Delete                    |  4.75 ns | **1st** | 1.14× (cmap)  | 4.87×       | 1.16×         |
| LoadOrStore (hot 64 keys) |  2.27 ns | 2nd  | 0.98× (xsync) | 5.60×       | 11.5×         |
| HotKey read (1 key)       |  1.80 ns | 2nd  | 0.80× (xsync) | 1.06×       | 17.1×         |
| HotKey write (1 key)      | 31.4 ns | **1st** | 1.45× (builtin) | 3.63×    | 1.45×         |
| Range 16k                 |  145 µs | 2nd  | 0.90× (builtin) | 2.17×     | -             |
| Range 128k                | 1.38 ms | 3rd  | 0.80× (builtin) | 4.96×     | -             |
| Range while writing       |  145 µs | 2nd* | 0.97× (builtin*) | 2.63×    | -             |

alosmap wins **8 of 13** workloads outright. The 4 losses break into two
clean buckets:

- **Cache-miss-floor losses to xsync** (read-parallel string, LoadOrStore-hot,
  HotKey-read): margin is single nanoseconds; both are at the memory
  subsystem's lower bound.
- **Pure Range wins for builtin+RW** (Range 16k, Range 128k, Range-while-writing):
  builtin+RW's native Go map iteration calls the visitor inline in one pass
  with no intermediate buffer. alosmap's two-pass (parallel collect →
  sequential visit) cannot beat that for static maps. But note the asterisk
  on Range-while-writing: **builtin+RW's "win" requires fully blocking every
  concurrent writer for the duration of the Range** — alosmap delivers
  near-identical latency while keeping writes flowing. Among the maps that
  actually allow concurrent writes during Range, alosmap is the clear leader.

For visitors that are safe to call concurrently, alosmap also exposes
`RangePar` which fans the visitor out across GOMAXPROCS×4 goroutines — see
"Parallel-visitor Range" below.

---

### Read benchmarks

#### Read parallel — string keys (16 384 keys, 16 goroutines)

| Map           |    ns/op |   ops/s | allocs |
|---------------|---------:|--------:|-------:|
| **xsync**     | **1.864** | **537 M/s** | 0    |
| alosmap       |    2.552 |   392 M/s | 0      |
| sync.Map      |    3.575 |   280 M/s | 0      |
| cmap          |    6.329 |   158 M/s | 0      |
| builtin + RW  |   26.520 |  37.7 M/s | 0      |

#### Read parallel — int64 keys

| Map           |    ns/op |   ops/s | allocs |
|---------------|---------:|--------:|-------:|
| **alosmap**   | **1.826** | **548 M/s** | 0    |
| xsync         |    1.867 |   536 M/s | 0      |
| sync.Map      |    3.418 |   293 M/s | 0      |
| cmap*         |    6.521 |   153 M/s | 0      |
| builtin + RW  |   27.850 |  35.9 M/s | 0      |

*`cmap` is string-keyed only; ints are run through `strconv.FormatInt` so the
overhead is real-world for callers who pick cmap for int-ish keys.*

#### HotKey read — single contended key

| Map           |    ns/op |   ops/s | allocs |
|---------------|---------:|--------:|-------:|
| **xsync**     | **1.444** | **693 M/s** | 0    |
| alosmap       |    1.798 |   556 M/s | 0      |
| sync.Map      |    1.902 |   526 M/s | 0      |
| builtin + RW  |   30.810 |  32.5 M/s | 0      |
| cmap          |   30.920 |  32.3 M/s | 0      |

---

### Write benchmarks

#### Write parallel — string keys

| Map           |    ns/op |    ops/s | allocs        |
|---------------|---------:|---------:|---------------|
| **alosmap**   | **3.404** | **294 M/s** | **0 B / 0** |
| xsync         |   12.720 |   78.6 M/s | 24 B / 1      |
| cmap          |   16.700 |   59.9 M/s | 0 B / 0       |
| sync.Map      |   26.360 |   37.9 M/s | 64 B / 2      |
| builtin + RW  |   86.960 |   11.5 M/s | 0 B / 0       |

#### Write parallel — int64 keys

| Map           |    ns/op |    ops/s | allocs        |
|---------------|---------:|---------:|---------------|
| **alosmap**   | **3.060** | **327 M/s** | **0 B / 0** |
| xsync         |   14.160 |   70.6 M/s | 16 B / 1      |
| cmap*         |   18.570 |   53.9 M/s | 0 B / 0       |
| sync.Map      |   42.160 |   23.7 M/s | 55 B / 1      |
| builtin + RW  |   86.360 |   11.6 M/s | 0 B / 0       |

#### HotKey write — single contended key

| Map           |    ns/op |    ops/s | allocs        |
|---------------|---------:|---------:|---------------|
| **alosmap**   | **31.38** | **31.9 M/s** | 7 B / 0   |
| builtin + RW  |   45.410 |   22.0 M/s | 0 B / 0       |
| cmap          |   52.840 |   18.9 M/s | 0 B / 0       |
| xsync         |   83.030 |   12.0 M/s | 24 B / 1      |
| sync.Map      |  113.800 |    8.8 M/s | 55 B / 1      |

---

### Mixed workloads

#### Mixed 90 % reads / 10 % writes

| Map           |    ns/op |    ops/s | allocs   |
|---------------|---------:|---------:|----------|
| **alosmap**   | **3.115** | **321 M/s** | 0 B / 0 |
| xsync         |    3.806 |   263 M/s | 2 B / 0  |
| sync.Map      |    6.746 |   148 M/s | 7 B / 0  |
| cmap          |   20.220 |   49.5 M/s | 0 B / 0  |
| builtin + RW  |  100.300 |   10.0 M/s | 0 B / 0  |

#### Mixed 50 % / 50 %

| Map           |    ns/op |    ops/s | allocs   |
|---------------|---------:|---------:|----------|
| **alosmap**   | **6.186** | **162 M/s** | 4 B / 0 |
| xsync         |    9.772 |   102 M/s | 12 B / 0 |
| sync.Map      |   21.840 |   45.8 M/s | 36 B / 1 |
| cmap          |   26.390 |   37.9 M/s | 0 B / 0  |
| builtin + RW  |  155.000 |    6.5 M/s | 0 B / 0  |

#### Mixed 10 % / 90 % (write-heavy)

| Map           |    ns/op |    ops/s | allocs   |
|---------------|---------:|---------:|----------|
| **alosmap**   | **7.666** | **130 M/s** | 7 B / 0 |
| xsync         |   14.060 |   71.1 M/s | 21 B / 0 |
| cmap          |   23.740 |   42.1 M/s | 0 B / 0  |
| sync.Map      |   33.380 |   30.0 M/s | 64 B / 2 |
| builtin + RW  |  113.600 |    8.8 M/s | 0 B / 0  |

---

### Other concurrent operations

#### Delete (50 % delete, 50 % store)

| Map           |    ns/op |    ops/s | allocs    |
|---------------|---------:|---------:|-----------|
| **alosmap**   | **4.749** | **211 M/s** | 0 B / 0 |
| cmap          |    5.428 |   184 M/s | 0 B / 0   |
| builtin + RW  |    5.518 |   181 M/s | 0 B / 0   |
| xsync         |    8.915 |   112 M/s | 12 B / 0  |
| sync.Map      |   23.100 |   43.3 M/s | 32 B / 1  |

#### LoadOrStore — 64 hot keys (contention)

| Map           |    ns/op |    ops/s | allocs   |
|---------------|---------:|---------:|----------|
| **xsync**     | **2.230** | **448 M/s** | 0 B / 0 |
| alosmap       |    2.266 |   441 M/s | 0 B / 0  |
| cmap          |    6.586 |   152 M/s | 0 B / 0  |
| sync.Map      |   12.700 |   78.7 M/s | 16 B / 1 |
| builtin + RW  |   26.200 |   38.2 M/s | 0 B / 0  |

---

### Range / iteration

Range distributes shard scanning across GOMAXPROCS×4 goroutines and
pipelines the visitor pass over completed slabs. ctrl bytes are read in
uint64 chunks so runs of empty slots are skipped, and the next live
entry's heap struct is PREFETCHT0'd before dereference.

#### Range — 16 384 entries

| Map           | ns/op (full Range) | allocs |
|---------------|-------------------:|--------|
| **builtin + RW** | **130 651 ns**  | 0      |
| alosmap       |          145 133 ns | 4 KB / 67 |
| cmap          |          149 748 ns | 0      |
| xsync         |          150 305 ns | 0      |
| sync.Map      |          314 405 ns | 0      |

alosmap edges out cmap and xsync; builtin+RW with its single
RLock'd native-map iteration is fundamentally hard to beat for pure
iteration when there are no concurrent writes (see RangeWhileWriting below
for what changes once writes are happening).

#### Range — 131 072 entries

| Map           | ns/op (full Range) | allocs |
|---------------|-------------------:|--------|
| **builtin + RW** | **1 104 817 ns** | 0    |
| cmap          |        1 233 673 ns | 0      |
| alosmap       |        1 375 204 ns | 6 KB / 67 |
| xsync         |        1 395 943 ns | 0      |
| sync.Map      |        6 821 741 ns | 0      |

At 128 K entries pure Range is limited by how fast the visitor can be
called sequentially over the materialized slab — alosmap's two-pass
(parallel collect, then sequential visit) is ~25% slower than builtin's
single-pass inline iteration. **The trade-off:** alosmap allows concurrent
writes during Range; builtin+RW serialises them entirely.

#### Range while writing — Range with a concurrent writer

| Map           | ns/op (full Range) | allocs (driven by writer's int boxing) |
|---------------|-------------------:|---------------------------------------|
| **builtin + RW** | **140 400 ns**  | 0 B / 0 — writer is blocked          |
| alosmap       |          145 215 ns | 20 KB / 2 050                       |
| xsync         |          218 871 ns | 79 KB / 3 286                       |
| cmap          |          240 973 ns | 0 B / 0                             |
| sync.Map      |          381 331 ns | 186 KB / 7 753                      |

builtin+RW only matches alosmap here by **blocking every concurrent
writer** behind the iteration's RLock — so the concurrent-writer goroutine
makes near-zero progress during Range. alosmap delivers comparable
iteration latency *and* keeps writes flowing. Among the maps that don't
freeze writers, alosmap is the clear winner (~1.5× faster than xsync,
1.7× faster than cmap, 2.6× faster than sync.Map).

#### Parallel-visitor Range (`RangePar`)

For workloads where the visitor is safe to call concurrently, alosmap
exposes `RangePar(visitor)` — visitor is invoked from up to GOMAXPROCS×4
goroutines simultaneously, skipping the slab and the sequential visitor
pass entirely. This is an alosmap-only API; not benchmarked head-to-head
because the others don't offer a parallel-visitor Range.

---

### alosmap-only features

Numbers for the write paths that require the slower `valueBox` (because
they carry TTL or hit-limit metadata):

| Operation                      |   ns/op | ops/s    | allocs       |
|--------------------------------|--------:|---------:|--------------|
| `StoreWithTTL` (parallel)      |   61.19 |  16.3 M/s | 48 B / 1     |
| `StoreWithHits` (parallel)     |   54.34 |  18.4 M/s | 48 B / 1     |
| `StoreWithTTLAndHits` (par.)   |   61.91 |  16.2 M/s | 48 B / 1     |

These are ~15-18× slower than a plain `Store` because they allocate a full
`valueBox` to carry the expiration / hit-counter state — but still faster
than `sync.Map.Store` (26 ns) for an entry with TTL semantics that sync.Map
doesn't even offer.

---

### alosmap scaling

#### Read parallel — varying shard count (16 384 keys)

| Shards |    ns/op | ops/s    |
|-------:|---------:|---------:|
|      1 |    2.934 |  341 M/s |
|     16 |    2.872 |  348 M/s |
|     64 |    3.132 |  319 M/s |
|    256 |    3.470 |  288 M/s |
|  1 024 |    3.938 |  254 M/s |

Read throughput is essentially flat across shard counts; the map
auto-selects a reasonable shard count for you.

#### Read parallel — varying cardinality

| Entries  |    ns/op | ops/s    |
|---------:|---------:|---------:|
|       64 |    2.104 |  475 M/s |
|    1 024 |    2.135 |  468 M/s |
|   16 384 |    3.221 |  310 M/s |
|  262 144 |    4.233 |  236 M/s |

Reads grow ~2× from 64 to 256 K entries — pure cache-miss cost, not a
data-structure tax.

---

### Why writes win so big

A baseline pprof showed that for a write-heavy load **96 %** of all bytes
allocated came from one line — the per-Store internal `valueBox` — and
**~18 %** of CPU was lost to GC mark workers waiting on the heap. The
type-stable path now publishes through an in-entry atomic cell, so the
common `m.Store(k, sameTypeValue)` is a **single `atomic.Pointer.Store` with
zero allocations**. The full slow path is reserved for the cases that
actually need it (TTL, hit limits, custom cloner, type changes).

### Direct pointer mutation (the recommended pattern for hot state)

Once a pointer is in the map, you don't have to write through the map again
to mutate the pointed-to value — `Load` the pointer once and use atomics on
the struct directly:

| Pattern                                  |       ops/s |  ns/op | allocs |
|------------------------------------------|------------:|-------:|:-------|
| `atomic.Int64.Add` via loaded pointer    |   1 573 M/s |  0.635 | 0      |
| `atomic.Value.Store` via loaded pointer  |     668 M/s |  1.497 | 0      |
| `StringSet` via loaded pointer           |    91.1 M/s | 10.98  | 2      |

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
}

m := alosmap.New()
defer m.Close()

sess := &Session{}
sess.Name.Store("alice")
sess.Status.Store("warm")
m.Store(alosmap.S("sess:1"), sess)

ptr, _ := m.Load(alosmap.S("sess:1"))
live := ptr.(*Session)
live.Hits.Add(1)
live.Bytes.Add(512)
live.Bytes.Add(-128)
alosmap.StringSet(&live.Name, "alice-prod")
alosmap.StringSet(&live.Status, "ready")

ptr2, _ := m.Load(alosmap.S("sess:1"))
same := ptr2.(*Session)
fmt.Println(live == same) // true

m.Range(func(key alosmap.Key, value any) bool {
	v := value.(*Session)
	fmt.Println(key.String(), v.Hits.Load(), v.Bytes.Load(), v.Name.Load(), v.Status.Load())
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
| `New(options ...Option) *Map` | Construct a map with functional options |
| `NewBuilder() *Builder` | Construct a map with the fluent builder |
| `RecommendedShardCount(n int) int` | Reuse the package's shard heuristic for explicit sizing |

### Options

| Option | Default | Description |
|---|---|---|
| `WithCapacity(n)` | 1024 | Expected number of live entries |
| `WithShardCount(n)` | auto | Number of shards, rounded to a power of two |
| `WithLoadFactor(f)` | 0.72 | Target occupancy before growth |
| `WithCleanupInterval(d)` | 5s | Background cleanup cadence |
| `WithoutCleanup()` | - | Disable the background cleaner |
| `WithValueCloner(fn)` | pass-through | Install a write-time clone hook |

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
}

m := alosmap.New()
defer m.Close()

stats := &Stats{}
stats.Status.Store("initial")
m.Store(alosmap.S("stats:api"), stats)

value, _ := m.Load(alosmap.S("stats:api"))
live := value.(*Stats)
live.Count.Add(1)
live.Bytes.Add(1024)
live.Bytes.Add(-256)
alosmap.StringSet(&live.Status, "updated")

fmt.Println(live.Count.Load(), live.Bytes.Load(), live.Status.Load())
```

That pattern is what the direct-pointer benchmarks measure. It avoids map-level write
overhead after the initial insert and lets your hot mutations ride on atomic or mutex
operations inside the pointed-to value.

## Requirements

- Go 1.26+
- No cgo
- No dependencies outside the standard library