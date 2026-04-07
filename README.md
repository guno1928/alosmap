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

- `TestAllCases`: `275/275` `testall.go` cases passed in `0.64s`
- Full package suite: `ok github.com/guno1928/alosmap 0.615s`

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

Benchmarks below were collected with:

```go
go test -bench="." -benchtime=2s -count=1
```

Machine: AMD Ryzen 7 5700X, 16 threads, Windows, Go 1.26.

Focused single-benchmark runs can be slightly faster. The tables below use one full-suite
run so the numbers are comparable to each other.

### String-Key Workloads

| Benchmark | alosmap | sync.Map | builtin map (serial) |
|---|---|---|---|
| Read (parallel) | **230.7 M ops/s**, 4.334 ns/op, 0 alloc | 120.4 M ops/s, 8.304 ns/op, 0 alloc | 35.9 M ops/s, 27.86 ns/op |
| Write (parallel) | **39.0 M ops/s**, 25.67 ns/op, 2 alloc | 29.2 M ops/s, 34.28 ns/op, 3 alloc | 16.2 M ops/s, 61.59 ns/op |
| Mixed 90/10 (parallel) | **118.2 M ops/s**, 8.458 ns/op | 66.4 M ops/s, 15.06 ns/op | 25.8 M ops/s, 38.72 ns/op |
| Mixed 50/50 (parallel) | **75.4 M ops/s**, 13.26 ns/op | 45.7 M ops/s, 21.89 ns/op | - |
| LoadOrStore hot contention | **184.7 M ops/s**, 5.413 ns/op, 1 alloc | 69.6 M ops/s, 14.38 ns/op, 2 alloc | - |
| LoadOrStore cold parallel | **12.0 M ops/s**, 83.12 ns/op, 2 alloc | 9.6 M ops/s, 104.0 ns/op, 4 alloc | - |

### Lifecycle-Specific Workloads

| Benchmark | alosmap |
|---|---|
| Hit-limited read | **235.2 M ops/s**, 4.252 ns/op, 0 alloc |
| TTL + hits write | **33.9 M ops/s**, 29.47 ns/op, 2 alloc |
| Cleanup expired | **16.9 M runs/s**, 59.11 ns/op |

### Int64-Key Workloads

| Benchmark | ops/s | ns/op | allocs |
|---|---|---|---|
| Read (parallel) | **268.8 M** | 3.721 | 0 |
| Write (parallel) | **45.0 M** | 22.23 | 2 |
| Mixed 90/10 | **126.6 M** | 7.900 | 0 |
| Mixed 50/50 | **75.8 M** | 13.19 | 0 |
| LoadOrStore cold | **14.9 M** | 67.19 | 1 |

### Direct Pointer Mutation

| Pattern | ops/s | ns/op | allocs |
|---|---|---|---|
| `atomic.Int64.Add` via loaded pointer | **1,573 M** | 0.6354 | 0 |
| `atomic.Value.Store` via loaded pointer | **667.9 M** | 1.497 | 0 |
| `StringSet` via loaded pointer | **91.1 M** | 10.98 | 2 |

The direct-pointer pattern is the fastest way to mutate shared state after the initial
insert: store a pointer once, then `Load` the pointer and mutate the fields directly.

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