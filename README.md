# alosmap

> **testall.go passed 200/200 tests** (including race detector). Run `go test -count=1 -race -timeout 120s ./...` to verify.

## About

`alosmap` is a high-performance concurrent Go map for `string` keys and `any` values with lock-free reads, atomic writes, TTL expiry, hit-limited entries, pointer-based mutable access, and scale-aware sharding.

Values are stored by reference. When you store a pointer, `Load` returns the same pointer, so you mutate the object directly with atomics or your own locks. For standalone numeric values, the built-in `Add`, `Sub`, and `Set` methods provide lock-free atomic updates via an internal CAS loop. For string fields inside stored pointers, use plain `atomic.Value` and call `alosmap.StringSet(&field, value)` when you want cloned string writes.

## Install

```bash
go get github.com/guno1928/alosmap
```

```go
import "github.com/guno1928/alosmap"
```

## 30 Examples

These examples are meant to look like real usage, not toy one-liners. Each snippet assumes `import "github.com/guno1928/alosmap"` plus the obvious standard library imports used in that snippet, such as `fmt`, `sort`, `strings`, `sync`, `sync/atomic`, and `time`.

Every write path clones the key string before publishing it, so keys built from short-lived buffers or temporary conversions do not leave caller-owned backing storage pinned inside the map.

### 1. Start with the default map for shared process state

Use the defaults when you just need a fast concurrent map and do not have workload numbers yet.

```go
store := alosmap.New()
defer store.Close()

store.Store("build.version", "1.4.7")
store.Store("feature.checkout", true)

version, _ := store.Load("build.version")
enabled, _ := store.Load("feature.checkout")
fmt.Println(version, enabled)
```

### 2. Tune a hot cache with the builder

The builder reads well when you want to set several knobs at once and keep the configuration close together.

```go
store := alosmap.NewBuilder().
    Capacity(500_000).
    Shards(64).
    LoadFactor(0.82).
    CleanupInterval(5 * time.Second).
    Build()
defer store.Close()

store.Store("cache:primed", true)
```

### 3. Pre-size a very large map up front

If you already know the expected entry count, pre-sizing avoids early resize churn and gives you a good shard count immediately.

```go
expectedUsers := 40_000_000

store := alosmap.New(
    alosmap.WithCapacity(expectedUsers),
    alosmap.WithShardCount(alosmap.RecommendedShardCount(expectedUsers)),
    alosmap.WithoutCleanup(),
)
defer store.Close()

store.Store("bootstrap.complete", time.Now().UTC())
```

### 4. Store, load, and fall back for ordinary values

This is the basic path for configuration, feature flags, and other scalar state.

```go
store := alosmap.New()
defer store.Close()

store.Store("user:42:name", "Alyx")
store.Store("user:42:plan", "pro")

name, _ := store.Load("user:42:name")
plan, _ := store.Get("user:42:plan")
timezone := "UTC"
if value, ok := store.Load("user:42:timezone"); ok {
    timezone = value.(string)
}

fmt.Println(name, plan, timezone)
```

### 5. Preview a hit-limited value without consuming it

`Peek` and `Has` are useful when you want to inspect or gate access before actually spending a limited-use entry.

```go
store := alosmap.New()
defer store.Close()

store.StoreWithHits("invite:preview", "alpha", 2)

preview, _ := store.Peek("invite:preview")
existsBefore := store.Has("invite:preview")
missingPreview, missingOK := store.Peek("invite:missing")

first, _ := store.Load("invite:preview")
second, _ := store.Load("invite:preview")
existsAfter := store.Has("invite:preview")

fmt.Println(preview, missingPreview, missingOK, existsBefore, first, second, existsAfter)
```

### 6. Keep sessions alive with TTL and clean them on demand

TTL is a natural fit for sessions, temporary cache lines, and background refresh work.

```go
store := alosmap.New()
defer store.Close()

store.StoreWithTTL("session:42", map[string]any{
    "user": "alice",
    "role": "admin",
}, 30*time.Second)

value, _ := store.Peek("session:42")
fmt.Println(value)

time.Sleep(31 * time.Second)
store.CleanupNow()

_, stillLive := store.Peek("session:42")
fmt.Println(stillLive)
```

### 7. Hand out a fixed number of download attempts

Hit limits are ideal for one-time links, invite codes, or capped retries.

```go
store := alosmap.New()
defer store.Close()

store.StoreWithHits("download:artifact-99", "https://cdn.example/artifact-99", 3)

for attempt := 1; attempt <= 4; attempt++ {
    value, ok := store.Load("download:artifact-99")
    fmt.Println("attempt", attempt, "ok", ok, "value", value)
}
```

### 8. Use the TTL-plus-hits alias for one-time codes

`SetWithTTLAndHits` is the same behavior as `StoreWithTTLAndHits`, just with a setter-style name that some codebases prefer.

```go
store := alosmap.New()
defer store.Close()

store.SetWithTTLAndHits("otp:alice", "493102", 45*time.Second, 2)

code1, ok1 := store.Load("otp:alice")
code2, ok2 := store.Load("otp:alice")
code3, ok3 := store.Load("otp:alice")

fmt.Println(code1, ok1, code2, ok2, code3, ok3)
```

### 9. Reuse one `EntryOptions` value across many keys

If several entries share the same lifecycle policy, build the options once and apply it everywhere.

```go
store := alosmap.New()
defer store.Close()

warmSession := alosmap.EntryOptions{
    TTL:  20 * time.Minute,
    Hits: 100,
}

for _, key := range []string{"session:100", "session:101", "session:102"} {
    store.StoreWithOptions(key, map[string]any{"state": "warm"}, warmSession)
}
```

### 10. Create a shared pointer once with `LoadOrStore`

This is the standard lazy-singleton pattern for per-key mutable state.

```go
type ClientState struct {
    Hits atomic.Int64
}

store := alosmap.New()
defer store.Close()

actual, loaded := store.LoadOrStore("client:payments", &ClientState{})
state := actual.(*ClientState)

if !loaded {
    fmt.Println("created new state")
}

state.Hits.Add(1)
```

### 11. Let the existing value win during racing initialization

If another goroutine already published a value, `LoadOrStore` returns that value and ignores your candidate.

```go
store := alosmap.New()
defer store.Close()

first, _ := store.LoadOrStore("report:daily", "generated-by-worker-1")
second, loaded := store.LoadOrStore("report:daily", "generated-by-worker-2")
current, _ := store.Load("report:daily")

fmt.Println(first, second, loaded, current)
```

### 12. Lazily create a short-lived entry with `LoadOrStoreWithOptions`

Use the options-aware form when the first insert should already have expiry or hit rules.

```go
type WarmupJob struct {
    StartedAt time.Time
}

store := alosmap.New()
defer store.Close()

job, loaded := store.LoadOrStoreWithOptions(
    "warmup:search-index",
    &WarmupJob{StartedAt: time.Now()},
    alosmap.EntryOptions{TTL: 2 * time.Minute},
)

fmt.Println(loaded, job.(*WarmupJob).StartedAt)
```

### 13. Swap a configuration and inspect the previous version

`Swap` is useful when you want atomic replacement plus access to the old payload for logging or rollback decisions.

```go
type FeatureConfig struct {
    Mode    string
    Rollout int
}

store := alosmap.New()
defer store.Close()

store.Store("config:checkout", FeatureConfig{Mode: "safe", Rollout: 10})

previous, _ := store.Swap("config:checkout", FeatureConfig{Mode: "full", Rollout: 100})
current, _ := store.Load("config:checkout")

fmt.Println(previous, current)
```

### 14. Rotate a lease and attach new expiry in one step

`SwapWithOptions` replaces the value and refreshes the entry policy at the same time.

```go
store := alosmap.New()
defer store.Close()

store.Store("lease:worker-7", "held-by-node-a")

previous, _ := store.SwapWithOptions(
    "lease:worker-7",
    "held-by-node-b",
    alosmap.EntryOptions{TTL: 45 * time.Second},
)

current, _ := store.Load("lease:worker-7")
fmt.Println(previous, current)
```

### 15. Guard state transitions with `CompareAndSwap`

This is the clean path for job lifecycles, workflow steps, and other explicit state machines.

```go
store := alosmap.New()
defer store.Close()

store.Store("job:77", "queued")

started := store.CompareAndSwap("job:77", "queued", "running")
finished := store.CompareAndSwap("job:77", "running", "done")
current, _ := store.Load("job:77")

fmt.Println(started, finished, current)
```

### 16. Acquire a short lease with `CompareAndSwapWithOptions`

This version is convenient when a successful transition also needs a TTL.

```go
store := alosmap.New()
defer store.Close()

store.Store("db:migration-lock", "free")

locked := store.CompareAndSwapWithOptions(
    "db:migration-lock",
    "free",
    "held-by-node-a",
    alosmap.EntryOptions{TTL: 30 * time.Second},
)

current, _ := store.Load("db:migration-lock")
fmt.Println(locked, current)
```

### 17. Remove entries unconditionally or only if they match

Use `Delete` when anything may be removed, and `CompareAndDelete` when you only want to clear a known state.

```go
store := alosmap.New()
defer store.Close()

store.Store("job:cleanup", "done")
removed, deleted := store.Delete("job:cleanup")

store.Store("job:cleanup", "stale")
deletedIfMatch := store.CompareAndDelete("job:cleanup", "stale")

fmt.Println(removed, deleted, deletedIfMatch)
```

### 18. Aggregate live values with `Range`

`Range` is a good fit for lightweight reporting when you do not need a materialized snapshot.

```go
store := alosmap.New()
defer store.Close()

store.Store("orders:paid", 41)
store.Store("orders:pending", 7)
store.Store("orders:failed", 2)

total := 0
store.Range(func(key string, value any) bool {
    if strings.HasPrefix(key, "orders:") {
        total += value.(int)
    }
    return true
})

fmt.Println(total)
```

### 19. Stop a `Range` as soon as you find what you need

Returning `false` lets you short-circuit a scan instead of traversing the whole map.

```go
type BackendStatus struct {
    Healthy bool
}

store := alosmap.New()
defer store.Close()

store.Store("api-a", BackendStatus{Healthy: true})
store.Store("api-b", BackendStatus{Healthy: false})
store.Store("api-c", BackendStatus{Healthy: true})

firstUnhealthy := ""
store.Range(func(key string, value any) bool {
    if !value.(BackendStatus).Healthy {
        firstUnhealthy = key
        return false
    }
    return true
})

fmt.Println(firstUnhealthy)
```

### 20. Take a snapshot and sort it for stable output

`Snapshot` is useful when another layer wants a slice that can be sorted, encoded, or handed off.

```go
store := alosmap.New()
defer store.Close()

store.Store("user:3", "moss")
store.Store("user:1", "alyx")
store.Store("user:2", "gordon")

snapshot := store.Snapshot()
sort.Slice(snapshot, func(i int, j int) bool {
    return snapshot[i].Key < snapshot[j].Key
})

for _, pair := range snapshot {
    fmt.Println(pair.Key, pair.Value)
}
```

### 21. Measure size with `Len` and reset with `Clear`

This is useful in tests, admin endpoints, and manual cache resets.

```go
store := alosmap.New()
defer store.Close()

store.Store("a", 1)
store.Store("b", 2)
store.Store("c", 3)

before := store.Len()
store.Clear()
after := store.Len()

fmt.Println(before, after)
```

### 22. Inspect operational counters with `Stats`

`Stats` gives you a point-in-time view of occupancy, maintenance work, and memory estimates.

```go
store := alosmap.New(
    alosmap.WithCapacity(1_000_000),
    alosmap.WithShardCount(alosmap.RecommendedShardCount(1_000_000)),
)
defer store.Close()

store.Store("user:1", "alyx")
store.Store("user:2", "gordon")

stats := store.Stats()
fmt.Println(
    stats.LiveEntries,
    stats.Shards,
    stats.SlotCapacity,
    stats.ResizeCount,
    stats.EstimatedResidentBytes,
)
```

### 23. Use numeric modifiers for top-level counters

`Add`, `Sub`, and `Set` are the simplest way to update plain numeric values without storing a pointer type.

```go
store := alosmap.New()
defer store.Close()

store.Store("rate-limit:requests", int64(100))

afterAdd, _ := store.Add("rate-limit:requests", int64(25))
afterSub, _ := store.Sub("rate-limit:requests", int64(10))
_ = store.Set("rate-limit:requests", int64(0))

current, _ := store.Load("rate-limit:requests")
fmt.Println(afterAdd, afterSub, current)
```

### 24. Keep fast counters inside a stored pointer

For frequently updated metrics, do one map lookup and then mutate your own atomic fields directly.

```go
type RequestStats struct {
    Count atomic.Int64
    Bytes atomic.Int64
}

store := alosmap.New()
defer store.Close()

store.Store("ip:203.0.113.10", &RequestStats{})

value, _ := store.Load("ip:203.0.113.10")
stats := value.(*RequestStats)

stats.Count.Add(1)
stats.Bytes.Add(4096)
fmt.Println(stats.Count.Load(), stats.Bytes.Load())
```

### 25. Use `atomic.Value` for mutable string fields

This is the plain Go pattern: keep the field as `atomic.Value` and load it back as a string.

```go
type UserProfile struct {
    DisplayName atomic.Value
}

store := alosmap.New()
defer store.Close()

store.Store("user:42", &UserProfile{})

value, _ := store.Load("user:42")
profile := value.(*UserProfile)

profile.DisplayName.Store("john")
fmt.Println(profile.DisplayName.Load().(string))
```

### 26. Use `StringSet` when the source string should be cloned first

`StringSet` is the one helper for string fields. It writes into an `atomic.Value`, but clones the string first so short-lived substrings do not pin larger backing data.

```go
profile := &struct {
    Name atomic.Value
}{}

large := strings.Repeat("x", 256) + "john"
shortName := large[len(large)-4:]

alosmap.StringSet(&profile.Name, shortName)
current := profile.Name.Load().(string)

fmt.Println(current)
```

### 27. Keep counters and an atomic name on the same stored object

This is the exact pattern for things like per-IP stats: numeric fields use atomics directly, and the name field stays a plain `atomic.Value`.

```go
type ClientStats struct {
    Requests atomic.Int64
    Bytes    atomic.Int64
    Name     atomic.Value
}

store := alosmap.New()
defer store.Close()

actual, _ := store.LoadOrStore("ip:203.0.113.10", &ClientStats{})
stats := actual.(*ClientStats)

stats.Requests.Add(1)
stats.Bytes.Add(2048)
stats.Name.Store("john")
alosmap.StringSet(&stats.Name, "johnny")

fmt.Println(stats.Requests.Load(), stats.Bytes.Load(), stats.Name.Load().(string))
```

### 28. Store a pointer and guard complex fields with your own mutex

For slices, maps, and nested mutable objects, keep synchronization inside the stored value.

```go
type Session struct {
    mu     sync.Mutex
    Tags   []string
    Visits int
}

store := alosmap.New()
defer store.Close()

store.Store("session:abc", &Session{})

value, _ := store.Load("session:abc")
session := value.(*Session)

session.mu.Lock()
session.Tags = append(session.Tags, "returning", "paid")
session.Visits++
session.mu.Unlock()
```

### 29. Clone mutable payloads on write with `WithValueCloner`

Use a custom cloner when callers should not be able to mutate the stored value through the original input object.

```go
cloneValue := func(value any) (any, int64) {
    src := value.([]byte)
    copied := append([]byte(nil), src...)
    return copied, int64(len(copied))
}

store := alosmap.New(alosmap.WithValueCloner(cloneValue))
defer store.Close()

payload := []byte("hello")
store.Store("blob:welcome", payload)

payload[0] = 'j'
stored, _ := store.Load("blob:welcome")

fmt.Println(string(stored.([]byte)))
```

### 30. Build a tiny route analytics table with `LoadOrStore`, atomics, and snapshots

This combines the main pattern the package is designed for: store a pointer once, update it directly, and enumerate later for reporting.

```go
type RouteStats struct {
    Hits     atomic.Int64
    Bytes    atomic.Int64
    LastUser atomic.Value
}

store := alosmap.New(
    alosmap.WithCapacity(1_000_000),
    alosmap.WithShardCount(alosmap.RecommendedShardCount(1_000_000)),
    alosmap.WithCleanupInterval(10 * time.Second),
)
defer store.Close()

record := func(route string, user string, bytes int64) {
    actual, _ := store.LoadOrStore(route, &RouteStats{})
    stats := actual.(*RouteStats)
    stats.Hits.Add(1)
    stats.Bytes.Add(bytes)
    alosmap.StringSet(&stats.LastUser, user)
}

record("/login", "alice", 512)
record("/login", "bob", 768)
record("/healthz", "system", 64)

snapshot := store.Snapshot()
sort.Slice(snapshot, func(i int, j int) bool {
    return snapshot[i].Key < snapshot[j].Key
})

for _, pair := range snapshot {
    stats := pair.Value.(*RouteStats)
    fmt.Println(pair.Key, stats.Hits.Load(), stats.Bytes.Load(), stats.LastUser.Load().(string))
}
```

## Benchmarks

| Workload | alosmap | sync.Map | plain Go map |
| --- | --- | --- | --- |
| Reads | ~133M ops/s | ~119M ops/s | ~35M ops/s |
| Writes | ~36M ops/s | ~28M ops/s | ~16M ops/s |
| Mixed 90/10 | ~89M ops/s | ~70M ops/s | not measured |
| Mixed 50/50 | ~55M ops/s | ~50M ops/s | ~25M ops/s |
| LoadOrStore cold insert | ~18M ops/s | ~12M ops/s | n/a |
| TTL + hits store | ~33M ops/s | n/a | n/a |
| Hit-limited reads | ~174M ops/s | n/a | n/a |
| Cleanup passes | ~17M runs/s | n/a | n/a |
| Direct atomic.Int64.Add | ~1.6B ops/s @ 0.63 ns | n/a | n/a |
| Direct atomic.Value.Store | ~707M ops/s @ 1.41 ns | n/a | n/a |

Notes:

- `alosmap` is faster than `sync.Map` on reads, writes, mixed workloads, and cold `LoadOrStore` inserts.
- `LoadOrStore cold insert` is the full insert path: every operation uses a unique key, so each call does a real insert instead of reusing an existing entry.
- Plain Go `map` is not thread-safe; its numbers are single-thread baselines, not concurrent comparisons.
- TTL, hit limits, cleanup, and numeric modifiers are `alosmap`-only features.

Detailed benchmark summary:

Cold `LoadOrStore` figures below use the median of 8 runs from `go test -bench="LoadOrStoreCold" -benchmem -count=8 -timeout 300s .`. The other rows are from the latest full benchmark sweep.

| Benchmark | ops/s | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| alosmap read parallel | 133,110,897 | 7.51 | 0 | 0 |
| alosmap write parallel | 36,247,830 | 27.59 | 56 | 2 |
| alosmap mixed 90/10 | 88,694,747 | 11.27 | 14 | 0 |
| alosmap mixed 50/50 | 55,047,611 | 18.17 | 27 | 0 |
| alosmap LoadOrStore cold insert | 18,377,194 | 54.42 | 144 | 2 |
| alosmap hit-limited read | 173,827,703 | 5.75 | 0 | 0 |
| alosmap cleanup expired | 17,204,336 | 58.12 | 0 | 0 |
| alosmap StoreWithTTLAndHits | 32,899,618 | 30.40 | 56 | 2 |
| direct atomic.Int64.Add | 1,575,465,589 | 0.63 | 0 | 0 |
| direct atomic.Value.Store | 707,017,751 | 1.41 | 0 | 0 |
| sync.Map read parallel | 118,876,120 | 8.41 | 0 | 0 |
| sync.Map write parallel | 27,841,266 | 35.92 | 72 | 3 |
| sync.Map mixed 90/10 | 70,087,087 | 14.27 | 17 | 0 |
| sync.Map mixed 50/50 | 49,667,333 | 20.13 | 35 | 1 |
| sync.Map LoadOrStore cold insert | 12,295,586 | 81.33 | 157 | 4 |
| builtin map read serial | 35,313,069 | 28.32 | 0 | 0 |
| builtin map write serial | 16,064,329 | 62.25 | 8 | 1 |
| builtin map mixed serial | 25,323,778 | 39.49 | 3 | 0 |

Recent cold LoadOrStore benchmark output:

```
goos: windows
goarch: amd64
pkg: github.com/guno1928/alosmap
cpu: AMD Ryzen 7 5700X 8-Core Processor
BenchmarkMapLoadOrStoreColdParallel-16          27975027                66.09 ns/op       15130816 ops/s             162 B/op          2 allocs/op
BenchmarkMapLoadOrStoreColdParallel-16          46149940                53.50 ns/op       18690314 ops/s             142 B/op          2 allocs/op
BenchmarkMapLoadOrStoreColdParallel-16          44528388                55.33 ns/op       18072256 ops/s             143 B/op          2 allocs/op
BenchmarkMapLoadOrStoreColdParallel-16          50734380                49.52 ns/op       20193939 ops/s             139 B/op          2 allocs/op
BenchmarkMapLoadOrStoreColdParallel-16          28018518                63.29 ns/op       15799932 ops/s             162 B/op          2 allocs/op
BenchmarkMapLoadOrStoreColdParallel-16          41103210                53.24 ns/op       18784259 ops/s             146 B/op          2 allocs/op
BenchmarkMapLoadOrStoreColdParallel-16          44589607                50.22 ns/op       19911919 ops/s             143 B/op          2 allocs/op
BenchmarkMapLoadOrStoreColdParallel-16          42223636                58.66 ns/op       17046154 ops/s             145 B/op          2 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      18260809                74.32 ns/op       13454481 ops/s             154 B/op          4 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      29057589                86.47 ns/op       11565059 ops/s             158 B/op          4 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      25765669                92.23 ns/op       10841914 ops/s             157 B/op          4 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      22436742                84.57 ns/op       11824009 ops/s             156 B/op          4 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      23377321                78.09 ns/op       12805533 ops/s             157 B/op          4 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      20830765                74.79 ns/op       13370429 ops/s             156 B/op          4 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      25759254                66.77 ns/op       14976751 ops/s             157 B/op          4 allocs/op
BenchmarkSyncMapLoadOrStoreColdParallel-16      26121424                91.46 ns/op       10934140 ops/s             158 B/op          4 allocs/op
PASS
ok      github.com/guno1928/alosmap     82.939s
```