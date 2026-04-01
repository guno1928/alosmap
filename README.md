# alosmap

## About

`alosmap` is a high-performance concurrent Go map for `string` keys and `any` values with lock-free reads, atomic writes, TTL expiry, hit-limited entries, copy-on-write storage, and scale-aware sharding that can be tuned from tiny in-memory workloads to very large capacity plans.

## Import And Use

```bash
go get github.com/guno1928/alosmap
```

```go
import "github.com/guno1928/alosmap"
```

## 15 Examples

1. Create a default map.

Use this when you want the package to choose sensible defaults for capacity, sharding, and cleanup behavior. This is the simplest starting point for most projects.

```go
store := alosmap.New()
defer store.Close()

store.Store("status", "ready")
value, ok := store.Load("status")
```

2. Store, load, and delete a string.

Use this for the most basic key/value workflow: write a value under a string key, read it back, and remove it when you no longer need it.

```go
store.Store("name", "Alyx")
value, ok := store.Load("name")
removed, deleted := store.Delete("name")
```

3. Store, read, and update a struct.

Use this when you want to keep typed application data like users, sessions, or configs in the map and then replace it with a newer version.

```go
type User struct {
    Name string
    Age  int
}

store.Store("user:1", User{Name: "Alyx", Age: 29})
value, ok := store.Load("user:1")
previous, loaded := store.Swap("user:1", User{Name: "Alyx", Age: 30})
```

4. Store a cache entry with TTL and clean it up.

Use this for values that should expire automatically after a fixed amount of time, such as sessions, temporary tokens, or cache entries. `CleanupNow()` lets you remove expired entries immediately instead of waiting for the background cleaner.

```go
store.StoreWithTTL("session:1", "live", 30*time.Second)
value, ok := store.Peek("session:1")
store.CleanupNow()
```

5. Store a value with a fixed number of reads.

Use this when a value should only survive a fixed number of successful reads. A common use case is one-time or limited-use tokens.

```go
store.StoreWithHits("ticket:1", "open", 3)
first, ok1 := store.Load("ticket:1")
second, ok2 := store.Load("ticket:1")
third, ok3 := store.Load("ticket:1")
_, existsAfterHits := store.Load("ticket:1")
```

6. Store with both TTL and hits.

Use this when you want both expiry rules at once: the entry disappears after enough reads or after enough time, whichever happens first.

```go
store.StoreWithTTLAndHits("token:1", "one-time", time.Minute, 2)
value, ok := store.Load("token:1")
stillThere, okAgain := store.Load("token:1")
_, existsAfterLimit := store.Load("token:1")
```

7. Peek and check existence without consuming hits.

Use this when you want to inspect a hit-limited entry without spending one of its remaining reads.

```go
store.StoreWithHits("token:1", "preview", 2)
value, ok := store.Peek("token:1")
exists := store.Has("token:1")
realLoad, loadOK := store.Load("token:1")
```

8. Load or store an initial value.

Use this when many goroutines may race to initialize the same key and you want only the first writer to win while everyone else gets the stored value.

```go
value, loaded := store.LoadOrStore("counter", 1)
sameValue, loadedAgain := store.LoadOrStore("counter", 99)
```

9. Replace a value and keep the previous one.

Use this when you want to replace a value and also know what was there before, such as updating state while keeping the previous version for logging or decisions.

```go
store.Store("mode", "safe")
previous, loaded := store.Swap("mode", "fast")
current, ok := store.Load("mode")
```

10. Update only if the current value still matches.

Use this for conditional updates under concurrency. It only changes the value if the current value still matches what you expect.

```go
store.Store("mode", "fast")
swapped := store.CompareAndSwap("mode", "fast", "turbo")
current, ok := store.Load("mode")
```

11. Delete unconditionally or delete only when the current value matches.

Use `Delete` for a normal unconditional remove. Use `CompareAndDelete` when you only want to remove the key if the current value is still the one you expect.

```go
store.Store("mode", "turbo")
deletedValue, deleted := store.Delete("mode")

store.Store("mode", "turbo")
deletedSafely := store.CompareAndDelete("mode", "turbo")
```

12. Range over all live entries.

Use this when you want to scan the current contents of the map for reporting, exporting, maintenance, or debugging.

```go
store.Store("a", 1)
store.Store("b", 2)
total := 0
store.Range(func(key string, value any) bool {
    fmt.Println(key, value)
    total += value.(int)
    return true
})
```

13. Take a snapshot you can use outside the map.

Use this when you want a slice copy of the current live entries that you can sort, return from an API, or work with outside the map.

```go
store.Store("user:1", "Alyx")
store.Store("user:2", "Gordon")
snapshot := store.Snapshot()
first := snapshot[0]
```

14. Force cleanup and read stats.

Use this when you do not want to wait for the background cleanup cycle and want expired or dead entries removed immediately, then inspect the current live metrics.

```go
store.StoreWithTTL("session:1", "live", time.Second)
store.CleanupNow()
stats := store.Stats()
```

15. Tune for a very large capacity plan.

Use this when you already know the map will hold a very large number of entries and you want the initial sizing and shard count to match that workload instead of growing from small defaults.

```go
store := alosmap.New(
    alosmap.WithCapacity(400_000_000),
    alosmap.WithShardCount(alosmap.RecommendedShardCount(400_000_000)),
)
defer store.Close()

store.StoreWithTTLAndHits("job:1", "queued", 10*time.Minute, 5)
value, ok := store.Load("job:1")
```

## Benchmarks

For people who just want the big picture:

| Workload | alosmap | sync.Map | plain Go map |
| --- | --- | --- | --- |
| Reads | about 149 million reads per second | about 130 million reads per second | about 42 million reads per second |
| Writes | about 39 million writes per second | about 34 million writes per second | about 18 million writes per second |
| Mixed 90/10 | about 84 million mixed operations per second | about 79 million mixed operations per second | not measured in concurrent mode |
| Mixed 50/50 | about 61 million mixed operations per second | about 54 million mixed operations per second | about 29 million mixed operations per second |
| LoadOrStore contention | about 46 million successful contested operations per second | about 74 million successful contested operations per second | not applicable |
| TTL + hits store | about 35 million TTL-and-hit-limited writes per second | not built in | not built in |
| Hit-limited reads | about 218 million hit-limited reads per second | not built in | not built in |
| Cleanup passes | about 17.6 million cleanup passes per second | not built in | not built in |

Plain-language notes:

- `alosmap` is faster than `sync.Map` in the measured read, write, and mixed workload benchmarks on this machine.
- `sync.Map` was faster than `alosmap` in the `LoadOrStore` contention benchmark on this machine.
- Plain Go `map` is not thread-safe, so its numbers here are single-thread baselines, not concurrent comparisons.
- TTL, hit limits, and cleanup are `alosmap` features, so there is no direct built-in equivalent row for `sync.Map` or plain `map`.

For people who want the detailed Go benchmark metrics:

| Benchmark | Throughput | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| alosmap read parallel | 148,788,016 ops/s | 6.721 ns/op | 0 B/op | 0 allocs/op |
| alosmap write parallel | 39,078,967 ops/s | 25.59 ns/op | 56 B/op | 2 allocs/op |
| alosmap mixed 90/10 | 84,079,284 ops/s | 11.89 ns/op | 14 B/op | 0 allocs/op |
| alosmap mixed 50/50 | 61,117,905 ops/s | 16.36 ns/op | 28 B/op | 1 allocs/op |
| alosmap LoadOrStore contention | 46,022,527 ops/s | 21.73 ns/op | 56 B/op | 2 allocs/op |
| alosmap hit-limited read | 218,174,319 ops/s | 4.583 ns/op | 0 B/op | 0 allocs/op |
| alosmap cleanup expired | 17,599,148 runs/s | 56.82 ns/op | 0 B/op | 0 allocs/op |
| alosmap StoreWithTTLAndHits | 34,905,875 ops/s | 28.65 ns/op | 56 B/op | 2 allocs/op |
| sync.Map read parallel | 130,421,517 ops/s | 7.667 ns/op | 0 B/op | 0 allocs/op |
| sync.Map write parallel | 34,439,865 ops/s | 29.04 ns/op | 72 B/op | 3 allocs/op |
| sync.Map mixed 90/10 | 78,759,814 ops/s | 12.70 ns/op | 17 B/op | 0 allocs/op |
| sync.Map mixed 50/50 | 54,147,239 ops/s | 18.47 ns/op | 36 B/op | 1 allocs/op |
| sync.Map LoadOrStore contention | 73,780,692 ops/s | 13.55 ns/op | 24 B/op | 2 allocs/op |
| plain Go map read serial | 42,379,745 ops/s | 23.60 ns/op | 0 B/op | 0 allocs/op |
| plain Go map write serial | 18,116,200 ops/s | 55.20 ns/op | 8 B/op | 1 allocs/op |
| plain Go map mixed serial | 28,652,949 ops/s | 34.90 ns/op | 3 B/op | 0 allocs/op |

Raw benchmark output:

```text
goos: windows
goarch: amd64
pkg: github.com/guno1928/alosmap
cpu: AMD Ryzen 7 5700X 8-Core Processor
BenchmarkMapReadParallel-16                     175853419                6.721 ns/op     148788016 ops/s               0 B/op          0 allocs/op
BenchmarkMapWriteParallel-16                    40219194                25.59 ns/op       39078967 ops/s              56 B/op          2 allocs/op
BenchmarkMapMixedParallel/90_10-16              91806288                11.89 ns/op       84079284 ops/s              14 B/op          0 allocs/op
BenchmarkMapMixedParallel/50_50-16              75906128                16.36 ns/op       61117905 ops/s              28 B/op          1 allocs/op
BenchmarkMapLoadOrStoreContention-16            54000054                21.73 ns/op       46022527 ops/s              56 B/op          2 allocs/op
BenchmarkMapHitLimitedReadParallel-16           269028135                4.583 ns/op     218174319 ops/s               0 B/op          0 allocs/op
BenchmarkMapCleanupExpired-16                   20590501                56.82 ns/op       17599148 runs/s              0 B/op          0 allocs/op
BenchmarkMapStoreWithTTLAndHitsParallel-16      38214003                28.65 ns/op       34905875 ops/s              56 B/op          2 allocs/op
BenchmarkSyncMapReadParallel-16                 160681330                7.667 ns/op     130421517 ops/s               0 B/op          0 allocs/op
BenchmarkSyncMapWriteParallel-16                34983484                29.04 ns/op       34439865 ops/s              72 B/op          3 allocs/op
BenchmarkSyncMapMixedParallel/90_10-16          90599541                12.70 ns/op       78759814 ops/s              17 B/op          0 allocs/op
BenchmarkSyncMapMixedParallel/50_50-16          65220934                18.47 ns/op       54147239 ops/s              36 B/op          1 allocs/op
BenchmarkSyncMapLoadOrStoreContention-16        91002160                13.55 ns/op       73780692 ops/s              24 B/op          2 allocs/op
BenchmarkBuiltinMapReadSerial-16                49379870                23.60 ns/op       42379745 ops/s               0 B/op          0 allocs/op
BenchmarkBuiltinMapWriteSerial-16               21606710                55.20 ns/op       18116200 ops/s               8 B/op          1 allocs/op
BenchmarkBuiltinMapMixedSerial-16               35683274                34.90 ns/op       28652949 ops/s               3 B/op          0 allocs/op
PASS
ok      github.com/guno1928/alosmap     21.387s
```