package benchmarks

// fair_test.go — realistic, apples-to-apples scenarios. See fair_setup_test.go for
// the harness and the fairness rules. Run everything with:
//
//   go test -run='^$' -bench=BenchmarkFair_ -benchmem -benchtime=200ms -count=3
//
// and the memory tests (these print tables, they are not timed) with:
//
//   go test -run='TestMemoryFootprint|TestChurnFootprint' -v
//
// Reading the columns:
//   - ns/op   : lower is better (per operation).
//   - allocs/op, B/op : allocations and bytes per op. For Churn these include the
//     per-op key formatting (identical for every map) — realistic, since cache keys
//     are usually freshly built; compare the maps to each other, not to zero.
//   - ns/key  : InsertGrow reports this custom metric (the auto ns/op there is per
//     whole-map fill); ns/key is the comparable number.

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	fairWorkSet  = 100_000 // live entries for steady-state read/update/mixed
	fairSeqLen   = 1 << 16 // length of the precomputed access sequence
	fairZipfS    = 1.2     // Zipf skew: a few keys very hot, long tail
	fairInsertN  = 100_000 // distinct keys inserted in the grow benchmark
	fairChurnWin = 10_000  // bounded working set for the churn benchmark
	fairRangeN   = 16_384  // entries iterated in the range benchmark
	fairLORSKeys = 64      // contended key set for LoadOrStore
)

// ---------------------------------------------------------------------------
// Drivers. Each runs the identical workload against every map.

// benchRead: parallel reads of existing keys, following the given access sequence
// (Zipfian = realistic hot-key skew, or uniform). All maps presized.
func benchRead[V any](b *testing.B, mk func(int) V, n int, seq []int) {
	keys := stringKeys(n)
	for _, nm := range allMaps[V]() {
		b.Run(nm.name, func(b *testing.B) {
			m := fill(nm.make(n), keys, mk)
			b.ReportAllocs()
			b.ResetTimer()
			var off int64
			b.RunParallel(func(pb *testing.PB) {
				i := int(atomic.AddInt64(&off, 1)) * 1009 % len(seq) // distinct start per goroutine
				for pb.Next() {
					m.Load(keys[seq[i]])
					if i++; i == len(seq) {
						i = 0
					}
				}
			})
		})
	}
}

// benchUpdate: parallel re-store of EXISTING keys (the in-place update fast path,
// labeled honestly). A new value is supplied each op. Zipfian write targets.
func benchUpdate[V any](b *testing.B, mk func(int) V, n int, seq []int) {
	keys := stringKeys(n)
	for _, nm := range allMaps[V]() {
		b.Run(nm.name, func(b *testing.B) {
			m := fill(nm.make(n), keys, mk)
			b.ReportAllocs()
			b.ResetTimer()
			var off int64
			b.RunParallel(func(pb *testing.PB) {
				i := int(atomic.AddInt64(&off, 1)) * 1009 % len(seq)
				for pb.Next() {
					m.Store(keys[seq[i]], mk(seq[i]))
					if i++; i == len(seq) {
						i = 0
					}
				}
			})
		})
	}
}

// benchMixed: readPct% reads, the rest in-place updates, on a stable key set with
// Zipfian skew. The realistic "hot cache / shared state" pattern.
func benchMixedFair[V any](b *testing.B, readPct int, mk func(int) V, n int, seq []int) {
	keys := stringKeys(n)
	for _, nm := range allMaps[V]() {
		b.Run(nm.name, func(b *testing.B) {
			m := fill(nm.make(n), keys, mk)
			b.ReportAllocs()
			b.ResetTimer()
			var off int64
			b.RunParallel(func(pb *testing.PB) {
				i := int(atomic.AddInt64(&off, 1)) * 1009 % len(seq)
				for pb.Next() {
					k := keys[seq[i]]
					if i%100 < readPct {
						m.Load(k)
					} else {
						m.Store(k, mk(seq[i]))
					}
					if i++; i == len(seq) {
						i = 0
					}
				}
			})
		})
	}
}

// benchInsertGrow: the benchmark the original suite never had — a CONTENDED fill of
// distinct keys into an initially-empty, non-presized map. Pays real key-clone,
// node-allocation, table-growth and shard-lock costs. A fresh map is built per
// outer iteration; only the parallel fill is timed; ns/key is the comparable metric.
func benchInsertGrow[V any](b *testing.B, mk func(int) V, n, procs int) {
	keys := stringKeys(n)
	chunk := (n + procs - 1) / procs
	for _, nm := range allMaps[V]() {
		b.Run(nm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for it := 0; it < b.N; it++ {
				b.StopTimer()
				m := nm.make(0) // NO presize: table growth is part of what we measure
				runtime.GC()    // each fill starts from a clean heap so mid-fill GC is consistent
				b.StartTimer()
				var wg sync.WaitGroup
				for p := 0; p < procs; p++ {
					lo := p * chunk
					hi := lo + chunk
					if hi > n {
						hi = n
					}
					if lo >= hi {
						break
					}
					wg.Add(1)
					go func(lo, hi int) {
						defer wg.Done()
						for j := lo; j < hi; j++ {
							m.Store(keys[j], mk(j))
						}
					}(lo, hi)
				}
				wg.Wait()
				b.StopTimer()
				if got := m.Len(); got != n {
					b.Fatalf("%s: expected %d distinct inserts, Len=%d", nm.name, n, got)
				}
				m = nil
				b.StartTimer()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/key")
		})
	}
}

// benchChurn: cache-eviction pattern. Bounded working set, but every inserted key
// is brand new and every deleted key is retired forever — so tombstones accumulate
// and a never-shrinking table ratchets up. Throughput here; the memory cost is in
// TestChurnFootprint. Per-op key formatting is identical across maps.
func benchChurn[V any](b *testing.B, mk func(int) V, win int) {
	for _, nm := range allMaps[V]() {
		b.Run(nm.name, func(b *testing.B) {
			m := nm.make(win)
			for i := 0; i < win; i++ {
				m.Store("k:"+strconv.Itoa(i), mk(i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			var ctr int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := atomic.AddInt64(&ctr, 1)
					m.Store("k:"+strconv.FormatInt(int64(win)+id, 10), mk(int(id)))
					m.Delete("k:" + strconv.FormatInt(id, 10))
				}
			})
		})
	}
}

// benchLoadOrStore: heavy contention on a small key set — the once-only init / get
// -or-create singleton pattern. After warmup almost every call is a "loaded" hit.
func benchLoadOrStore[V any](b *testing.B, mk func(int) V, nkeys int) {
	keys := stringKeys(nkeys)
	for _, nm := range allMaps[V]() {
		b.Run(nm.name, func(b *testing.B) {
			m := nm.make(nkeys)
			b.ReportAllocs()
			b.ResetTimer()
			var off int64
			b.RunParallel(func(pb *testing.PB) {
				i := int(atomic.AddInt64(&off, 1))
				for pb.Next() {
					m.LoadOrStore(keys[i%nkeys], mk(i))
					i++
				}
			})
		})
	}
}

// benchRange: full iteration of n entries. Sanity-checks that every map visits all
// entries (also catches any iteration-time reaping).
func benchRange[V any](b *testing.B, mk func(int) V, n int) {
	keys := stringKeys(n)
	for _, nm := range allMaps[V]() {
		b.Run(nm.name, func(b *testing.B) {
			m := fill(nm.make(n), keys, mk)
			b.ReportAllocs()
			b.ResetTimer()
			for it := 0; it < b.N; it++ {
				cnt := 0
				m.Range(func(_ string, _ V) bool { cnt++; return true })
				if cnt != n {
					b.Fatalf("%s: visited %d of %d entries", nm.name, cnt, n)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Benchmark entry points.

func BenchmarkFair_Read_ZipfInt(b *testing.B) {
	benchRead[int64](b, makeInt, fairWorkSet, zipfSeq(fairWorkSet, fairSeqLen, fairZipfS))
}
func BenchmarkFair_Read_UniformInt(b *testing.B) {
	benchRead[int64](b, makeInt, fairWorkSet, uniformSeq(fairWorkSet, fairSeqLen))
}
func BenchmarkFair_Read_ZipfPtr(b *testing.B) {
	benchRead[*Session](b, makePtr, fairWorkSet, zipfSeq(fairWorkSet, fairSeqLen, fairZipfS))
}

func BenchmarkFair_Update_ZipfInt(b *testing.B) {
	benchUpdate[int64](b, makeInt, fairWorkSet, zipfSeq(fairWorkSet, fairSeqLen, fairZipfS))
}
func BenchmarkFair_Update_ZipfPtr(b *testing.B) {
	benchUpdate[*Session](b, makePtr, fairWorkSet, zipfSeq(fairWorkSet, fairSeqLen, fairZipfS))
}

func BenchmarkFair_Mixed95Read_ZipfInt(b *testing.B) {
	benchMixedFair[int64](b, 95, makeInt, fairWorkSet, zipfSeq(fairWorkSet, fairSeqLen, fairZipfS))
}
func BenchmarkFair_Mixed50Read_ZipfInt(b *testing.B) {
	benchMixedFair[int64](b, 50, makeInt, fairWorkSet, zipfSeq(fairWorkSet, fairSeqLen, fairZipfS))
}

func BenchmarkFair_InsertGrow_Int(b *testing.B) {
	benchInsertGrow[int64](b, makeInt, fairInsertN, runtime.GOMAXPROCS(0))
}
func BenchmarkFair_InsertGrow_Ptr(b *testing.B) {
	benchInsertGrow[*Session](b, makePtr, fairInsertN, runtime.GOMAXPROCS(0))
}

func BenchmarkFair_Churn_Int(b *testing.B) { benchChurn[int64](b, makeInt, fairChurnWin) }
func BenchmarkFair_Churn_Ptr(b *testing.B) { benchChurn[*Session](b, makePtr, fairChurnWin) }

func BenchmarkFair_LoadOrStore_Int(b *testing.B) { benchLoadOrStore[int64](b, makeInt, fairLORSKeys) }

func BenchmarkFair_Range_Int(b *testing.B) { benchRange[int64](b, makeInt, fairRangeN) }
func BenchmarkFair_Range_Ptr(b *testing.B) { benchRange[*Session](b, makePtr, fairRangeN) }

// ---------------------------------------------------------------------------
// Memory footprint — the dimension throughput hides. These are tests, not
// benchmarks: they build each map, force GC, and report retained heap bytes.

func TestMemoryFootprint(t *testing.T) {
	const N = 500_000
	keys := stringKeys(N)
	t.Logf("retained heap after GC, %d live entries (lower B/entry is leaner):", N)
	reportFootprint[int64](t, "int64", N, keys, makeInt)
	reportFootprint[*Session](t, "*Session (incl. the records)", N, keys, makePtr)
}

func reportFootprint[V any](t *testing.T, label string, n int, keys []string, mk func(int) V) {
	t.Logf("--- %s values ---", label)
	for _, nm := range allMaps[V]() {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		m := fill(nm.make(n), keys, mk)

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		extra := ""
		if ci, ok := m.(capInfoer); ok {
			extra = "  [" + ci.capInfo() + "]"
		}
		t.Logf("  %-13s %13d B  %8.1f B/entry%s", nm.name, held, float64(held)/float64(n), extra)
		runtime.KeepAlive(m)
		m = nil
	}
}

// TestChurnFootprint is the headline memory test: after heavy distinct-key churn on
// a bounded working set, how much heap does each map still hold? Maps that reclaim
// (xsync shrinks, plain map/RWMutex rehashes down) settle near the working set;
// alosmap tombstones and never shrinks its table, so its held heap stays inflated.
func TestChurnFootprint(t *testing.T) {
	const win = 10_000
	const ops = 2_000_000
	t.Logf("cache churn: working set %d, then %d insert+delete ops of DISTINCT keys", win, ops)
	t.Logf("lower held heap = reclaims better; watch alosmap SlotCapacity/Tombstones:")
	for _, nm := range allMaps[int64]() {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		m := nm.make(win)
		for i := 0; i < win; i++ {
			m.Store("k:"+strconv.Itoa(i), int64(i))
		}
		for i := 0; i < ops; i++ {
			m.Store("k:"+strconv.Itoa(win+i), int64(i))
			m.Delete("k:" + strconv.Itoa(i))
		}

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		live := m.Len()
		perLive := 0.0
		if live > 0 {
			perLive = float64(held) / float64(live)
		}
		extra := ""
		if ci, ok := m.(capInfoer); ok {
			extra = "  [" + ci.capInfo() + "]"
		}
		t.Logf("  %-13s live=%-7d held=%11d B  %9.1f B/live%s", nm.name, live, held, perLive, extra)
		runtime.KeepAlive(m)
		m = nil
	}
}
