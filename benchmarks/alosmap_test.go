package benchmarks

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guno1928/alosmap"
)

func BenchmarkAlos_AnyVsTyped(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("any_store", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(alosmap.S(keys[i%benchN]), int64(i))
		}
	})

	b.Run("typed_store", func(b *testing.B) {
		m := filledTyped(keys)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(keys[i%benchN], int64(i))
		}
	})

	b.Run("any_load", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		var sink int64
		for i := 0; i < b.N; i++ {
			if v, ok := m.Load(alosmap.S(keys[i%benchN])); ok {
				sink += v.(int64)
			}
		}
		_ = sink
	})

	b.Run("typed_load", func(b *testing.B) {
		m := filledTyped(keys)
		b.ReportAllocs()
		b.ResetTimer()
		var sink int64
		for i := 0; i < b.N; i++ {
			if v, ok := m.Load(keys[i%benchN]); ok {
				sink += v
			}
		}
		_ = sink
	})
}

func BenchmarkAlos_SequentialLatency(b *testing.B) {
	const val = int64(7)
	skeys := stringKeys(benchN)
	sm := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup())
	im := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup())
	for i := 0; i < benchN; i++ {
		sm.Store(alosmap.S(skeys[i]), val)
		im.Store(alosmap.I(int64(i)), val)
	}
	defer sm.Close()
	defer im.Close()

	b.Run("Load/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sm.Load(alosmap.S(skeys[i%benchN]))
		}
	})
	b.Run("Load/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			im.Load(alosmap.I(int64(i % benchN)))
		}
	})
	b.Run("Store/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sm.Store(alosmap.S(skeys[i%benchN]), val)
		}
	})
	b.Run("Store/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			im.Store(alosmap.I(int64(i%benchN)), val)
		}
	})
	b.Run("Peek/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sm.Peek(alosmap.S(skeys[i%benchN]))
		}
	})
	b.Run("Peek/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			im.Peek(alosmap.I(int64(i % benchN)))
		}
	})
	b.Run("Has/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sm.Has(alosmap.S(skeys[i%benchN]))
		}
	})
	b.Run("Has/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			im.Has(alosmap.I(int64(i % benchN)))
		}
	})
	b.Run("Swap/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sm.Swap(alosmap.S(skeys[i%benchN]), val)
		}
	})
	b.Run("Swap/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			im.Swap(alosmap.I(int64(i%benchN)), val)
		}
	})
	b.Run("CompareAndSwap/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sm.CompareAndSwap(alosmap.S(skeys[i%benchN]), val, val)
		}
	})
	b.Run("CompareAndSwap/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			im.CompareAndSwap(alosmap.I(int64(i%benchN)), val, val)
		}
	})
	b.Run("DeleteReStore/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			k := alosmap.S(skeys[i%benchN])
			sm.Delete(k)
			sm.Store(k, val)
		}
	})
	b.Run("DeleteReStore/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			k := alosmap.I(int64(i % benchN))
			im.Delete(k)
			im.Store(k, val)
		}
	})
	b.Run("LoadOrStore/string", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sm.LoadOrStore(alosmap.S(skeys[i%benchN]), val)
		}
	})
	b.Run("LoadOrStore/int64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			im.LoadOrStore(alosmap.I(int64(i%benchN)), val)
		}
	})
}

func BenchmarkAlos_TTL(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("StoreWithTTL", func(b *testing.B) {
		m := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup())
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.StoreWithTTL(alosmap.S(keys[i%benchN]), int64(i), time.Minute)
				i++
			}
		})
	})

	b.Run("StoreWithHits", func(b *testing.B) {
		m := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup())
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.StoreWithHits(alosmap.S(keys[i%benchN]), int64(i), 100)
				i++
			}
		})
	})

	b.Run("StoreWithTTLAndHits", func(b *testing.B) {
		m := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup())
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.StoreWithTTLAndHits(alosmap.S(keys[i%benchN]), int64(i), time.Minute, 100)
				i++
			}
		})
	})
}

func BenchmarkAlos_ShardScaling(b *testing.B) {
	keys := stringKeys(benchN)
	for _, shards := range []int{1, 16, 64, 256, 1024} {
		shards := shards
		b.Run(strconv.Itoa(shards)+"_shards", func(b *testing.B) {
			m := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithShardCount(shards), alosmap.WithoutCleanup())
			defer m.Close()
			for i, k := range keys {
				m.Store(alosmap.S(k), int64(i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					m.Load(alosmap.S(keys[i%benchN]))
					i++
				}
			})
		})
	}
}

func BenchmarkAlos_CapacityScaling(b *testing.B) {
	for _, entries := range []int{64, 1024, 16384, 262144} {
		entries := entries
		b.Run(strconv.Itoa(entries)+"_entries", func(b *testing.B) {
			keys := stringKeys(entries)
			m := alosmap.NewTypedSized[string, int64](entries, 0)
			for i, k := range keys {
				m.Store(k, int64(i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					m.Load(keys[i%entries])
					i++
				}
			})
		})
	}
}

type benchSession struct {
	Hits   atomic.Int64
	Name   atomic.Value
	Active atomic.Bool
}

func BenchmarkAlos_LoadedPointer(b *testing.B) {
	newLoaded := func() *benchSession {
		m := alosmap.New(alosmap.WithoutCleanup())
		b.Cleanup(m.Close)
		sess := &benchSession{}
		sess.Name.Store("warm")
		m.Store(alosmap.S("sess:1"), sess)
		ptr, _ := m.Load(alosmap.S("sess:1"))
		return ptr.(*benchSession)
	}

	b.Run("atomic.Int64.Add", func(b *testing.B) {
		sess := newLoaded()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sess.Hits.Add(1)
		}
	})

	b.Run("atomic.Bool.Store", func(b *testing.B) {
		sess := newLoaded()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sess.Active.Store(i&1 == 0)
		}
	})

	b.Run("atomic.Value.Store", func(b *testing.B) {
		sess := newLoaded()
		var v any = "ready"
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sess.Name.Store(v)
		}
	})

	b.Run("StringSet", func(b *testing.B) {
		sess := newLoaded()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			alosmap.StringSet(&sess.Name, "ready")
		}
	})
}
