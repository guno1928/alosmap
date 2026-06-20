package benchmarks

import (
	"sync"
	"testing"

	"github.com/guno1928/alosmap"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/puzpuzpuz/xsync/v4"
)

func BenchmarkX_Insert(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("alosmap_any", func(b *testing.B) {
		b.ReportAllocs()
		m := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(alosmap.S(keys[i%benchN]), int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m.Close()
				m = alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup())
				b.StartTimer()
			}
		}
		m.Close()
	})

	b.Run("alosmap_any_prealloc", func(b *testing.B) {
		b.ReportAllocs()
		m := alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup(), alosmap.WithPrealloc(benchN))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(alosmap.S(keys[i%benchN]), int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m.Close()
				m = alosmap.New(alosmap.WithCapacity(benchN), alosmap.WithoutCleanup(), alosmap.WithPrealloc(benchN))
				b.StartTimer()
			}
		}
		m.Close()
	})

	b.Run("alosmap_typed", func(b *testing.B) {
		b.ReportAllocs()
		m := alosmap.NewTypedSized[string, int64](benchN, 0)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(keys[i%benchN], int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m = alosmap.NewTypedSized[string, int64](benchN, 0)
				b.StartTimer()
			}
		}
	})

	b.Run("alosmap_typed_prealloc", func(b *testing.B) {
		b.ReportAllocs()
		m := alosmap.NewTypedSized[string, int64](benchN, 0).Prealloc(1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(keys[i%benchN], int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m = alosmap.NewTypedSized[string, int64](benchN, 0).Prealloc(1024)
				b.StartTimer()
			}
		}
	})

	b.Run("sync.Map", func(b *testing.B) {
		b.ReportAllocs()
		m := &sync.Map{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(keys[i%benchN], int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m = &sync.Map{}
				b.StartTimer()
			}
		}
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		b.ReportAllocs()
		m := newRWMap(benchN)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(keys[i%benchN], int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m = newRWMap(benchN)
				b.StartTimer()
			}
		}
	})

	b.Run("xsync", func(b *testing.B) {
		b.ReportAllocs()
		m := xsync.NewMap[string, int64](xsync.WithPresize(benchN))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Store(keys[i%benchN], int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m = xsync.NewMap[string, int64](xsync.WithPresize(benchN))
				b.StartTimer()
			}
		}
	})

	b.Run("cmap", func(b *testing.B) {
		b.ReportAllocs()
		m := cmap.New[int64]()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Set(keys[i%benchN], int64(i))
			if (i+1)%benchN == 0 {
				b.StopTimer()
				m = cmap.New[int64]()
				b.StartTimer()
			}
		}
	})
}

func BenchmarkX_ParallelRead(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("alosmap_any", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
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

	b.Run("alosmap_typed", func(b *testing.B) {
		m := filledTyped(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Load(keys[i%benchN])
				i++
			}
		})
	})

	b.Run("sync.Map", func(b *testing.B) {
		m := filledSyncMap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Load(keys[i%benchN])
				i++
			}
		})
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		m := filledRW(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Load(keys[i%benchN])
				i++
			}
		})
	})

	b.Run("xsync", func(b *testing.B) {
		m := filledXsync(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Load(keys[i%benchN])
				i++
			}
		})
	})

	b.Run("cmap", func(b *testing.B) {
		m := filledCmap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Get(keys[i%benchN])
				i++
			}
		})
	})
}

func BenchmarkX_ParallelStore(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("alosmap_any", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(alosmap.S(keys[i%benchN]), int64(i))
				i++
			}
		})
	})

	b.Run("alosmap_typed", func(b *testing.B) {
		m := filledTyped(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(keys[i%benchN], int64(i))
				i++
			}
		})
	})

	b.Run("sync.Map", func(b *testing.B) {
		m := filledSyncMap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(keys[i%benchN], int64(i))
				i++
			}
		})
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		m := filledRW(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(keys[i%benchN], int64(i))
				i++
			}
		})
	})

	b.Run("xsync", func(b *testing.B) {
		m := filledXsync(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(keys[i%benchN], int64(i))
				i++
			}
		})
	})

	b.Run("cmap", func(b *testing.B) {
		m := filledCmap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Set(keys[i%benchN], int64(i))
				i++
			}
		})
	})
}

func benchMixed(b *testing.B, readPct int) {
	keys := stringKeys(benchN)

	b.Run("alosmap_any", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := alosmap.S(keys[i%benchN])
				if i%100 < readPct {
					m.Load(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("alosmap_typed", func(b *testing.B) {
		m := filledTyped(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i%100 < readPct {
					m.Load(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("sync.Map", func(b *testing.B) {
		m := filledSyncMap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i%100 < readPct {
					m.Load(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		m := filledRW(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i%100 < readPct {
					m.Load(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("xsync", func(b *testing.B) {
		m := filledXsync(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i%100 < readPct {
					m.Load(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("cmap", func(b *testing.B) {
		m := filledCmap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i%100 < readPct {
					m.Get(k)
				} else {
					m.Set(k, int64(i))
				}
				i++
			}
		})
	})
}

func BenchmarkX_Mixed90Read(b *testing.B) { benchMixed(b, 90) }

func BenchmarkX_Mixed50Read(b *testing.B) { benchMixed(b, 50) }

func BenchmarkX_Delete(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("alosmap_any", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := alosmap.S(keys[i%benchN])
				if i&1 == 0 {
					m.Delete(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("alosmap_typed", func(b *testing.B) {
		m := filledTyped(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i&1 == 0 {
					m.Delete(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("sync.Map", func(b *testing.B) {
		m := filledSyncMap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i&1 == 0 {
					m.Delete(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		m := filledRW(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i&1 == 0 {
					m.Delete(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("xsync", func(b *testing.B) {
		m := filledXsync(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i&1 == 0 {
					m.Delete(k)
				} else {
					m.Store(k, int64(i))
				}
				i++
			}
		})
	})

	b.Run("cmap", func(b *testing.B) {
		m := filledCmap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%benchN]
				if i&1 == 0 {
					m.Remove(k)
				} else {
					m.Set(k, int64(i))
				}
				i++
			}
		})
	})
}

func BenchmarkX_Range(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("alosmap_any", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_ alosmap.Key, _ any) bool { return true })
		}
	})

	b.Run("alosmap_typed", func(b *testing.B) {
		m := filledTyped(keys)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_ string, _ int64) bool { return true })
		}
	})

	b.Run("sync.Map", func(b *testing.B) {
		m := filledSyncMap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_, _ any) bool { return true })
		}
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		m := filledRW(keys)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_ string, _ int64) bool { return true })
		}
	})

	b.Run("xsync", func(b *testing.B) {
		m := filledXsync(keys)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_ string, _ int64) bool { return true })
		}
	})

	b.Run("cmap", func(b *testing.B) {
		m := filledCmap(keys)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.IterCb(func(_ string, _ int64) {})
		}
	})
}

func BenchmarkX_RangeWhileWriting(b *testing.B) {
	keys := stringKeys(benchN)

	b.Run("alosmap_any", func(b *testing.B) {
		m := filledAlos(keys)
		defer m.Close()
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					m.Store(alosmap.S(keys[i%benchN]), int64(i))
					i++
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_ alosmap.Key, _ any) bool { return true })
		}
		b.StopTimer()
		close(stop)
		wg.Wait()
	})

	b.Run("sync.Map", func(b *testing.B) {
		m := filledSyncMap(keys)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					m.Store(keys[i%benchN], int64(i))
					i++
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_, _ any) bool { return true })
		}
		b.StopTimer()
		close(stop)
		wg.Wait()
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		m := filledRW(keys)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					m.Store(keys[i%benchN], int64(i))
					i++
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_ string, _ int64) bool { return true })
		}
		b.StopTimer()
		close(stop)
		wg.Wait()
	})

	b.Run("xsync", func(b *testing.B) {
		m := filledXsync(keys)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					m.Store(keys[i%benchN], int64(i))
					i++
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Range(func(_ string, _ int64) bool { return true })
		}
		b.StopTimer()
		close(stop)
		wg.Wait()
	})

	b.Run("cmap", func(b *testing.B) {
		m := filledCmap(keys)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					m.Set(keys[i%benchN], int64(i))
					i++
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.IterCb(func(_ string, _ int64) {})
		}
		b.StopTimer()
		close(stop)
		wg.Wait()
	})
}

func BenchmarkX_HotKeyRead(b *testing.B) {
	const hot = "hot"

	b.Run("alosmap_any", func(b *testing.B) {
		m := alosmap.New(alosmap.WithoutCleanup())
		defer m.Close()
		m.Store(alosmap.S(hot), int64(42))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Load(alosmap.S(hot))
			}
		})
	})

	b.Run("alosmap_typed", func(b *testing.B) {
		m := alosmap.NewTypedSized[string, int64](8, 0)
		m.Store(hot, 42)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Load(hot)
			}
		})
	})

	b.Run("sync.Map", func(b *testing.B) {
		m := &sync.Map{}
		m.Store(hot, int64(42))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Load(hot)
			}
		})
	})

	b.Run("map+RWMutex", func(b *testing.B) {
		m := newRWMap(8)
		m.Store(hot, 42)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Load(hot)
			}
		})
	})

	b.Run("xsync", func(b *testing.B) {
		m := xsync.NewMap[string, int64]()
		m.Store(hot, 42)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Load(hot)
			}
		})
	})

	b.Run("cmap", func(b *testing.B) {
		m := cmap.New[int64]()
		m.Set(hot, 42)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Get(hot)
			}
		})
	})
}
