package benchmarks

import (
	"testing"
	"time"

	"github.com/guno1928/alosmap"
)

func benchFilledTyped(n int) (*alosmap.TypedMap[string, int64], []string) {
	keys := stringKeys(n)
	m := alosmap.NewTypedSized[string, int64](n, 0, alosmap.WithoutCleanup())
	for i, k := range keys {
		m.Store(k, int64(i))
	}
	return m, keys
}

func BenchmarkTypedExt_LoadOrStore_Hit(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.LoadOrStore(keys[i%benchN], 0)
	}
}

func BenchmarkTypedExt_Swap(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Swap(keys[i%benchN], int64(i))
	}
}

func BenchmarkTypedExt_CompareAndSwap(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	for _, k := range keys {
		m.Store(k, 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.CompareAndSwap(keys[i%benchN], 1, 1)
	}
}

func BenchmarkTypedExt_CompareAndDelete(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % benchN
		m.CompareAndDelete(keys[idx], int64(idx))
		m.Store(keys[idx], int64(idx))
	}
}

func BenchmarkTypedExt_Has(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Has(keys[i%benchN])
	}
}

func BenchmarkTypedExt_Peek(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Peek(keys[i%benchN])
	}
}

func BenchmarkTypedExt_Get(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(keys[i%benchN])
	}
}

func BenchmarkTypedExt_StoreWithTTL(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.StoreWithTTL(keys[i%benchN], int64(i), time.Minute)
	}
}

func BenchmarkTypedExt_StoreWithHits(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.StoreWithHits(keys[i%benchN], int64(i), 100)
	}
}

func BenchmarkTypedExt_StoreWithTTLAndHits(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.StoreWithTTLAndHits(keys[i%benchN], int64(i), time.Minute, 100)
	}
}

func BenchmarkTypedExt_Snapshot(b *testing.B) {
	m, _ := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Snapshot()
	}
}

func BenchmarkTypedExt_RangePar(b *testing.B) {
	m, _ := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.RangePar(func(_ string, _ int64) bool { return true })
	}
}

func BenchmarkTypedExt_Stats(b *testing.B) {
	m, _ := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Stats()
	}
}

func BenchmarkTypedExt_CleanupNow(b *testing.B) {
	m, _ := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.CleanupNow()
	}
}

func BenchmarkTypedExt_Clear(b *testing.B) {
	m, keys := benchFilledTyped(benchN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Clear()
		b.StopTimer()
		for j, k := range keys {
			m.Store(k, int64(j))
		}
		b.StartTimer()
	}
}
