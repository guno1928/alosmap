package benchmarks

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/guno1928/alosmap"
	"github.com/puzpuzpuz/xsync/v4"
	"gitlab.skybyte.cloud/tumult1337/tmap"
)

type intMap interface {
	Store(k int, v int64)
	Load(k int) (int64, bool)
	Delete(k int)
	Range(fn func(k int, v int64) bool)
}

type intAlos struct{ m *alosmap.TypedMap[int, int64] }

func (a intAlos) Store(k int, v int64)               { a.m.Store(k, v) }
func (a intAlos) Load(k int) (int64, bool)           { return a.m.Load(k) }
func (a intAlos) Delete(k int)                       { a.m.Delete(k) }
func (a intAlos) Range(fn func(k int, v int64) bool) { a.m.Range(fn) }

type intXsync struct{ m *xsync.Map[int, int64] }

func (a intXsync) Store(k int, v int64)               { a.m.Store(k, v) }
func (a intXsync) Load(k int) (int64, bool)           { return a.m.Load(k) }
func (a intXsync) Delete(k int)                       { a.m.Delete(k) }
func (a intXsync) Range(fn func(k int, v int64) bool) { a.m.Range(fn) }

type intTmap struct{ m *tmap.Map[int, int64] }

func (a intTmap) Store(k int, v int64)               { a.m.Store(k, v) }
func (a intTmap) Load(k int) (int64, bool)           { return a.m.Load(k) }
func (a intTmap) Delete(k int)                       { a.m.Delete(k) }
func (a intTmap) Range(fn func(k int, v int64) bool) { a.m.Range(fn) }

type intNamed struct {
	name string
	make func(n int) intMap
}

func intMaps() []intNamed {
	return []intNamed{
		{"alosmap_typed", func(n int) intMap {
			if n > 0 {
				return intAlos{alosmap.NewTypedSized[int, int64](n, 0, alosmap.WithoutCleanup())}
			}
			return intAlos{alosmap.NewTyped[int, int64](alosmap.WithoutCleanup())}
		}},
		{"xsync", func(n int) intMap {
			if n > 0 {
				return intXsync{xsync.NewMap[int, int64](xsync.WithPresize(n))}
			}
			return intXsync{xsync.NewMap[int, int64]()}
		}},
		{"tmap", func(n int) intMap {
			if n > 0 {
				return intTmap{tmap.New[int, int64](tmap.WithPresize(n))}
			}
			return intTmap{tmap.New[int, int64]()}
		}},
	}
}

func intFill(m intMap, n int) intMap {
	for i := 0; i < n; i++ {
		m.Store(i*7919, int64(i))
	}
	return m
}

func benchIntRead(b *testing.B, n int, seq []int) {
	for _, nm := range intMaps() {
		b.Run(nm.name, func(b *testing.B) {
			m := intFill(nm.make(n), n)
			b.ResetTimer()
			var off int64
			b.RunParallel(func(pb *testing.PB) {
				i := int(atomic.AddInt64(&off, 1)) * 1009 % len(seq)
				for pb.Next() {
					m.Load(seq[i] * 7919)
					if i++; i == len(seq) {
						i = 0
					}
				}
			})
		})
	}
}

func BenchmarkInt_Read_Uniform(b *testing.B) {
	benchIntRead(b, fairWorkSet, uniformSeq(fairWorkSet, fairSeqLen))
}

func BenchmarkInt_Read_Zipf(b *testing.B) {
	benchIntRead(b, fairWorkSet, zipfSeq(fairWorkSet, fairSeqLen, fairZipfS))
}

func BenchmarkInt_Update_Zipf(b *testing.B) {
	seq := zipfSeq(fairWorkSet, fairSeqLen, fairZipfS)
	for _, nm := range intMaps() {
		b.Run(nm.name, func(b *testing.B) {
			m := intFill(nm.make(fairWorkSet), fairWorkSet)
			b.ResetTimer()
			var off int64
			b.RunParallel(func(pb *testing.PB) {
				i := int(atomic.AddInt64(&off, 1)) * 1009 % len(seq)
				for pb.Next() {
					m.Store(seq[i]*7919, int64(i))
					if i++; i == len(seq) {
						i = 0
					}
				}
			})
		})
	}
}

func BenchmarkInt_InsertGrow(b *testing.B) {
	for _, nm := range intMaps() {
		b.Run(nm.name, func(b *testing.B) {
			b.ReportAllocs()
			for it := 0; it < b.N; it++ {
				m := nm.make(0)
				var wg sync.WaitGroup
				workers := 8
				per := fairInsertN / workers
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for i := w * per; i < (w+1)*per; i++ {
							m.Store(i*7919, int64(i))
						}
					}(w)
				}
				wg.Wait()
			}
		})
	}
}

func BenchmarkInt_InsertGrowBig(b *testing.B) {
	for _, nm := range intMaps() {
		b.Run(nm.name, func(b *testing.B) {
			b.ReportAllocs()
			for it := 0; it < b.N; it++ {
				m := nm.make(0)
				var wg sync.WaitGroup
				workers := 8
				per := 448_000 / workers
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for i := w * per; i < (w+1)*per; i++ {
							m.Store(i*7919, int64(i))
						}
					}(w)
				}
				wg.Wait()
			}
		})
	}
}

func BenchmarkInt_Churn(b *testing.B) {
	for _, nm := range intMaps() {
		b.Run(nm.name, func(b *testing.B) {
			m := nm.make(fairChurnWin)
			for i := 0; i < fairChurnWin; i++ {
				m.Store(i*7919, int64(i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			var ctr int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := int(atomic.AddInt64(&ctr, 1))
					m.Store((fairChurnWin+id)*7919, int64(id))
					m.Delete(id * 7919)
				}
			})
		})
	}
}

func BenchmarkInt_Range(b *testing.B) {
	for _, nm := range intMaps() {
		b.Run(nm.name, func(b *testing.B) {
			m := intFill(nm.make(fairRangeN), fairRangeN)
			b.ResetTimer()
			for it := 0; it < b.N; it++ {
				cnt := 0
				m.Range(func(int, int64) bool { cnt++; return true })
				if cnt != fairRangeN {
					b.Fatalf("visited %d", cnt)
				}
			}
		})
	}
}
