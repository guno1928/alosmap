package benchmarks

// fair_setup_test.go — a deliberately fair head-to-head harness.
//
// The original compare_test.go is a micro-benchmark suite: it measures peak
// throughput on a few hand-written hot loops. Those numbers are real but narrow,
// and three workload choices quietly flatter alosmap:
//
//   1. every "write"/"mixed" loop re-stores keys that already exist, so only the
//      in-place update fast path runs and the insert path (key clone, node alloc,
//      table growth, shard lock) is never measured;
//   2. the delete loop alternates Delete+Store on the same key, so each tombstone
//      is immediately revived and the unbounded-growth cost never appears;
//   3. the only value type is int64, simultaneously alosmap.Typed's best case and
//      the competitors' worst (interface boxing).
//
// This suite fixes all three. Goals:
//
//   - Identical workload code for every map. Each implementation is driven through
//     the concMap[V] interface, so the scenario body is byte-identical across maps.
//     This costs one interface indirection per op (a few ns, applied EQUALLY to
//     all maps); relative comparisons are unaffected, and real code routinely
//     reaches maps behind an abstraction anyway.
//   - Realistic scenarios: contended distinct-key inserts, cache-style churn,
//     Zipfian hot-key skew, read-mostly and write-heavy mixes, and both scalar
//     (int64) and pointer (*Session) value types.
//   - Memory is a first-class metric. TestMemoryFootprint and TestChurnFootprint
//     measure retained heap bytes per entry — the dimension throughput hides.
//
// Fairness rules applied consistently:
//   - Steady-state scenarios (read/update/mixed/range/loadorstore) presize EVERY
//     map that supports it to the working-set size, so resize cost is excluded for
//     all of them. sync.Map and cmap cannot presize — that is their nature, not a
//     handicap we imposed.
//   - Growth scenarios (insert/fill) presize NO map, so table-growth cost is paid
//     by everyone and actually measured.
//   - Each map uses its most natural / optimal public API for the operation.

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/guno1928/alosmap"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/puzpuzpuz/xsync/v4"
)

// concMap is the uniform surface every map is driven through.
type concMap[V any] interface {
	Store(k string, v V)
	Load(k string) (V, bool)
	Delete(k string)
	LoadOrStore(k string, v V) (V, bool)
	Range(fn func(k string, v V) bool)
	Len() int
}

// capInfoer is implemented by maps that can report internal capacity/tombstones,
// so the memory tests can show *why* a map retains the heap it does.
type capInfoer interface{ capInfo() string }

// ---------------------------------------------------------------------------
// Value types. int64 exercises the scalar path (boxing for any/sync.Map);
// *Session exercises the realistic "store a pointer to a record" path that the
// original suite never compared.

type Session struct {
	ID, Created, LastSeen, Bytes, Hits int64
	Region                             string
	Active                             bool
}

func makeInt(i int) int64 { return int64(i) }
func makePtr(i int) *Session {
	return &Session{ID: int64(i), Created: int64(i) * 2, Bytes: int64(i & 1023), Region: "ap-southeast-2", Active: i&1 == 0}
}

// ---------------------------------------------------------------------------
// Adapters. Each wraps one map behind concMap[V] using that map's natural API.

type alosAny[V any] struct{ m *alosmap.Map }

func (a alosAny[V]) Store(k string, v V) { a.m.Store(alosmap.S(k), v) }
func (a alosAny[V]) Load(k string) (V, bool) {
	r, ok := a.m.Load(alosmap.S(k))
	if !ok {
		var z V
		return z, false
	}
	return r.(V), true
}
func (a alosAny[V]) Delete(k string) { a.m.Delete(alosmap.S(k)) }
func (a alosAny[V]) LoadOrStore(k string, v V) (V, bool) {
	r, loaded := a.m.LoadOrStore(alosmap.S(k), v)
	return r.(V), loaded
}
func (a alosAny[V]) Range(fn func(k string, v V) bool) {
	a.m.Range(func(key alosmap.Key, val any) bool { return fn(key.StringVal(), val.(V)) })
}
func (a alosAny[V]) Len() int { return a.m.Len() }
func (a alosAny[V]) capInfo() string {
	s := a.m.Stats()
	return fmt.Sprintf("SlotCapacity=%d Tombstones=%d Live=%d", s.SlotCapacity, s.Tombstones, s.LiveEntries)
}

type alosTyped[V any] struct{ m *alosmap.TypedMap[string, V] }

func (a alosTyped[V]) Store(k string, v V)                 { a.m.Store(k, v) }
func (a alosTyped[V]) Load(k string) (V, bool)             { return a.m.Load(k) }
func (a alosTyped[V]) Delete(k string)                     { a.m.Delete(k) }
func (a alosTyped[V]) LoadOrStore(k string, v V) (V, bool) { return a.m.LoadOrStore(k, v) }
func (a alosTyped[V]) Range(fn func(k string, v V) bool)   { a.m.Range(fn) }
func (a alosTyped[V]) Len() int                            { return a.m.Len() }
func (a alosTyped[V]) capInfo() string {
	s := a.m.Stats()
	return fmt.Sprintf("SlotCapacity=%d Tombstones=%d Live=%d", s.SlotCapacity, s.Tombstones, s.LiveEntries)
}

type syncMapAd[V any] struct{ m *sync.Map }

func (a syncMapAd[V]) Store(k string, v V) { a.m.Store(k, v) }
func (a syncMapAd[V]) Load(k string) (V, bool) {
	r, ok := a.m.Load(k)
	if !ok {
		var z V
		return z, false
	}
	return r.(V), true
}
func (a syncMapAd[V]) Delete(k string) { a.m.Delete(k) }
func (a syncMapAd[V]) LoadOrStore(k string, v V) (V, bool) {
	r, loaded := a.m.LoadOrStore(k, v)
	return r.(V), loaded
}
func (a syncMapAd[V]) Range(fn func(k string, v V) bool) {
	a.m.Range(func(key, val any) bool { return fn(key.(string), val.(V)) })
}
func (a syncMapAd[V]) Len() int {
	n := 0
	a.m.Range(func(_, _ any) bool { n++; return true })
	return n
}

type rwAdapter[V any] struct {
	mu *sync.RWMutex
	m  map[string]V
}

func (a rwAdapter[V]) Store(k string, v V) { a.mu.Lock(); a.m[k] = v; a.mu.Unlock() }
func (a rwAdapter[V]) Load(k string) (V, bool) {
	a.mu.RLock()
	v, ok := a.m[k]
	a.mu.RUnlock()
	return v, ok
}
func (a rwAdapter[V]) Delete(k string) { a.mu.Lock(); delete(a.m, k); a.mu.Unlock() }
func (a rwAdapter[V]) LoadOrStore(k string, v V) (V, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ex, ok := a.m[k]; ok {
		return ex, true
	}
	a.m[k] = v
	return v, false
}
func (a rwAdapter[V]) Range(fn func(k string, v V) bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for k, v := range a.m {
		if !fn(k, v) {
			break
		}
	}
}
func (a rwAdapter[V]) Len() int { a.mu.RLock(); n := len(a.m); a.mu.RUnlock(); return n }

type xsyncAd[V any] struct{ m *xsync.Map[string, V] }

func (a xsyncAd[V]) Store(k string, v V)                 { a.m.Store(k, v) }
func (a xsyncAd[V]) Load(k string) (V, bool)             { return a.m.Load(k) }
func (a xsyncAd[V]) Delete(k string)                     { a.m.Delete(k) }
func (a xsyncAd[V]) LoadOrStore(k string, v V) (V, bool) { return a.m.LoadOrStore(k, v) }
func (a xsyncAd[V]) Range(fn func(k string, v V) bool)   { a.m.Range(fn) }
func (a xsyncAd[V]) Len() int                            { return a.m.Size() }

type cmapAd[V any] struct{ m cmap.ConcurrentMap[string, V] }

func (a cmapAd[V]) Store(k string, v V)     { a.m.Set(k, v) }
func (a cmapAd[V]) Load(k string) (V, bool) { return a.m.Get(k) }
func (a cmapAd[V]) Delete(k string)         { a.m.Remove(k) }
func (a cmapAd[V]) LoadOrStore(k string, v V) (V, bool) {
	if a.m.SetIfAbsent(k, v) { // atomic per-shard insert-if-absent; exactly one winner
		return v, false
	}
	ex, _ := a.m.Get(k)
	return ex, true
}
func (a cmapAd[V]) Range(fn func(k string, v V) bool) {
	// cmap's IterCb cannot early-terminate; emulate so a stopping visitor still
	// returns, even though every shard is still walked (a real cmap limitation).
	stop := false
	a.m.IterCb(func(k string, v V) {
		if stop {
			return
		}
		if !fn(k, v) {
			stop = true
		}
	})
}
func (a cmapAd[V]) Len() int { return a.m.Count() }

// ---------------------------------------------------------------------------
// Construction registry. presize > 0 requests a capacity hint; presize <= 0 means
// "no hint" (used by growth scenarios). Maps that cannot presize ignore the hint.

type namedMap[V any] struct {
	name string
	make func(presize int) concMap[V]
}

func allMaps[V any]() []namedMap[V] {
	return []namedMap[V]{
		{"alosmap_any", func(n int) concMap[V] {
			if n > 0 {
				return alosAny[V]{alosmap.New(alosmap.WithCapacity(n), alosmap.WithoutCleanup())}
			}
			return alosAny[V]{alosmap.New(alosmap.WithoutCleanup())}
		}},
		{"alosmap_typed", func(n int) concMap[V] {
			if n > 0 {
				return alosTyped[V]{alosmap.NewTypedSized[string, V](n, 0, alosmap.WithoutCleanup())}
			}
			return alosTyped[V]{alosmap.NewTyped[string, V](alosmap.WithoutCleanup())}
		}},
		{"sync.Map", func(n int) concMap[V] { return syncMapAd[V]{&sync.Map{}} }},
		{"map+RWMutex", func(n int) concMap[V] {
			c := n
			if c < 0 {
				c = 0
			}
			return rwAdapter[V]{mu: &sync.RWMutex{}, m: make(map[string]V, c)}
		}},
		{"xsync", func(n int) concMap[V] {
			if n > 0 {
				return xsyncAd[V]{xsync.NewMap[string, V](xsync.WithPresize(n))}
			}
			return xsyncAd[V]{xsync.NewMap[string, V]()}
		}},
		{"cmap", func(n int) concMap[V] { return cmapAd[V]{cmap.New[V]()} }},
	}
}

// ---------------------------------------------------------------------------
// Key universe and access distributions. All sequences are precomputed with fixed
// seeds so the RNG is never measured and runs are reproducible.

// zipfSeq returns `length` indices into [0,n) following a Zipf (hot-key) skew.
func zipfSeq(n, length int, s float64) []int {
	r := rand.New(rand.NewSource(42))
	z := rand.NewZipf(r, s, 1, uint64(n-1))
	seq := make([]int, length)
	for i := range seq {
		seq[i] = int(z.Uint64())
	}
	return seq
}

// uniformSeq returns `length` uniformly-random indices into [0,n).
func uniformSeq(n, length int) []int {
	r := rand.New(rand.NewSource(7))
	seq := make([]int, length)
	for i := range seq {
		seq[i] = r.Intn(n)
	}
	return seq
}

func fill[V any](m concMap[V], keys []string, mk func(int) V) concMap[V] {
	for i, k := range keys {
		m.Store(k, mk(i))
	}
	return m
}
