package benchmarks

import (
	"strconv"
	"sync"

	"github.com/guno1928/alosmap"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/puzpuzpuz/xsync/v3"
)

const benchN = 8192

func stringKeys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = "key:" + strconv.Itoa(i)
	}
	return ks
}

type rwMap struct {
	mu sync.RWMutex
	m  map[string]int64
}

func newRWMap(capacity int) *rwMap {
	return &rwMap{m: make(map[string]int64, capacity)}
}

func (r *rwMap) Load(k string) (int64, bool) {
	r.mu.RLock()
	v, ok := r.m[k]
	r.mu.RUnlock()
	return v, ok
}

func (r *rwMap) Store(k string, v int64) {
	r.mu.Lock()
	r.m[k] = v
	r.mu.Unlock()
}

func (r *rwMap) Delete(k string) {
	r.mu.Lock()
	delete(r.m, k)
	r.mu.Unlock()
}

func (r *rwMap) Range(fn func(string, int64) bool) {
	r.mu.RLock()
	for k, v := range r.m {
		if !fn(k, v) {
			break
		}
	}
	r.mu.RUnlock()
}

func filledAlos(keys []string) *alosmap.Map {
	m := alosmap.New(alosmap.WithCapacity(len(keys)), alosmap.WithoutCleanup())
	for i, k := range keys {
		m.Store(alosmap.S(k), int64(i))
	}
	return m
}

func filledTyped(keys []string) *alosmap.TypedMap[string, int64] {
	m := alosmap.NewTypedSized[string, int64](len(keys), 0)
	for i, k := range keys {
		m.Store(k, int64(i))
	}
	return m
}

func filledSyncMap(keys []string) *sync.Map {
	m := &sync.Map{}
	for i, k := range keys {
		m.Store(k, int64(i))
	}
	return m
}

func filledRW(keys []string) *rwMap {
	m := newRWMap(len(keys))
	for i, k := range keys {
		m.Store(k, int64(i))
	}
	return m
}

func filledXsync(keys []string) *xsync.MapOf[string, int64] {
	m := xsync.NewMapOf[string, int64](xsync.WithPresize(len(keys)))
	for i, k := range keys {
		m.Store(k, int64(i))
	}
	return m
}

func filledCmap(keys []string) cmap.ConcurrentMap[string, int64] {
	m := cmap.New[int64]()
	for i, k := range keys {
		m.Set(k, int64(i))
	}
	return m
}
