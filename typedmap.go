package alosmap

import (
	"hash/maphash"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// TypedMap is a concurrent map keyed by K with values of type V stored inline.
//
// Unlike the any-valued Map, values are not boxed into an interface: V is held in
// an atomic machine word inside each entry, so Store performs no per-value heap
// allocation and Load returns V directly. Reads are lock-free; writes take a
// short per-shard lock. V must be no wider than 8 bytes (int, int64, float64, any
// pointer, or a struct that fits in a word); for larger values use a pointer type
// such as *MyStruct.
type TypedMap[K comparable, V any] struct {
	seed      maphash.Seed
	seed64    uint64
	keyKind   uint8
	valPtr    bool
	hasMeta   atomic.Bool
	shardMask uint64
	tomb      *typedEntry[K]
	shards    []typedShard[K]

	cleanupInterval time.Duration
	cleanerStarted  atomic.Bool
	cleanupClosed   atomic.Bool
	stopCleanup     chan struct{}
	cleanupDone     chan struct{}
}

type typedShard[K comparable] struct {
	table     atomic.Pointer[typedTable[K]]
	resizeGen int64
	_         [32]byte
	mu        sync.Mutex
	chunk     []typedEntry[K]
	off       int
	depth     int
	_         [16]byte
}

type typedTable[K comparable] struct {
	slots      []atomic.Pointer[typedEntry[K]]
	mask       uint64
	_          [40]byte
	count      int64
	tombstones int64
}

type typedEntry[K comparable] struct {
	hash uint64
	key  K
	bits atomic.Uint64
	ptr  atomic.Pointer[byte]
	meta atomic.Pointer[typedMeta]
}

const (
	keyKindOther uint8 = iota
	keyKindString
	keyKindInt64
)

func detectKeyKind[K comparable]() uint8 {
	var z K
	switch any(z).(type) {
	case string:
		return keyKindString
	case int, int64, uint, uint64, uintptr:
		if unsafe.Sizeof(z) == 8 {
			return keyKindInt64
		}
	}
	return keyKindOther
}

func toBits[V any](v V) uint64 {
	var b uint64
	*(*V)(unsafe.Pointer(&b)) = v
	return b
}

func fromBits[V any](b uint64) V {
	return *(*V)(unsafe.Pointer(&b))
}

// valueHasPointer reports whether V contains a pointer the garbage collector
// must track. A V that contains a pointer and fits in a machine word is itself a
// single pointer, so such values are stored in a GC-scanned pointer field rather
// than the unscanned bits word (otherwise GC would reclaim the pointee).
func valueHasPointer[V any]() bool {
	return typeHasPointer(reflect.TypeFor[V]())
}

func typeHasPointer(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Chan, reflect.Map,
		reflect.Func, reflect.Slice, reflect.String, reflect.Interface:
		return true
	case reflect.Array:
		return t.Len() > 0 && typeHasPointer(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if typeHasPointer(t.Field(i).Type) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// storeVal publishes val into the entry's GC-correct slot. Pointer-typed values
// go into the scanned ptr word so the collector keeps the pointee alive; scalar
// values go into the unscanned bits word.
func (m *TypedMap[K, V]) storeVal(e *typedEntry[K], val V) {
	if m.valPtr {
		e.ptr.Store(*(**byte)(unsafe.Pointer(&val)))
	} else {
		e.bits.Store(toBits(val))
	}
}

func (m *TypedMap[K, V]) loadVal(e *typedEntry[K]) V {
	if m.valPtr {
		p := e.ptr.Load()
		return *(*V)(unsafe.Pointer(&p))
	}
	return fromBits[V](e.bits.Load())
}

func (m *TypedMap[K, V]) valEqual(e *typedEntry[K], val V) bool {
	if m.valPtr {
		return e.ptr.Load() == *(**byte)(unsafe.Pointer(&val))
	}
	return e.bits.Load() == toBits(val)
}

// NewTyped returns an empty TypedMap with a default capacity and an automatically
// chosen shard count.
//
// Example, a string-keyed map of int64 counters:
//
//	m := alosmap.NewTyped[string, int64]()
//	m.Store("hits", 1)
//
// Example, a map of pointer values (anything wider than 8 bytes must be a pointer):
//
//	m := alosmap.NewTyped[int64, *Session]()
//	m.Store(42, &Session{})
func NewTyped[K comparable, V any](opts ...Option) *TypedMap[K, V] {
	cfg := defaultConfig()
	cfg.capacity = 64
	for _, o := range opts {
		o(&cfg)
	}
	return newTypedFromConfig[K, V](cfg)
}

// NewTypedSized returns an empty TypedMap pre-sized for capacity entries spread
// across shardCount shards.
//
// capacity is the expected number of live entries; it sizes the initial tables so
// the map avoids early growth. Example: NewTypedSized[string, int64](100_000, 0)
// pre-sizes for 100k entries.
//
// shardCount is the number of independent write shards. Pass 0 (or any value below
// 1) to let the map choose a count from capacity and GOMAXPROCS; positive values
// are rounded up to the next power of two. Example: NewTypedSized[string, int64](10_000, 16)
// forces 16 shards, while NewTypedSized[string, int64](10_000, 0) auto-selects.
//
// V must be no wider than 8 bytes; a larger value type panics at construction —
// use a pointer instead.
func NewTypedSized[K comparable, V any](capacity, shardCount int, opts ...Option) *TypedMap[K, V] {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	cfg.capacity = capacity
	cfg.shardCount = shardCount
	return newTypedFromConfig[K, V](cfg)
}

func newTypedFromConfig[K comparable, V any](cfg config) *TypedMap[K, V] {
	var zero V
	if unsafe.Sizeof(zero) > 8 {
		panic("alosmap: TypedMap value type V must be <= 8 bytes; use a pointer (*T) for larger structs")
	}
	cfg.normalize()
	shardCount := cfg.shardCount
	perShard := nextPowerOfTwo(maxInt(8, cfg.capacity/shardCount*2))
	seed64 := avalanche(uint64(time.Now().UnixNano()) ^ (uint64(shardCount) << 32) ^ hashSeed2)
	if seed64 == 0 {
		seed64 = hashSeed1
	}
	m := &TypedMap[K, V]{
		seed:            maphash.MakeSeed(),
		seed64:          seed64,
		keyKind:         detectKeyKind[K](),
		valPtr:          valueHasPointer[V](),
		shardMask:       uint64(shardCount - 1),
		tomb:            &typedEntry[K]{},
		shards:          make([]typedShard[K], shardCount),
		cleanupInterval: cfg.cleanupInterval,
	}
	for i := range m.shards {
		t := &typedTable[K]{
			slots: make([]atomic.Pointer[typedEntry[K]], perShard),
			mask:  uint64(perShard - 1),
		}
		m.shards[i].table.Store(t)
	}
	if m.cleanupInterval > 0 {
		m.stopCleanup = make(chan struct{})
		m.cleanupDone = make(chan struct{})
	}
	return m
}

func (m *TypedMap[K, V]) maybeStartCleaner() {
	if m.cleanupInterval <= 0 {
		return
	}
	if m.cleanerStarted.CompareAndSwap(false, true) {
		go m.cleanupLoop()
	}
}

func (m *TypedMap[K, V]) cleanupLoop() {
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()
	defer close(m.cleanupDone)
	for {
		select {
		case <-ticker.C:
			m.CleanupNow()
		case <-m.stopCleanup:
			return
		}
	}
}

// Prealloc enables chunked allocation of entry nodes and returns the map for
// chaining. Each shard allocates chunk entries at once and hands them out as keys
// are inserted, allocating the next chunk only when the current one is exhausted.
// Call it once after construction, before concurrent use.
//
// Example, batch 256 entries at a time:
//
//	m := alosmap.NewTyped[string, int64]().Prealloc(256)
//
// Example, a larger batch for an insert-heavy, long-lived map:
//
//	m := alosmap.NewTypedSized[string, int64](1_000_000, 0).Prealloc(1024)
//
// A chunk below 1 is treated as 1.
func (m *TypedMap[K, V]) Prealloc(chunk int) *TypedMap[K, V] {
	if chunk < 1 {
		chunk = 1
	}
	for i := range m.shards {
		m.shards[i].depth = chunk
	}
	return m
}

func (s *typedShard[K]) newEntry() *typedEntry[K] {
	if s.depth <= 0 {
		return &typedEntry[K]{}
	}
	if s.off >= len(s.chunk) {
		s.chunk = make([]typedEntry[K], s.depth)
		s.off = 0
	}
	e := &s.chunk[s.off]
	s.off++
	return e
}

func (m *TypedMap[K, V]) hash(key K) uint64 {
	switch m.keyKind {
	case keyKindString:
		return hashString(m.seed64, *(*string)(unsafe.Pointer(&key)))
	case keyKindInt64:
		return hashInt64(m.seed64, *(*int64)(unsafe.Pointer(&key)))
	default:
		return maphash.Comparable(m.seed, key)
	}
}

func (m *TypedMap[K, V]) find(t *typedTable[K], h uint64, key K) *typedEntry[K] {
	idx := h & t.mask
	for probes := 0; probes < len(t.slots); probes++ {
		e := t.slots[idx].Load()
		if e == nil {
			return nil
		}
		if e != m.tomb && e.hash == h && e.key == key {
			return e
		}
		idx = (idx + 1) & t.mask
	}
	return nil
}

// Load returns the value stored for key and true, or the zero V and false when
// the key is absent.
//
// Example, key present:
//
//	v, ok := m.Load("hits") // v is the stored value, ok is true
//
// Example, key absent:
//
//	v, ok := m.Load("missing") // v is the zero value, ok is false
func (m *TypedMap[K, V]) Load(key K) (V, bool) {
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]
	e := m.find(s.table.Load(), h, key)
	if e == nil {
		var zero V
		return zero, false
	}
	if m.hasMeta.Load() {
		if md := e.meta.Load(); md != nil {
			if md.expireAt != 0 && time.Now().UnixNano() >= md.expireAt {
				var zero V
				return zero, false
			}
			if md.hitLimited && md.hits.Add(-1) < 0 {
				var zero V
				return zero, false
			}
		}
	}
	return m.loadVal(e), true
}

// Store sets key to val, inserting a new entry or replacing an existing one.
// Replacing the value of an existing key is lock-free and allocation-free.
//
// Example, insert then update an int64 value:
//
//	m.Store("hits", 1)
//	m.Store("hits", 2) // replaces in place
//
// Example, store a pointer value:
//
//	m.Store("sess", &Session{ID: 7})
func (m *TypedMap[K, V]) Store(key K, val V) {
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]

	if e := m.find(s.table.Load(), h, key); e != nil {
		m.storeVal(e, val)
		if m.hasMeta.Load() && e.meta.Load() != nil {
			e.meta.Store(nil)
		}
		return
	}

	s.mu.Lock()
	t := s.table.Load()
	used := int(atomic.LoadInt64(&t.count) + atomic.LoadInt64(&t.tombstones))
	if (used+1)*4 >= len(t.slots)*3 {
		t = m.growLocked(s, t)
	}
	idx := h & t.mask
	firstTomb := -1
	for {
		e := t.slots[idx].Load()
		if e == nil {
			ne := s.newEntry()
			ne.hash = h
			ne.key = key
			m.storeVal(ne, val)
			if firstTomb >= 0 {
				if t.slots[firstTomb].CompareAndSwap(m.tomb, ne) {
					atomic.AddInt64(&t.tombstones, -1)
					atomic.AddInt64(&t.count, 1)
					s.mu.Unlock()
					return
				}
				firstTomb = -1
				continue
			}
			if t.slots[idx].CompareAndSwap(nil, ne) {
				atomic.AddInt64(&t.count, 1)
				s.mu.Unlock()
				return
			}
			continue
		}
		if e == m.tomb {
			if firstTomb < 0 {
				firstTomb = int(idx)
			}
		} else if e.hash == h && e.key == key {
			m.storeVal(e, val)
			if e.meta.Load() != nil {
				e.meta.Store(nil)
			}
			s.mu.Unlock()
			return
		}
		idx = (idx + 1) & t.mask
	}
}

func (m *TypedMap[K, V]) repairDelete(s *typedShard[K], h uint64, key K) {
	s.mu.Lock()
	t := s.table.Load()
	idx := h & t.mask
	for {
		e := t.slots[idx].Load()
		if e == nil {
			s.mu.Unlock()
			return
		}
		if e != m.tomb && e.hash == h && e.key == key {
			t.slots[idx].Store(m.tomb)
			atomic.AddInt64(&t.count, -1)
			atomic.AddInt64(&t.tombstones, 1)
			s.mu.Unlock()
			return
		}
		idx = (idx + 1) & t.mask
	}
}

// Delete removes key and returns the previous value with true, or the zero V and
// false if the key was absent.
//
// Example, deleting a present key:
//
//	prev, existed := m.Delete("hits") // existed is true
//
// Example, deleting an absent key:
//
//	_, existed := m.Delete("missing") // existed is false
func (m *TypedMap[K, V]) Delete(key K) (V, bool) {
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]

	if !m.hasMeta.Load() {
		gen := atomic.LoadInt64(&s.resizeGen)
		t := s.table.Load()
		if gen&1 == 0 {
			idx := h & t.mask
			for i := 0; i < 8; i++ {
				e := t.slots[idx].Load()
				if e == nil {
					var zero V
					return zero, false
				}
				if e != m.tomb && e.hash == h && e.key == key {
					if e.meta.Load() == nil {
						v := m.loadVal(e)
						if t.slots[idx].CompareAndSwap(e, m.tomb) {
							atomic.AddInt64(&t.count, -1)
							atomic.AddInt64(&t.tombstones, 1)
							if s.table.Load() != t || atomic.LoadInt64(&s.resizeGen) != gen {
								m.repairDelete(s, h, key)
							}
							return v, true
						}
					}
					break
				}
				idx = (idx + 1) & t.mask
			}
		}
	}

	s.mu.Lock()
	t := s.table.Load()
	idx := h & t.mask
	for probes := 0; probes < len(t.slots); probes++ {
		e := t.slots[idx].Load()
		if e == nil {
			s.mu.Unlock()
			var zero V
			return zero, false
		}
		if e != m.tomb && e.hash == h && e.key == key {
			v := m.loadVal(e)
			dead := false
			if md := e.meta.Load(); md != nil && md.deadForView(time.Now().UnixNano()) {
				dead = true
			}
			t.slots[idx].Store(m.tomb)
			atomic.AddInt64(&t.count, -1)
			atomic.AddInt64(&t.tombstones, 1)
			s.mu.Unlock()
			if dead {
				var zero V
				return zero, false
			}
			return v, true
		}
		idx = (idx + 1) & t.mask
	}
	s.mu.Unlock()
	var zero V
	return zero, false
}

// Range calls visitor for each live key/value pair, stopping early if visitor
// returns false. Iteration order is unspecified, and under concurrent writes
// Range observes an eventually consistent view rather than a locked snapshot.
//
// Example, visit every entry:
//
//	m.Range(func(k string, v int64) bool {
//		fmt.Println(k, v)
//		return true
//	})
//
// Example, stop at the first match:
//
//	m.Range(func(k string, v int64) bool {
//		return v != target // returning false stops iteration
//	})
func (m *TypedMap[K, V]) Range(visitor func(key K, value V) bool) {
	hasMeta := m.hasMeta.Load()
	var now int64
	if hasMeta {
		now = time.Now().UnixNano()
	}
	for si := range m.shards {
		t := m.shards[si].table.Load()
		slots := t.slots
		for j := range slots {
			e := slots[j].Load()
			if e == nil || e == m.tomb {
				continue
			}
			if hasMeta {
				if md := e.meta.Load(); md != nil && md.deadForView(now) {
					continue
				}
			}
			if !visitor(e.key, m.loadVal(e)) {
				return
			}
		}
	}
}

// Len returns the number of live entries currently in the map.
func (m *TypedMap[K, V]) Len() int {
	hasMeta := m.hasMeta.Load()
	var now int64
	if hasMeta {
		now = time.Now().UnixNano()
	}
	n := 0
	for si := range m.shards {
		t := m.shards[si].table.Load()
		for j := range t.slots {
			e := t.slots[j].Load()
			if e == nil || e == m.tomb {
				continue
			}
			if hasMeta {
				if md := e.meta.Load(); md != nil && md.deadForView(now) {
					continue
				}
			}
			n++
		}
	}
	return n
}

func targetTypedSlots(curSlots, live, tombstones int) int {
	required := nextPowerOfTwo(maxInt(8, live*2))
	if required > curSlots {
		return maxInt(curSlots*4, required)
	}
	return required
}

func (m *TypedMap[K, V]) growLocked(s *typedShard[K], old *typedTable[K]) *typedTable[K] {
	atomic.AddInt64(&s.resizeGen, 1)
	size := targetTypedSlots(len(old.slots), int(atomic.LoadInt64(&old.count)), int(atomic.LoadInt64(&old.tombstones)))
	nt := &typedTable[K]{
		slots: make([]atomic.Pointer[typedEntry[K]], size),
		mask:  uint64(size - 1),
	}
	var cnt int64
	for i := range old.slots {
		e := old.slots[i].Load()
		if e == nil || e == m.tomb {
			continue
		}
		idx := e.hash & nt.mask
		for nt.slots[idx].Load() != nil {
			idx = (idx + 1) & nt.mask
		}
		nt.slots[idx].Store(e)
		cnt++
	}
	nt.count = cnt
	nt.tombstones = 0
	s.table.Store(nt)
	atomic.AddInt64(&s.resizeGen, 1)
	return nt
}
