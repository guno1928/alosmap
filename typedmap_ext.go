package alosmap

import (
	"sync"
	"sync/atomic"
	"time"
)

type typedMeta struct {
	expireAt   int64
	hits       atomic.Int64
	hitLimited bool
}

func (md *typedMeta) deadForView(now int64) bool {
	if md.expireAt != 0 && now >= md.expireAt {
		return true
	}
	if md.hitLimited && md.hits.Load() <= 0 {
		return true
	}
	return false
}

func newTypedMeta(opt EntryOptions) *typedMeta {
	if opt.TTL <= 0 && opt.Hits <= 0 {
		return nil
	}
	md := &typedMeta{}
	if opt.TTL > 0 {
		md.expireAt = time.Now().Add(opt.TTL).UnixNano()
	}
	if opt.Hits > 0 {
		md.hitLimited = true
		md.hits.Store(opt.Hits)
	}
	return md
}

// TypedPair is a single key/value pair returned by Snapshot.
type TypedPair[K comparable, V any] struct {
	Key   K
	Value V
}

// TypedStats is a point-in-time view of a TypedMap's occupancy.
type TypedStats struct {
	Shards       int
	SlotCapacity int
	LiveEntries  int64
	UsedSlots    int64
	Tombstones   int64
	LoadFactor   float64
}

// Get returns the value stored for key and true, or the zero V and false when the
// key is absent or expired. Like Load, Get consumes one hit on a hit-limited entry.
func (m *TypedMap[K, V]) Get(key K) (V, bool) {
	return m.Load(key)
}

// Has reports whether key is present and still live, without consuming a hit.
func (m *TypedMap[K, V]) Has(key K) bool {
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]
	e := m.find(s.table.Load(), h, key)
	if e == nil {
		return false
	}
	if md := e.meta.Load(); md != nil && md.deadForView(time.Now().UnixNano()) {
		return false
	}
	return true
}

// Peek returns the value for key and true without consuming a hit, or the zero V
// and false when the key is absent or expired.
func (m *TypedMap[K, V]) Peek(key K) (V, bool) {
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]
	e := m.find(s.table.Load(), h, key)
	if e == nil {
		var zero V
		return zero, false
	}
	if md := e.meta.Load(); md != nil && md.deadForView(time.Now().UnixNano()) {
		var zero V
		return zero, false
	}
	return m.loadVal(e), true
}

// StoreWithTTL sets key to val with a time-to-live; the entry becomes invisible
// once the TTL elapses. A ttl of zero or less stores a non-expiring entry.
func (m *TypedMap[K, V]) StoreWithTTL(key K, val V, ttl time.Duration) {
	m.putMeta(key, val, newTypedMeta(EntryOptions{TTL: ttl}))
}

// StoreWithHits sets key to val with a read budget; each Load or Get consumes one
// hit and the entry disappears once the budget is exhausted. A hits of zero or less
// stores an entry with an unlimited budget.
func (m *TypedMap[K, V]) StoreWithHits(key K, val V, hits int64) {
	m.putMeta(key, val, newTypedMeta(EntryOptions{Hits: hits}))
}

// StoreWithTTLAndHits sets key to val with both a TTL and a read budget; whichever
// limit is reached first removes the entry.
func (m *TypedMap[K, V]) StoreWithTTLAndHits(key K, val V, ttl time.Duration, hits int64) {
	m.putMeta(key, val, newTypedMeta(EntryOptions{TTL: ttl, Hits: hits}))
}

// StoreWithOptions sets key to val applying the supplied EntryOptions (TTL, Hits).
func (m *TypedMap[K, V]) StoreWithOptions(key K, val V, opt EntryOptions) {
	m.putMeta(key, val, newTypedMeta(opt))
}

// SetWithTTLAndHits is an alias for StoreWithTTLAndHits.
func (m *TypedMap[K, V]) SetWithTTLAndHits(key K, val V, ttl time.Duration, hits int64) {
	m.StoreWithTTLAndHits(key, val, ttl, hits)
}

func (m *TypedMap[K, V]) putMeta(key K, val V, md *typedMeta) {
	if md != nil {
		m.hasMeta.Store(true)
		m.maybeStartCleaner()
	}
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]

	if e := m.find(s.table.Load(), h, key); e != nil {
		m.storeVal(e, val)
		e.meta.Store(md)
		return
	}

	s.mu.Lock()
	t := s.table.Load()
	used := int(t.count.Load() + t.tombstones.Load())
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
			ne.meta.Store(md)
			if firstTomb >= 0 {
				if t.slots[firstTomb].CompareAndSwap(m.tomb, ne) {
					t.tombstones.Add(-1)
					t.count.Add(1)
					s.mu.Unlock()
					return
				}
				firstTomb = -1
				continue
			}
			if t.slots[idx].CompareAndSwap(nil, ne) {
				t.count.Add(1)
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
			e.meta.Store(md)
			s.mu.Unlock()
			return
		}
		idx = (idx + 1) & t.mask
	}
}

// LoadOrStore returns the existing value for key and true if present and live,
// otherwise stores val and returns val with false.
func (m *TypedMap[K, V]) LoadOrStore(key K, val V) (V, bool) {
	return m.loadOrStoreMeta(key, val, nil)
}

// LoadOrStoreWithOptions is LoadOrStore that applies opt to a newly stored entry.
func (m *TypedMap[K, V]) LoadOrStoreWithOptions(key K, val V, opt EntryOptions) (V, bool) {
	return m.loadOrStoreMeta(key, val, newTypedMeta(opt))
}

func (m *TypedMap[K, V]) loadOrStoreMeta(key K, val V, md *typedMeta) (V, bool) {
	if md != nil {
		m.hasMeta.Store(true)
		m.maybeStartCleaner()
	}
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]
	if e := m.find(s.table.Load(), h, key); e != nil {
		if emd := e.meta.Load(); emd == nil || !emd.deadForView(time.Now().UnixNano()) {
			return m.loadVal(e), true
		}
	}

	s.mu.Lock()
	t := s.table.Load()
	used := int(t.count.Load() + t.tombstones.Load())
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
			ne.meta.Store(md)
			if firstTomb >= 0 {
				if t.slots[firstTomb].CompareAndSwap(m.tomb, ne) {
					t.tombstones.Add(-1)
					t.count.Add(1)
					s.mu.Unlock()
					return val, false
				}
				firstTomb = -1
				continue
			}
			if t.slots[idx].CompareAndSwap(nil, ne) {
				t.count.Add(1)
				s.mu.Unlock()
				return val, false
			}
			continue
		}
		if e == m.tomb {
			if firstTomb < 0 {
				firstTomb = int(idx)
			}
		} else if e.hash == h && e.key == key {
			if emd := e.meta.Load(); emd == nil || !emd.deadForView(time.Now().UnixNano()) {
				v := m.loadVal(e)
				s.mu.Unlock()
				return v, true
			}
			m.storeVal(e, val)
			e.meta.Store(md)
			s.mu.Unlock()
			return val, false
		}
		idx = (idx + 1) & t.mask
	}
}

// Swap stores val for key and returns the previous value with true, or the zero V
// and false when the key was absent or expired.
func (m *TypedMap[K, V]) Swap(key K, val V) (V, bool) {
	return m.swapMeta(key, val, nil)
}

// SwapWithOptions is Swap that applies opt to the replacement value.
func (m *TypedMap[K, V]) SwapWithOptions(key K, val V, opt EntryOptions) (V, bool) {
	return m.swapMeta(key, val, newTypedMeta(opt))
}

func (m *TypedMap[K, V]) swapMeta(key K, val V, md *typedMeta) (V, bool) {
	if md != nil {
		m.hasMeta.Store(true)
		m.maybeStartCleaner()
	}
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]

	s.mu.Lock()
	t := s.table.Load()
	used := int(t.count.Load() + t.tombstones.Load())
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
			ne.meta.Store(md)
			if firstTomb >= 0 {
				if t.slots[firstTomb].CompareAndSwap(m.tomb, ne) {
					t.tombstones.Add(-1)
					t.count.Add(1)
					s.mu.Unlock()
					var zero V
					return zero, false
				}
				firstTomb = -1
				continue
			}
			if t.slots[idx].CompareAndSwap(nil, ne) {
				t.count.Add(1)
				s.mu.Unlock()
				var zero V
				return zero, false
			}
			continue
		}
		if e == m.tomb {
			if firstTomb < 0 {
				firstTomb = int(idx)
			}
		} else if e.hash == h && e.key == key {
			prev := m.loadVal(e)
			prevDead := false
			if pmd := e.meta.Load(); pmd != nil && pmd.deadForView(time.Now().UnixNano()) {
				prevDead = true
			}
			m.storeVal(e, val)
			e.meta.Store(md)
			s.mu.Unlock()
			if prevDead {
				var zero V
				return zero, false
			}
			return prev, true
		}
		idx = (idx + 1) & t.mask
	}
}

type ComputeOp int

const (
	CancelOp ComputeOp = iota
	UpdateOp
	DeleteOp
)

const computeFastAttempts = 2

func (m *TypedMap[K, V]) Compute(key K, fn func(oldValue V, loaded bool) (newValue V, op ComputeOp)) (V, bool) {
	var zero V
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]

	if !m.valPtr && !m.hasMeta.Load() {
		if e := m.find(s.table.Load(), h, key); e != nil {
			for attempt := 0; attempt < computeFastAttempts; attempt++ {
				ob := e.bits.Load()
				old := fromBits[V](ob)
				newValue, op := fn(old, true)
				if op == UpdateOp {
					if e.bits.CompareAndSwap(ob, toBits(newValue)) {
						return newValue, true
					}
					continue
				}
				if op == CancelOp {
					return old, true
				}
				break
			}
		}
	}

	s.mu.Lock()
	t := s.table.Load()
	used := int(t.count.Load() + t.tombstones.Load())
	if (used+1)*4 >= len(t.slots)*3 {
		t = m.growLocked(s, t)
	}
	idx := h & t.mask
	firstTomb := -1
	for probes := 0; probes < len(t.slots); probes++ {
		e := t.slots[idx].Load()
		if e == nil {
			newValue, op := fn(zero, false)
			if op != UpdateOp {
				s.mu.Unlock()
				return zero, false
			}
			ne := s.newEntry()
			ne.hash = h
			ne.key = key
			m.storeVal(ne, newValue)
			if firstTomb >= 0 {
				if t.slots[firstTomb].CompareAndSwap(m.tomb, ne) {
					t.tombstones.Add(-1)
					t.count.Add(1)
					s.mu.Unlock()
					return newValue, true
				}
				firstTomb = -1
				continue
			}
			if t.slots[idx].CompareAndSwap(nil, ne) {
				t.count.Add(1)
				s.mu.Unlock()
				return newValue, true
			}
			continue
		}
		if e == m.tomb {
			if firstTomb < 0 {
				firstTomb = int(idx)
			}
		} else if e.hash == h && e.key == key {
			if !m.valPtr {
				for {
					ob := e.bits.Load()
					old := zero
					loaded := true
					if md := e.meta.Load(); md != nil && md.deadForView(time.Now().UnixNano()) {
						loaded = false
					} else {
						old = fromBits[V](ob)
					}
					newValue, op := fn(old, loaded)
					switch op {
					case UpdateOp:
						if !e.bits.CompareAndSwap(ob, toBits(newValue)) {
							continue
						}
						if e.meta.Load() != nil {
							e.meta.Store(nil)
						}
						s.mu.Unlock()
						return newValue, true
					case DeleteOp:
						if t.slots[idx].CompareAndSwap(e, m.tomb) {
							t.count.Add(-1)
							t.tombstones.Add(1)
						}
						s.mu.Unlock()
						return old, false
					default:
						s.mu.Unlock()
						return old, loaded
					}
				}
			}
			old := zero
			loaded := true
			if md := e.meta.Load(); md != nil && md.deadForView(time.Now().UnixNano()) {
				loaded = false
			} else {
				old = m.loadVal(e)
			}
			newValue, op := fn(old, loaded)
			switch op {
			case UpdateOp:
				m.storeVal(e, newValue)
				if e.meta.Load() != nil {
					e.meta.Store(nil)
				}
				s.mu.Unlock()
				return newValue, true
			case DeleteOp:
				if t.slots[idx].CompareAndSwap(e, m.tomb) {
					t.count.Add(-1)
					t.tombstones.Add(1)
				}
				s.mu.Unlock()
				return old, false
			default:
				s.mu.Unlock()
				return old, loaded
			}
		}
		idx = (idx + 1) & t.mask
	}
	s.mu.Unlock()
	return zero, false
}

// CompareAndSwap atomically replaces old with new for key, returning true on
// success. It fails if the key is absent, expired, or its current value differs.
func (m *TypedMap[K, V]) CompareAndSwap(key K, old, new V) bool {
	return m.compareAndSwapMeta(key, old, new, nil)
}

// CompareAndSwapWithOptions is CompareAndSwap that applies opt to new on success.
func (m *TypedMap[K, V]) CompareAndSwapWithOptions(key K, old, new V, opt EntryOptions) bool {
	return m.compareAndSwapMeta(key, old, new, newTypedMeta(opt))
}

func (m *TypedMap[K, V]) compareAndSwapMeta(key K, old, new V, md *typedMeta) bool {
	if md != nil {
		m.hasMeta.Store(true)
		m.maybeStartCleaner()
	}
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]
	s.mu.Lock()
	defer s.mu.Unlock()
	e := m.find(s.table.Load(), h, key)
	if e == nil {
		return false
	}
	if emd := e.meta.Load(); emd != nil && emd.deadForView(time.Now().UnixNano()) {
		return false
	}
	if !m.valEqual(e, old) {
		return false
	}
	m.storeVal(e, new)
	e.meta.Store(md)
	return true
}

// CompareAndDelete removes key only if its current value equals old, returning true
// on success.
func (m *TypedMap[K, V]) CompareAndDelete(key K, old V) bool {
	h := m.hash(key)
	s := &m.shards[(h>>32)&m.shardMask]
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.table.Load()
	idx := h & t.mask
	for probes := 0; probes < len(t.slots); probes++ {
		e := t.slots[idx].Load()
		if e == nil {
			return false
		}
		if e != m.tomb && e.hash == h && e.key == key {
			if emd := e.meta.Load(); emd != nil && emd.deadForView(time.Now().UnixNano()) {
				return false
			}
			if !m.valEqual(e, old) {
				return false
			}
			t.slots[idx].Store(m.tomb)
			t.count.Add(-1)
			t.tombstones.Add(1)
			return true
		}
		idx = (idx + 1) & t.mask
	}
	return false
}

// Snapshot returns every live key/value pair at the moment of the call.
func (m *TypedMap[K, V]) Snapshot() []TypedPair[K, V] {
	now := time.Now().UnixNano()
	out := make([]TypedPair[K, V], 0, m.Len())
	for si := range m.shards {
		t := m.shards[si].table.Load()
		for j := range t.slots {
			e := t.slots[j].Load()
			if e == nil || e == m.tomb {
				continue
			}
			if md := e.meta.Load(); md != nil && md.deadForView(now) {
				continue
			}
			out = append(out, TypedPair[K, V]{Key: e.key, Value: m.loadVal(e)})
		}
	}
	return out
}

// RangePar visits every live pair using one goroutine per shard, stopping early if
// visitor returns false. visitor must be safe for concurrent use.
func (m *TypedMap[K, V]) RangePar(visitor func(key K, value V) bool) {
	now := time.Now().UnixNano()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for si := range m.shards {
		t := m.shards[si].table.Load()
		wg.Add(1)
		go func(t *typedTable[K]) {
			defer wg.Done()
			for j := range t.slots {
				if stop.Load() {
					return
				}
				e := t.slots[j].Load()
				if e == nil || e == m.tomb {
					continue
				}
				if md := e.meta.Load(); md != nil && md.deadForView(now) {
					continue
				}
				if !visitor(e.key, m.loadVal(e)) {
					stop.Store(true)
					return
				}
			}
		}(t)
	}
	wg.Wait()
}

// Clear removes every entry, leaving an empty map.
func (m *TypedMap[K, V]) Clear() {
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		old := s.table.Load()
		nt := &typedTable[K]{
			slots: make([]atomic.Pointer[typedEntry[K]], len(old.slots)),
			mask:  old.mask,
		}
		s.table.Store(nt)
		s.chunk = nil
		s.off = 0
		s.mu.Unlock()
	}
}

// CleanupNow rebuilds every shard, dropping expired, hit-exhausted, and tombstoned
// slots. TypedMap has no background cleanup, so call this to reclaim space.
func (m *TypedMap[K, V]) CleanupNow() {
	now := time.Now().UnixNano()
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		old := s.table.Load()
		reclaimable := false
		for j := range old.slots {
			e := old.slots[j].Load()
			if e == m.tomb {
				reclaimable = true
				break
			}
			if e != nil {
				if md := e.meta.Load(); md != nil && md.deadForView(now) {
					reclaimable = true
					break
				}
			}
		}
		if !reclaimable {
			s.mu.Unlock()
			continue
		}
		s.resizeGen.Add(1)
		size := targetTypedSlots(len(old.slots), int(old.count.Load()), int(old.tombstones.Load()))
		nt := &typedTable[K]{
			slots: make([]atomic.Pointer[typedEntry[K]], size),
			mask:  uint64(size - 1),
		}
		var cnt int64
		for j := range old.slots {
			e := old.slots[j].Load()
			if e == nil || e == m.tomb {
				continue
			}
			if md := e.meta.Load(); md != nil && md.deadForView(now) {
				continue
			}
			idx := e.hash & nt.mask
			for nt.slots[idx].Load() != nil {
				idx = (idx + 1) & nt.mask
			}
			nt.slots[idx].Store(e)
			cnt++
		}
		nt.count.Store(cnt)
		s.table.Store(nt)
		s.resizeGen.Add(1)
		s.mu.Unlock()
	}
}

// Close stops the background cleanup goroutine, if one was started. It is safe to
// call multiple times and leaves the map fully usable for reads and writes.
func (m *TypedMap[K, V]) Close() {
	if m.cleanupInterval <= 0 {
		return
	}
	if m.cleanupClosed.CompareAndSwap(false, true) {
		close(m.stopCleanup)
		if m.cleanerStarted.Load() {
			<-m.cleanupDone
		}
	}
}

// Stats returns a point-in-time view of occupancy across all shards.
func (m *TypedMap[K, V]) Stats() TypedStats {
	now := time.Now().UnixNano()
	st := TypedStats{Shards: len(m.shards)}
	for i := range m.shards {
		t := m.shards[i].table.Load()
		st.SlotCapacity += len(t.slots)
		tomb := t.tombstones.Load()
		if tomb < 0 {
			tomb = 0
		}
		st.Tombstones += tomb
		for j := range t.slots {
			e := t.slots[j].Load()
			if e == nil || e == m.tomb {
				continue
			}
			st.UsedSlots++
			if md := e.meta.Load(); md != nil && md.deadForView(now) {
				continue
			}
			st.LiveEntries++
		}
	}
	if st.SlotCapacity > 0 {
		st.LoadFactor = float64(st.UsedSlots) / float64(st.SlotCapacity)
	}
	return st
}
