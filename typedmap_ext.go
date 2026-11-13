package alosmap

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type typedMeta struct {
	expireAt   int64
	hits       atomic.Int64
	bits       uint64
	ptr        unsafe.Pointer
	hitLimited bool
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
	return m.lookup(key, true)
}

// Has reports whether key is present and still live, without consuming a hit.
func (m *TypedMap[K, V]) Has(key K) bool {
	_, live := m.lookup(key, false)
	return live
}

// Peek returns the value for key and true without consuming a hit, or the zero V
// and false when the key is absent or expired.
func (m *TypedMap[K, V]) Peek(key K) (V, bool) {
	return m.lookup(key, false)
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
	if md == nil {
		m.Store(key, val)
		return
	}
	m.maybeStartCleaner()
	_, h, _ := m.seek(key)
	m.storeLocked(h, key, val, md)
}

// LoadOrStore returns the existing value for key and true if present and live,
// otherwise stores val and returns val with false.
func (m *TypedMap[K, V]) LoadOrStore(key K, val V) (V, bool) {
	return m.loadOrStoreMeta(key, val, nil)
}

// LoadOrStoreWithOptions is LoadOrStore that applies opt to a newly stored entry.
func (m *TypedMap[K, V]) LoadOrStoreWithOptions(key K, val V, opt EntryOptions) (V, bool) {
	md := newTypedMeta(opt)
	if md != nil {
		m.maybeStartCleaner()
	}
	return m.loadOrStoreMeta(key, val, md)
}

func (m *TypedMap[K, V]) loadOrStoreMeta(key K, val V, md *typedMeta) (V, bool) {
	e, h, _ := m.seek(key)
	if e != nil {
		if v, state := m.peekEntry(e); state == typedStateLive {
			return v, true
		}
	}
	shardIndex := h & m.shardMask
	s := &m.shards[shardIndex]
	if !s.mu.TryLock() {
		s.lockContended()
	}
	t := m.roomLocked(s, shardIndex, m.needsEntry(val, md))
	e, idx := m.locate(t, h, key)
	if e == nil {
		m.insertLocked(s, t, idx, h, key, val, md)
		s.mu.Unlock()
		return val, false
	}
	if v, state := m.readEntry(e, 0, false); state == typedStateLive {
		s.mu.Unlock()
		return v, true
	}
	m.exchangeLocked(s, t, idx, e, val, md)
	s.mu.Unlock()
	return val, false
}

// Swap stores val for key and returns the previous value with true, or the zero V
// and false when the key was absent or expired.
func (m *TypedMap[K, V]) Swap(key K, val V) (V, bool) {
	e, h, _ := m.seek(key)
	if e != nil && !m.isMeta(e) {
		if prev, swapped := m.tryFastSwap(e, val); swapped {
			return prev, true
		}
	}
	return m.swapLocked(h, key, val, nil)
}

// SwapWithOptions is Swap that applies opt to the replacement value.
func (m *TypedMap[K, V]) SwapWithOptions(key K, val V, opt EntryOptions) (V, bool) {
	md := newTypedMeta(opt)
	if md != nil {
		m.maybeStartCleaner()
	}
	_, h, _ := m.seek(key)
	return m.swapLocked(h, key, val, md)
}

func (m *TypedMap[K, V]) swapLocked(h uint64, key K, val V, md *typedMeta) (V, bool) {
	shardIndex := h & m.shardMask
	s := &m.shards[shardIndex]
	if !s.mu.TryLock() {
		s.lockContended()
	}
	t := m.roomLocked(s, shardIndex, m.needsEntry(val, md))
	e, idx := m.locate(t, h, key)
	if e == nil {
		m.insertLocked(s, t, idx, h, key, val, md)
		s.mu.Unlock()
		var zero V
		return zero, false
	}
	prev, live := m.exchangeLocked(s, t, idx, e, val, md)
	s.mu.Unlock()
	if !live {
		var zero V
		return zero, false
	}
	return prev, true
}

type ComputeOp int

const (
	CancelOp ComputeOp = iota
	UpdateOp
	DeleteOp
)

const computeFastAttempts = 2

func (m *TypedMap[K, V]) Compute(key K, fn func(oldValue V, loaded bool) (newValue V, op ComputeOp)) (V, bool) {
	found, h, _ := m.seek(key)
	if e := found; e != nil && !m.isMeta(e) {
		for attempt := 0; attempt < computeFastAttempts; attempt++ {
			old, live := m.loadCell(e)
			if !live {
				break
			}
			newValue, op := fn(old, true)
			if op == CancelOp {
				return old, true
			}
			if op != UpdateOp || m.isReservedLiteral(newValue) {
				break
			}
			if m.casCell(e, old, newValue) {
				return newValue, true
			}
		}
	}

	shardIndex := h & m.shardMask
	s := &m.shards[shardIndex]
	if !s.mu.TryLock() {
		s.lockContended()
	}
	defer s.mu.Unlock()
	t := m.roomLocked(s, shardIndex, false)
	e, idx := m.locate(t, h, key)
	var zero V
	if e == nil {
		return m.computeAbsentLocked(s, t, idx, h, key, fn)
	}
	if m.isMeta(e) {
		old, state := m.readEntry(e, 0, false)
		loaded := state == typedStateLive
		if !loaded {
			old = zero
		}
		newValue, op := fn(old, loaded)
		switch op {
		case UpdateOp:
			m.exchangeLocked(s, t, idx, e, newValue, nil)
			return newValue, true
		case DeleteOp:
			m.sealCell(e)
			m.vacateLocked(s, t, idx)
			return old, false
		default:
			return old, loaded
		}
	}
	for {
		old, live := m.loadCell(e)
		if !live {
			return m.computeAbsentLocked(s, t, idx, h, key, fn)
		}
		newValue, op := fn(old, true)
		switch op {
		case UpdateOp:
			if md := m.metaFor(newValue, nil); md == nil {
				if m.casCell(e, old, newValue) {
					return newValue, true
				}
			} else if t.flat() {
				t = m.promoteLocked(s, shardIndex, t)
				if e, idx = m.locate(t, h, key); e == nil {
					return m.computeAbsentLocked(s, t, idx, h, key, fn)
				}
			} else if m.sealCellIf(e, old) {
				m.replaceLocked(s, t, idx, e, newValue, md)
				return newValue, true
			}
		case DeleteOp:
			if m.retireIf(s, t, idx, e, old) {
				return old, false
			}
		default:
			return old, true
		}
	}
}

// CompareAndSwap atomically replaces old with new for key, returning true on
// success. It fails if the key is absent, expired, or its current value differs.
func (m *TypedMap[K, V]) computeAbsentLocked(s *typedShard, t typedTable, idx, h uint64, key K, fn func(oldValue V, loaded bool) (newValue V, op ComputeOp)) (V, bool) {
	var zero V
	newValue, op := fn(zero, false)
	if op != UpdateOp {
		return zero, false
	}
	if t.flat() && m.isReservedLiteral(newValue) {
		t = m.promoteLocked(s, h&m.shardMask, t)
		_, idx = m.locate(t, h, key)
	}
	m.insertLocked(s, t, idx, h, key, newValue, nil)
	return newValue, true
}

func (m *TypedMap[K, V]) CompareAndSwap(key K, old, new V) bool {
	e, h, _ := m.seek(key)
	if e == nil {
		return false
	}
	if !m.isMeta(e) && !m.isReservedLiteral(old) && !m.isReservedLiteral(new) {
		if m.casCell(e, old, new) {
			return true
		}
		if _, live := m.loadCell(e); live {
			return false
		}
	}
	return m.compareAndSwapLocked(h, key, old, new, nil)
}

// CompareAndSwapWithOptions is CompareAndSwap that applies opt to new on success.
func (m *TypedMap[K, V]) CompareAndSwapWithOptions(key K, old, new V, opt EntryOptions) bool {
	md := newTypedMeta(opt)
	if md != nil {
		m.maybeStartCleaner()
	}
	_, h, _ := m.seek(key)
	return m.compareAndSwapLocked(h, key, old, new, md)
}

func (m *TypedMap[K, V]) compareAndSwapLocked(h uint64, key K, old, new V, md *typedMeta) bool {
	shardIndex := h & m.shardMask
	s := &m.shards[shardIndex]
	if !s.mu.TryLock() {
		s.lockContended()
	}
	t := m.tableAt(shardIndex)
	if t.flat() && m.needsEntry(new, md) {
		t = m.promoteLocked(s, shardIndex, t)
	}
	e, idx := m.locate(t, h, key)
	swapped := false
	switch {
	case e == nil:
	case m.isMeta(e):
		if current, state := m.readEntry(e, 0, false); state == typedStateLive && m.sameValue(current, old) {
			m.exchangeLocked(s, t, idx, e, new, md)
			swapped = true
		}
	case m.isReservedLiteral(old):
	default:
		if next := m.metaFor(new, md); next == nil {
			swapped = m.casCell(e, old, new)
		} else if m.sealCellIf(e, old) {
			m.replaceLocked(s, t, idx, e, new, next)
			swapped = true
		}
	}
	s.mu.Unlock()
	return swapped
}

// CompareAndDelete removes key only if its current value equals old, returning true
// on success.
func (m *TypedMap[K, V]) CompareAndDelete(key K, old V) bool {
	found, h, _ := m.seek(key)
	if found == nil {
		return false
	}
	shardIndex := h & m.shardMask
	s := &m.shards[shardIndex]
	if !s.mu.TryLock() {
		s.lockContended()
	}
	t := m.tableAt(shardIndex)
	e, idx := m.locate(t, h, key)
	deleted := false
	switch {
	case e == nil:
	case m.isMeta(e):
		if current, state := m.readEntry(e, 0, false); state == typedStateLive && m.sameValue(current, old) {
			m.sealCell(e)
			m.vacateLocked(s, t, idx)
			deleted = true
		}
	default:
		deleted = !m.isReservedLiteral(old) && m.retireIf(s, t, idx, e, old)
	}
	s.mu.Unlock()
	return deleted
}

// Snapshot returns every live key/value pair at the moment of the call.
func (m *TypedMap[K, V]) Snapshot() []TypedPair[K, V] {
	out := make([]TypedPair[K, V], 0, m.Len())
	m.Range(func(key K, value V) bool {
		out = append(out, TypedPair[K, V]{Key: key, Value: value})
		return true
	})
	return out
}

// RangePar visits every live pair using one goroutine per shard, stopping early if
// visitor returns false. visitor must be safe for concurrent use.
func (m *TypedMap[K, V]) RangePar(visitor func(key K, value V) bool) {
	now := time.Now().UnixNano()
	var stop atomic.Bool
	var wg sync.WaitGroup
	guarded := func(key K, value V) bool {
		if stop.Load() {
			return false
		}
		if !visitor(key, value) {
			stop.Store(true)
			return false
		}
		return true
	}
	for shardIndex := range m.shards {
		wg.Add(1)
		go func(shardIndex uint64) {
			defer wg.Done()
			m.scanShard(shardIndex, now, guarded)
		}(uint64(shardIndex))
	}
	wg.Wait()
}

// Clear removes every entry, leaving an empty map.
func (m *TypedMap[K, V]) Clear() {
	for i := range m.shards {
		shardIndex := uint64(i)
		s := &m.shards[i]
		if !s.mu.TryLock() {
			s.lockContended()
		}
		m.resetLocked(s, shardIndex)
		s.chunk = nil
		s.chunkLen = 0
		s.off = 0
		s.chunked = 0
		s.mu.Unlock()
	}
}

func (m *TypedMap[K, V]) staleLocked(s *typedShard, t typedTable, hasMeta bool, now int64) bool {
	if t.flat() {
		return s.used > s.live(t)
	}
	return hasMeta && m.hasDeadLocked(t, now)
}

func (m *TypedMap[K, V]) hasDeadLocked(t typedTable, now int64) bool {
	for pos := uint64(0); pos < t.words(); pos++ {
		p := t.entryAt(pos)
		if p == nil {
			continue
		}
		if e := (*typedEntry[K])(p); m.isMeta(e) {
			if _, state := m.readEntry(e, now, false); state == typedStateDead {
				return true
			}
		}
	}
	return false
}

// CleanupNow rebuilds every shard that holds expired or hit-exhausted entries or
// an oversized table. TypedMap has no background cleanup, so call this to reclaim
// space.
func (m *TypedMap[K, V]) CleanupNow() {
	now := time.Now().UnixNano()
	hasMeta := m.hasMeta.Load()
	for i := range m.shards {
		shardIndex := uint64(i)
		s := &m.shards[i]
		if !s.mu.TryLock() {
			s.lockContended()
		}
		t := m.tableAt(shardIndex)
		if s.wantGroups(t) < t.groups() || m.staleLocked(s, t, hasMeta, now) {
			m.rebuildLocked(s, shardIndex, t, true, now, nil)
		}
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
		t := m.viewAt(uint64(i))
		st.SlotCapacity += t.slotCapacity()
		if t.flat() {
			m.flatStats(t, &st)
			continue
		}
		for pos := uint64(0); pos < t.words(); pos++ {
			p := t.entryAt(pos)
			if p == nil {
				continue
			}
			st.UsedSlots++
			if _, state := m.readEntry((*typedEntry[K])(p), now, false); state == typedStateLive {
				st.LiveEntries++
			}
		}
	}
	if st.SlotCapacity > 0 {
		st.LoadFactor = float64(st.UsedSlots) / float64(st.SlotCapacity)
	}
	return st
}
