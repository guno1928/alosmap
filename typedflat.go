package alosmap

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

const (
	typedMovedBits        uint64 = typedSealBits + 2
	typedDeadBits         uint64 = typedSealBits + 3
	typedReservedCount           = 4
	typedFlatTouchSlot           = 0
	typedFlatTouchOffset         = 8
	typedFlatSlots               = 4
	typedFlatSlotBytes           = 16
	typedFlatLineBytes           = typedFlatSlots * typedFlatSlotBytes
	typedFlatPresizeLoad         = 2
	typedFlatMaxLoad             = 3
	typedLaneBits                = 16
	typedLaneTop                 = typedLaneBits - 1
	typedLaneFull         uint64 = 1 << typedLaneTop
	typedLaneTagMask      uint64 = typedLaneFull - 1
	typedLaneMask         uint64 = 1<<typedLaneBits - 1
	typedLaneOnes         uint64 = 0x0001000100010001
	typedLaneHigh         uint64 = typedLaneOnes << typedLaneTop
	typedLaneBlocked      uint64 = typedLaneOnes
	typedChurnRatio              = 4
	typedChurnLoad               = 1
	typedFlatSteadyGroups        = 256
	typedFlatSteadyGrowth        = 2
)

var typedPromoted = newPromotedTable()

func newFlatTable(groups int) unsafe.Pointer {
	slotBytes := groups * typedFlatLineBytes
	ctrlBytes := max(groups*typedWordBytes, typedHandleAlign)
	start := unsafe.Pointer(unsafe.SliceData(make([]uint64, (slotBytes+ctrlBytes+2*typedHandleAlign)/typedWordBytes)))
	pad := -(uintptr(start) + uintptr(slotBytes)) & typedHandleMask
	return unsafe.Add(start, slotBytes+int(pad)+64-bits.TrailingZeros64(uint64(groups)))
}

func newTable(groups int, flat bool) unsafe.Pointer {
	if flat {
		return newFlatTable(groups)
	}
	return newTypedTable(groups)
}

func newPromotedTable() unsafe.Pointer {
	handle := newFlatTable(typedMinGroups)
	t := decodeFlatTable(handle)
	for group := uint64(0); group <= t.mask; group++ {
		t.lanes(group).Store(typedLaneBlocked)
	}
	return handle
}

func decodeFlatTable(handle unsafe.Pointer) typedTable {
	t := decodeTypedTable(handle)
	t.inline = true
	return t
}

func typedZeroLanes(word uint64) uint64 {
	return (word - typedLaneOnes) &^ word & typedLaneHigh
}

func typedFlatTag(h uint64) uint64 {
	return h>>typedFingerprintShift&typedLaneTagMask | typedLaneFull
}

func typedFlatPos(group, bit uint64) uint64 {
	return group*typedFlatSlots + uint64(bits.TrailingZeros64(bit))/typedLaneBits + 1
}

func typedLaneShift(pos uint64) uint64 {
	return (pos - 1) % typedFlatSlots * typedLaneBits
}

func (t typedTable) lanes(group uint64) *atomic.Uint64 {
	return (*atomic.Uint64)(unsafe.Add(t.base, uintptr(group)*typedWordBytes))
}

func (t typedTable) flat() bool {
	return t.inline
}

func (t typedTable) slotCapacity() int {
	if t.flat() {
		return t.groups() * typedFlatSlots
	}
	return t.groups() * typedGroupSlots
}

func (t typedTable) flatSlot(pos uint64) unsafe.Pointer {
	return unsafe.Add(t.base, int((pos-1)%typedFlatSlots)*typedFlatSlotBytes-int((pos-1)/typedFlatSlots+1)*typedFlatLineBytes)
}

func (t typedTable) holds(slot unsafe.Pointer) bool {
	return uintptr(t.base)-uintptr(slot)-1 < uintptr(t.groups())*typedFlatLineBytes
}

func (t typedTable) gone() *atomic.Int64 {
	return (*atomic.Int64)(unsafe.Add(t.base, max(t.groups()*typedWordBytes, typedHandleAlign)))
}

func (s *typedShard) live(t typedTable) int {
	if t.flat() {
		return int(s.count.Load() - t.gone().Load())
	}
	return int(s.count.Load())
}

func (s *typedShard) steadyLive(t typedTable) (int, bool) {
	if !t.flat() {
		return int(s.count.Load()), true
	}
	gone := t.gone()
	before := gone.Load()
	count := s.count.Load()
	return int(count - before), gone.Load() == before
}

func (s *typedShard) groupLoad(t typedTable) int {
	if !t.flat() {
		return typedPresizeGroupLoad
	}
	if dead := s.used - s.live(t); dead*typedChurnRatio >= max(s.used, 1) {
		return typedChurnLoad
	}
	return typedFlatPresizeLoad
}

func (t typedTable) tagged(pos uint64) bool {
	return t.lanes((pos-1)/typedFlatSlots).Load()>>typedLaneShift(pos)&typedLaneMask != 0
}

func (m *TypedMap[K, V]) entryCell(shardIndex uint64) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(m.entryTables, uintptr(shardIndex)*typedHandleBytes))
}

func (m *TypedMap[K, V]) flatProbe(handle unsafe.Pointer, h uint64, key K) *typedEntry[K] {
	shift := uintptr(handle) & typedHandleMask
	base := unsafe.Add(handle, -int(shift))
	want := typedFlatTag(h) * typedLaneOnes
	home := h >> shift
	for group := home; ; {
		ctrl := atomic.LoadUint64((*uint64)(unsafe.Add(base, int(group)*typedWordBytes)))
		for match := typedZeroLanes(ctrl ^ want); match != 0; match &= match - 1 {
			e := (*typedEntry[K])(unsafe.Add(base, bits.TrailingZeros64(match)-typedLaneTop+int(^group)*typedFlatLineBytes))
			if e.key == key {
				return e
			}
		}
		if typedZeroLanes(ctrl) != 0 {
			return nil
		}
		if group = (group + 1) & (^uint64(0) >> shift); group == home {
			return nil
		}
	}
}

func (m *TypedMap[K, V]) flatLookup(h uint64, key K) (V, uint8) {
	shardIndex := h & m.shardMask
	cell := m.handleCell(shardIndex)
	var zero V
	for {
		handle := atomic.LoadPointer(cell)
		if handle == typedPromoted {
			return zero, typedStatePromoted
		}
		e := m.flatProbe(handle, h, key)
		if e == nil {
			return zero, typedStateDead
		}
		b := e.val.Load()
		if b == typedMovedBits {
			next := atomic.LoadPointer(&m.shards[shardIndex].next)
			if atomic.LoadPointer(cell) != handle {
				continue
			}
			if next == nil {
				return zero, typedStateSealed
			}
			if e = m.flatProbe(next, h, key); e == nil {
				continue
			}
			if b = e.val.Load(); b == typedMovedBits {
				continue
			}
		}
		if b == typedDeadBits {
			return zero, typedStateDead
		}
		return fromBits[V](b), typedStateLive
	}
}

func (m *TypedMap[K, V]) flatLocate(t typedTable, h uint64, key K) (*typedEntry[K], uint64) {
	want := typedFlatTag(h) * typedLaneOnes
	for group := h >> t.shift; ; group = (group + 1) & t.mask {
		ctrl := t.lanes(group).Load()
		for match := typedZeroLanes(ctrl ^ want); match != 0; match &= match - 1 {
			pos := typedFlatPos(group, match)
			if e := (*typedEntry[K])(t.flatSlot(pos)); e.key == key {
				if e.val.Load() == typedDeadBits {
					return nil, pos
				}
				return e, pos
			}
		}
		if empty := typedZeroLanes(ctrl); empty != 0 {
			return nil, typedFlatPos(group, empty)
		}
	}
}

func settleFlatValue(cell *atomic.Uint64, pos, bits uint64) {
	if (pos-1)%typedFlatSlots == typedFlatTouchSlot {
		cell.Store(bits)
		return
	}
	*(*uint64)(unsafe.Pointer(cell)) = bits
}

func (m *TypedMap[K, V]) flatInsertLocked(s *typedShard, t typedTable, pos, h uint64, key K, val V) {
	e := (*typedEntry[K])(t.flatSlot(pos))
	s.count.Add(1)
	if t.tagged(pos) {
		e.val.Store(toBits(val))
		return
	}
	e.key = key
	settleFlatValue(&e.val, pos, toBits(val))
	c := t.lanes((pos - 1) / typedFlatSlots)
	c.Store(c.Load() | typedFlatTag(h)<<typedLaneShift(pos))
	s.used++
}

func (m *TypedMap[K, V]) flatExchangeLocked(s *typedShard, e *typedEntry[K], val V) (V, bool) {
	next := toBits(val)
	for {
		old := e.val.Load()
		if old == typedDeadBits {
			s.count.Add(1)
			e.val.Store(next)
			var zero V
			return zero, false
		}
		if e.val.CompareAndSwap(old, next) {
			return fromBits[V](old), true
		}
	}
}

func (m *TypedMap[K, V]) flatDelete(shardIndex uint64, e *typedEntry[K]) (V, uint8) {
	var zero V
	handle := atomic.LoadPointer(m.handleCell(shardIndex))
	if handle == typedPromoted {
		return zero, typedStateSealed
	}
	t := decodeFlatTable(handle)
	if !t.holds(unsafe.Pointer(e)) {
		return zero, typedStateSealed
	}
	for {
		b := e.val.Load()
		if b == typedDeadBits {
			return zero, typedStateDead
		}
		if b == typedMovedBits {
			return zero, typedStateSealed
		}
		if e.val.CompareAndSwap(b, typedDeadBits) {
			t.gone().Add(1)
			return fromBits[V](b), typedStateLive
		}
	}
}

func (m *TypedMap[K, V]) flatRemoveLocked(s *typedShard, e *typedEntry[K]) (V, bool) {
	b := e.val.Swap(typedDeadBits)
	if b == typedDeadBits {
		var zero V
		return zero, false
	}
	s.count.Add(-1)
	return fromBits[V](b), true
}

func (m *TypedMap[K, V]) retireIf(s *typedShard, t typedTable, pos uint64, e *typedEntry[K], old V) bool {
	if t.flat() {
		if !e.val.CompareAndSwap(toBits(old), typedDeadBits) {
			return false
		}
		s.count.Add(-1)
		return true
	}
	if !m.sealCellIf(e, old) {
		return false
	}
	m.vacateLocked(s, t, pos)
	return true
}

func (m *TypedMap[K, V]) flatMove(nt typedTable, src *typedEntry[K], b uint64) bool {
	h := m.entryHash(src)
	for group := h >> nt.shift; ; group = (group + 1) & nt.mask {
		c := nt.lanes(group)
		word := c.Load()
		empty := typedZeroLanes(word)
		if empty == 0 {
			continue
		}
		pos := typedFlatPos(group, empty)
		dst := (*typedEntry[K])(nt.flatSlot(pos))
		dst.key = src.key
		dst.val.Store(b)
		c.Store(word | typedFlatTag(h)<<typedLaneShift(pos))
		for !src.val.CompareAndSwap(b, typedMovedBits) {
			b = src.val.Load()
			dst.val.Store(b)
		}
		return b != typedDeadBits
	}
}

func (m *TypedMap[K, V]) flatRebuildLocked(s *typedShard, shardIndex uint64, old typedTable, handle unsafe.Pointer) typedTable {
	nt := decodeFlatTable(handle)
	atomic.StorePointer(&s.next, handle)
	s.epoch.Add(1)
	var moved, kept int
	for group := uint64(0); group <= old.mask; group++ {
		for full := old.lanes(group).Load() & typedLaneHigh; full != 0; full &= full - 1 {
			src := (*typedEntry[K])(old.flatSlot(typedFlatPos(group, full)))
			if b := src.val.Load(); b != typedDeadBits {
				moved++
				if m.flatMove(nt, src, b) {
					kept++
				}
			}
		}
	}
	s.used = moved
	s.count.Store(int64(kept))
	atomic.StorePointer(m.handleCell(shardIndex), handle)
	atomic.StorePointer(&s.next, nil)
	s.epoch.Add(1)
	return nt
}

func (m *TypedMap[K, V]) promoteLocked(s *typedShard, shardIndex uint64, old typedTable) typedTable {
	s.floor = max(typedMinGroups, s.floor*typedFlatPresizeLoad/typedPresizeGroupLoad)
	handle := newTypedTable(max(s.floor, targetTypedGroups(typedMinGroups, s.live(old), typedPresizeGroupLoad, typedGrowthFactor)))
	nt := decodeTypedTable(handle)
	s.epoch.Add(1)
	var kept int64
	for group := uint64(0); group <= old.mask; group++ {
		for full := old.lanes(group).Load() & typedLaneHigh; full != 0; full &= full - 1 {
			src := (*typedEntry[K])(old.flatSlot(typedFlatPos(group, full)))
			if src.val.Load() == typedDeadBits {
				continue
			}
			b := src.val.Swap(typedMovedBits)
			if b == typedDeadBits {
				continue
			}
			h := m.entryHash(src)
			e := m.newEntry(s, h, src.key, fromBits[V](b), nil)
			nt.placeFresh(unsafe.Pointer(e), h)
			kept++
		}
	}
	s.used = 0
	s.count.Store(kept)
	atomic.StorePointer(m.entryCell(shardIndex), handle)
	atomic.StorePointer(m.handleCell(shardIndex), typedPromoted)
	s.epoch.Add(1)
	return nt
}

func (m *TypedMap[K, V]) flatScan(t typedTable, visitor func(key K, value V) bool) bool {
	for group := uint64(0); group <= t.mask; group++ {
		for full := t.lanes(group).Load() & typedLaneHigh; full != 0; full &= full - 1 {
			e := (*typedEntry[K])(t.flatSlot(typedFlatPos(group, full)))
			b := e.val.Load()
			if b == typedDeadBits {
				continue
			}
			v := fromBits[V](b)
			if b == typedMovedBits {
				var live bool
				if v, live = m.lookupLocked(m.entryHash(e), e.key, false); !live {
					continue
				}
			}
			if !visitor(e.key, v) {
				return false
			}
		}
	}
	return true
}

func (m *TypedMap[K, V]) flatStats(t typedTable, st *TypedStats) {
	for group := uint64(0); group <= t.mask; group++ {
		for full := t.lanes(group).Load() & typedLaneHigh; full != 0; full &= full - 1 {
			st.UsedSlots++
			if (*typedEntry[K])(t.flatSlot(typedFlatPos(group, full))).val.Load() == typedDeadBits {
				st.Tombstones++
			} else {
				st.LiveEntries++
			}
		}
	}
}

func (m *TypedMap[K, V]) resetLocked(s *typedShard, shardIndex uint64) {
	t := m.tableAt(shardIndex)
	s.epoch.Add(1)
	if t.flat() {
		atomic.StorePointer(m.handleCell(shardIndex), newFlatTable(t.groups()))
	} else {
		atomic.StorePointer(m.entryCell(shardIndex), newTypedTable(t.groups()))
	}
	s.used = 0
	s.count.Store(0)
	s.epoch.Add(1)
}

func (m *TypedMap[K, V]) viewAt(shardIndex uint64) typedTable {
	if m.flat {
		if handle := atomic.LoadPointer(m.handleCell(shardIndex)); handle != typedPromoted {
			return decodeFlatTable(handle)
		}
	}
	return decodeTypedTable(atomic.LoadPointer(m.entryCell(shardIndex)))
}

func (m *TypedMap[K, V]) settledLive(shardIndex uint64) int {
	s := &m.shards[shardIndex]
	if !m.flat {
		return int(s.count.Load())
	}
	for attempt := 0; attempt < typedSettleAttempts; attempt++ {
		epoch := s.epoch.Load()
		if epoch&1 != 0 {
			spinPause()
			continue
		}
		live, steady := s.steadyLive(m.viewAt(shardIndex))
		if steady && s.epoch.Load() == epoch {
			return live
		}
	}
	if !s.mu.TryLock() {
		s.lockContended()
	}
	live := s.live(m.tableAt(shardIndex))
	s.mu.Unlock()
	return live
}
