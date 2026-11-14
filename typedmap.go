package alosmap

import (
	"hash/maphash"
	"math/bits"
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
// allocation and Load returns V directly. Reads and updates of existing keys are
// lock-free; inserts and deletes take a short per-shard lock. V must be no wider than 8 bytes (int, int64, float64, any
// pointer, or a struct that fits in a word); for larger values use a pointer type
// such as *MyStruct.
type TypedMap[K comparable, V any] struct {
	tables       unsafe.Pointer
	shardMask    uint64
	seed64       uint64
	plainStride  uintptr
	keyKind      uint8
	valPtr       bool
	padPlain     bool
	bare         bool
	flat         bool
	seed         maphash.Seed
	handles      []unsafe.Pointer
	entryTables  unsafe.Pointer
	entryHandles []unsafe.Pointer
	shards       []typedShard

	hasMeta         atomic.Bool
	cleanupInterval time.Duration
	cleanerStarted  atomic.Bool
	cleanupClosed   atomic.Bool
	stopCleanup     chan struct{}
	cleanupDone     chan struct{}
}

type typedShard struct {
	mu         sync.Mutex
	count      atomic.Int64
	floor      int
	used       int
	next       unsafe.Pointer
	epoch      atomic.Uint32
	pending    bool
	chunk      unsafe.Pointer
	chunkLen   int
	off        int
	depth      int
	firstChunk int
	chunked    int
	_          [32]byte
}

type typedEntry[K comparable] struct {
	key K
	val atomic.Uint64
}

type typedPtrEntry[K comparable] struct {
	key K
	_   [0]atomic.Uint64
	val unsafe.Pointer
}

type typedHashedEntry[K comparable] struct {
	entry typedEntry[K]
	hashw uint64
}

type typedPtrHashedEntry[K comparable] struct {
	entry typedPtrEntry[K]
	hashw uint64
}

type typedPadEntry[K comparable] struct {
	hashed typedHashedEntry[K]
	_      uint64
}

type typedPtrPadEntry[K comparable] struct {
	hashed typedPtrHashedEntry[K]
	_      uint64
}

type typedMetaEntry[K comparable] struct {
	hashed typedHashedEntry[K]
	meta   atomic.Pointer[typedMeta]
}

type typedPtrMetaEntry[K comparable] struct {
	hashed typedPtrHashedEntry[K]
	meta   atomic.Pointer[typedMeta]
}

const (
	typedSealBits          uint64  = 0xd1b54a32d192ed03
	typedMetaBits          uint64  = typedSealBits + 1
	typedSentinelCount             = 2
	typedHandleAlign               = 64
	typedHandleMask        uintptr = typedHandleAlign - 1
	typedHandleBytes               = unsafe.Sizeof(unsafe.Pointer(nil))
	typedByteBits                  = 8
	typedWordBytes                 = 8
	typedSlotPad                   = typedWordBytes - unsafe.Sizeof(uintptr(0))
	typedSlotBias                  = 1 + int(typedSlotPad)
	typedGroupSlots                = 7
	typedGroupWords                = typedGroupSlots + 1
	typedGroupBytes                = typedGroupWords * typedWordBytes
	typedMinGroups                 = 2
	typedShardBoost                = 2
	typedHeaderedMinGroups         = 16
	typedLargeTableGroups          = 512
	typedPresizeGroupLoad          = 4
	typedMaxGroupLoad              = 6
	typedGrowthFactor              = 4
	typedPrefetchGroups            = 2
	typedLockSpins                 = 16
	typedDefaultChunk              = 8
	typedBusySpins                 = 4096
	typedSettleAttempts            = 64
	typedOffLockGroups             = 64
	typedShrinkGroupRatio          = 8
	typedShrinkProbeMask           = 63
	typedFingerprintShift          = 25
	typedFingerprintMask   uint64  = 0x7f
	typedFullBit           uint64  = 0x80
	typedCtrlByteMask      uint64  = 0xff
	typedCtrlOnes          uint64  = 0x0001010101010101
	typedCtrlHigh          uint64  = 0x0080808080808080
	typedOverflowShift             = 56
	typedOverflowUp        uint64  = 1 << typedOverflowShift
	typedOverflowDown      uint64  = ^(typedOverflowUp - 1)
	typedOverflowMax       uint64  = typedCtrlByteMask << typedOverflowShift
	typedPadAlign                  = 16
	typedPadBytes                  = 8
)

const (
	typedStateLive uint8 = iota
	typedStateDead
	typedStateSealed
	typedStatePromoted
)

var typedSentinels [typedSentinelCount]byte

type typedSlot struct {
	_ [typedSlotPad]byte
	p unsafe.Pointer
}

type typedGroup struct {
	ctrl  atomic.Uint64
	slots [typedGroupSlots]typedSlot
}

type typedTailGroup struct {
	slots [typedGroupSlots]typedSlot
	ctrl  atomic.Uint64
}

type typedTable struct {
	base   unsafe.Pointer
	mask   uint64
	shift  uint
	inline bool
}

func alignedTypedHead(groups int) unsafe.Pointer {
	head := unsafe.Pointer(unsafe.SliceData(make([]typedGroup, groups)))
	if uintptr(head)&typedHandleMask != 0 {
		return nil
	}
	return head
}

func alignedTypedTail(groups int) unsafe.Pointer {
	tail := unsafe.Pointer(unsafe.SliceData(make([]typedTailGroup, groups+1)))
	if uintptr(tail)&typedHandleMask != typedWordBytes {
		return nil
	}
	return unsafe.Add(tail, typedGroupSlots*typedWordBytes)
}

func newTypedTable(groups int) unsafe.Pointer {
	var base unsafe.Pointer
	if groups >= typedHeaderedMinGroups && groups < typedLargeTableGroups {
		if base = alignedTypedTail(groups); base == nil {
			base = alignedTypedHead(groups)
		}
	} else if base = alignedTypedHead(groups); base == nil {
		base = alignedTypedTail(groups)
	}
	if base == nil {
		base = alignedTypedHead(max(groups, typedLargeTableGroups))
	}
	if base == nil {
		panic("alosmap: runtime returned a misaligned table allocation")
	}
	return unsafe.Add(base, 64-bits.TrailingZeros64(uint64(groups)))
}

func decodeTypedTable(handle unsafe.Pointer) typedTable {
	shift := uintptr(handle) & typedHandleMask
	return typedTable{
		base:  unsafe.Add(handle, -int(shift)),
		mask:  ^uint64(0) >> shift,
		shift: uint(shift),
	}
}

func typedZeroBytes(word uint64) uint64 {
	return (word - typedCtrlOnes) &^ word & typedCtrlHigh
}

func typedFingerprint(h uint64) uint64 {
	return h>>typedFingerprintShift&typedFingerprintMask | typedFullBit
}

func typedSlotPos(group, bit uint64) uint64 {
	return group*typedGroupWords + uint64(bits.TrailingZeros64(bit)+1)/typedByteBits
}

func typedCtrlByteShift(pos uint64) uint64 {
	return (pos%typedGroupWords - 1) * typedByteBits
}

func (t typedTable) groups() int {
	return int(t.mask) + 1
}

func (t typedTable) words() uint64 {
	return (t.mask + 1) * typedGroupWords
}

func (t typedTable) ctrl(group uint64) *atomic.Uint64 {
	return (*atomic.Uint64)(unsafe.Add(t.base, uintptr(group)*typedGroupBytes))
}

func (t typedTable) slot(pos uint64) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(t.base, uintptr(pos)*typedWordBytes+typedSlotPad))
}

func (t typedTable) posOf(slot *unsafe.Pointer) uint64 {
	offset := uintptr(unsafe.Pointer(slot)) - uintptr(t.base) - typedSlotPad
	if slot == nil || uint64(offset) >= t.words()*typedWordBytes {
		return 0
	}
	return uint64(offset) / typedWordBytes
}

func (t typedTable) owns(found unsafe.Pointer, slot *unsafe.Pointer, pos uint64) bool {
	if t.flat() {
		return t.holds(found)
	}
	return pos != 0 && atomic.LoadPointer(slot) == found
}

func (t typedTable) entryAt(pos uint64) unsafe.Pointer {
	if pos%typedGroupWords == 0 {
		return nil
	}
	return atomic.LoadPointer(t.slot(pos))
}

func (t typedTable) stepOverflow(h, group, delta uint64) {
	for home := h >> t.shift; home != group; home = (home + 1) & t.mask {
		if c := t.ctrl(home); c.Load() < typedOverflowMax {
			c.Store(c.Load() + delta)
		}
	}
}

func (t typedTable) occupy(pos, h uint64) {
	group := pos / typedGroupWords
	c := t.ctrl(group)
	c.Store(c.Load() | typedFingerprint(h)<<typedCtrlByteShift(pos))
	t.stepOverflow(h, group, typedOverflowUp)
}

func (t typedTable) release(pos, h uint64) {
	group := pos / typedGroupWords
	c := t.ctrl(group)
	c.Store(c.Load() &^ (typedCtrlByteMask << typedCtrlByteShift(pos)))
	t.stepOverflow(h, group, typedOverflowDown)
}

func (t typedTable) prefetchAhead(group uint64) {
	if ahead := group + typedPrefetchGroups; ahead <= t.mask {
		prefetchGroupSlots(unsafe.Pointer(t.ctrl(ahead)))
	}
}

func (t typedTable) placeFresh(p unsafe.Pointer, h uint64) {
	for group := h >> t.shift; ; group = (group + 1) & t.mask {
		word := (*uint64)(unsafe.Pointer(t.ctrl(group)))
		if empty := typedZeroBytes(*word); empty != 0 {
			pos := typedSlotPos(group, empty)
			*t.slot(pos) = p
			*word |= typedFingerprint(h) << typedCtrlByteShift(pos)
			return
		}
		if *word < typedOverflowMax {
			*word += typedOverflowUp
		}
	}
}

func (e *typedEntry[K]) ptrCell() *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Pointer(&e.val))
}

func (e *typedEntry[K]) hashCell() *uint64 {
	return (*uint64)(unsafe.Add(unsafe.Pointer(e), unsafe.Sizeof(*e)))
}

func (e *typedEntry[K]) metaCell() *atomic.Pointer[typedMeta] {
	return (*atomic.Pointer[typedMeta])(unsafe.Add(unsafe.Pointer(e), unsafe.Sizeof(*e)+typedWordBytes))
}

func typedSealPointer() unsafe.Pointer {
	return unsafe.Pointer(&typedSentinels[0])
}

func typedMetaPointer() unsafe.Pointer {
	return unsafe.Pointer(&typedSentinels[1])
}

func typedReservedPointer(p unsafe.Pointer) bool {
	return uintptr(p)-uintptr(unsafe.Pointer(&typedSentinels)) < typedSentinelCount
}

func typedReservedBits(b uint64) bool {
	return b-typedSealBits < typedReservedCount
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

func toPointerWord[V any](v V) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&v))
}

func fromPointerWord[V any](p unsafe.Pointer) V {
	return *(*V)(unsafe.Pointer(&p))
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

func (m *TypedMap[K, V]) isReservedLiteral(val V) bool {
	return !m.valPtr && typedReservedBits(toBits(val))
}

func (m *TypedMap[K, V]) needsEntry(val V, md *typedMeta) bool {
	return md != nil || m.isReservedLiteral(val)
}

func (m *TypedMap[K, V]) sameValue(left, right V) bool {
	if m.valPtr {
		return toPointerWord(left) == toPointerWord(right)
	}
	return toBits(left) == toBits(right)
}

func (m *TypedMap[K, V]) isMeta(e *typedEntry[K]) bool {
	if m.valPtr {
		return atomic.LoadPointer(e.ptrCell()) == typedMetaPointer()
	}
	return e.val.Load() == typedMetaBits
}

func (m *TypedMap[K, V]) entryHash(e *typedEntry[K]) uint64 {
	switch {
	case !m.bare:
		return *e.hashCell()
	case unsafe.Sizeof(e.key) == unsafe.Sizeof(int64(0)) && m.keyKind == keyKindInt64:
		return hashInt64(m.seed64, *(*int64)(unsafe.Pointer(&e.key)))
	default:
		return m.hash(e.key)
	}
}

func (m *TypedMap[K, V]) metaValue(md *typedMeta) V {
	if m.valPtr {
		return fromPointerWord[V](md.ptr)
	}
	return fromBits[V](md.bits)
}

func (m *TypedMap[K, V]) loadCell(e *typedEntry[K]) (V, bool) {
	if m.valPtr {
		p := atomic.LoadPointer(e.ptrCell())
		return fromPointerWord[V](p), !typedReservedPointer(p)
	}
	b := e.val.Load()
	return fromBits[V](b), !typedReservedBits(b)
}

func (m *TypedMap[K, V]) initCell(e *typedEntry[K], val V) {
	if m.valPtr {
		*e.ptrCell() = toPointerWord(val)
		return
	}
	*(*uint64)(unsafe.Pointer(&e.val)) = toBits(val)
}

func (m *TypedMap[K, V]) initMetaCell(e *typedEntry[K]) {
	if m.valPtr {
		*e.ptrCell() = typedMetaPointer()
		return
	}
	*(*uint64)(unsafe.Pointer(&e.val)) = typedMetaBits
}

func (m *TypedMap[K, V]) swapCell(e *typedEntry[K], val V) V {
	if m.valPtr {
		return fromPointerWord[V](atomic.SwapPointer(e.ptrCell(), toPointerWord(val)))
	}
	return fromBits[V](e.val.Swap(toBits(val)))
}

func (m *TypedMap[K, V]) casCell(e *typedEntry[K], old, new V) bool {
	if m.valPtr {
		return atomic.CompareAndSwapPointer(e.ptrCell(), toPointerWord(old), toPointerWord(new))
	}
	return e.val.CompareAndSwap(toBits(old), toBits(new))
}

func (m *TypedMap[K, V]) sealCell(e *typedEntry[K]) V {
	if m.valPtr {
		return fromPointerWord[V](atomic.SwapPointer(e.ptrCell(), typedSealPointer()))
	}
	return fromBits[V](e.val.Swap(typedSealBits))
}

func (m *TypedMap[K, V]) sealCellIf(e *typedEntry[K], old V) bool {
	if m.valPtr {
		return atomic.CompareAndSwapPointer(e.ptrCell(), toPointerWord(old), typedSealPointer())
	}
	return e.val.CompareAndSwap(toBits(old), typedSealBits)
}

func (m *TypedMap[K, V]) tryFastSwap(e *typedEntry[K], val V) (V, bool) {
	if m.isReservedLiteral(val) {
		var zero V
		return zero, false
	}
	for {
		old, live := m.loadCell(e)
		if !live {
			return old, false
		}
		if m.casCell(e, old, val) {
			return old, true
		}
	}
}

func (m *TypedMap[K, V]) readEntry(e *typedEntry[K], now int64, consume bool) (V, uint8) {
	if m.valPtr {
		p := atomic.LoadPointer(e.ptrCell())
		if p == typedSealPointer() {
			return fromPointerWord[V](nil), typedStateSealed
		}
		if p != typedMetaPointer() {
			return fromPointerWord[V](p), typedStateLive
		}
	} else {
		b := e.val.Load()
		if b != typedMetaBits {
			if typedReservedBits(b) {
				return fromBits[V](0), typedStateSealed
			}
			return fromBits[V](b), typedStateLive
		}
	}
	md := e.metaCell().Load()
	v := m.metaValue(md)
	if md.expireAt != 0 {
		if now == 0 {
			now = time.Now().UnixNano()
		}
		if now >= md.expireAt {
			return v, typedStateDead
		}
	}
	if md.hitLimited {
		if consume {
			if md.hits.Add(-1) < 0 {
				return v, typedStateDead
			}
		} else if md.hits.Load() <= 0 {
			return v, typedStateDead
		}
	}
	return v, typedStateLive
}

func (m *TypedMap[K, V]) peekEntry(e *typedEntry[K]) (V, uint8) {
	if v, live := m.loadCell(e); live {
		return v, typedStateLive
	}
	return m.readEntry(e, 0, false)
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
	autoShards := cfg.shardCount <= 0
	cfg.normalize()
	shardCount := cfg.shardCount
	if autoShards {
		shardCount = min(maxAutoShards, shardCount*typedShardBoost)
	}
	seed64 := avalanche(uint64(time.Now().UnixNano()) ^ (uint64(shardCount) << 32) ^ hashSeed2)
	if seed64 == 0 {
		seed64 = hashSeed1
	}
	keyKind := detectKeyKind[K]()
	bare := keyKind != keyKindOther
	valPtr := valueHasPointer[V]()
	if valPtr && unsafe.Sizeof(zero) != unsafe.Sizeof(unsafe.Pointer(nil)) {
		panic("alosmap: TypedMap value type V holds a pointer but is wider than one pointer on this platform; use a pointer (*T) instead")
	}
	flat := keyKind == keyKindInt64 && !valPtr
	groupLoad := typedPresizeGroupLoad
	if flat {
		groupLoad = typedFlatPresizeLoad
	}
	perShard := nextPowerOfTwo(maxInt(typedMinGroups, (cfg.capacity/shardCount+groupLoad-1)/groupLoad))
	entryBytes := unsafe.Sizeof(typedHashedEntry[K]{})
	if bare {
		entryBytes = unsafe.Sizeof(typedEntry[K]{})
	}
	m := &TypedMap[K, V]{
		seed:            maphash.MakeSeed(),
		seed64:          seed64,
		keyKind:         keyKind,
		bare:            bare,
		valPtr:          valPtr,
		flat:            flat,
		padPlain:        !bare && entryBytes%typedPadAlign == typedPadBytes,
		plainStride:     entryBytes,
		shardMask:       uint64(shardCount - 1),
		handles:         make([]unsafe.Pointer, shardCount),
		shards:          make([]typedShard, shardCount),
		cleanupInterval: cfg.cleanupInterval,
	}
	if m.padPlain {
		m.plainStride += typedPadBytes
	}
	for i := range m.handles {
		if m.flat {
			m.handles[i] = newFlatTable(perShard)
		} else {
			m.handles[i] = newTypedTable(perShard)
		}
		m.shards[i].floor = perShard
		m.shards[i].depth = typedDefaultChunk
		m.shards[i].firstChunk = typedDefaultChunk
	}
	m.tables = unsafe.Pointer(unsafe.SliceData(m.handles))
	m.entryTables = m.tables
	if m.flat {
		m.entryHandles = make([]unsafe.Pointer, shardCount)
		m.entryTables = unsafe.Pointer(unsafe.SliceData(m.entryHandles))
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

// Prealloc sets how many entry nodes each shard allocates per batch and returns
// the map for chaining. Each shard allocates entries in batches and hands them out as keys
// are inserted. A shard's first batch covers its share of the initial capacity,
// and later batches grow with the shard up to chunk entries.
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
		s := &m.shards[i]
		s.depth = chunk
		s.firstChunk = max(minSlabNodes, m.tableAt(uint64(i)).groups()*typedPresizeGroupLoad)
	}
	return m
}

func (m *TypedMap[K, V]) allocPlain(count int) *typedEntry[K] {
	switch {
	case m.bare && m.valPtr:
		return (*typedEntry[K])(unsafe.Pointer(unsafe.SliceData(make([]typedPtrEntry[K], count))))
	case m.bare:
		return unsafe.SliceData(make([]typedEntry[K], count))
	case m.valPtr && m.padPlain:
		return (*typedEntry[K])(unsafe.Pointer(unsafe.SliceData(make([]typedPtrPadEntry[K], count))))
	case m.valPtr:
		return (*typedEntry[K])(unsafe.Pointer(unsafe.SliceData(make([]typedPtrHashedEntry[K], count))))
	case m.padPlain:
		return (*typedEntry[K])(unsafe.Pointer(unsafe.SliceData(make([]typedPadEntry[K], count))))
	default:
		return (*typedEntry[K])(unsafe.Pointer(unsafe.SliceData(make([]typedHashedEntry[K], count))))
	}
}

func (m *TypedMap[K, V]) allocMeta() *typedEntry[K] {
	if m.valPtr {
		return (*typedEntry[K])(unsafe.Pointer(new(typedPtrMetaEntry[K])))
	}
	return (*typedEntry[K])(unsafe.Pointer(new(typedMetaEntry[K])))
}

func (m *TypedMap[K, V]) newPlain(s *typedShard) *typedEntry[K] {
	if s.off >= s.chunkLen {
		size := min(s.depth, s.firstChunk)
		if s.chunked > 0 {
			size = min(s.depth, max(minSlabNodes, s.chunked/2))
		}
		s.chunk = unsafe.Pointer(m.allocPlain(size))
		s.chunkLen = size
		s.chunked += size
		s.off = 0
	}
	e := (*typedEntry[K])(unsafe.Add(s.chunk, uintptr(s.off)*m.plainStride))
	s.off++
	return e
}

func (m *TypedMap[K, V]) metaFor(val V, md *typedMeta) *typedMeta {
	if md == nil {
		if !m.isReservedLiteral(val) {
			return nil
		}
		md = &typedMeta{}
	}
	if m.valPtr {
		md.ptr = toPointerWord(val)
	} else {
		md.bits = toBits(val)
	}
	return md
}

func (m *TypedMap[K, V]) newEntry(s *typedShard, h uint64, key K, val V, md *typedMeta) *typedEntry[K] {
	if md == nil {
		e := m.newPlain(s)
		if !m.bare {
			*e.hashCell() = h
		}
		e.key = key
		m.initCell(e, val)
		return e
	}
	if !m.hasMeta.Load() {
		m.hasMeta.Store(true)
	}
	e := m.allocMeta()
	*e.hashCell() = h
	e.metaCell().Store(md)
	e.key = key
	m.initMetaCell(e)
	return e
}

func (m *TypedMap[K, V]) handleCell(shardIndex uint64) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(m.tables, uintptr(shardIndex)*typedHandleBytes))
}

func (m *TypedMap[K, V]) tableAt(shardIndex uint64) typedTable {
	handle := atomic.LoadPointer(m.entryCell(shardIndex))
	inline := handle == nil
	if inline {
		handle = atomic.LoadPointer(m.handleCell(shardIndex))
	}
	shift := uintptr(handle) & typedHandleMask
	return typedTable{base: unsafe.Add(handle, -int(shift)), mask: ^uint64(0) >> shift, shift: uint(shift), inline: inline}
}

func (m *TypedMap[K, V]) hash(key K) uint64 {
	switch m.keyKind {
	case keyKindInt64:
		return hashInt64(m.seed64, *(*int64)(unsafe.Pointer(&key)))
	case keyKindString:
		return hashString(m.seed64, *(*string)(unsafe.Pointer(&key)))
	default:
		return maphash.Comparable(m.seed, key)
	}
}

func (m *TypedMap[K, V]) seek(key K) (*typedEntry[K], uint64, *unsafe.Pointer) {
	var h uint64
	switch {
	case unsafe.Sizeof(key) == unsafe.Sizeof(int64(0)) && m.flat:
		h = hashInt64(m.seed64, *(*int64)(unsafe.Pointer(&key)))
		if handle := atomic.LoadPointer(m.handleCell(h & m.shardMask)); handle != typedPromoted {
			shift := uintptr(handle) & typedHandleMask
			base := unsafe.Add(handle, -int(shift))
			group := int(h >> shift)
			ctrl := atomic.LoadUint64((*uint64)(unsafe.Add(base, group*typedWordBytes)))
			match := typedZeroLanes(ctrl ^ typedFlatTag(h)*typedLaneOnes)
			if match == 0 {
				if typedZeroLanes(ctrl) != 0 {
					return nil, h, nil
				}
				return m.seekCold(key)
			}
			line := unsafe.Add(base, ^group*typedFlatLineBytes)
			touchLine(line)
			e := (*typedEntry[K])(unsafe.Add(line, bits.TrailingZeros64(match)-typedLaneTop))
			if e.key != key {
				return m.seekCold(key)
			}
			return e, h, nil
		}
	case unsafe.Sizeof(key) == unsafe.Sizeof(int64(0)) && m.keyKind == keyKindInt64:
		h = hashInt64(m.seed64, *(*int64)(unsafe.Pointer(&key)))
	case unsafe.Sizeof(key) == unsafe.Sizeof("") && m.keyKind == keyKindString:
		text := *(*string)(unsafe.Pointer(&key))
		if length := len(text); uint(length-hashMidStringMin) <= hashMidStringMax-hashMidStringMin {
			h = hashMidString(m.seed64, unsafe.Pointer(unsafe.StringData(text)), length)
		} else if uint(length-hashShortStringMin) < hashMidStringMin-hashShortStringMin {
			h = hashShortString(m.seed64, unsafe.Pointer(unsafe.StringData(text)), length)
		} else if uint(length-hashTinyStringMin) < hashShortStringMin-hashTinyStringMin {
			h = hashTinyString(m.seed64, unsafe.Pointer(unsafe.StringData(text)), length)
		} else {
			h = hashString(m.seed64, text)
		}
	default:
		return m.seekCold(key)
	}
	handle := atomic.LoadPointer(m.entryCell(h & m.shardMask))
	shift := uintptr(handle) & typedHandleMask
	g := unsafe.Add(unsafe.Add(handle, -int(shift)), int(h>>shift)*typedGroupBytes)
	slots := unsafe.Add(g, typedSlotBias)
	ctrl := atomic.LoadUint64((*uint64)(g))
	match := typedZeroBytes(ctrl ^ typedFingerprint(h)*typedCtrlOnes)
	if match == 0 {
		if ctrl>>typedOverflowShift == 0 {
			return nil, h, nil
		}
		return m.seekCold(key)
	}
	slot := (*unsafe.Pointer)(unsafe.Add(slots, bits.TrailingZeros64(match)))
	e := (*typedEntry[K])(atomic.LoadPointer(slot))
	if e != nil && e.key == key {
		return e, h, slot
	}
	return m.seekCold(key)
}

func (m *TypedMap[K, V]) seekCold(key K) (*typedEntry[K], uint64, *unsafe.Pointer) {
	h := m.hash(key)
	if m.flat {
		if handle := atomic.LoadPointer(m.handleCell(h & m.shardMask)); handle != typedPromoted {
			return m.flatProbe(handle, h, key), h, nil
		}
	}
	return m.probe(atomic.LoadPointer(m.entryCell(h&m.shardMask)), h, key), h, nil
}

func (m *TypedMap[K, V]) probe(handle unsafe.Pointer, h uint64, key K) *typedEntry[K] {
	shift := uintptr(handle) & typedHandleMask
	want := typedFingerprint(h) * typedCtrlOnes
	home := h >> shift
	for group := home; ; {
		g := unsafe.Add(handle, int(group)*typedGroupBytes-int(shift))
		ctrl := atomic.LoadUint64((*uint64)(g))
		for match := typedZeroBytes(ctrl ^ want); match != 0; match &= match - 1 {
			e := (*typedEntry[K])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Add(g, bits.TrailingZeros64(match)+typedSlotBias))))
			if e != nil && e.key == key {
				return e
			}
		}
		if ctrl>>typedOverflowShift == 0 {
			return nil
		}
		if group = (group + 1) & (^uint64(0) >> shift); group == home {
			return nil
		}
	}
}

func (m *TypedMap[K, V]) locate(t typedTable, h uint64, key K) (*typedEntry[K], uint64) {
	if t.flat() {
		return m.flatLocate(t, h, key)
	}
	want := typedFingerprint(h) * typedCtrlOnes
	var free uint64
	absent := false
	home := h >> t.shift
	for group := home; ; {
		ctrl := t.ctrl(group).Load()
		for match := typedZeroBytes(ctrl ^ want); match != 0; match &= match - 1 {
			pos := typedSlotPos(group, match)
			if e := (*typedEntry[K])(atomic.LoadPointer(t.slot(pos))); e != nil && e.key == key {
				return e, pos
			}
		}
		if free == 0 {
			if empty := typedZeroBytes(ctrl); empty != 0 {
				free = typedSlotPos(group, empty)
			}
		}
		group = (group + 1) & t.mask
		absent = absent || ctrl>>typedOverflowShift == 0 || group == home
		if absent && free != 0 {
			return nil, free
		}
	}
}

func (s *typedShard) unsettled() bool {
	return s.epoch.Load()&1 != 0
}

func (s *typedShard) lockContended() {
	for spin := 0; spin < typedLockSpins; spin++ {
		spinPause()
		if s.mu.TryLock() {
			return
		}
	}
	for spin := 0; spin < typedBusySpins && s.unsettled(); spin++ {
		spinPause()
	}
	s.mu.Lock()
}

func targetTypedGroups(curGroups, live, groupLoad, growth int) int {
	required := nextPowerOfTwo(maxInt(typedMinGroups, (live+groupLoad-1)/groupLoad))
	if required > curGroups {
		return maxInt(curGroups*growth, required)
	}
	return required
}

func (s *typedShard) wantGroups(t typedTable) int {
	live, groupLoad := s.live(t), s.groupLoad(t)
	if groupLoad == typedChurnLoad {
		return max(s.floor, nextPowerOfTwo(max(typedMinGroups, live)))
	}
	growth := typedGrowthFactor
	if t.flat() && t.groups() >= typedFlatSteadyGroups {
		growth = typedFlatSteadyGrowth
	}
	return max(s.floor, targetTypedGroups(t.groups(), live, groupLoad, growth))
}

func (m *TypedMap[K, V]) rebuildLocked(s *typedShard, shardIndex uint64, old typedTable, dropDead bool, now int64, handle unsafe.Pointer) typedTable {
	if handle == nil {
		handle = newTable(s.wantGroups(old), old.flat())
	}
	if old.flat() {
		return m.flatRebuildLocked(s, shardIndex, old, handle)
	}
	nt := decodeTypedTable(handle)
	s.epoch.Add(1)
	var kept int64
	for group := uint64(0); group <= old.mask; group++ {
		old.prefetchAhead(group)
		for full := old.ctrl(group).Load() & typedCtrlHigh; full != 0; full &= full - 1 {
			p := atomic.LoadPointer(old.slot(typedSlotPos(group, full)))
			e := (*typedEntry[K])(p)
			if dropDead && m.isMeta(e) {
				if _, state := m.readEntry(e, now, false); state == typedStateDead {
					m.sealCell(e)
					continue
				}
			}
			nt.placeFresh(p, m.entryHash(e))
			kept++
		}
	}
	s.count.Store(kept)
	atomic.StorePointer(m.entryCell(shardIndex), handle)
	s.epoch.Add(1)
	return nt
}

func (s *typedShard) packed(groups int) bool {
	live := int(s.count.Load())
	return live+1 >= groups*typedMaxGroupLoad || (groups > s.floor && live*typedShrinkGroupRatio < groups)
}

func (s *typedShard) flatPacked(t typedTable) bool {
	groups := t.groups()
	return s.used+1 >= groups*typedFlatMaxLoad || (groups > s.floor && s.used&typedShrinkProbeMask == 0 && s.live(t)*typedShrinkGroupRatio < groups)
}

func (s *typedShard) crowded(t typedTable) bool {
	if t.flat() {
		return s.flatPacked(t)
	}
	return s.packed(t.groups())
}

func (s *typedShard) roomy(t typedTable) bool {
	if t.flat() {
		return s.used+1 < t.groups()*typedFlatSlots
	}
	return int(s.count.Load())+1 < t.groups()*typedGroupSlots
}

func (m *TypedMap[K, V]) roomLocked(s *typedShard, shardIndex uint64, entryOnly bool) typedTable {
	t := m.tableAt(shardIndex)
	if t.flat() {
		if entryOnly {
			return m.promoteLocked(s, shardIndex, t)
		}
		if !s.flatPacked(t) {
			return t
		}
	} else if !s.packed(t.groups()) {
		return t
	}
	return m.regrowLocked(s, shardIndex, t)
}

func (m *TypedMap[K, V]) regrowLocked(s *typedShard, shardIndex uint64, t typedTable) typedTable {
	want, flat := s.wantGroups(t), t.flat()
	if s.pending && s.roomy(t) {
		return t
	}
	if s.pending || want < typedOffLockGroups {
		return m.rebuildLocked(s, shardIndex, t, false, 0, nil)
	}
	s.pending = true
	s.mu.Unlock()
	handle := newTable(want, flat)
	if !s.mu.TryLock() {
		s.lockContended()
	}
	s.pending = false
	t = m.tableAt(shardIndex)
	if !s.crowded(t) {
		return t
	}
	if t.flat() != flat || s.wantGroups(t) != want {
		handle = nil
	}
	return m.rebuildLocked(s, shardIndex, t, false, 0, handle)
}

func (m *TypedMap[K, V]) insertLocked(s *typedShard, t typedTable, pos uint64, h uint64, key K, val V, md *typedMeta) {
	if t.flat() {
		m.flatInsertLocked(s, t, pos, h, key, val)
		return
	}
	atomic.StorePointer(t.slot(pos), unsafe.Pointer(m.newEntry(s, h, key, val, m.metaFor(val, md))))
	t.occupy(pos, h)
	s.count.Add(1)
}

func (m *TypedMap[K, V]) replaceLocked(s *typedShard, t typedTable, pos uint64, old *typedEntry[K], val V, md *typedMeta) {
	atomic.StorePointer(t.slot(pos), unsafe.Pointer(m.newEntry(s, m.entryHash(old), old.key, val, md)))
}

func (m *TypedMap[K, V]) exchangeLocked(s *typedShard, t typedTable, idx uint64, e *typedEntry[K], val V, md *typedMeta) (V, bool) {
	if t.flat() {
		return m.flatExchangeLocked(s, e, val)
	}
	md = m.metaFor(val, md)
	if !m.isMeta(e) {
		if md == nil {
			return m.swapCell(e, val), true
		}
		prev := m.sealCell(e)
		m.replaceLocked(s, t, idx, e, val, md)
		return prev, true
	}
	prev, state := m.readEntry(e, 0, false)
	if md == nil {
		m.sealCell(e)
		m.replaceLocked(s, t, idx, e, val, nil)
		return prev, state == typedStateLive
	}
	e.metaCell().Store(md)
	return prev, state == typedStateLive
}

func (m *TypedMap[K, V]) vacateLocked(s *typedShard, t typedTable, pos uint64) {
	e := (*typedEntry[K])(atomic.LoadPointer(t.slot(pos)))
	t.release(pos, m.entryHash(e))
	atomic.StorePointer(t.slot(pos), nil)
	s.count.Add(-1)
}

func (m *TypedMap[K, V]) removeLocked(s *typedShard, t typedTable, idx uint64, e *typedEntry[K]) (V, bool) {
	if t.flat() {
		return m.flatRemoveLocked(s, e)
	}
	state := typedStateLive
	var prev V
	if m.isMeta(e) {
		prev, state = m.readEntry(e, 0, false)
		m.sealCell(e)
	} else {
		prev = m.sealCell(e)
	}
	m.vacateLocked(s, t, idx)
	return prev, state == typedStateLive
}

func (m *TypedMap[K, V]) lookup(key K, consume bool) (V, bool) {
	var h uint64
	switch {
	case unsafe.Sizeof(key) == unsafe.Sizeof(int64(0)) && m.flat:
		h = hashInt64(m.seed64, *(*int64)(unsafe.Pointer(&key)))
		if handle := atomic.LoadPointer(m.handleCell(h & m.shardMask)); handle != typedPromoted {
			shift := uintptr(handle) & typedHandleMask
			base := unsafe.Add(handle, -int(shift))
			group := int(h >> shift)
			ctrl := atomic.LoadUint64((*uint64)(unsafe.Add(base, group*typedWordBytes)))
			match := typedZeroLanes(ctrl ^ typedFlatTag(h)*typedLaneOnes)
			if match == 0 {
				if typedZeroLanes(ctrl) != 0 {
					var zero V
					return zero, false
				}
				return m.lookupCold(key, consume)
			}
			line := unsafe.Add(base, ^group*typedFlatLineBytes)
			touchLine(line)
			e := (*typedEntry[K])(unsafe.Add(line, bits.TrailingZeros64(match)-typedLaneTop))
			if e.key != key {
				return m.lookupCold(key, consume)
			}
			if b := e.val.Load(); !typedReservedBits(b) {
				return fromBits[V](b), true
			}
			return m.lookupCold(key, consume)
		}
	case unsafe.Sizeof(key) == unsafe.Sizeof(int64(0)) && m.keyKind == keyKindInt64:
		h = hashInt64(m.seed64, *(*int64)(unsafe.Pointer(&key)))
	case unsafe.Sizeof(key) == unsafe.Sizeof("") && m.keyKind == keyKindString:
		text := *(*string)(unsafe.Pointer(&key))
		if length := len(text); uint(length-hashMidStringMin) <= hashMidStringMax-hashMidStringMin {
			h = hashMidString(m.seed64, unsafe.Pointer(unsafe.StringData(text)), length)
		} else if uint(length-hashShortStringMin) < hashMidStringMin-hashShortStringMin {
			h = hashShortString(m.seed64, unsafe.Pointer(unsafe.StringData(text)), length)
		} else if uint(length-hashTinyStringMin) < hashShortStringMin-hashTinyStringMin {
			h = hashTinyString(m.seed64, unsafe.Pointer(unsafe.StringData(text)), length)
		} else {
			h = hashString(m.seed64, text)
		}
	default:
		return m.lookupCold(key, consume)
	}
	handle := atomic.LoadPointer(m.entryCell(h & m.shardMask))
	shift := uintptr(handle) & typedHandleMask
	g := unsafe.Add(unsafe.Add(handle, -int(shift)), int(h>>shift)*typedGroupBytes)
	slots := unsafe.Add(g, typedSlotBias)
	ctrl := atomic.LoadUint64((*uint64)(g))
	match := typedZeroBytes(ctrl ^ typedFingerprint(h)*typedCtrlOnes)
	if match == 0 {
		if ctrl>>typedOverflowShift == 0 {
			var zero V
			return zero, false
		}
		return m.lookupCold(key, consume)
	}
	e := (*typedEntry[K])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Add(slots, bits.TrailingZeros64(match)))))
	if e != nil && e.key == key {
		if m.valPtr {
			if p := atomic.LoadPointer(e.ptrCell()); !typedReservedPointer(p) {
				return fromPointerWord[V](p), true
			}
		} else if b := e.val.Load(); !typedReservedBits(b) {
			return fromBits[V](b), true
		}
	}
	return m.lookupCold(key, consume)
}

func (m *TypedMap[K, V]) lookupCold(key K, consume bool) (V, bool) {
	if m.flat {
		h := m.hash(key)
		switch v, state := m.flatLookup(h, key); state {
		case typedStateLive:
			return v, true
		case typedStateDead:
			return v, false
		case typedStateSealed:
			return m.lookupLocked(h, key, consume)
		}
	}
	e, h, _ := m.seekCold(key)
	if e == nil {
		var zero V
		return zero, false
	}
	if v, live := m.loadCell(e); live {
		return v, true
	}
	return m.lookupSlow(e, h, key, consume)
}

func (m *TypedMap[K, V]) lookupSlow(e *typedEntry[K], h uint64, key K, consume bool) (V, bool) {
	if m.isMeta(e) {
		v, state := m.readEntry(e, 0, consume)
		if state == typedStateLive {
			return v, true
		}
		if state == typedStateDead {
			var zero V
			return zero, false
		}
	}
	return m.lookupLocked(h, key, consume)
}

func (m *TypedMap[K, V]) lookupLocked(h uint64, key K, consume bool) (V, bool) {
	shardIndex := h & m.shardMask
	s := &m.shards[shardIndex]
	if !s.mu.TryLock() {
		s.lockContended()
	}
	e, _ := m.locate(m.tableAt(shardIndex), h, key)
	var v V
	state := typedStateDead
	if e != nil {
		v, state = m.readEntry(e, 0, consume)
	}
	s.mu.Unlock()
	if state != typedStateLive {
		var zero V
		return zero, false
	}
	return v, true
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
	return m.lookup(key, true)
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
	e, h, _ := m.seek(key)
	if e != nil {
		if m.valPtr {
			cell := e.ptrCell()
			next := toPointerWord(val)
			for {
				old := atomic.LoadPointer(cell)
				if typedReservedPointer(old) {
					break
				}
				if atomic.CompareAndSwapPointer(cell, old, next) {
					return
				}
			}
		} else if next := toBits(val); !typedReservedBits(next) {
			for {
				old := e.val.Load()
				if typedReservedBits(old) {
					break
				}
				if e.val.CompareAndSwap(old, next) {
					return
				}
			}
		}
	}
	m.storeLocked(h, key, val, nil)
}

func (m *TypedMap[K, V]) storeLocked(h uint64, key K, val V, md *typedMeta) {
	shardIndex := h & m.shardMask
	s := &m.shards[shardIndex]
	if !s.mu.TryLock() {
		s.lockContended()
	}
	t := m.roomLocked(s, shardIndex, m.needsEntry(val, md))
	if e, idx := m.locate(t, h, key); e == nil {
		m.insertLocked(s, t, idx, h, key, val, md)
	} else {
		m.exchangeLocked(s, t, idx, e, val, md)
	}
	s.mu.Unlock()
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
	if found, h, slot := m.seek(key); found != nil {
		shardIndex := h & m.shardMask
		if m.flat {
			if prev, state := m.flatDelete(shardIndex, found); state != typedStateSealed {
				return prev, state == typedStateLive
			}
		}
		s := &m.shards[shardIndex]
		if !s.mu.TryLock() {
			s.lockContended()
		}
		t := m.tableAt(shardIndex)
		e, idx := found, t.posOf(slot)
		if !t.owns(unsafe.Pointer(found), slot, idx) {
			e, idx = m.locate(t, h, key)
		}
		if e != nil {
			prev, live := m.removeLocked(s, t, idx, e)
			s.mu.Unlock()
			if live {
				return prev, true
			}
			var zero V
			return zero, false
		}
		s.mu.Unlock()
	}
	var zero V
	return zero, false
}

func (m *TypedMap[K, V]) scanShard(shardIndex uint64, now int64, visitor func(key K, value V) bool) bool {
	t := m.viewAt(shardIndex)
	if t.flat() {
		return m.flatScan(t, visitor)
	}
	for group := uint64(0); group <= t.mask; group++ {
		t.prefetchAhead(group)
		for full := t.ctrl(group).Load() & typedCtrlHigh; full != 0; full &= full - 1 {
			e := (*typedEntry[K])(atomic.LoadPointer(t.slot(typedSlotPos(group, full))))
			if e == nil {
				continue
			}
			v, live := m.loadCell(e)
			if !live {
				if v, live = m.scanSlow(shardIndex, e, now); !live {
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

func (m *TypedMap[K, V]) scanSlow(shardIndex uint64, e *typedEntry[K], now int64) (V, bool) {
	v, state := m.readEntry(e, now, false)
	if state == typedStateSealed {
		return m.lookupLocked(m.entryHash(e), e.key, false)
	}
	return v, state == typedStateLive
}

func (m *TypedMap[K, V]) scanClock() int64 {
	if m.hasMeta.Load() {
		return time.Now().UnixNano()
	}
	return 0
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
	now := m.scanClock()
	for shardIndex := range m.shards {
		if !m.scanShard(uint64(shardIndex), now, visitor) {
			return
		}
	}
}

// Len returns the number of live entries currently in the map.
func (m *TypedMap[K, V]) Len() int {
	if !m.hasMeta.Load() {
		total := 0
		for shardIndex := range m.shards {
			total += m.settledLive(uint64(shardIndex))
		}
		return total
	}
	n := 0
	m.Range(func(K, V) bool {
		n++
		return true
	})
	return n
}
