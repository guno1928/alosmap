package alosmap

import (
	"math"
	"math/bits"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	defaultCapacity               = 1024
	defaultLoadFactor             = 0.72
	defaultShardMultiplier        = 4
	defaultCleanupInterval        = 5 * time.Second
	largeScaleCapacity            = 1_000_000
	veryLargeScaleCapacity        = 10_000_000
	massiveScaleCapacity          = 100_000_000
	maxAutoShards                 = 1024
	minTableSlots                 = 32
	minShardTargetEntries         = 128
	resizeSpinYieldInterval       = 32
	shardResizing           int64 = 1 << 62
)

type Option func(*config)

type ValueCloneFunc func(value any) (cloned any, trackedBytes int64)

// EntryOptions configures optional per-entry lifetime rules used by StoreWithOptions,
// LoadOrStoreWithOptions, and SwapWithOptions.
//
// All fields are optional. The zero value means the entry has no TTL and no hit
// limit, so it remains available until it is overwritten, deleted, or removed by
// some other operation.
//
// Field behavior:
//
// TTL is optional. Its default is 0. When TTL is greater than 0, the entry expires
// that far in the future relative to the write that created or replaced it. When TTL
// is 0 or negative, time-based expiry is disabled for that entry.
//
// Hits is optional. Its default is 0. When Hits is greater than 0, each successful
// Load call decrements the remaining hit budget and the entry disappears after the
// final allowed load. When Hits is 0 or negative, hit limiting is disabled and the
// entry allows unlimited successful loads. Peek, Has, Range, and Snapshot do not
// consume hits.
//
// Example:
//
//	options := alosmap.EntryOptions{
//		TTL:  30 * time.Second,
//		Hits: 3,
//	}
//	store.StoreWithOptions("session:123", "ready", options)
type EntryOptions struct {
	TTL  time.Duration
	Hits int64
}

type MapCloner interface {
	CloneForMap() any
}

type SizedMapCloner interface {
	CloneForMapWithSize() (any, int64)
}

type MapEqualer interface {
	EqualForMap(other any) bool
}

type config struct {
	capacity        int
	loadFactor      float64
	shardCount      int
	cleanupInterval time.Duration
	cloneValue      ValueCloneFunc
	hasCustomCloner bool
}

// Builder incrementally configures a Map before construction.
//
// Builder provides a fluent alternative to functional options. Each setter records
// the requested value and returns the same Builder so calls can be chained. Build
// applies the same normalization rules as New, including default capacity,
// auto-selected shard count, load-factor clamping, and cleanup interval handling.
type Builder struct {
	cfg config
}

// Pair represents one key/value item returned by Snapshot.
//
// Snapshot returns a flat slice of Pair values so callers can inspect or copy a
// point-in-time view of the map without holding references to internal tables.
type Pair struct {
	Key   string
	Value any
}

// Stats is a point-in-time snapshot of operational counters and size estimates for a Map.
//
// Occupancy-related fields include Shards, SlotCapacity, LiveEntries, UsedSlots,
// Tombstones, LoadFactor, and MaxShardLive. Maintenance-related fields include
// ResizeCount, WriterSpins, CleanupRuns, CleanupSkips, ExpiredDeletes, and
// HitDeletes. Memory-related fields include TrackedKeyBytes, TrackedValueBytes,
// EstimatedOverheadBytes, and EstimatedResidentBytes.
//
// All values are observational. In a concurrent program they may change again
// immediately after Stats returns.
type Stats struct {
	Shards                 int
	SlotCapacity           int
	LiveEntries            int64
	UsedSlots              int64
	Tombstones             int64
	ResizeCount            int64
	WriterSpins            int64
	CleanupRuns            int64
	CleanupSkips           int64
	ExpiredDeletes         int64
	HitDeletes             int64
	TrackedKeyBytes        int64
	TrackedValueBytes      int64
	EstimatedOverheadBytes int64
	EstimatedResidentBytes int64
	LoadFactor             float64
	MaxShardLive           int64
}

// Map is a concurrent string-key map optimized for high-throughput reads, mixed workloads,
// and feature-rich entry lifecycle control.
//
// Map stores values as any and publishes updates atomically so readers observe either
// the old value or the new value, never a partially written state. Read operations on
// live entries are lock-free. Write operations synchronize only within the shard that
// owns the key, which allows unrelated keys to proceed in parallel.
//
// Each entry may optionally use a TTL, a hit limit, or both. Expired entries and
// exhausted hit-limited entries are treated as absent. Values are stored by reference
// unless a custom ValueCloneFunc is supplied, so storing a pointer returns that same
// pointer from later loads.
type Map struct {
	seed            uint64
	shardMask       uint64
	shards          []shard
	cleanupInterval time.Duration
	cloneValue      ValueCloneFunc
	noClone         bool
	stopCleanup     chan struct{}
	cleanupDone     chan struct{}
	cleanupClosed   atomic.Bool
}

type shard struct {
	table        atomic.Pointer[table]
	loadFactor   float64
	initialSlots int
	_readPad     [40]byte

	state        atomic.Int64
	needsCleanup atomic.Bool
	live         atomic.Int64
	used         atomic.Int64
	tombstones   atomic.Int64
	keyBytes     atomic.Int64
	valueBytes   atomic.Int64
	_writePad    [16]byte

	resizes        atomic.Int64
	spinWaits      atomic.Int64
	cleanupRuns    atomic.Int64
	cleanupSkips   atomic.Int64
	expiredDeletes atomic.Int64
	hitDeletes     atomic.Int64
	_coldPad       [16]byte
}

type table struct {
	slots  []slot
	mask   uint64
	growAt int
}

type slot struct {
	entry atomic.Pointer[entry]
}

type entry struct {
	hash     uint64
	key      string
	keyBytes int64
	value    atomic.Pointer[valueBox]
}

type valueBox struct {
	value       any
	clonedBytes int64
	expiresAt   int64
	hits        atomic.Int64
}

type entryBundle struct {
	ent entry
	box valueBox
}

// NewBuilder returns a Builder initialized with the package defaults.
//
// This is the fluent alternative to calling New with functional options directly.
// Call the Builder methods you need and finish with Build.
func NewBuilder() *Builder {
	return &Builder{cfg: defaultConfig()}
}

// Capacity sets the expected live entry count used for initial sizing.
//
// Capacity is optional. When omitted, Build falls back to the package default of
// 1024 expected entries. Values less than 1 are normalized to that same default.
// Capacity is not a hard limit; it only guides the initial table size.
func (b *Builder) Capacity(capacity int) *Builder {
	b.cfg.capacity = capacity
	return b
}

// Shards sets the requested shard count for the map.
//
// The requested value is normalized during Build. Values greater than 0 are rounded
// up to the next power of two. Values less than or equal to 0 cause Build to use the
// package's automatic shard-selection heuristic.
func (b *Builder) Shards(count int) *Builder {
	b.cfg.shardCount = count
	return b
}

// LoadFactor sets the target occupancy before a shard grows.
//
// LoadFactor is optional. When omitted, the default is 0.72. Values smaller than
// 0.50 are clamped to 0.50, and values larger than 0.90 are clamped to 0.90.
func (b *Builder) LoadFactor(loadFactor float64) *Builder {
	b.cfg.loadFactor = loadFactor
	return b
}

// CleanupInterval sets the cadence of the background cleanup goroutine.
//
// CleanupInterval is optional. The default is 5 seconds. A value of 0 disables the
// background cleaner entirely. Negative values are normalized to 0 during Build.
func (b *Builder) CleanupInterval(interval time.Duration) *Builder {
	b.cfg.cleanupInterval = interval
	return b
}

// ValueCloner installs a custom write-time cloning function.
//
// By default the map stores values exactly as supplied. Providing a ValueCloner lets
// you copy mutable values before publication and report an approximate byte size for
// Stats. A nil cloner falls back to the default pass-through behavior.
func (b *Builder) ValueCloner(cloner ValueCloneFunc) *Builder {
	b.cfg.cloneValue = cloner
	return b
}

// Build constructs a Map from the Builder configuration.
//
// Build applies the same normalization and defaulting rules as New, so the result is
// identical to assembling the equivalent functional options manually.
func (b *Builder) Build() *Map {
	return New(
		WithCapacity(b.cfg.capacity),
		WithShardCount(b.cfg.shardCount),
		WithLoadFactor(b.cfg.loadFactor),
		WithCleanupInterval(b.cfg.cleanupInterval),
		WithValueCloner(b.cfg.cloneValue),
	)
}

// WithCapacity returns an Option that sets the expected live entry count for initial sizing.
//
// The default is 1024 when this option is omitted. Values less than 1 are normalized
// to the default. Capacity influences initial allocation and resize behavior; it does
// not enforce a maximum size.
func WithCapacity(capacity int) Option {
	return func(cfg *config) {
		cfg.capacity = capacity
	}
}

// WithShardCount returns an Option that sets the requested shard count.
//
// Positive values are rounded up to the next power of two. Zero or negative values
// cause New to select a shard count automatically from the expected capacity.
func WithShardCount(count int) Option {
	return func(cfg *config) {
		cfg.shardCount = count
	}
}

// WithLoadFactor returns an Option that sets the target occupancy before a shard grows.
//
// The default is 0.72. Values are clamped into the inclusive range [0.50, 0.90].
func WithLoadFactor(loadFactor float64) Option {
	return func(cfg *config) {
		cfg.loadFactor = loadFactor
	}
}

// WithCleanupInterval returns an Option that sets the background cleanup interval.
//
// The default is 5 seconds. A value of 0 disables background cleanup. Negative values
// are normalized to 0.
func WithCleanupInterval(interval time.Duration) Option {
	return func(cfg *config) {
		cfg.cleanupInterval = interval
	}
}

// WithoutCleanup returns an Option that disables the background cleanup goroutine.
//
// This is equivalent to setting the cleanup interval to 0.
func WithoutCleanup() Option {
	return func(cfg *config) {
		cfg.cleanupInterval = 0
	}
}

// WithValueCloner returns an Option that installs a custom write-time clone function.
//
// Use this when stored values should be copied before publication or when Stats should
// track approximate cloned value sizes. A nil cloner falls back to pass-through storage.
func WithValueCloner(cloner ValueCloneFunc) Option {
	return func(cfg *config) {
		cfg.cloneValue = cloner
	}
}

// New constructs a Map from the provided options.
//
// Defaults used when options are omitted are:
//
//	Capacity: 1024 expected live entries
//	Shard count: automatically selected from capacity and GOMAXPROCS
//	Load factor: 0.72
//	Cleanup interval: 5 seconds
//	Value cloner: pass-through storage with zero tracked value bytes
//
// Options are normalized before construction. Capacity values below 1 fall back to
// the default, shard counts are rounded to a power of two or auto-selected, load
// factor is clamped to [0.50, 0.90], and negative cleanup intervals are treated as 0.
func New(options ...Option) *Map {
	cfg := defaultConfig()
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	cfg.normalize()

	seed := avalanche(uint64(time.Now().UnixNano()) ^ uint64(cfg.capacity) ^ (uint64(cfg.shardCount) << 32) ^ hashSeed3)
	if seed == 0 {
		seed = hashSeed1
	}

	instance := &Map{
		seed:            seed,
		shardMask:       uint64(cfg.shardCount - 1),
		shards:          make([]shard, cfg.shardCount),
		cleanupInterval: cfg.cleanupInterval,
		cloneValue:      cfg.cloneValue,
		noClone:         !cfg.hasCustomCloner,
	}

	perShardEntries := divideRoundUp(cfg.capacity, cfg.shardCount)
	for index := range instance.shards {
		currentShard := &instance.shards[index]
		currentShard.loadFactor = cfg.loadFactor
		initial := newTableForEntries(perShardEntries, cfg.loadFactor)
		currentShard.initialSlots = minTableSlots
		currentShard.table.Store(initial)
	}

	if cfg.cleanupInterval > 0 {
		instance.stopCleanup = make(chan struct{})
		instance.cleanupDone = make(chan struct{})
		go instance.cleanupLoop()
	}

	return instance
}

func defaultConfig() config {
	return config{
		capacity:        defaultCapacity,
		loadFactor:      defaultLoadFactor,
		cleanupInterval: defaultCleanupInterval,
		cloneValue:      defaultCloneValue,
	}
}

func (cfg *config) normalize() {
	if cfg.capacity < 1 {
		cfg.capacity = defaultCapacity
	}
	if cfg.loadFactor < 0.50 {
		cfg.loadFactor = 0.50
	}
	if cfg.loadFactor > 0.90 {
		cfg.loadFactor = 0.90
	}
	if cfg.shardCount <= 0 {
		cfg.shardCount = autoShardCount(cfg.capacity)
	} else {
		cfg.shardCount = nextPowerOfTwo(cfg.shardCount)
	}
	if cfg.shardCount < 1 {
		cfg.shardCount = 1
	}
	if cfg.cleanupInterval < 0 {
		cfg.cleanupInterval = 0
	}
	if cfg.cloneValue == nil {
		cfg.cloneValue = defaultCloneValue
	} else {
		cfg.hasCustomCloner = true
	}
}

func autoShardCount(capacity int) int {
	target := runtime.GOMAXPROCS(0) * defaultShardMultiplier
	switch {
	case capacity >= massiveScaleCapacity:
		target *= 16
	case capacity >= veryLargeScaleCapacity:
		target *= 8
	case capacity >= largeScaleCapacity:
		target *= 4
	}
	if target < 1 {
		target = 1
	}
	if target > maxAutoShards {
		target = maxAutoShards
	}
	maxUseful := capacity / minShardTargetEntries
	if maxUseful < 1 {
		maxUseful = 1
	}
	if target > maxUseful {
		target = maxUseful
	}
	return floorPowerOfTwo(target)
}

// RecommendedShardCount returns the shard count heuristic used for a given expected capacity.
//
// This is useful when you want to size the map explicitly but still reuse the package's
// automatic shard-selection logic for large deployments.
func RecommendedShardCount(expectedEntries int) int {
	if expectedEntries < 1 {
		expectedEntries = 1
	}
	return autoShardCount(expectedEntries)
}

func (m *Map) cleanupLoop() {
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

// Close stops the background cleanup goroutine, if one was started.
//
// Close is idempotent and safe to call multiple times. It does not invalidate the map;
// it only disables automatic cleanup work running in the background.
func (m *Map) Close() {
	if m.stopCleanup == nil {
		return
	}
	if m.cleanupClosed.CompareAndSwap(false, true) {
		close(m.stopCleanup)
		<-m.cleanupDone
	}
}

// Store writes value at key using the default entry behavior.
//
// Store replaces any existing live value for the key. The new entry has no TTL and no
// hit limit. Values are stored by reference unless a custom ValueCloneFunc is configured.
func (m *Map) Store(key string, value any) {
	m.StoreWithOptions(key, value, EntryOptions{})
}

// StoreWithTTL writes value at key and applies a TTL.
//
// When ttl is greater than 0, the entry expires relative to the time of this write.
// When ttl is 0 or negative, the entry behaves like Store and has no time-based expiry.
func (m *Map) StoreWithTTL(key string, value any, ttl time.Duration) {
	m.StoreWithOptions(key, value, EntryOptions{TTL: ttl})
}

// StoreWithHits writes value at key and applies a hit limit.
//
// Hits greater than 0 allow that many successful Load calls before the entry is removed.
// Hits equal to 0 or less disable hit limiting and allow unlimited successful loads.
func (m *Map) StoreWithHits(key string, value any, hits int64) {
	m.StoreWithOptions(key, value, EntryOptions{Hits: hits})
}

// StoreWithTTLAndHits writes value at key with both TTL and hit-limit behavior.
//
// TTL and Hits follow the same rules documented by EntryOptions. Either control may
// effectively be disabled by passing a non-positive value.
func (m *Map) StoreWithTTLAndHits(key string, value any, ttl time.Duration, hits int64) {
	m.StoreWithOptions(key, value, EntryOptions{TTL: ttl, Hits: hits})
}

// SetWithTTLAndHits writes value at key with both TTL and hit-limit behavior.
//
// It is provided as a naming alias for callers that prefer Set-style method names.
// Its behavior is identical to StoreWithTTLAndHits.
func (m *Map) SetWithTTLAndHits(key string, value any, ttl time.Duration, hits int64) {
	m.StoreWithTTLAndHits(key, value, ttl, hits)
}

// StoreWithOptions writes value at key and applies the supplied EntryOptions.
//
// This is the most general write API for creating or replacing an entry. The zero
// value of EntryOptions gives the same behavior as Store.
func (m *Map) StoreWithOptions(key string, value any, options EntryOptions) {
	hash := hashString(m.seed, key)
	boxed := m.newValueBox(value, options)
	m.pickShard(hash).store(hash, key, boxed)
}

// Load returns the current live value for key.
//
// For hit-limited entries, each successful Load consumes one remaining hit. Expired
// entries and exhausted hit-limited entries are treated as missing and return ok=false.
func (m *Map) Load(key string) (any, bool) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).load(hash, key)
}

// Get returns the current live value for key.
//
// Get is a naming alias for Load and therefore has identical behavior, including
// hit consumption on successful reads of hit-limited entries.
func (m *Map) Get(key string) (any, bool) {
	return m.Load(key)
}

// Peek returns the current live value for key without consuming hit counters.
//
// Expired and exhausted entries are treated as absent in the same way as Load.
func (m *Map) Peek(key string) (any, bool) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).peek(hash, key)
}

// Has reports whether key currently resolves to a live entry.
//
// Has does not consume hit counters.
func (m *Map) Has(key string) bool {
	_, ok := m.Peek(key)
	return ok
}

// Delete removes key and returns the previous live value when present.
//
// If the key is absent, expired, or already exhausted, Delete returns nil, false.
func (m *Map) Delete(key string) (any, bool) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).delete(hash, key)
}

// LoadOrStore returns the existing live value for key or stores value when the key is absent.
//
// The inserted value uses the default entry behavior: no TTL and no hit limit.
func (m *Map) LoadOrStore(key string, value any) (any, bool) {
	return m.LoadOrStoreWithOptions(key, value, EntryOptions{})
}

// LoadOrStoreWithOptions returns the existing live value for key or stores value with EntryOptions.
//
// If the current entry is expired or already exhausted, it is treated as absent and a
// new entry may be installed.
func (m *Map) LoadOrStoreWithOptions(key string, value any, options EntryOptions) (any, bool) {
	hash := hashString(m.seed, key)
	s := m.pickShard(hash)
	var cf ValueCloneFunc
	if !m.noClone {
		cf = m.cloneValue
	}
	return s.loadOrStoreDeferred(hash, key, value, options, cf)
}

// Swap atomically replaces the value for key and returns the previous live value when present.
//
// The replacement entry uses the default behavior of no TTL and no hit limit.
func (m *Map) Swap(key string, value any) (any, bool) {
	return m.SwapWithOptions(key, value, EntryOptions{})
}

// SwapWithOptions atomically replaces the value for key and applies EntryOptions to the replacement.
//
// It returns the previous live value and true when a value existed, or nil and false
// when the key was absent.
func (m *Map) SwapWithOptions(key string, value any, options EntryOptions) (any, bool) {
	hash := hashString(m.seed, key)
	boxed := m.newValueBox(value, options)
	return m.pickShard(hash).swap(hash, key, boxed)
}

// CompareAndSwap replaces the current value only when it matches oldValue.
//
// Value comparison uses the package's equality rules, including MapEqualer support and
// special handling for several slice and map forms used by the implementation.
func (m *Map) CompareAndSwap(key string, oldValue any, newValue any) bool {
	return m.CompareAndSwapWithOptions(key, oldValue, newValue, EntryOptions{})
}

// CompareAndSwapWithOptions replaces the current value only when it matches oldValue and
// applies EntryOptions to the replacement.
func (m *Map) CompareAndSwapWithOptions(key string, oldValue any, newValue any, options EntryOptions) bool {
	hash := hashString(m.seed, key)
	boxed := m.newValueBox(newValue, options)
	return m.pickShard(hash).compareAndSwap(hash, key, oldValue, boxed)
}

// CompareAndDelete removes key only when the current value matches oldValue.
//
// It returns true when the delete succeeds and false otherwise.
func (m *Map) CompareAndDelete(key string, oldValue any) bool {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).compareAndDelete(hash, key, oldValue)
}

// Len returns the current number of live entries.
//
// Len excludes deleted, expired, and exhausted entries.
func (m *Map) Len() int {
	return int(m.len64())
}

func (m *Map) len64() int64 {
	var total int64
	for index := range m.shards {
		total += m.shards[index].live.Load()
	}
	return total
}

// Clear removes all keys from the map and resets shard tables toward their initial size.
//
// Clear does not stop the background cleaner and does not make the map unusable.
func (m *Map) Clear() {
	for index := range m.shards {
		m.shards[index].clear()
	}
}

// CleanupNow forces an immediate maintenance pass across all shards.
//
// Cleanup removes expired entries, removes exhausted hit-limited entries, and may
// compact or shrink shard tables when the implementation determines that rebuilding
// would be beneficial.
func (m *Map) CleanupNow() {
	now := time.Now().UnixNano()
	for index := range m.shards {
		m.shards[index].cleanup(now)
	}
}

// Range visits the current live entries in the map.
//
// Range does not consume hit counters. Iteration order is unspecified. When writes
// happen concurrently, Range provides an eventually consistent view rather than a
// locked snapshot. Returning false from visitor stops the iteration early.
func (m *Map) Range(visitor func(key string, value any) bool) {
	for index := range m.shards {
		currentShard := &m.shards[index]
		currentTable := currentShard.table.Load()
		for slotIndex := range currentTable.slots {
			current := currentTable.slots[slotIndex].entry.Load()
			if current == nil {
				continue
			}
			value, ok := currentShard.readEntry(current, false)
			if !ok {
				continue
			}
			if !visitor(current.key, value) {
				return
			}
		}
	}
}

// Snapshot returns a flat slice containing the map's current live entries.
//
// Snapshot is built by calling Range and therefore has the same eventual-consistency
// behavior under concurrent writes.
func (m *Map) Snapshot() []Pair {
	pairs := make([]Pair, 0, m.Len())
	m.Range(func(key string, value any) bool {
		pairs = append(pairs, Pair{Key: key, Value: value})
		return true
	})
	return pairs
}

// Stats returns a point-in-time snapshot of aggregated operational metrics.
//
// The returned counters and estimates are useful for sizing, observability, and
// debugging. Because the map may be changing concurrently, the snapshot is approximate.
func (m *Map) Stats() Stats {
	stats := Stats{Shards: len(m.shards)}
	for index := range m.shards {
		currentShard := &m.shards[index]
		currentTable := currentShard.table.Load()
		live := currentShard.live.Load()
		stats.SlotCapacity += len(currentTable.slots)
		stats.LiveEntries += live
		stats.UsedSlots += currentShard.used.Load()
		stats.Tombstones += currentShard.tombstones.Load()
		stats.ResizeCount += currentShard.resizes.Load()
		stats.WriterSpins += currentShard.spinWaits.Load()
		stats.CleanupRuns += currentShard.cleanupRuns.Load()
		stats.CleanupSkips += currentShard.cleanupSkips.Load()
		stats.ExpiredDeletes += currentShard.expiredDeletes.Load()
		stats.HitDeletes += currentShard.hitDeletes.Load()
		stats.TrackedKeyBytes += currentShard.keyBytes.Load()
		stats.TrackedValueBytes += currentShard.valueBytes.Load()
		stats.EstimatedOverheadBytes += int64(len(currentTable.slots)) * int64(unsafe.Sizeof(slot{}))
		stats.EstimatedOverheadBytes += currentShard.used.Load() * int64(unsafe.Sizeof(entry{}))
		stats.EstimatedOverheadBytes += live * int64(unsafe.Sizeof(valueBox{}))
		if live > stats.MaxShardLive {
			stats.MaxShardLive = live
		}
	}
	if stats.SlotCapacity > 0 {
		stats.LoadFactor = float64(stats.LiveEntries) / float64(stats.SlotCapacity)
	}
	stats.EstimatedResidentBytes = stats.TrackedKeyBytes + stats.TrackedValueBytes + stats.EstimatedOverheadBytes
	return stats
}

func (m *Map) newValueBox(value any, options EntryOptions) *valueBox {
	clonedValue, trackedBytes := m.cloneValue(value)
	boxed := &valueBox{
		value:       clonedValue,
		clonedBytes: trackedBytes,
	}
	if options.Hits > 0 {
		boxed.hits.Store(options.Hits)
	}
	if options.TTL > 0 {
		boxed.expiresAt = time.Now().Add(options.TTL).UnixNano()
	}
	return boxed
}

func (m *Map) pickShard(hash uint64) *shard {
	if len(m.shards) == 1 {
		return &m.shards[0]
	}
	index := int((hash >> 32) & m.shardMask)
	return &m.shards[index]
}

func (s *shard) load(hash uint64, key string) (any, bool) {
	currentTable := s.table.Load()
	current := findEntry(currentTable, hash, key)
	if current == nil {
		return nil, false
	}
	return s.readEntry(current, true)
}

func (s *shard) peek(hash uint64, key string) (any, bool) {
	currentTable := s.table.Load()
	current := findEntry(currentTable, hash, key)
	if current == nil {
		return nil, false
	}
	return s.readEntry(current, false)
}

func (s *shard) readEntry(current *entry, consumeHits bool) (any, bool) {
	for {
		boxed := current.value.Load()
		if boxed == nil {
			return nil, false
		}

		if boxed.expiresAt > 0 && time.Now().UnixNano() >= boxed.expiresAt {
			if s.clearIfMatch(current, boxed, true, false) {
				s.maybeResize()
				return nil, false
			}
			continue
		}

		if !consumeHits {
			if boxed.hits.Load() < 0 {
				if s.clearIfMatch(current, boxed, false, true) {
					s.maybeResize()
					return nil, false
				}
				continue
			}
			return boxed.value, true
		}

		consumed, exhausted := boxed.consumeHit()
		if !consumed {
			if s.clearIfMatch(current, boxed, false, true) {
				s.maybeResize()
				return nil, false
			}
			continue
		}

		value := boxed.value
		if exhausted {
			s.clearIfMatch(current, boxed, false, true)
			s.maybeResize()
		}
		return value, true
	}
}

func (s *shard) store(hash uint64, key string, boxed *valueBox) {
outer:
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		index := int(hash & currentTable.mask)

		for probes := 0; probes < len(currentTable.slots); probes++ {
			current := currentTable.slots[index].entry.Load()
			if current == nil {
				inserted, retry, resizeNeeded := s.insertFresh(currentTable, index, hash, key, boxed)
				s.endWrite()
				if retry {
					continue outer
				}
				if resizeNeeded {
					s.resize(int(s.live.Load()) + 1)
				}
				if inserted {
					return
				}
				continue outer
			}

			if current.hash == hash && current.key == key {
				s.replaceOrRevive(current, boxed)
				resizeNeeded := s.shouldResizeWithTable(currentTable)
				s.endWrite()
				if resizeNeeded {
					s.resize(int(s.live.Load()))
				}
				return
			}

			index = (index + 1) & int(currentTable.mask)
		}

		s.endWrite()
		s.resize(int(s.live.Load()) + 1)
	}
}

func (s *shard) loadOrStore(hash uint64, key string, boxed *valueBox) (any, bool) {
outer:
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		index := int(hash & currentTable.mask)

		for probes := 0; probes < len(currentTable.slots); probes++ {
			current := currentTable.slots[index].entry.Load()
			if current == nil {
				inserted, retry, resizeNeeded := s.insertFresh(currentTable, index, hash, key, boxed)
				s.endWrite()
				if retry {
					continue outer
				}
				if resizeNeeded {
					s.resize(int(s.live.Load()) + 1)
				}
				if inserted {
					return boxed.value, false
				}
				continue outer
			}

			if current.hash == hash && current.key == key {
				actual, loaded := s.loadOrStoreCurrent(current, boxed)
				resizeNeeded := !loaded && s.shouldResizeWithTable(currentTable)
				s.endWrite()
				if resizeNeeded {
					s.resize(int(s.live.Load()))
				}
				return actual, loaded
			}

			index = (index + 1) & int(currentTable.mask)
		}

		s.endWrite()
		s.resize(int(s.live.Load()) + 1)
	}
}
func (s *shard) loadOrStoreDeferred(hash uint64, key string, value any, options EntryOptions, cloneFunc ValueCloneFunc) (any, bool) {
	currentTable := s.table.Load()
	idx := int(hash & currentTable.mask)
	emptyIdx := -1
	for probes := 0; probes < len(currentTable.slots); probes++ {
		current := currentTable.slots[idx].entry.Load()
		if current == nil {
			emptyIdx = idx
			break
		}
		if current.hash == hash && current.key == key {
			existing := current.value.Load()
			if existing != nil && existing.expiresAt == 0 && existing.hits.Load() == 0 {
				return existing.value, true
			}
			break
		}
		idx = (idx + 1) & int(currentTable.mask)
	}

	if emptyIdx >= 0 {
		var cloned any
		var trackedBytes int64
		if cloneFunc != nil {
			cloned, trackedBytes = cloneFunc(value)
		} else {
			cloned = value
		}
		bundle := &entryBundle{}
		bundle.ent.hash = hash
		bundle.ent.key = strings.Clone(key)
		bundle.ent.keyBytes = int64(len(key))
		bundle.box.value = cloned
		bundle.box.clonedBytes = trackedBytes
		if options.Hits > 0 {
			bundle.box.hits.Store(options.Hits)
		}
		if options.TTL > 0 {
			bundle.box.expiresAt = time.Now().Add(options.TTL).UnixNano()
		}
		bundle.ent.value.Store(&bundle.box)

		if currentTable.slots[emptyIdx].entry.CompareAndSwap(nil, &bundle.ent) {
			if s.table.Load() == currentTable {
				s.live.Add(1)
				used := s.used.Add(1)
				s.keyBytes.Add(bundle.ent.keyBytes)
				if trackedBytes > 0 {
					s.valueBytes.Add(trackedBytes)
				}
				if bundle.box.requiresCleanup() {
					s.needsCleanup.Store(true)
				}
				if used >= int64(currentTable.growAt) {
					s.resize(int(s.live.Load()) + 1)
				}
				return cloned, false
			}
			if findEntry(s.table.Load(), hash, key) == &bundle.ent {
				return cloned, false
			}
		}
	}

	return s.loadOrStoreSlow(hash, key, value, options, cloneFunc)
}

func (s *shard) loadOrStoreSlow(hash uint64, key string, value any, options EntryOptions, cloneFunc ValueCloneFunc) (any, bool) {
outer:
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		index := int(hash & currentTable.mask)

		for probes := 0; probes < len(currentTable.slots); probes++ {
			current := currentTable.slots[index].entry.Load()
			if current == nil {
				var cloned any
				var trackedBytes int64
				if cloneFunc != nil {
					cloned, trackedBytes = cloneFunc(value)
				} else {
					cloned = value
				}
				bundle := &entryBundle{}
				bundle.ent.hash = hash
				bundle.ent.key = strings.Clone(key)
				bundle.ent.keyBytes = int64(len(key))
				bundle.box.value = cloned
				bundle.box.clonedBytes = trackedBytes
				if options.Hits > 0 {
					bundle.box.hits.Store(options.Hits)
				}
				if options.TTL > 0 {
					bundle.box.expiresAt = time.Now().Add(options.TTL).UnixNano()
				}
				bundle.ent.value.Store(&bundle.box)

				if currentTable.slots[index].entry.CompareAndSwap(nil, &bundle.ent) {
					s.live.Add(1)
					used := s.used.Add(1)
					s.keyBytes.Add(bundle.ent.keyBytes)
					if trackedBytes > 0 {
						s.valueBytes.Add(trackedBytes)
					}
					if bundle.box.requiresCleanup() {
						s.needsCleanup.Store(true)
					}
					resizeNeeded := used >= int64(currentTable.growAt)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()) + 1)
					}
					return cloned, false
				}
				s.endWrite()
				continue outer
			}

			if current.hash == hash && current.key == key {
				revive := false
				for {
					existing := current.value.Load()
					if existing == nil {
						revive = true
						break
					}
					if existing.expiresAt > 0 && time.Now().UnixNano() >= existing.expiresAt {
						if s.clearIfMatch(current, existing, true, false) {
							revive = true
							break
						}
						continue
					}
					if existing.hits.Load() < 0 {
						if s.clearIfMatch(current, existing, false, true) {
							revive = true
							break
						}
						continue
					}
					s.endWrite()
					return existing.value, true
				}
				if revive {
					var cloned any
					var trackedBytes int64
					if cloneFunc != nil {
						cloned, trackedBytes = cloneFunc(value)
					} else {
						cloned = value
					}
					boxed := &valueBox{
						value:       cloned,
						clonedBytes: trackedBytes,
					}
					if options.Hits > 0 {
						boxed.hits.Store(options.Hits)
					}
					if options.TTL > 0 {
						boxed.expiresAt = time.Now().Add(options.TTL).UnixNano()
					}
					actual, loaded := s.loadOrStoreCurrent(current, boxed)
					resizeNeeded := !loaded && s.shouldResizeWithTable(currentTable)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()))
					}
					return actual, loaded
				}
			}

			index = (index + 1) & int(currentTable.mask)
		}

		s.endWrite()
		s.resize(int(s.live.Load()) + 1)
	}
}

func (s *shard) swap(hash uint64, key string, boxed *valueBox) (any, bool) {
outer:
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		index := int(hash & currentTable.mask)

		for probes := 0; probes < len(currentTable.slots); probes++ {
			current := currentTable.slots[index].entry.Load()
			if current == nil {
				inserted, retry, resizeNeeded := s.insertFresh(currentTable, index, hash, key, boxed)
				s.endWrite()
				if retry {
					continue outer
				}
				if resizeNeeded {
					s.resize(int(s.live.Load()) + 1)
				}
				if inserted {
					return nil, false
				}
				continue outer
			}

			if current.hash == hash && current.key == key {
				previous, loaded := s.replaceOrRevive(current, boxed)
				resizeNeeded := s.shouldResizeWithTable(currentTable)
				s.endWrite()
				if resizeNeeded {
					s.resize(int(s.live.Load()))
				}
				return previous, loaded
			}

			index = (index + 1) & int(currentTable.mask)
		}

		s.endWrite()
		s.resize(int(s.live.Load()) + 1)
	}
}

func (s *shard) delete(hash uint64, key string) (any, bool) {
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		current := findEntry(currentTable, hash, key)
		if current == nil {
			s.endWrite()
			return nil, false
		}

		for {
			boxed := current.value.Load()
			if boxed == nil {
				s.endWrite()
				return nil, false
			}

			if boxed.expiresAt > 0 && time.Now().UnixNano() >= boxed.expiresAt {
				if s.clearIfMatch(current, boxed, true, false) {
					resizeNeeded := s.shouldResizeWithTable(currentTable)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()))
					}
					return nil, false
				}
				continue
			}

			if boxed.hits.Load() < 0 {
				if s.clearIfMatch(current, boxed, false, true) {
					resizeNeeded := s.shouldResizeWithTable(currentTable)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()))
					}
					return nil, false
				}
				continue
			}

			if current.value.CompareAndSwap(boxed, nil) {
				s.live.Add(-1)
				s.tombstones.Add(1)
				s.keyBytes.Add(-current.keyBytes)
				s.valueBytes.Add(-boxed.clonedBytes)
				s.needsCleanup.Store(true)
				resizeNeeded := s.shouldResizeWithTable(currentTable)
				s.endWrite()
				if resizeNeeded {
					s.resize(int(s.live.Load()))
				}
				return boxed.value, true
			}
		}
	}
}

func (s *shard) compareAndSwap(hash uint64, key string, oldValue any, boxed *valueBox) bool {
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		current := findEntry(currentTable, hash, key)
		if current == nil {
			s.endWrite()
			return false
		}

		for {
			existing := current.value.Load()
			if existing == nil {
				s.endWrite()
				return false
			}

			if existing.expiresAt > 0 && time.Now().UnixNano() >= existing.expiresAt {
				if s.clearIfMatch(current, existing, true, false) {
					resizeNeeded := s.shouldResizeWithTable(currentTable)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()))
					}
					return false
				}
				continue
			}

			if existing.hits.Load() < 0 {
				if s.clearIfMatch(current, existing, false, true) {
					resizeNeeded := s.shouldResizeWithTable(currentTable)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()))
					}
					return false
				}
				continue
			}

			if !valuesEqual(existing.value, oldValue) {
				s.endWrite()
				return false
			}

			if current.value.CompareAndSwap(existing, boxed) {
				s.valueBytes.Add(boxed.clonedBytes - existing.clonedBytes)
				if boxed.requiresCleanup() {
					s.needsCleanup.Store(true)
				}
				resizeNeeded := s.shouldResizeWithTable(currentTable)
				s.endWrite()
				if resizeNeeded {
					s.resize(int(s.live.Load()))
				}
				return true
			}
		}
	}
}

func (s *shard) compareAndDelete(hash uint64, key string, oldValue any) bool {
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		current := findEntry(currentTable, hash, key)
		if current == nil {
			s.endWrite()
			return false
		}

		for {
			existing := current.value.Load()
			if existing == nil {
				s.endWrite()
				return false
			}

			if existing.expiresAt > 0 && time.Now().UnixNano() >= existing.expiresAt {
				if s.clearIfMatch(current, existing, true, false) {
					resizeNeeded := s.shouldResizeWithTable(currentTable)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()))
					}
					return false
				}
				continue
			}

			if existing.hits.Load() < 0 {
				if s.clearIfMatch(current, existing, false, true) {
					resizeNeeded := s.shouldResizeWithTable(currentTable)
					s.endWrite()
					if resizeNeeded {
						s.resize(int(s.live.Load()))
					}
					return false
				}
				continue
			}

			if !valuesEqual(existing.value, oldValue) {
				s.endWrite()
				return false
			}

			if current.value.CompareAndSwap(existing, nil) {
				s.live.Add(-1)
				s.tombstones.Add(1)
				s.keyBytes.Add(-current.keyBytes)
				s.valueBytes.Add(-existing.clonedBytes)
				s.needsCleanup.Store(true)
				resizeNeeded := s.shouldResizeWithTable(currentTable)
				s.endWrite()
				if resizeNeeded {
					s.resize(int(s.live.Load()))
				}
				return true
			}
		}
	}
}

func (s *shard) clear() {
	for {
		old := s.state.Load()
		if old&shardResizing != 0 {
			runtime.Gosched()
			continue
		}
		if s.state.CompareAndSwap(old, old|shardResizing) {
			break
		}
	}
	defer s.state.Add(-shardResizing)

	spins := int64(0)
	for s.state.Load() != shardResizing {
		spins++
		if spins%resizeSpinYieldInterval == 0 {
			runtime.Gosched()
		}
	}
	if spins != 0 {
		s.spinWaits.Add(spins)
	}

	empty := newTableWithSlots(s.initialSlots, s.loadFactor)
	s.table.Store(empty)
	s.live.Store(0)
	s.used.Store(0)
	s.tombstones.Store(0)
	s.keyBytes.Store(0)
	s.valueBytes.Store(0)
	s.needsCleanup.Store(false)
	s.resizes.Add(1)
}

func (s *shard) cleanup(now int64) {
	currentTable := s.table.Load()
	live := s.live.Load()
	if !s.needsCleanup.Load() && s.tombstones.Load() == 0 && !s.shouldShrink(currentTable, int(live)) {
		s.cleanupSkips.Add(1)
		return
	}
	s.cleanupRuns.Add(1)
	s.resizeAt(int(live), now, true)
}

func (s *shard) beginWrite() {
	v := s.state.Add(1)
	if v&shardResizing != 0 {
		s.state.Add(-1)
		s.beginWriteSlow()
	}
}

func (s *shard) beginWriteSlow() {
	spins := int64(0)
	for {
		for s.state.Load()&shardResizing != 0 {
			spins++
			if spins%resizeSpinYieldInterval == 0 {
				runtime.Gosched()
			}
		}

		v := s.state.Add(1)
		if v&shardResizing == 0 {
			if spins != 0 {
				s.spinWaits.Add(spins)
			}
			return
		}

		s.state.Add(-1)
		spins++
		if spins%resizeSpinYieldInterval == 0 {
			runtime.Gosched()
		}
	}
}

func (s *shard) endWrite() {
	s.state.Add(-1)
}

func (s *shard) insertFresh(currentTable *table, index int, hash uint64, key string, boxed *valueBox) (bool, bool, bool) {
	fresh := &entry{
		hash:     hash,
		key:      strings.Clone(key),
		keyBytes: int64(len(key)),
	}
	fresh.value.Store(boxed)
	if currentTable.slots[index].entry.CompareAndSwap(nil, fresh) {
		s.live.Add(1)
		used := s.used.Add(1)
		s.keyBytes.Add(fresh.keyBytes)
		if boxed.clonedBytes > 0 {
			s.valueBytes.Add(boxed.clonedBytes)
		}
		if boxed.requiresCleanup() {
			s.needsCleanup.Store(true)
		}
		return true, false, used >= int64(currentTable.growAt)
	}
	return false, true, false
}

func (s *shard) replaceOrRevive(current *entry, boxed *valueBox) (any, bool) {
	for {
		existing := current.value.Load()
		if existing == nil {
			if current.value.CompareAndSwap(nil, boxed) {
				s.live.Add(1)
				s.tombstones.Add(-1)
				s.keyBytes.Add(current.keyBytes)
				s.valueBytes.Add(boxed.clonedBytes)
				if boxed.requiresCleanup() {
					s.needsCleanup.Store(true)
				}
				return nil, false
			}
			continue
		}

		if existing.expiresAt > 0 && time.Now().UnixNano() >= existing.expiresAt {
			if s.clearIfMatch(current, existing, true, false) {
				continue
			}
			continue
		}

		if existing.hits.Load() < 0 {
			if s.clearIfMatch(current, existing, false, true) {
				continue
			}
			continue
		}

		if current.value.CompareAndSwap(existing, boxed) {
			s.valueBytes.Add(boxed.clonedBytes - existing.clonedBytes)
			if boxed.requiresCleanup() {
				s.needsCleanup.Store(true)
			}
			return existing.value, true
		}
	}
}

func (s *shard) loadOrStoreCurrent(current *entry, boxed *valueBox) (any, bool) {
	for {
		existing := current.value.Load()
		if existing == nil {
			if current.value.CompareAndSwap(nil, boxed) {
				s.live.Add(1)
				s.tombstones.Add(-1)
				s.keyBytes.Add(current.keyBytes)
				s.valueBytes.Add(boxed.clonedBytes)
				if boxed.requiresCleanup() {
					s.needsCleanup.Store(true)
				}
				return boxed.value, false
			}
			continue
		}

		if existing.expiresAt > 0 && time.Now().UnixNano() >= existing.expiresAt {
			if s.clearIfMatch(current, existing, true, false) {
				continue
			}
			continue
		}

		if existing.hits.Load() < 0 {
			if s.clearIfMatch(current, existing, false, true) {
				continue
			}
			continue
		}

		return existing.value, true
	}
}

func (s *shard) clearIfMatch(current *entry, boxed *valueBox, expired bool, depleted bool) bool {
	if current.value.CompareAndSwap(boxed, nil) {
		s.live.Add(-1)
		s.tombstones.Add(1)
		s.keyBytes.Add(-current.keyBytes)
		s.valueBytes.Add(-boxed.clonedBytes)
		s.needsCleanup.Store(true)
		if expired {
			s.expiredDeletes.Add(1)
		}
		if depleted {
			s.hitDeletes.Add(1)
		}
		return true
	}
	return false
}

func (s *shard) maybeResize() {
	currentTable := s.table.Load()
	if s.shouldResizeWithTable(currentTable) {
		s.resize(int(s.live.Load()))
	}
}

func (s *shard) resize(minLiveEntries int) {
	s.resizeAt(minLiveEntries, time.Now().UnixNano(), false)
}

func (s *shard) resizeAt(minLiveEntries int, now int64, force bool) {
	old := s.state.Load()
	if old&shardResizing != 0 {
		return
	}
	if !s.state.CompareAndSwap(old, old|shardResizing) {
		return
	}
	defer s.state.Add(-shardResizing)

	spins := int64(0)
	for s.state.Load() != shardResizing {
		spins++
		if spins%resizeSpinYieldInterval == 0 {
			runtime.Gosched()
		}
	}
	if spins != 0 {
		s.spinWaits.Add(spins)
	}

	currentTable := s.table.Load()
	if !force && !s.shouldResizeWithTable(currentTable) {
		return
	}

	actualLive := 0
	var keyBytes int64
	var valueBytes int64
	var expiredRemoved int64
	var hitRemoved int64
	needsCleanup := false

	for index := range currentTable.slots {
		current := currentTable.slots[index].entry.Load()
		if current == nil {
			continue
		}
		boxed := current.value.Load()
		if boxed == nil {
			continue
		}
		if boxed.expiresAt > 0 {
			needsCleanup = true
			if now >= boxed.expiresAt {
				expiredRemoved++
				continue
			}
		}
		hits := boxed.hits.Load()
		if hits != 0 {
			needsCleanup = true
			if hits < 0 {
				hitRemoved++
				continue
			}
		}
		actualLive++
		keyBytes += current.keyBytes
		valueBytes += boxed.clonedBytes
	}

	targetLive := actualLive
	if targetLive < minLiveEntries {
		targetLive = minLiveEntries
	}
	if targetLive < 1 {
		targetLive = 1
	}

	newSlotCount := s.targetSlotCount(currentTable, targetLive, force)
	rebuilt := newTableWithSlots(newSlotCount, s.loadFactor)
	for index := range currentTable.slots {
		current := currentTable.slots[index].entry.Load()
		if current == nil {
			continue
		}
		boxed := current.value.Load()
		if boxed == nil {
			continue
		}
		if boxed.expiresAt > 0 && now >= boxed.expiresAt {
			continue
		}
		if hits := boxed.hits.Load(); hits < 0 {
			continue
		}

		rebuilt.insertClone(current)
	}

	s.table.Store(rebuilt)
	s.live.Store(int64(actualLive))
	s.used.Store(int64(actualLive))
	s.tombstones.Store(0)
	s.keyBytes.Store(keyBytes)
	s.valueBytes.Store(valueBytes)
	s.expiredDeletes.Add(expiredRemoved)
	s.hitDeletes.Add(hitRemoved)
	s.needsCleanup.Store(needsCleanup)
	s.resizes.Add(1)
}

func (s *shard) targetSlotCount(currentTable *table, liveEntries int, force bool) int {
	required := slotsForEntries(liveEntries, s.loadFactor)
	if required < s.initialSlots {
		required = s.initialSlots
	}
	target := len(currentTable.slots)
	used := int(s.used.Load())
	tombstones := int(s.tombstones.Load())

	switch {
	case used >= currentTable.growAt || required > target:
		target = maxInt(target<<2, required)
	case target > s.initialSlots && tombstones > 0 && tombstones*4 >= maxInt(used, 1):
		target = required
	case target > s.initialSlots && required*2 < target:
		target = required
	case force && target > s.initialSlots && required < target:
		target = required
	}

	if target < s.initialSlots {
		target = s.initialSlots
	}
	return nextPowerOfTwo(target)
}

func (s *shard) shouldResizeWithTable(currentTable *table) bool {
	used := s.used.Load()
	if used >= int64(currentTable.growAt) {
		return true
	}
	tombstones := s.tombstones.Load()
	if used > 0 && tombstones > 0 && tombstones*4 >= used {
		return true
	}
	return s.shouldShrink(currentTable, int(s.live.Load()))
}

func (s *shard) shouldShrink(currentTable *table, liveEntries int) bool {
	if len(currentTable.slots) <= s.initialSlots {
		return false
	}
	required := slotsForEntries(maxInt(liveEntries, 1), s.loadFactor)
	if required < s.initialSlots {
		required = s.initialSlots
	}
	return required*2 < len(currentTable.slots)
}

func (t *table) insertClone(current *entry) {
	index := int(current.hash & t.mask)
	for {
		if t.slots[index].entry.Load() == nil {
			t.slots[index].entry.Store(current)
			return
		}
		index = (index + 1) & int(t.mask)
	}
}

func findEntry(currentTable *table, hash uint64, key string) *entry {
	index := int(hash & currentTable.mask)
	for probes := 0; probes < len(currentTable.slots); probes++ {
		current := currentTable.slots[index].entry.Load()
		if current == nil {
			return nil
		}
		if current.hash == hash && current.key == key {
			return current
		}
		index = (index + 1) & int(currentTable.mask)
	}
	return nil
}

func newTableForEntries(entries int, loadFactor float64) *table {
	return newTableWithSlots(slotsForEntries(entries, loadFactor), loadFactor)
}

func newTableWithSlots(slotCount int, loadFactor float64) *table {
	if slotCount < minTableSlots {
		slotCount = minTableSlots
	}
	slotCount = nextPowerOfTwo(slotCount)
	growAt := int(float64(slotCount) * loadFactor)
	if growAt >= slotCount {
		growAt = slotCount - 1
	}
	if growAt < 1 {
		growAt = 1
	}
	return &table{
		slots:  make([]slot, slotCount),
		mask:   uint64(slotCount - 1),
		growAt: growAt,
	}
}

func slotsForEntries(entries int, loadFactor float64) int {
	if entries < 1 {
		entries = 1
	}
	slots := int(math.Ceil(float64(entries) / loadFactor))
	if slots < minTableSlots {
		slots = minTableSlots
	}
	return nextPowerOfTwo(slots)
}

func normalizeHits(hits int64) int64 {
	if hits <= 0 {
		return 0
	}
	return hits
}

func (b *valueBox) requiresCleanup() bool {
	return b.expiresAt > 0 || b.hits.Load() != 0
}

func (b *valueBox) consumeHit() (consumed bool, exhausted bool) {
	for {
		hits := b.hits.Load()
		switch {
		case hits == 0:
			return true, false
		case hits < 0:
			return false, true
		default:
			next := hits - 1
			if next == 0 {
				next = -1
			}
			if b.hits.CompareAndSwap(hits, next) {
				return true, next < 0
			}
		}
	}
}

func defaultCloneValue(value any) (any, int64) {
	return value, 0
}

func valuesEqual(left any, right any) bool {
	if left == nil || right == nil {
		return left == right
	}
	if equaler, ok := left.(MapEqualer); ok {
		return equaler.EqualForMap(right)
	}
	if equaler, ok := right.(MapEqualer); ok {
		return equaler.EqualForMap(left)
	}

	switch leftValue := left.(type) {
	case []byte:
		rightValue, ok := right.([]byte)
		return ok && slices.Equal(leftValue, rightValue)
	case []string:
		rightValue, ok := right.([]string)
		return ok && slices.Equal(leftValue, rightValue)
	case []int:
		rightValue, ok := right.([]int)
		return ok && slices.Equal(leftValue, rightValue)
	case []int64:
		rightValue, ok := right.([]int64)
		return ok && slices.Equal(leftValue, rightValue)
	case []uint64:
		rightValue, ok := right.([]uint64)
		return ok && slices.Equal(leftValue, rightValue)
	case []bool:
		rightValue, ok := right.([]bool)
		return ok && slices.Equal(leftValue, rightValue)
	case []float64:
		rightValue, ok := right.([]float64)
		return ok && slices.Equal(leftValue, rightValue)
	case []rune:
		rightValue, ok := right.([]rune)
		return ok && slices.Equal(leftValue, rightValue)
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for index := range leftValue {
			if !valuesEqual(leftValue[index], rightValue[index]) {
				return false
			}
		}
		return true
	case map[string]string:
		rightValue, ok := right.(map[string]string)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, value := range leftValue {
			if rightValue[key] != value {
				return false
			}
		}
		return true
	case map[string]int:
		rightValue, ok := right.(map[string]int)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, value := range leftValue {
			if rightValue[key] != value {
				return false
			}
		}
		return true
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, value := range leftValue {
			rightItem, exists := rightValue[key]
			if !exists || !valuesEqual(value, rightItem) {
				return false
			}
		}
		return true
	}

	return safeComparableEqual(left, right)
}

func safeComparableEqual(left any, right any) (equal bool) {
	defer func() {
		if recover() != nil {
			equal = false
		}
	}()
	return left == right
}

func nextPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(value-1))
}

func floorPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}
	return 1 << (bits.Len(uint(value)) - 1)
}

func divideRoundUp(value int, divisor int) int {
	if divisor <= 0 {
		return value
	}
	return (value + divisor - 1) / divisor
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
