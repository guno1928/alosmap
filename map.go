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
	unlimitedHits           int64 = -1
)

// Option configures a Map at construction time.
type Option func(*config)

// ValueCloneFunc clones a value at write time and returns an approximate byte size.
type ValueCloneFunc func(value any) (cloned any, trackedBytes int64)

// EntryOptions controls per-entry expiry and hit limits.
type EntryOptions struct {
	TTL  time.Duration
	Hits int64
}

// MapCloner lets callers provide deep-copy semantics for custom values without reflection.
type MapCloner interface {
	CloneForMap() any
}

// SizedMapCloner lets callers provide deep-copy semantics plus approximate byte tracking.
type SizedMapCloner interface {
	CloneForMapWithSize() (any, int64)
}

// MapEqualer provides non-reflect equality for custom values used with CAS operations.
type MapEqualer interface {
	EqualForMap(other any) bool
}

type config struct {
	capacity        int
	loadFactor      float64
	shardCount      int
	cleanupInterval time.Duration
	cloneValue      ValueCloneFunc
}

// Builder provides a fluent constructor for the public API.
type Builder struct {
	cfg config
}

// Pair is a key/value snapshot item returned by Snapshot.
type Pair struct {
	Key   string
	Value any
}

// Stats exposes live operational metrics for the map.
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

// Map is a sharded, mostly lock-free concurrent hash map for string keys.
// Reads are lock-free for normal entries. Writes use atomic publication and only
// stall when a shard is rebuilding or resizing.
type Map struct {
	seed            uint64
	shardMask       uint64
	shards          []shard
	cleanupInterval time.Duration
	cloneValue      ValueCloneFunc
	stopCleanup     chan struct{}
	cleanupDone     chan struct{}
	cleanupClosed   atomic.Bool
}

type shard struct {
	table          atomic.Pointer[table]
	writers        atomic.Int64
	live           atomic.Int64
	used           atomic.Int64
	tombstones     atomic.Int64
	resizes        atomic.Int64
	spinWaits      atomic.Int64
	cleanupRuns    atomic.Int64
	cleanupSkips   atomic.Int64
	expiredDeletes atomic.Int64
	hitDeletes     atomic.Int64
	keyBytes       atomic.Int64
	valueBytes     atomic.Int64
	resizing       atomic.Bool
	needsCleanup   atomic.Bool
	loadFactor     float64
	initialSlots   int
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

// NewBuilder returns a fluent builder for the map API.
func NewBuilder() *Builder {
	return &Builder{cfg: defaultConfig()}
}

// Capacity sets the expected live entry count.
func (b *Builder) Capacity(capacity int) *Builder {
	b.cfg.capacity = capacity
	return b
}

// Shards sets the shard count. Values are rounded up to the next power of two.
func (b *Builder) Shards(count int) *Builder {
	b.cfg.shardCount = count
	return b
}

// LoadFactor sets the live-entry occupancy target before a shard grows.
func (b *Builder) LoadFactor(loadFactor float64) *Builder {
	b.cfg.loadFactor = loadFactor
	return b
}

// CleanupInterval sets the background cleanup tick. Zero disables the background cleaner.
func (b *Builder) CleanupInterval(interval time.Duration) *Builder {
	b.cfg.cleanupInterval = interval
	return b
}

// ValueCloner overrides the write-time clone function.
func (b *Builder) ValueCloner(cloner ValueCloneFunc) *Builder {
	b.cfg.cloneValue = cloner
	return b
}

// Build constructs the map from the builder configuration.
func (b *Builder) Build() *Map {
	return New(
		WithCapacity(b.cfg.capacity),
		WithShardCount(b.cfg.shardCount),
		WithLoadFactor(b.cfg.loadFactor),
		WithCleanupInterval(b.cfg.cleanupInterval),
		WithValueCloner(b.cfg.cloneValue),
	)
}

// WithCapacity sets the expected live entry count.
func WithCapacity(capacity int) Option {
	return func(cfg *config) {
		cfg.capacity = capacity
	}
}

// WithShardCount sets the number of shards. Values are rounded up to the next power of two.
func WithShardCount(count int) Option {
	return func(cfg *config) {
		cfg.shardCount = count
	}
}

// WithLoadFactor sets the live-entry occupancy target before a shard grows.
func WithLoadFactor(loadFactor float64) Option {
	return func(cfg *config) {
		cfg.loadFactor = loadFactor
	}
}

// WithCleanupInterval configures the background cleanup interval. Zero disables it.
func WithCleanupInterval(interval time.Duration) Option {
	return func(cfg *config) {
		cfg.cleanupInterval = interval
	}
}

// WithoutCleanup disables the background cleanup ticker.
func WithoutCleanup() Option {
	return func(cfg *config) {
		cfg.cleanupInterval = 0
	}
}

// WithValueCloner overrides the write-time clone function.
func WithValueCloner(cloner ValueCloneFunc) Option {
	return func(cfg *config) {
		cfg.cloneValue = cloner
	}
}

// New constructs a new custom concurrent map.
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

// RecommendedShardCount returns the auto-tuned shard count for the requested capacity.
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

// Close stops the background cleanup goroutine.
func (m *Map) Close() {
	if m.stopCleanup == nil {
		return
	}
	if m.cleanupClosed.CompareAndSwap(false, true) {
		close(m.stopCleanup)
		<-m.cleanupDone
	}
}

// Set stores a value for the provided key.
func (m *Map) Set(key string, value any) {
	m.Store(key, value)
}

// Store stores a value for the provided key.
func (m *Map) Store(key string, value any) {
	m.StoreWithOptions(key, value, EntryOptions{})
}

// StoreWithTTL stores a value with a time-to-live.
func (m *Map) StoreWithTTL(key string, value any, ttl time.Duration) {
	m.StoreWithOptions(key, value, EntryOptions{TTL: ttl})
}

// StoreWithHits stores a value that is automatically deleted after the configured number of successful loads.
func (m *Map) StoreWithHits(key string, value any, hits int64) {
	m.StoreWithOptions(key, value, EntryOptions{Hits: hits})
}

// StoreWithTTLAndHits stores a value with both a TTL and a hit limit.
func (m *Map) StoreWithTTLAndHits(key string, value any, ttl time.Duration, hits int64) {
	m.StoreWithOptions(key, value, EntryOptions{TTL: ttl, Hits: hits})
}

// SetWithTTLAndHits is an alias for StoreWithTTLAndHits.
func (m *Map) SetWithTTLAndHits(key string, value any, ttl time.Duration, hits int64) {
	m.StoreWithTTLAndHits(key, value, ttl, hits)
}

// StoreWithOptions stores a value with per-entry expiry settings.
func (m *Map) StoreWithOptions(key string, value any, options EntryOptions) {
	hash := hashString(m.seed, key)
	boxed := m.newValueBox(value, options)
	m.pickShard(hash).store(hash, key, boxed)
}

// Load retrieves a value by key and consumes one hit when the entry is hit-limited.
func (m *Map) Load(key string) (any, bool) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).load(hash, key)
}

// Get is an alias for Load.
func (m *Map) Get(key string) (any, bool) {
	return m.Load(key)
}

// Peek retrieves a value by key without consuming hit counters.
func (m *Map) Peek(key string) (any, bool) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).peek(hash, key)
}

// GetOrDefault returns the stored value or the provided fallback.
func (m *Map) GetOrDefault(key string, fallback any) any {
	if value, ok := m.Load(key); ok {
		return value
	}
	return fallback
}

// PeekOrDefault returns the stored value without consuming hits or the provided fallback.
func (m *Map) PeekOrDefault(key string, fallback any) any {
	if value, ok := m.Peek(key); ok {
		return value
	}
	return fallback
}

// Has reports whether the key currently exists in the map without consuming hits.
func (m *Map) Has(key string) bool {
	_, ok := m.Peek(key)
	return ok
}

// Delete removes a key and returns the prior value when present.
func (m *Map) Delete(key string) (any, bool) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).delete(hash, key)
}

// LoadOrStore returns the existing value if present or stores the supplied one.
func (m *Map) LoadOrStore(key string, value any) (any, bool) {
	return m.LoadOrStoreWithOptions(key, value, EntryOptions{})
}

// LoadOrStoreWithOptions returns the existing value if present or stores the supplied one using the provided entry options.
func (m *Map) LoadOrStoreWithOptions(key string, value any, options EntryOptions) (any, bool) {
	hash := hashString(m.seed, key)
	boxed := m.newValueBox(value, options)
	return m.pickShard(hash).loadOrStore(hash, key, boxed)
}

// Swap replaces the value for a key and returns the previous value if there was one.
func (m *Map) Swap(key string, value any) (any, bool) {
	return m.SwapWithOptions(key, value, EntryOptions{})
}

// SwapWithOptions replaces the value for a key using the provided entry options.
func (m *Map) SwapWithOptions(key string, value any, options EntryOptions) (any, bool) {
	hash := hashString(m.seed, key)
	boxed := m.newValueBox(value, options)
	return m.pickShard(hash).swap(hash, key, boxed)
}

// CompareAndSwap swaps the value if the current value matches oldValue.
func (m *Map) CompareAndSwap(key string, oldValue any, newValue any) bool {
	return m.CompareAndSwapWithOptions(key, oldValue, newValue, EntryOptions{})
}

// CompareAndSwapWithOptions swaps the value if the current value matches oldValue and applies entry options to the new value.
func (m *Map) CompareAndSwapWithOptions(key string, oldValue any, newValue any, options EntryOptions) bool {
	hash := hashString(m.seed, key)
	boxed := m.newValueBox(newValue, options)
	return m.pickShard(hash).compareAndSwap(hash, key, oldValue, boxed)
}

// CompareAndDelete deletes a key if the current value matches oldValue.
func (m *Map) CompareAndDelete(key string, oldValue any) bool {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).compareAndDelete(hash, key, oldValue)
}

// Len returns the current live entry count.
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

// Clear removes all keys and scales the table back toward its initial footprint.
func (m *Map) Clear() {
	for index := range m.shards {
		m.shards[index].clear()
	}
}

// CleanupNow runs an immediate cleanup pass across all shards.
func (m *Map) CleanupNow() {
	now := time.Now().UnixNano()
	for index := range m.shards {
		m.shards[index].cleanup(now)
	}
}

// Range iterates over the current contents of the map without consuming hit counters.
// It provides an eventually-consistent view when writes are happening concurrently.
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

// Snapshot returns the map contents as a flat slice of key/value pairs.
func (m *Map) Snapshot() []Pair {
	pairs := make([]Pair, 0, m.Len())
	m.Range(func(key string, value any) bool {
		pairs = append(pairs, Pair{Key: key, Value: value})
		return true
	})
	return pairs
}

// Stats returns operational metrics aggregated across all shards.
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
	boxed.hits.Store(normalizeHits(options.Hits))
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
			if boxed.hits.Load() == 0 {
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

			if boxed.hits.Load() == 0 {
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

			if existing.hits.Load() == 0 {
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

			if existing.hits.Load() == 0 {
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
	for !s.resizing.CompareAndSwap(false, true) {
		runtime.Gosched()
	}
	defer s.resizing.Store(false)

	spins := int64(0)
	for s.writers.Load() != 0 {
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
	spins := int64(0)

	for {
		for s.resizing.Load() {
			spins++
			if spins%resizeSpinYieldInterval == 0 {
				runtime.Gosched()
			}
		}

		s.writers.Add(1)
		if !s.resizing.Load() {
			if spins != 0 {
				s.spinWaits.Add(spins)
			}
			return
		}

		s.writers.Add(-1)
		spins++
		if spins%resizeSpinYieldInterval == 0 {
			runtime.Gosched()
		}
	}
}

func (s *shard) endWrite() {
	s.writers.Add(-1)
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
		s.valueBytes.Add(boxed.clonedBytes)
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

		if existing.hits.Load() == 0 {
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

		if existing.hits.Load() == 0 {
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
	if !s.resizing.CompareAndSwap(false, true) {
		return
	}
	defer s.resizing.Store(false)

	spins := int64(0)
	for s.writers.Load() != 0 {
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
		if hits >= 0 {
			needsCleanup = true
			if hits == 0 {
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
		if hits := boxed.hits.Load(); hits == 0 {
			continue
		}

		clone := &entry{hash: current.hash, key: current.key, keyBytes: current.keyBytes}
		clone.value.Store(boxed)
		rebuilt.insertClone(clone)
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
		target = maxInt(target<<1, required)
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
		return unlimitedHits
	}
	return hits
}

func (b *valueBox) requiresCleanup() bool {
	return b.expiresAt > 0 || b.hits.Load() >= 0
}

func (b *valueBox) consumeHit() (consumed bool, exhausted bool) {
	for {
		hits := b.hits.Load()
		switch {
		case hits < 0:
			return true, false
		case hits == 0:
			return false, true
		default:
			if b.hits.CompareAndSwap(hits, hits-1) {
				return true, hits == 1
			}
		}
	}
}

func defaultCloneValue(value any) (any, int64) {
	switch typed := value.(type) {
	case nil:
		return nil, 0
	case SizedMapCloner:
		return typed.CloneForMapWithSize()
	case MapCloner:
		return typed.CloneForMap(), 0
	case string:
		cloned := strings.Clone(typed)
		return cloned, int64(len(cloned))
	case []byte:
		cloned := slices.Clone(typed)
		return cloned, int64(len(cloned))
	case []string:
		cloned := make([]string, len(typed))
		total := int64(len(cloned)) * int64(unsafe.Sizeof(string("")))
		for index := range typed {
			cloned[index] = strings.Clone(typed[index])
			total += int64(len(cloned[index]))
		}
		return cloned, total
	case []int:
		cloned := slices.Clone(typed)
		return cloned, int64(len(cloned)) * int64(unsafe.Sizeof(int(0)))
	case []int64:
		cloned := slices.Clone(typed)
		return cloned, int64(len(cloned)) * int64(unsafe.Sizeof(int64(0)))
	case []uint64:
		cloned := slices.Clone(typed)
		return cloned, int64(len(cloned)) * int64(unsafe.Sizeof(uint64(0)))
	case []bool:
		cloned := slices.Clone(typed)
		return cloned, int64(len(cloned)) * int64(unsafe.Sizeof(false))
	case []float64:
		cloned := slices.Clone(typed)
		return cloned, int64(len(cloned)) * int64(unsafe.Sizeof(float64(0)))
	case []rune:
		cloned := slices.Clone(typed)
		return cloned, int64(len(cloned)) * int64(unsafe.Sizeof(rune(0)))
	case []any:
		cloned := make([]any, len(typed))
		total := int64(len(cloned)) * int64(unsafe.Sizeof(any(nil)))
		for index := range typed {
			item, bytes := defaultCloneValue(typed[index])
			cloned[index] = item
			total += bytes
		}
		return cloned, total
	case [][]byte:
		cloned := make([][]byte, len(typed))
		total := int64(len(cloned)) * int64(unsafe.Sizeof([]byte(nil)))
		for index := range typed {
			cloned[index] = slices.Clone(typed[index])
			total += int64(len(cloned[index]))
		}
		return cloned, total
	case map[string]string:
		cloned := make(map[string]string, len(typed))
		total := int64(0)
		for key, value := range typed {
			copyKey := strings.Clone(key)
			copyValue := strings.Clone(value)
			cloned[copyKey] = copyValue
			total += int64(len(copyKey) + len(copyValue))
		}
		return cloned, total
	case map[string]int:
		cloned := make(map[string]int, len(typed))
		total := int64(0)
		for key, value := range typed {
			copyKey := strings.Clone(key)
			cloned[copyKey] = value
			total += int64(len(copyKey)) + int64(unsafe.Sizeof(value))
		}
		return cloned, total
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		total := int64(0)
		for key, value := range typed {
			copyKey := strings.Clone(key)
			copyValue, bytes := defaultCloneValue(value)
			cloned[copyKey] = copyValue
			total += int64(len(copyKey)) + bytes
		}
		return cloned, total
	default:
		return value, 0
	}
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
