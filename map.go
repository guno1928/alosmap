package alosmap

import (
	"math"
	"math/bits"
	"runtime"
	"slices"
	"strconv"
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

// EntryOptions defines optional per-entry lifecycle controls for write operations
// such as StoreWithOptions, LoadOrStoreWithOptions, SwapWithOptions, and
// CompareAndSwapWithOptions.
//
// EntryOptions is a configuration struct. All fields are optional, the zero value is
// valid, and no field is required. When the zero value is used, the entry behaves
// like a normal persistent map value: it has no time-based expiration, no hit limit,
// and remains available until it is overwritten, explicitly deleted, cleared, or
// removed by some other map operation.
//
// Field behavior:
//
// TTL controls time-based expiration. Default: 0. Optional: yes. Valid values: any
// time.Duration. When TTL is greater than 0, the entry expires that far in the
// future relative to the write that created or replaced it. When TTL is 0 or
// negative, TTL-based expiration is disabled.
//
// Hits controls read-count expiration for Load and Get. Default: 0. Optional: yes.
// Valid values: any int64. When Hits is greater than 0, each successful Load or Get
// decrements the remaining budget and the entry is removed after the final allowed
// successful consuming read. When Hits is 0 or negative, hit limiting is disabled.
// Peek, Has, Range, Snapshot, Stats, and Delete do not consume hits.
//
// TTL and Hits may be combined. In that case, the entry becomes unavailable as soon
// as either limit is reached first.
//
// Example:
//
//	builder := alosmap.NewBuilder().
//		Capacity(50_000).
//		Shards(64).
//		LoadFactor(0.80).
//		CleanupInterval(2 * time.Second)
//	cache := builder.Build()
//	defer cache.Close()
//
//	sessionOptions := alosmap.EntryOptions{
//		TTL:  30 * time.Minute,
//		Hits: 5,
//	}
//	cache.StoreWithOptions(alosmap.S("session:42"), []byte("ready"), sessionOptions)
//
//	value, ok := cache.Load(alosmap.S("session:42"))
//	_ = value
//	_ = ok
type EntryOptions struct {
	TTL  time.Duration
	Hits int64
}

// Key represents a user-supplied map key.
//
// A Key can carry either a string or an int64 and is the only key type accepted by
// Map. Construct keys with S for string values and I for int64 values. Key is a
// small value type intended to be passed by value.
type Key struct {
	s     string
	i     int64
	isInt bool
}

// S constructs a Key from a string.
//
// Use S for natural application identifiers such as usernames, cache paths,
// request IDs, or composite text keys.
func S(key string) Key { return Key{s: key} }

// I constructs a Key from an int64.
//
// Use I when numeric identifiers are already available in int64 form and you want
// to avoid converting them to strings before accessing the map.
func I(key int64) Key { return Key{i: key, isInt: true} }

// String returns the human-readable form of the key.
//
// String returns the underlying string for string keys and the base-10 decimal
// representation for int64 keys. It is intended for logging, debugging, and
// diagnostic output.
func (k Key) String() string {
	if k.isInt {
		return strconv.FormatInt(k.i, 10)
	}
	return k.s
}

// IsString reports whether the key was created from a string.
func (k Key) IsString() bool { return !k.isInt }

// IsInt64 reports whether the key was created from an int64.
func (k Key) IsInt64() bool { return k.isInt }

// StringVal returns the string payload of the key.
//
// For string keys, StringVal returns the original string. For int64 keys, it
// returns the zero string. Use IsString when you need to distinguish those cases.
func (k Key) StringVal() string { return k.s }

// Int64Val returns the int64 payload of the key.
//
// For int64 keys, Int64Val returns the original integer. For string keys, it
// returns 0. Use IsInt64 when you need to distinguish those cases.
func (k Key) Int64Val() int64 { return k.i }

// Raw returns the underlying key value as either a string or an int64.
//
// This is useful when generic logging, formatting, or adapter code needs the raw
// user key without inspecting Key's internal representation directly.
func (k Key) Raw() any {
	if k.isInt {
		return k.i
	}
	return k.s
}

func cloneKey(k Key) Key {
	return k
}

func keySize(k Key) int64 {
	if k.isInt {
		return 8
	}
	return int64(len(k.s))
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
// Builder is a user-facing configuration struct for callers who prefer a fluent,
// chainable setup style instead of passing functional options to New. Each setter
// records a requested value and returns the same Builder so calls can be chained.
// Build applies the same validation, normalization, and defaulting rules as New.
//
// Builder configuration fields:
//
// Capacity is optional. Default: 1024 expected live entries. Constraint: values less
// than 1 are normalized to the default. Capacity influences initial allocation and
// resize behavior only; it is not a hard maximum.
//
// Shards is optional. Default: automatic heuristic based on expected capacity and
// GOMAXPROCS. Constraint: positive values are rounded up to the next power of two;
// zero or negative values trigger automatic selection.
//
// LoadFactor is optional. Default: 0.72. Constraint: values below 0.50 are clamped
// to 0.50 and values above 0.90 are clamped to 0.90.
//
// CleanupInterval is optional. Default: 5 seconds. Constraint: negative values are
// normalized to 0. A value of 0 disables the background cleanup goroutine.
//
// ValueCloner is optional. Default: pass-through storage with zero tracked value
// bytes. When provided, it is called at write time so mutable values can be cloned
// before publication and approximate value memory can be recorded in Stats.
//
// Example:
//
//	cloneBytes := func(value any) (any, int64) {
//		payload := value.([]byte)
//		cloned := append([]byte(nil), payload...)
//		return cloned, int64(len(cloned))
//	}
//
//	cache := alosmap.NewBuilder().
//		Capacity(250_000).
//		Shards(128).
//		LoadFactor(0.78).
//		CleanupInterval(3 * time.Second).
//		ValueCloner(cloneBytes).
//		Build()
//	defer cache.Close()
type Builder struct {
	cfg config
}

// Pair represents one key-value item returned by Snapshot.
//
// Pair is primarily used when callers need a materialized slice view of the map for
// further processing, sorting, copying, or serialization.
type Pair struct {
	Key   Key
	Value any
}

// Stats is a point-in-time snapshot of aggregated operational counters and size
// estimates for a Map.
//
// Stats is observational data intended for monitoring, sizing, diagnostics, and
// performance tuning. In concurrent programs the map may change again immediately
// after Stats returns, so every field should be treated as approximate.
//
// Occupancy-related fields include Shards, SlotCapacity, LiveEntries, UsedSlots,
// Tombstones, LoadFactor, and MaxShardLive. Maintenance-related fields include
// ResizeCount, WriterSpins, CleanupRuns, CleanupSkips, ExpiredDeletes, and
// HitDeletes. Memory-related fields include TrackedKeyBytes, TrackedValueBytes,
// EstimatedOverheadBytes, and EstimatedResidentBytes.
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

// Map is a concurrent map for string and int64 keys with lock-free reads for live
// entries, sharded write coordination, and built-in entry lifecycle controls.
//
// Map is designed for high-throughput read-heavy and mixed workloads where callers
// need more than a plain map or sync.Map, including conditional updates, TTL-based
// expiration, hit-limited entries, background cleanup, snapshots, and lightweight
// operational statistics.
//
// Keys are supplied through Key values created with S or I. Values are stored as any.
// By default the map stores the value exactly as supplied, including pointer values.
// If you need copy-on-write behavior for mutable data, provide a ValueCloneFunc when
// constructing the map.
//
// Map remains usable after Close. Close only stops the optional background cleanup
// goroutine.
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
	ctrl   []byte
	mask   uint64
	growAt int
}

type slot struct {
	entry atomic.Pointer[entry]
}

type entry struct {
	hash  uint64
	key   Key
	value atomic.Pointer[valueBox]
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
// Use NewBuilder when you want fluent map configuration instead of passing options
// directly to New. The returned Builder starts with the same defaults that New uses.
func NewBuilder() *Builder {
	return &Builder{cfg: defaultConfig()}
}

// Capacity sets the expected live entry count used for initial sizing.
//
// This setting is optional. When left unset, Build uses the default of 1024. Values
// below 1 are normalized to that default. Capacity improves startup sizing and can
// reduce early resizes, but it does not impose a maximum entry count.
func (b *Builder) Capacity(capacity int) *Builder {
	b.cfg.capacity = capacity
	return b
}

// Shards sets the requested shard count for the map.
//
// This setting is optional. Positive values are rounded up to the next power of two.
// Zero and negative values tell Build to use the package's shard-selection heuristic.
// More shards can reduce cross-key write contention at the cost of extra overhead.
func (b *Builder) Shards(count int) *Builder {
	b.cfg.shardCount = count
	return b
}

// LoadFactor sets the target occupancy before a shard grows.
//
// This setting is optional. The default is 0.72. Values below 0.50 are clamped to
// 0.50 and values above 0.90 are clamped to 0.90. Higher values trade memory for
// denser tables; lower values trade space for shorter probe sequences.
func (b *Builder) LoadFactor(loadFactor float64) *Builder {
	b.cfg.loadFactor = loadFactor
	return b
}

// CleanupInterval sets the cadence of the background cleanup goroutine.
//
// This setting is optional. The default is 5 seconds. A value of 0 disables
// background cleanup entirely. Negative values are normalized to 0 during Build.
// Even when background cleanup is disabled, manual cleanup via CleanupNow remains
// available.
func (b *Builder) CleanupInterval(interval time.Duration) *Builder {
	b.cfg.cleanupInterval = interval
	return b
}

// ValueCloner installs a custom write-time cloning function.
//
// This setting is optional. By default the map stores values exactly as supplied.
// Provide a cloner when callers should never observe shared mutable state directly,
// or when Stats should include tracked value bytes for stored objects. A nil cloner
// falls back to the default pass-through behavior.
func (b *Builder) ValueCloner(cloner ValueCloneFunc) *Builder {
	b.cfg.cloneValue = cloner
	return b
}

// Build constructs a Map from the Builder configuration.
//
// Build applies the same defaults and normalization rules as New. Calling Build is
// equivalent to translating the recorded Builder state into the corresponding option
// list and passing that list to New.
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
// The option is optional. The default is 1024 when it is omitted. Values below 1 are
// normalized to that default. Capacity affects initial allocation and resize behavior,
// but it does not enforce an upper bound on the number of entries.
func WithCapacity(capacity int) Option {
	return func(cfg *config) {
		cfg.capacity = capacity
	}
}

// WithShardCount returns an Option that sets the requested shard count.
//
// The option is optional. Positive values are rounded up to the next power of two.
// Zero and negative values cause New to choose the shard count automatically.
func WithShardCount(count int) Option {
	return func(cfg *config) {
		cfg.shardCount = count
	}
}

// WithLoadFactor returns an Option that sets the target occupancy before a shard grows.
//
// The option is optional. The default is 0.72. Values are clamped into the inclusive
// range [0.50, 0.90].
func WithLoadFactor(loadFactor float64) Option {
	return func(cfg *config) {
		cfg.loadFactor = loadFactor
	}
}

// WithCleanupInterval returns an Option that sets the background cleanup interval.
//
// The option is optional. The default is 5 seconds. A value of 0 disables background
// cleanup. Negative values are normalized to 0.
func WithCleanupInterval(interval time.Duration) Option {
	return func(cfg *config) {
		cfg.cleanupInterval = interval
	}
}

// WithoutCleanup returns an Option that disables the background cleanup goroutine.
//
// This is a convenience alias for WithCleanupInterval(0).
func WithoutCleanup() Option {
	return func(cfg *config) {
		cfg.cleanupInterval = 0
	}
}

// WithValueCloner returns an Option that installs a custom write-time clone function.
//
// Use this option when mutable values should be copied before publication or when
// Stats should record approximate value memory for stored objects. A nil cloner falls
// back to pass-through storage.
func WithValueCloner(cloner ValueCloneFunc) Option {
	return func(cfg *config) {
		cfg.cloneValue = cloner
	}
}

// New constructs a Map from the provided options.
//
// All options are optional. When omitted, New uses these defaults:
//
//	Capacity: 1024 expected live entries.
//	Shard count: selected automatically from expected capacity and GOMAXPROCS.
//	Load factor: 0.72.
//	Cleanup interval: 5 seconds.
//	Value cloner: pass-through storage with zero tracked value bytes.
//
// Option values are normalized before construction. Capacity values below 1 fall
// back to the default, shard counts are rounded to a power of two or auto-selected,
// load factor is clamped to [0.50, 0.90], and negative cleanup intervals are treated
// as 0.
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

// RecommendedShardCount returns the shard count heuristic for the supplied expected entry count.
//
// Use this helper when you want to choose an explicit shard count but still follow
// the package's built-in sizing heuristic. Values below 1 are treated as 1.
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
// Close is safe to call multiple times. It does not invalidate the map, clear any
// entries, or prevent future reads and writes. Its only effect is stopping automatic
// background cleanup.
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
// Store creates or replaces the entry at key with no TTL and no hit limit. If the key
// already exists, the previous live value is replaced. Unless the map was constructed
// with a ValueCloneFunc, the supplied value is stored by reference exactly as given.
func (m *Map) Store(key Key, value any) {
	hash := hashKey(m.seed, key)
	s := m.pickShard(hash)

	var boxed *valueBox
	if m.noClone {
		boxed = &valueBox{value: value}
	} else {
		cloned, tracked := m.cloneValue(value)
		boxed = &valueBox{value: cloned, clonedBytes: tracked}
	}

	currentTable := s.table.Load()
	idx := int(hash & currentTable.mask)
	fp := byte(hash>>56) | 1
	ctrl := currentTable.ctrl
	mask := int(currentTable.mask)
	for probes := 0; probes <= mask; probes++ {
		c := ctrl[idx]
		if c == 0 {
			break
		}
		if c == fp {
			current := currentTable.slots[idx].entry.Load()
			if current != nil && current.hash == hash && keysEqual(current.key, key) {
				existing := current.value.Load()
				if existing != nil && existing.expiresAt == 0 && existing.hits.Load() >= 0 {
					if current.value.CompareAndSwap(existing, boxed) {
						s.valueBytes.Add(boxed.clonedBytes - existing.clonedBytes)
						return
					}
				}
				break
			}
		}
		idx = (idx + 1) & mask
	}

	s.store(hash, key, boxed)
}

// StoreWithTTL writes value at key and applies time-based expiration.
//
// When ttl is greater than 0, the entry expires that far in the future relative to
// this write. When ttl is 0 or negative, the entry behaves like Store and does not
// expire by time.
func (m *Map) StoreWithTTL(key Key, value any, ttl time.Duration) {
	m.StoreWithOptions(key, value, EntryOptions{TTL: ttl})
}

// StoreWithHits writes value at key and applies a hit limit.
//
// Hits greater than 0 allow that many successful consuming reads through Load or Get
// before the entry is removed. Hits equal to 0 or less disable hit limiting.
func (m *Map) StoreWithHits(key Key, value any, hits int64) {
	m.StoreWithOptions(key, value, EntryOptions{Hits: hits})
}

// StoreWithTTLAndHits writes value at key with both TTL and hit-limit behavior.
//
// TTL and Hits follow the same rules documented by EntryOptions. The entry becomes
// unavailable when either limit is reached first.
func (m *Map) StoreWithTTLAndHits(key Key, value any, ttl time.Duration, hits int64) {
	m.StoreWithOptions(key, value, EntryOptions{TTL: ttl, Hits: hits})
}

// SetWithTTLAndHits writes value at key with both TTL and hit-limit behavior.
//
// This is a naming alias for StoreWithTTLAndHits for callers who prefer Set-style
// API naming. Its behavior is identical.
func (m *Map) SetWithTTLAndHits(key Key, value any, ttl time.Duration, hits int64) {
	m.StoreWithTTLAndHits(key, value, ttl, hits)
}

// StoreWithOptions writes value at key and applies the supplied EntryOptions.
//
// This is the most general write API for creating or replacing a value. Passing the
// zero value of EntryOptions produces the same behavior as Store.
func (m *Map) StoreWithOptions(key Key, value any, options EntryOptions) {
	hash := hashKey(m.seed, key)
	boxed := m.newValueBox(value, options)
	m.pickShard(hash).store(hash, key, boxed)
}

// Load returns the current live value for key.
//
// Load is the primary consuming read operation. For hit-limited entries, each
// successful Load consumes one remaining hit. Expired entries and exhausted
// hit-limited entries are treated as absent and return ok false.
func (m *Map) Load(key Key) (any, bool) {
	var hash uint64
	if key.isInt {
		hash = mix(m.seed^uint64(key.i)^hashSeed0, uint64(key.i)^hashSeed1)
	} else {
		hash = hashString(m.seed, key.s)
	}

	s := &m.shards[int((hash>>32)&m.shardMask)]

	currentTable := s.table.Load()
	index := int(hash & currentTable.mask)
	fp := byte(hash>>56) | 1
	ctrl := currentTable.ctrl
	slots := currentTable.slots
	mask := int(currentTable.mask)
	for probes := 0; probes <= mask; probes++ {
		c := ctrl[index]
		if c == 0 {
			return nil, false
		}
		if c == fp {
			current := slots[index].entry.Load()
			if current != nil && current.hash == hash && keysEqual(current.key, key) {
				boxed := current.value.Load()
				if boxed == nil {
					return nil, false
				}
				if boxed.expiresAt == 0 {
					hits := boxed.hits.Load()
					if hits == 0 {
						return boxed.value, true
					}
					if hits > 0 {
						return s.readEntryConsumeHit(current, boxed)
					}
					if hits < 0 {
						if s.clearIfMatch(current, boxed, false, true) {
							s.maybeResize()
						}
						return nil, false
					}
					return boxed.value, true
				}
				return s.readEntrySlow(current, true)
			}
		}
		index = (index + 1) & mask
	}
	return nil, false
}

// Get returns the current live value for key.
//
// Get is a naming alias for Load and has identical behavior, including hit
// consumption for hit-limited entries.
func (m *Map) Get(key Key) (any, bool) {
	return m.Load(key)
}

// Peek returns the current live value for key without consuming hits.
//
// Peek is useful for inspection code that should not advance hit-limited entries.
// Expired and exhausted entries are treated as absent, just as they are for Load.
func (m *Map) Peek(key Key) (any, bool) {
	hash := hashKey(m.seed, key)
	return m.pickShard(hash).peek(hash, key)
}

// Has reports whether key currently resolves to a live entry.
//
// Has does not consume hits and is equivalent to discarding the value returned by
// Peek.
func (m *Map) Has(key Key) bool {
	_, ok := m.Peek(key)
	return ok
}

// Delete removes key and returns the previous live value when present.
//
// If the key is absent, expired, or already exhausted, Delete returns nil and false.
func (m *Map) Delete(key Key) (any, bool) {
	hash := hashKey(m.seed, key)
	return m.pickShard(hash).delete(hash, key)
}

// LoadOrStore returns the existing live value for key or stores value when the key is absent.
//
// When the key already resolves to a live entry, LoadOrStore returns that value and
// loaded true. Otherwise it stores the supplied value with default behavior and
// returns the stored value with loaded false.
func (m *Map) LoadOrStore(key Key, value any) (any, bool) {
	hash := hashKey(m.seed, key)
	s := m.pickShard(hash)

	currentTable := s.table.Load()
	idx := int(hash & currentTable.mask)
	fp := byte(hash>>56) | 1
	ctrl := currentTable.ctrl
	mask := int(currentTable.mask)
	for probes := 0; probes <= mask; probes++ {
		c := ctrl[idx]
		if c == 0 {
			break
		}
		if c == fp {
			current := currentTable.slots[idx].entry.Load()
			if current != nil && current.hash == hash && keysEqual(current.key, key) {
				existing := current.value.Load()
				if existing != nil && existing.expiresAt == 0 && existing.hits.Load() == 0 {
					return existing.value, true
				}
				break
			}
		}
		idx = (idx + 1) & mask
	}

	var cf ValueCloneFunc
	if !m.noClone {
		cf = m.cloneValue
	}
	return s.loadOrStoreDeferred(hash, key, value, EntryOptions{}, cf)
}

// LoadOrStoreWithOptions returns the existing live value for key or stores value with EntryOptions.
//
// If the current entry is expired or already exhausted, it is treated as absent and a
// replacement may be installed using the supplied options.
func (m *Map) LoadOrStoreWithOptions(key Key, value any, options EntryOptions) (any, bool) {
	hash := hashKey(m.seed, key)
	s := m.pickShard(hash)
	var cf ValueCloneFunc
	if !m.noClone {
		cf = m.cloneValue
	}
	return s.loadOrStoreDeferred(hash, key, value, options, cf)
}

// Swap atomically replaces the value for key and returns the previous live value when present.
//
// The replacement entry uses the default behavior of no TTL and no hit limit. If the
// key was absent, Swap stores the new value and returns nil, false.
func (m *Map) Swap(key Key, value any) (any, bool) {
	return m.SwapWithOptions(key, value, EntryOptions{})
}

// SwapWithOptions atomically replaces the value for key and applies EntryOptions to the replacement.
//
// When a live value exists, SwapWithOptions returns that previous value with true.
// When the key is absent, it installs the replacement and returns nil, false.
func (m *Map) SwapWithOptions(key Key, value any, options EntryOptions) (any, bool) {
	hash := hashKey(m.seed, key)
	boxed := m.newValueBox(value, options)
	return m.pickShard(hash).swap(hash, key, boxed)
}

// CompareAndSwap replaces the current value only when it matches oldValue.
//
// Value comparison uses the package's equality rules, including MapEqualer support
// and built-in handling for several slice and map forms. The replacement uses the
// default entry behavior with no TTL and no hit limit.
func (m *Map) CompareAndSwap(key Key, oldValue any, newValue any) bool {
	return m.CompareAndSwapWithOptions(key, oldValue, newValue, EntryOptions{})
}

// CompareAndSwapWithOptions replaces the current value only when it matches oldValue and applies EntryOptions to the replacement.
//
// It returns true only when the key currently resolves to a live value and that value
// compares equal to oldValue under the package's equality rules.
func (m *Map) CompareAndSwapWithOptions(key Key, oldValue any, newValue any, options EntryOptions) bool {
	hash := hashKey(m.seed, key)
	boxed := m.newValueBox(newValue, options)
	return m.pickShard(hash).compareAndSwap(hash, key, oldValue, boxed)
}

// CompareAndDelete removes key only when the current value matches oldValue.
//
// It returns true when the key exists, resolves to a live value, and that value
// compares equal to oldValue under the package's equality rules.
func (m *Map) CompareAndDelete(key Key, oldValue any) bool {
	hash := hashKey(m.seed, key)
	return m.pickShard(hash).compareAndDelete(hash, key, oldValue)
}

// Len returns the current number of live entries.
//
// Len excludes deleted entries, expired entries, and hit-limited entries that have
// already been exhausted.
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
// Clear keeps the map fully usable for future operations. It does not stop the
// background cleaner and does not change the map's construction options.
func (m *Map) Clear() {
	for index := range m.shards {
		m.shards[index].clear()
	}
}

// CleanupNow forces an immediate maintenance pass across all shards.
//
// CleanupNow removes expired entries, removes exhausted hit-limited entries, and may
// rebuild shard tables to reclaim space or reduce tombstones.
func (m *Map) CleanupNow() {
	now := time.Now().UnixNano()
	for index := range m.shards {
		m.shards[index].cleanup(now)
	}
}

// Range visits the current live entries in the map.
//
// Range does not consume hits. Iteration order is unspecified. When concurrent writes
// occur, Range provides an eventually consistent traversal rather than a locked
// snapshot. Returning false from visitor stops iteration early.
func (m *Map) Range(visitor func(key Key, value any) bool) {
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
// Snapshot is built from Range and therefore has the same eventual-consistency
// behavior under concurrent writes. The returned slice is suitable for sorting,
// copying, or further offline processing.
func (m *Map) Snapshot() []Pair {
	pairs := make([]Pair, 0, m.Len())
	m.Range(func(key Key, value any) bool {
		pairs = append(pairs, Pair{Key: key, Value: value})
		return true
	})
	return pairs
}

// Stats returns a point-in-time snapshot of aggregated operational metrics.
//
// The returned values are useful for observability, sizing validation, benchmarking,
// and debugging. Because the map may be changing concurrently, the snapshot is
// approximate rather than transactional.
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

func (s *shard) load(hash uint64, key Key) (any, bool) {
	currentTable := s.table.Load()
	current := findEntry(currentTable, hash, key)
	if current == nil {
		return nil, false
	}
	return s.readEntry(current, true)
}

func (s *shard) peek(hash uint64, key Key) (any, bool) {
	currentTable := s.table.Load()
	current := findEntry(currentTable, hash, key)
	if current == nil {
		return nil, false
	}
	return s.readEntry(current, false)
}

func (s *shard) readEntry(current *entry, consumeHits bool) (any, bool) {
	boxed := current.value.Load()
	if boxed == nil {
		return nil, false
	}

	if boxed.expiresAt == 0 {
		hits := boxed.hits.Load()
		if hits == 0 {
			return boxed.value, true
		}
		if hits > 0 && consumeHits {
			return s.readEntryConsumeHit(current, boxed)
		}
		if hits < 0 {
			if s.clearIfMatch(current, boxed, false, true) {
				s.maybeResize()
			}
			return nil, false
		}
		return boxed.value, true
	}

	return s.readEntrySlow(current, consumeHits)
}

func (s *shard) readEntryConsumeHit(current *entry, boxed *valueBox) (any, bool) {
	for {
		consumed, exhausted := boxed.consumeHit()
		if !consumed {
			if s.clearIfMatch(current, boxed, false, true) {
				s.maybeResize()
				return nil, false
			}
			boxed = current.value.Load()
			if boxed == nil {
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

func (s *shard) readEntrySlow(current *entry, consumeHits bool) (any, bool) {
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

func (s *shard) store(hash uint64, key Key, boxed *valueBox) {
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

			if current.hash == hash && keysEqual(current.key, key) {
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

func (s *shard) loadOrStore(hash uint64, key Key, boxed *valueBox) (any, bool) {
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

			if current.hash == hash && keysEqual(current.key, key) {
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
func (s *shard) loadOrStoreDeferred(hash uint64, key Key, value any, options EntryOptions, cloneFunc ValueCloneFunc) (any, bool) {
	currentTable := s.table.Load()
	idx := int(hash & currentTable.mask)
	fp := fingerprint(hash)
	emptyIdx := -1
	mask := int(currentTable.mask)
	for probes := 0; probes <= mask; probes++ {
		c := currentTable.ctrl[idx]
		if c == 0 {
			emptyIdx = idx
			break
		}
		if c == fp {
			current := currentTable.slots[idx].entry.Load()
			if current != nil && current.hash == hash && keysEqual(current.key, key) {
				existing := current.value.Load()
				if existing != nil && existing.expiresAt == 0 && existing.hits.Load() == 0 {
					return existing.value, true
				}
				break
			}
		}
		idx = (idx + 1) & mask
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
		bundle.ent.key = cloneKey(key)
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
			currentTable.ctrl[emptyIdx] = fp
			if s.table.Load() == currentTable {
				s.live.Add(1)
				used := s.used.Add(1)
				s.keyBytes.Add(keySize(key))
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

func (s *shard) loadOrStoreSlow(hash uint64, key Key, value any, options EntryOptions, cloneFunc ValueCloneFunc) (any, bool) {
outer:
	for {
		s.beginWrite()
		currentTable := s.table.Load()
		index := int(hash & currentTable.mask)
		fp := fingerprint(hash)

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
				bundle.ent.key = cloneKey(key)
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
					currentTable.ctrl[index] = fp
					s.live.Add(1)
					used := s.used.Add(1)
					s.keyBytes.Add(keySize(key))
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

			if current.hash == hash && keysEqual(current.key, key) {
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

func (s *shard) swap(hash uint64, key Key, boxed *valueBox) (any, bool) {
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

			if current.hash == hash && keysEqual(current.key, key) {
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

func (s *shard) delete(hash uint64, key Key) (any, bool) {
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
				s.keyBytes.Add(-keySize(current.key))
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

func (s *shard) compareAndSwap(hash uint64, key Key, oldValue any, boxed *valueBox) bool {
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

func (s *shard) compareAndDelete(hash uint64, key Key, oldValue any) bool {
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
				s.keyBytes.Add(-keySize(current.key))
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

func (s *shard) insertFresh(currentTable *table, index int, hash uint64, key Key, boxed *valueBox) (bool, bool, bool) {
	fresh := &entry{
		hash: hash,
		key:  cloneKey(key),
	}
	fresh.value.Store(boxed)
	if currentTable.slots[index].entry.CompareAndSwap(nil, fresh) {
		currentTable.ctrl[index] = fingerprint(hash)
		s.live.Add(1)
		used := s.used.Add(1)
		s.keyBytes.Add(keySize(key))
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
				s.keyBytes.Add(keySize(current.key))
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
				s.keyBytes.Add(keySize(current.key))
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
		s.keyBytes.Add(-keySize(current.key))
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
		keyBytes += keySize(current.key)
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
			t.ctrl[index] = fingerprint(current.hash)
			return
		}
		index = (index + 1) & int(t.mask)
	}
}

func findEntry(currentTable *table, hash uint64, key Key) *entry {
	index := int(hash & currentTable.mask)
	fp := fingerprint(hash)
	ctrl := currentTable.ctrl
	slots := currentTable.slots
	mask := int(currentTable.mask)
	for probes := 0; probes <= mask; probes++ {
		c := ctrl[index]
		if c == 0 {
			return nil
		}
		if c == fp {
			current := slots[index].entry.Load()
			if current != nil && current.hash == hash && keysEqual(current.key, key) {
				return current
			}
		}
		index = (index + 1) & mask
	}
	return nil
}

func fingerprint(hash uint64) byte {
	fp := byte(hash>>56) | 1
	return fp
}

func keysEqual(a, b Key) bool {
	if a.isInt != b.isInt {
		return false
	}
	if a.isInt {
		return a.i == b.i
	}
	if len(a.s) != len(b.s) {
		return false
	}
	if len(a.s) == 0 {
		return true
	}
	ap := unsafe.StringData(a.s)
	bp := unsafe.StringData(b.s)
	if ap == bp {
		return true
	}
	return a.s == b.s
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
		ctrl:   make([]byte, slotCount),
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
