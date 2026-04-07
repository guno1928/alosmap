package alosmap

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type TestCase struct {
	ID   int
	Name string
	Fn   func() error
}

type exampleUser struct {
	Name string
	Age  int
}

type cloneableUser struct {
	Name   string
	Labels []string
}

func (u cloneableUser) CloneForMap() any {
	cloned := cloneableUser{
		Name:   u.Name,
		Labels: make([]string, len(u.Labels)),
	}
	copy(cloned.Labels, u.Labels)
	return cloned
}

type atomicStats struct {
	Count   atomic.Int64
	Bytes   atomic.Int64
	Blocked atomic.Int64
	Name    atomic.Value
}

type sizedCloneableUser struct {
	Name   string
	Labels []string
}

func (u sizedCloneableUser) CloneForMapWithSize() (any, int64) {
	cloned := sizedCloneableUser{
		Name:   u.Name,
		Labels: append([]string(nil), u.Labels...),
	}
	return cloned, int64(len(cloned.Name) + len(cloned.Labels)*8)
}

type equalerVersion struct {
	Generation int
	Label      string
}

func (v equalerVersion) EqualForMap(other any) bool {
	candidate, ok := other.(equalerVersion)
	return ok && candidate.Generation == v.Generation
}

type mutexStats struct {
	mu   sync.Mutex
	Tags []string
	Hits int
}

func cloneFromInterfaces(value any) (any, int64) {
	if sized, ok := value.(SizedMapCloner); ok {
		return sized.CloneForMapWithSize()
	}
	if plain, ok := value.(MapCloner); ok {
		return plain.CloneForMap(), 0
	}
	return value, 0
}

func isPowerOfTwo(value int) bool {
	return value > 0 && value&(value-1) == 0
}

func countRangeEntries(m *Map) int {
	count := 0
	m.Range(func(_ Key, _ any) bool {
		count++
		return true
	})
	return count
}

func assertStableViews(label string, m *Map) error {
	length := m.Len()
	rangeCount := countRangeEntries(m)
	snapshotCount := len(m.Snapshot())
	stats := m.Stats()

	if length != rangeCount || length != snapshotCount {
		return fmt.Errorf("%s views diverged: Len=%d Range=%d Snapshot=%d", label, length, rangeCount, snapshotCount)
	}
	if stats.LiveEntries != int64(length) {
		return fmt.Errorf("%s Stats.LiveEntries=%d Len=%d", label, stats.LiveEntries, length)
	}
	if stats.UsedSlots < stats.LiveEntries {
		return fmt.Errorf("%s Stats.UsedSlots=%d LiveEntries=%d", label, stats.UsedSlots, stats.LiveEntries)
	}
	return nil
}

func assertLiveIntEntries(label string, m *Map, keyKind string) error {
	snapshot := m.Snapshot()
	seen := make(map[Key]struct{}, len(snapshot))
	for _, pair := range snapshot {
		if _, exists := seen[pair.Key]; exists {
			return fmt.Errorf("%s duplicate key in snapshot: %v", label, pair.Key)
		}
		seen[pair.Key] = struct{}{}

		switch keyKind {
		case "string":
			if !pair.Key.IsString() {
				return fmt.Errorf("%s key %v is not a string key", label, pair.Key)
			}
		case "int64":
			if !pair.Key.IsInt64() {
				return fmt.Errorf("%s key %v is not an int64 key", label, pair.Key)
			}
		}

		if _, ok := pair.Value.(int); !ok {
			return fmt.Errorf("%s value for %v has type %T, want int", label, pair.Key, pair.Value)
		}
	}
	return nil
}

func testMap(options ...Option) *Map {
	base := []Option{
		WithCapacity(256),
		WithShardCount(8),
		WithCleanupInterval(0),
	}
	base = append(base, options...)
	return New(base...)
}

func testKey(index int) string {
	return fmt.Sprintf("key-%d", index)
}

func waitUntil(deadline time.Duration, condition func() bool) bool {
	expires := time.Now().Add(deadline)
	for time.Now().Before(expires) {
		if condition() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return condition()
}

func xorshift(seed uint64) uint64 {
	if seed == 0 {
		seed = 1
	}
	seed ^= seed << 13
	seed ^= seed >> 7
	seed ^= seed << 17
	return seed
}

func runCase001StoreLoadString() error {
	m := testMap()
	defer m.Close()
	m.Store(S("name"), "alos")
	value, ok := m.Load(S("name"))
	if !ok || value != "alos" {
		return fmt.Errorf("Load(name) = %v, %v", value, ok)
	}
	return nil
}

func runCase002StoreLoadInt() error {
	m := testMap()
	defer m.Close()
	m.Store(S("count"), 42)
	value, ok := m.Load(S("count"))
	if !ok || value != 42 {
		return fmt.Errorf("Load(count) = %v, %v", value, ok)
	}
	return nil
}

func runCase003StoreLoadStruct() error {
	m := testMap()
	defer m.Close()
	expected := exampleUser{Name: "Alyx", Age: 29}
	m.Store(S("user"), expected)
	value, ok := m.Load(S("user"))
	if !ok || value != expected {
		return fmt.Errorf("Load(user) = %v, %v", value, ok)
	}
	return nil
}

func runCase004StorePointerLoadSamePointer() error {
	m := testMap()
	defer m.Close()
	stats := &atomicStats{}
	m.Store(S("ip:1"), stats)
	value, ok := m.Load(S("ip:1"))
	if !ok {
		return fmt.Errorf("Load(ip:1) missing")
	}
	loaded := value.(*atomicStats)
	if loaded != stats {
		return fmt.Errorf("pointer mismatch: stored=%p loaded=%p", stats, loaded)
	}
	return nil
}

func runCase005StoreByteSliceDirectReference() error {
	m := testMap()
	defer m.Close()
	payload := []byte("alpha")
	m.Store(S("blob"), payload)
	payload[0] = 'o'
	value, ok := m.Load(S("blob"))
	if !ok || string(value.([]byte)) != "olpha" {
		return fmt.Errorf("Load(blob) = %v; want olpha (direct reference)", value)
	}
	return nil
}

func runCase006StoreStringSliceDirectReference() error {
	m := testMap()
	defer m.Close()
	payload := []string{"one", "two"}
	m.Store(S("slice"), payload)
	payload[0] = "changed"
	value, ok := m.Load(S("slice"))
	if !ok {
		return fmt.Errorf("Load(slice) missing")
	}
	stored := value.([]string)
	if len(stored) != 2 || stored[0] != "changed" || stored[1] != "two" {
		return fmt.Errorf("stored slice = %#v; want [changed two]", stored)
	}
	return nil
}

func runCase007KeyCloneOnInsert() error {
	checkStoredKeyRoundTrip := func(label string, expected any, write func(m *Map, key string) error) error {
		m := testMap()
		defer m.Close()

		large := string(make([]byte, 256)) + label
		key := large[len(large)-len(label):]

		if err := write(m, key); err != nil {
			return err
		}
		large = ""

		value, ok := m.Load(S(key))
		if !ok || !valuesEqual(value, expected) {
			return fmt.Errorf("Load(%s) = %v, %v", key, value, ok)
		}

		snapshot := m.Snapshot()
		if len(snapshot) != 1 {
			return fmt.Errorf("%s snapshot length = %d", label, len(snapshot))
		}
		if snapshot[0].Key != S(key) {
			return fmt.Errorf("%s snapshot key = %v, want %v", label, snapshot[0].Key, S(key))
		}
		return nil
	}

	if err := checkStoredKeyRoundTrip("store", "value", func(m *Map, key string) error {
		m.Store(S(key), "value")
		return nil
	}); err != nil {
		return err
	}

	if err := checkStoredKeyRoundTrip("loadorstore", "value", func(m *Map, key string) error {
		value, loaded := m.LoadOrStore(S(key), "value")
		if loaded || value != "value" {
			return fmt.Errorf("LoadOrStore(%s) = %v, %v", key, value, loaded)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := checkStoredKeyRoundTrip("swap", "value", func(m *Map, key string) error {
		previous, loaded := m.Swap(S(key), "value")
		if loaded || previous != nil {
			return fmt.Errorf("Swap(%s) = %v, %v", key, previous, loaded)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func runCase008LoadOrStoreMissingStores() error {
	m := testMap()
	defer m.Close()
	value, loaded := m.LoadOrStore(S("shared"), 7)
	if loaded || value != 7 {
		return fmt.Errorf("LoadOrStore(shared) = %v, %v", value, loaded)
	}
	return nil
}

func runCase009LoadOrStoreExistingReturns() error {
	m := testMap()
	defer m.Close()
	m.Store(S("shared"), 7)
	value, loaded := m.LoadOrStore(S("shared"), 9)
	if !loaded || value != 7 {
		return fmt.Errorf("LoadOrStore(existing) = %v, %v", value, loaded)
	}
	return nil
}

func runCase010SwapMissingStores() error {
	m := testMap()
	defer m.Close()
	previous, loaded := m.Swap(S("user"), "first")
	if loaded || previous != nil {
		return fmt.Errorf("Swap(user) = %v, %v", previous, loaded)
	}
	return nil
}

func runCase011SwapExistingReturnsPrevious() error {
	m := testMap()
	defer m.Close()
	m.Store(S("user"), "first")
	previous, loaded := m.Swap(S("user"), "second")
	if !loaded || previous != "first" {
		return fmt.Errorf("Swap(user) = %v, %v", previous, loaded)
	}
	value, ok := m.Load(S("user"))
	if !ok || value != "second" {
		return fmt.Errorf("Load(user) = %v, %v", value, ok)
	}
	return nil
}

func runCase012DeleteExistingReturnsPrevious() error {
	m := testMap()
	defer m.Close()
	m.Store(S("user"), "first")
	previous, ok := m.Delete(S("user"))
	if !ok || previous != "first" {
		return fmt.Errorf("Delete(user) = %v, %v", previous, ok)
	}
	return nil
}

func runCase013DeleteMissingReturnsFalse() error {
	m := testMap()
	defer m.Close()
	previous, ok := m.Delete(S("missing"))
	if ok || previous != nil {
		return fmt.Errorf("Delete(missing) = %v, %v", previous, ok)
	}
	return nil
}

func runCase014CompareAndSwapSuccess() error {
	m := testMap()
	defer m.Close()
	m.Store(S("payload"), []int{1, 2, 3})
	if !m.CompareAndSwap(S("payload"), []int{1, 2, 3}, []int{3, 2, 1}) {
		return fmt.Errorf("CompareAndSwap(payload) = false")
	}
	value, ok := m.Load(S("payload"))
	if !ok {
		return fmt.Errorf("Load(payload) missing")
	}
	stored := value.([]int)
	if len(stored) != 3 || stored[0] != 3 || stored[2] != 1 {
		return fmt.Errorf("stored payload = %#v", stored)
	}
	return nil
}

func runCase015CompareAndSwapFailure() error {
	m := testMap()
	defer m.Close()
	m.Store(S("payload"), []int{1, 2, 3})
	if m.CompareAndSwap(S("payload"), []int{9, 9, 9}, []int{3, 2, 1}) {
		return fmt.Errorf("CompareAndSwap(payload) unexpectedly succeeded")
	}
	return nil
}

func runCase016CompareAndDeleteSuccess() error {
	m := testMap()
	defer m.Close()
	m.Store(S("payload"), []int{1, 2, 3})
	if !m.CompareAndDelete(S("payload"), []int{1, 2, 3}) {
		return fmt.Errorf("CompareAndDelete(payload) = false")
	}
	if _, ok := m.Load(S("payload")); ok {
		return fmt.Errorf("payload still present after CompareAndDelete")
	}
	return nil
}

func runCase017CompareAndDeleteFailure() error {
	m := testMap()
	defer m.Close()
	m.Store(S("payload"), []int{1, 2, 3})
	if m.CompareAndDelete(S("payload"), []int{0, 0, 0}) {
		return fmt.Errorf("CompareAndDelete(payload) unexpectedly succeeded")
	}
	return nil
}

func runCase018CompareAndSwapNilOld() error {
	m := testMap()
	defer m.Close()
	m.Store(S("val"), nil)
	if !m.CompareAndSwap(S("val"), nil, "new") {
		return fmt.Errorf("CompareAndSwap(nil  new) = false")
	}
	v, ok := m.Load(S("val"))
	if !ok || v != "new" {
		return fmt.Errorf("Load(val) = %v, %v", v, ok)
	}
	return nil
}

func runCase019CompareAndSwapPointers() error {
	m := testMap()
	defer m.Close()
	p1 := &atomicStats{}
	p2 := &atomicStats{}
	m.Store(S("s"), p1)
	if !m.CompareAndSwap(S("s"), p1, p2) {
		return fmt.Errorf("CompareAndSwap(p1  p2) = false")
	}
	v, _ := m.Load(S("s"))
	if v.(*atomicStats) != p2 {
		return fmt.Errorf("pointer mismatch after CAS")
	}
	return nil
}

func runCase020SwapReplacesPointerOldStillUsable() error {
	m := testMap()
	defer m.Close()
	old := &atomicStats{}
	old.Count.Store(42)
	m.Store(S("s"), old)
	newS := &atomicStats{}
	prev, loaded := m.Swap(S("s"), newS)
	if !loaded {
		return fmt.Errorf("Swap not loaded")
	}
	if prev.(*atomicStats).Count.Load() != 42 {
		return fmt.Errorf("old pointer count = %d", prev.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase021StoreOverwritesExisting() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), "first")
	m.Store(S("k"), "second")
	v, ok := m.Load(S("k"))
	if !ok || v != "second" {
		return fmt.Errorf("Load(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase022StoreNilValue() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), nil)
	v, ok := m.Load(S("k"))
	if !ok || v != nil {
		return fmt.Errorf("Load(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase023DeleteReturnsPointer() error {
	m := testMap()
	defer m.Close()
	p := &atomicStats{}
	p.Count.Store(99)
	m.Store(S("s"), p)
	prev, ok := m.Delete(S("s"))
	if !ok || prev.(*atomicStats).Count.Load() != 99 {
		return fmt.Errorf("Delete(s) = %v, %v", prev, ok)
	}
	return nil
}

func runCase024MultipleStoresOnlyLatestVisible() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.Store(S("k"), i)
	}
	v, ok := m.Load(S("k"))
	if !ok || v != 9 {
		return fmt.Errorf("Load(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase025StoreEmptyStringKey() error {
	m := testMap()
	defer m.Close()
	m.Store(S(""), "empty-key")
	v, ok := m.Load(S(""))
	if !ok || v != "empty-key" {
		return fmt.Errorf("Load('') = %v, %v", v, ok)
	}
	return nil
}

func runCase026HasDoesNotConsumeHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("ticket"), "open", 2)
	if !m.Has(S("ticket")) || !m.Has(S("ticket")) {
		return fmt.Errorf("Has(ticket) = false")
	}
	if _, ok := m.Load(S("ticket")); !ok {
		return fmt.Errorf("ticket missing after Has checks")
	}
	return nil
}

func runCase027PeekDoesNotConsumeHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("ticket"), "open", 2)
	if _, ok := m.Peek(S("ticket")); !ok {
		return fmt.Errorf("Peek(ticket) missing")
	}
	if _, ok := m.Peek(S("ticket")); !ok {
		return fmt.Errorf("Peek(ticket) missing on second call")
	}
	if _, ok := m.Load(S("ticket")); !ok {
		return fmt.Errorf("ticket missing after Peek checks")
	}
	return nil
}

func runCase028HasMissingKeyReturnsFalse() error {
	m := testMap()
	defer m.Close()
	if m.Has(S("missing")) {
		return fmt.Errorf("Has(missing) = true")
	}
	return nil
}

func runCase029PeekMissingKeyReturnsFalse() error {
	m := testMap()
	defer m.Close()
	v, ok := m.Peek(S("missing"))
	if ok || v != nil {
		return fmt.Errorf("Peek(missing) = %v, %v", v, ok)
	}
	return nil
}

func runCase030GetReturnsValue() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), "present")
	v, ok := m.Get(S("k"))
	if !ok || v != "present" {
		return fmt.Errorf("Get(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase031GetMissingReturnsFalse() error {
	m := testMap()
	defer m.Close()
	v, ok := m.Get(S("k"))
	if ok || v != nil {
		return fmt.Errorf("Get(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase032GetConsumesHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("ticket"), "open", 1)
	if v, ok := m.Get(S("ticket")); !ok || v != "open" {
		return fmt.Errorf("Get(ticket) = %v, %v", v, ok)
	}
	if v, ok := m.Get(S("ticket")); ok || v != nil {
		return fmt.Errorf("Get(ticket) after exhaust = %v, %v", v, ok)
	}
	return nil
}

func runCase033PeekReturnsCorrectValue() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), 42)
	v, ok := m.Peek(S("k"))
	if !ok || v != 42 {
		return fmt.Errorf("Peek(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase034HasAfterDeleteReturnsFalse() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), 1)
	m.Delete(S("k"))
	if m.Has(S("k")) {
		return fmt.Errorf("Has(k) after delete = true")
	}
	return nil
}

func runCase035PeekAfterStoreReturnsValue() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), "hello")
	v, ok := m.Peek(S("k"))
	if !ok || v != "hello" {
		return fmt.Errorf("Peek(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase036TTLBeforeExpiryStillPresent() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits(S("session"), "live", 50*time.Millisecond, 4)
	time.Sleep(10 * time.Millisecond)
	if value, ok := m.Load(S("session")); !ok || value != "live" {
		return fmt.Errorf("Load(session) = %v, %v", value, ok)
	}
	return nil
}

func runCase037TTLExpiryOnLoad() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("session"), "live", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, ok := m.Load(S("session")); ok {
		return fmt.Errorf("session still present after TTL expiry")
	}
	return nil
}

func runCase038CleanupNowRemovesExpired() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("session"), "live", 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	m.CleanupNow()
	if _, ok := m.Peek(S("session")); ok {
		return fmt.Errorf("session still present after CleanupNow")
	}
	return nil
}

func runCase039BackgroundCleanupRemovesExpired() error {
	m := New(WithCapacity(64), WithShardCount(4), WithCleanupInterval(10*time.Millisecond))
	defer m.Close()
	m.StoreWithTTL(S("session"), "live", 5*time.Millisecond)
	if !waitUntil(100*time.Millisecond, func() bool {
		_, ok := m.Peek(S("session"))
		return !ok
	}) {
		return fmt.Errorf("background cleanup did not remove expired entry")
	}
	return nil
}

func runCase040TTLPeekBeforeExpiry() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("s"), "alive", 50*time.Millisecond)
	v, ok := m.Peek(S("s"))
	if !ok || v != "alive" {
		return fmt.Errorf("Peek(s) = %v, %v", v, ok)
	}
	return nil
}

func runCase041TTLPeekAfterExpiry() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("s"), "alive", 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Peek(S("s")); ok {
		return fmt.Errorf("Peek(s) should be expired")
	}
	return nil
}

func runCase042ShortTTLExpiresQuickly() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("s"), "x", 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if _, ok := m.Load(S("s")); ok {
		return fmt.Errorf("1ms TTL entry still present after 5ms")
	}
	return nil
}

func runCase043StoreOverwriteResetsTTL() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("s"), "v1", 5*time.Millisecond)
	m.StoreWithTTL(S("s"), "v2", time.Minute)
	time.Sleep(10 * time.Millisecond)
	v, ok := m.Load(S("s"))
	if !ok || v != "v2" {
		return fmt.Errorf("Load(s) = %v, %v; want v2 (new TTL)", v, ok)
	}
	return nil
}

func runCase044TTLExpiresBeforeHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits(S("s"), "v", 5*time.Millisecond, 100)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Load(S("s")); ok {
		return fmt.Errorf("TTL should have expired before hits exhausted")
	}
	return nil
}

func runCase045HitsExhaustBeforeTTL() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits(S("s"), "v", time.Minute, 2)
	m.Load(S("s"))
	m.Load(S("s"))
	if _, ok := m.Load(S("s")); ok {
		return fmt.Errorf("hits should have exhausted before TTL")
	}
	return nil
}

func runCase046ExpiredInvisibleToRange() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.StoreWithTTL(S("b"), 2, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	count := 0
	m.Range(func(_ Key, _ any) bool { count++; return true })
	if count != 1 {
		return fmt.Errorf("Range count = %d, want 1", count)
	}
	return nil
}

func runCase047ExpiredInvisibleToSnapshot() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.StoreWithTTL(S("b"), 2, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].Key != S("a") {
		return fmt.Errorf("Snapshot = %#v", snap)
	}
	return nil
}

func runCase048ExpiredInvisibleToHas() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("s"), "v", 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if m.Has(S("s")) {
		return fmt.Errorf("Has(s) should be false after TTL")
	}
	return nil
}

func runCase049CleanupNowOnEmptyMap() error {
	m := testMap()
	defer m.Close()
	m.CleanupNow()
	return nil
}

func runCase050MultipleTTLEntries() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL(S("short"), 1, 5*time.Millisecond)
	m.StoreWithTTL(S("long"), 2, time.Minute)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Load(S("short")); ok {
		return fmt.Errorf("short TTL still present")
	}
	if v, ok := m.Load(S("long")); !ok || v != 2 {
		return fmt.Errorf("long TTL missing or wrong: %v, %v", v, ok)
	}
	return nil
}

func runCase051HitsOneReturnsOnceThenGone() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("once"), 99, 1)
	if v, ok := m.Load(S("once")); !ok || v != 99 {
		return fmt.Errorf("Load(once) = %v, %v", v, ok)
	}
	if _, ok := m.Load(S("once")); ok {
		return fmt.Errorf("once still present after single hit")
	}
	return nil
}

func runCase052LoadConsumesHitsUntilDead() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits(S("ticket"), "open", time.Minute, 3)
	for i := 0; i < 3; i++ {
		if v, ok := m.Load(S("ticket")); !ok || v != "open" {
			return fmt.Errorf("Load(ticket) #%d = %v, %v", i+1, v, ok)
		}
	}
	if _, ok := m.Load(S("ticket")); ok {
		return fmt.Errorf("ticket still present after 3 hits")
	}
	return nil
}

func runCase053ZeroHitsImmediatelyDead() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("zero"), "val", 0)
	if _, ok := m.Load(S("zero")); !ok {
		return fmt.Errorf("zero-hit entry should be loadable (normalized to unlimited)")
	}
	return nil
}

func runCase054NegativeHitsUnlimited() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("inf"), "val", -1)
	for i := 0; i < 100; i++ {
		if _, ok := m.Load(S("inf")); !ok {
			return fmt.Errorf("unlimited-hit entry missing at read %d", i)
		}
	}
	return nil
}

func runCase055TTLAndHitsPreservedTogether() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits(S("combo"), "val", time.Minute, 5)
	for i := 0; i < 4; i++ {
		if _, ok := m.Load(S("combo")); !ok {
			return fmt.Errorf("combo missing at read %d", i)
		}
	}
	if _, ok := m.Load(S("combo")); !ok {
		return fmt.Errorf("combo should still have 1 hit left")
	}
	if _, ok := m.Load(S("combo")); ok {
		return fmt.Errorf("combo should be dead after 5 hits")
	}
	return nil
}

func runCase056HitExhaustedInvisibleToRange() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.StoreWithHits(S("b"), 2, 1)
	m.Load(S("b"))
	count := 0
	m.Range(func(_ Key, _ any) bool { count++; return true })
	if count != 1 {
		return fmt.Errorf("Range count = %d, want 1", count)
	}
	return nil
}

func runCase057HitExhaustedInvisibleToSnapshot() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.StoreWithHits(S("b"), 2, 1)
	m.Load(S("b"))
	snap := m.Snapshot()
	if len(snap) != 1 {
		return fmt.Errorf("Snapshot length = %d, want 1", len(snap))
	}
	return nil
}

func runCase058HitExhaustedInvisibleToHas() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("b"), 2, 1)
	m.Load(S("b"))
	if m.Has(S("b")) {
		return fmt.Errorf("Has(b) should be false after hits exhausted")
	}
	return nil
}

func runCase059PeekDoesNotDecrementHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("t"), "val", 1)
	m.Peek(S("t"))
	m.Peek(S("t"))
	if v, ok := m.Load(S("t")); !ok || v != "val" {
		return fmt.Errorf("Load(t) = %v, %v; peek should not have consumed hits", v, ok)
	}
	return nil
}

func runCase060LoadThenPeekOnHitLimited() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits(S("t"), "val", 2)
	m.Load(S("t"))
	m.Peek(S("t"))
	if v, ok := m.Load(S("t")); !ok || v != "val" {
		return fmt.Errorf("Load(t) after load+peek = %v, %v", v, ok)
	}
	if _, ok := m.Load(S("t")); ok {
		return fmt.Errorf("t should be dead after 2 loads")
	}
	return nil
}

func runCase061RangeVisitsAllLive() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.Store(S("b"), 2)
	m.Store(S("c"), 3)
	sum := 0
	m.Range(func(_ Key, v any) bool { sum += v.(int); return true })
	if sum != 6 {
		return fmt.Errorf("Range sum = %d, want 6", sum)
	}
	return nil
}

func runCase062RangeEarlyStop() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.Store(S(testKey(i)), i)
	}
	count := 0
	m.Range(func(_ Key, _ any) bool { count++; return count < 3 })
	if count != 3 {
		return fmt.Errorf("Range count = %d, want 3", count)
	}
	return nil
}

func runCase063RangeSkipsExpiredAndDead() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.StoreWithTTL(S("b"), 2, 5*time.Millisecond)
	m.StoreWithHits(S("c"), 3, 1)
	_, _ = m.Load(S("c"))
	time.Sleep(10 * time.Millisecond)
	count := 0
	m.Range(func(_ Key, v any) bool { count++; return true })
	if count != 1 {
		return fmt.Errorf("Range count = %d, want 1", count)
	}
	return nil
}

func runCase064SnapshotReturnsAllLive() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.Store(S("b"), 2)
	snap := m.Snapshot()
	if len(snap) != 2 {
		return fmt.Errorf("Snapshot length = %d, want 2", len(snap))
	}
	return nil
}

func runCase065SnapshotSkipsExpiredAndDead() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.StoreWithTTL(S("b"), 2, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].Key != S("a") {
		return fmt.Errorf("Snapshot = %#v", snap)
	}
	return nil
}

func runCase066SnapshotAfterDeleteExcludes() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.Store(S("b"), 2)
	m.Delete(S("b"))
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].Key != S("a") {
		return fmt.Errorf("Snapshot = %#v", snap)
	}
	return nil
}

func runCase067RangeOnEmptyMap() error {
	m := testMap()
	defer m.Close()
	count := 0
	m.Range(func(_ Key, _ any) bool { count++; return true })
	if count != 0 {
		return fmt.Errorf("Range on empty map: count = %d", count)
	}
	return nil
}

func runCase068SnapshotOnEmptyMap() error {
	m := testMap()
	defer m.Close()
	snap := m.Snapshot()
	if len(snap) != 0 {
		return fmt.Errorf("Snapshot on empty map: length = %d", len(snap))
	}
	return nil
}

func runCase069RangeWithConcurrentModifications() error {
	m := testMap(WithCapacity(128), WithShardCount(4))
	defer m.Close()
	for i := 0; i < 50; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; i < 100; i++ {
			m.Store(S(testKey(i)), i)
		}
	}()
	count := 0
	m.Range(func(_ Key, _ any) bool { count++; return true })
	wg.Wait()
	if count < 50 {
		return fmt.Errorf("Range count = %d, want >= 50", count)
	}
	return nil
}

func runCase070SnapshotPointInTime() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.Store(S("b"), 2)
	snap := m.Snapshot()
	m.Store(S("c"), 3)
	if len(snap) != 2 {
		return fmt.Errorf("Snapshot should capture 2 entries, got %d", len(snap))
	}
	return nil
}

func runCase071ClearEmptiesMap() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.Store(S(testKey(i)), i)
	}
	m.Clear()
	if m.Len() != 0 {
		return fmt.Errorf("Len after Clear = %d", m.Len())
	}
	return nil
}

func runCase072ClearShrinksTable() error {
	m := testMap(WithCapacity(32), WithShardCount(2))
	defer m.Close()
	for i := 0; i < 128; i++ {
		m.Store(S(testKey(i)), i)
	}
	before := m.Stats().SlotCapacity
	m.Clear()
	after := m.Stats().SlotCapacity
	if after >= before {
		return fmt.Errorf("Clear did not shrink: before=%d after=%d", before, after)
	}
	return nil
}

func runCase073LenTracksStoreCount() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.Store(S(testKey(i)), i)
	}
	if m.Len() != 10 {
		return fmt.Errorf("Len = %d, want 10", m.Len())
	}
	return nil
}

func runCase074LenDecrementsOnDelete() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.Store(S("b"), 2)
	m.Delete(S("a"))
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return nil
}

func runCase075LenAfterClearIsZero() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(S(testKey(i)), i)
	}
	m.Clear()
	if m.Len() != 0 {
		return fmt.Errorf("Len after Clear = %d", m.Len())
	}
	return nil
}

func runCase076LenTracksConcurrentDistinctKeys() error {
	m := testMap(WithCapacity(4096), WithShardCount(16))
	defer m.Close()
	const workers = 8
	const perWorker = 256
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				m.Store(S(testKey(w*perWorker+i)), w*perWorker+i)
			}
		}()
	}
	wg.Wait()
	if m.Len() != workers*perWorker {
		return fmt.Errorf("Len = %d, want %d", m.Len(), workers*perWorker)
	}
	return nil
}

func runCase077LenWithHitExhausted() error {
	m := testMap()
	defer m.Close()
	m.Store(S("a"), 1)
	m.StoreWithHits(S("b"), 2, 1)
	m.Load(S("b"))
	m.CleanupNow()
	if m.Len() > 1 {
		return fmt.Errorf("Len = %d after hit exhaustion + cleanup", m.Len())
	}
	return nil
}

func runCase078ClearAfterTTLEntries() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.StoreWithTTL(S(testKey(i)), i, time.Minute)
	}
	m.Clear()
	if m.Len() != 0 {
		return fmt.Errorf("Len after Clear = %d", m.Len())
	}
	return nil
}

func runCase079LenWithMixedStoreDelete() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 20; i++ {
		m.Store(S(testKey(i)), i)
	}
	for i := 0; i < 10; i++ {
		m.Delete(S(testKey(i)))
	}
	if m.Len() != 10 {
		return fmt.Errorf("Len = %d, want 10", m.Len())
	}
	return nil
}

func runCase080ClearResetsStats() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 50; i++ {
		m.Store(S(testKey(i)), i)
	}
	m.Clear()
	stats := m.Stats()
	if stats.LiveEntries != 0 {
		return fmt.Errorf("LiveEntries after Clear = %d", stats.LiveEntries)
	}
	return nil
}

func runCase081StatsLiveEntriesMatchesLen() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 20; i++ {
		m.Store(S(testKey(i)), i)
	}
	stats := m.Stats()
	if stats.LiveEntries != int64(m.Len()) {
		return fmt.Errorf("LiveEntries=%d Len=%d", stats.LiveEntries, m.Len())
	}
	return nil
}

func runCase082StatsTrackKeyBytes() error {
	m := testMap()
	defer m.Close()
	m.Store(S("alpha"), []byte("abcdef"))
	stats := m.Stats()
	if stats.TrackedKeyBytes < int64(len("alpha")) {
		return fmt.Errorf("TrackedKeyBytes = %d", stats.TrackedKeyBytes)
	}
	return nil
}

func runCase083StatsValueBytesZeroNoClone() error {
	m := testMap()
	defer m.Close()
	m.Store(S("alpha"), []byte("abcdef"))
	stats := m.Stats()
	if stats.TrackedValueBytes != 0 {
		return fmt.Errorf("TrackedValueBytes = %d, want 0", stats.TrackedValueBytes)
	}
	return nil
}

func runCase084MemoryScaleDownAfterDeleteCleanup() error {
	m := testMap(WithCapacity(2048), WithShardCount(8))
	defer m.Close()
	data := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		m.Store(S(testKey(i)), data)
	}
	before := m.Stats()
	for i := 0; i < 1000; i++ {
		m.Delete(S(testKey(i)))
	}
	m.CleanupNow()
	after := m.Stats()
	if after.EstimatedResidentBytes >= before.EstimatedResidentBytes {
		return fmt.Errorf("resident bytes did not shrink: before=%d after=%d", before.EstimatedResidentBytes, after.EstimatedResidentBytes)
	}
	return nil
}

func runCase085StatsSlotCapacityGrows() error {
	m := testMap(WithCapacity(16), WithShardCount(2))
	defer m.Close()
	initial := m.Stats().SlotCapacity
	for i := 0; i < 256; i++ {
		m.Store(S(testKey(i)), i)
	}
	grown := m.Stats().SlotCapacity
	if grown <= initial {
		return fmt.Errorf("SlotCapacity did not grow: initial=%d grown=%d", initial, grown)
	}
	return nil
}

func runCase086CleanupShrinksOversizedTable() error {
	m := testMap(WithCapacity(4096), WithShardCount(4))
	defer m.Close()
	for i := 0; i < 4096; i++ {
		m.Store(S(testKey(i)), i)
	}
	large := m.Stats().SlotCapacity
	for i := 0; i < 4000; i++ {
		m.Delete(S(testKey(i)))
	}
	m.CleanupNow()
	shrunk := m.Stats().SlotCapacity
	if shrunk >= large {
		return fmt.Errorf("CleanupNow did not shrink: before=%d after=%d", large, shrunk)
	}
	return nil
}

func runCase087StatsAfterClear() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 50; i++ {
		m.Store(S(testKey(i)), i)
	}
	m.Clear()
	s := m.Stats()
	if s.LiveEntries != 0 || s.Tombstones != 0 {
		return fmt.Errorf("stats after Clear: live=%d tomb=%d", s.LiveEntries, s.Tombstones)
	}
	return nil
}

func runCase088StatsTombstonesAfterDeletes() error {
	m := testMap(WithCapacity(1024), WithShardCount(1))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(S(testKey(i)), i)
	}
	for i := 0; i < 5; i++ {
		m.Delete(S(testKey(i)))
	}
	s := m.Stats()
	if s.Tombstones < 3 {
		return fmt.Errorf("Tombstones = %d, want >= 3", s.Tombstones)
	}
	return nil
}

func runCase089EstimatedResidentBytesPositive() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(S(testKey(i)), i)
	}
	if m.Stats().EstimatedResidentBytes <= 0 {
		return fmt.Errorf("EstimatedResidentBytes <= 0")
	}
	return nil
}

func runCase090StatsShardsMatchConfig() error {
	m := New(WithCapacity(64), WithShardCount(16), WithCleanupInterval(0))
	defer m.Close()
	if m.Stats().Shards != 16 {
		return fmt.Errorf("Shards = %d, want 16", m.Stats().Shards)
	}
	return nil
}

func runCase091CloseIsIdempotent() error {
	m := New(WithCleanupInterval(10 * time.Millisecond))
	m.Store(S("before"), 1)
	m.Close()
	m.Close()
	m.Close()
	m.Store(S("after"), 2)
	if value, ok := m.Load(S("before")); !ok || value != 1 {
		return fmt.Errorf("Load(before) = %v, %v", value, ok)
	}
	if value, ok := m.Load(S("after")); !ok || value != 2 {
		return fmt.Errorf("Load(after) = %v, %v", value, ok)
	}
	if m.Len() != 2 {
		return fmt.Errorf("Len = %d, want 2", m.Len())
	}
	return assertStableViews("close idempotent", m)
}

func runCase092CloseStopsBackgroundCleanup() error {
	m := New(WithCapacity(64), WithShardCount(4), WithCleanupInterval(2*time.Millisecond))
	m.StoreWithTTL(S("s"), "v", time.Minute)
	if !waitUntil(50*time.Millisecond, func() bool {
		return m.Stats().CleanupRuns > 0
	}) {
		return fmt.Errorf("background cleanup never ran before Close")
	}
	m.Close()
	runsAfterClose := m.Stats().CleanupRuns
	time.Sleep(15 * time.Millisecond)
	if runsLater := m.Stats().CleanupRuns; runsLater != runsAfterClose {
		return fmt.Errorf("CleanupRuns changed after Close: after=%d later=%d", runsAfterClose, runsLater)
	}
	m.Store(S("after-close"), "live")
	if value, ok := m.Load(S("after-close")); !ok || value != "live" {
		return fmt.Errorf("Load(after-close) = %v, %v", value, ok)
	}
	return nil
}

func runCase093MultipleCloseNoPanic() error {
	m := testMap()
	m.Store(S("a"), 1)
	m.Close()
	m.Close()
	m.Close()
	m.Store(S("b"), 2)
	if value, ok := m.Load(S("a")); !ok || value != 1 {
		return fmt.Errorf("Load(a) = %v, %v", value, ok)
	}
	if value, ok := m.Load(S("b")); !ok || value != 2 {
		return fmt.Errorf("Load(b) = %v, %v", value, ok)
	}
	if m.Len() != 2 {
		return fmt.Errorf("Len = %d, want 2", m.Len())
	}
	return assertStableViews("multiple close no panic", m)
}

func runCase094StoreAfterCloseNoPanic() error {
	m := testMap()
	m.Close()
	m.Store(S("a"), 1)
	value, ok := m.Load(S("a"))
	if !ok || value != 1 {
		return fmt.Errorf("Load(a) = %v, %v", value, ok)
	}
	previous, ok := m.Delete(S("a"))
	if !ok || previous != 1 {
		return fmt.Errorf("Delete(a) = %v, %v", previous, ok)
	}
	if m.Len() != 0 {
		return fmt.Errorf("Len = %d, want 0", m.Len())
	}
	return assertStableViews("store after close", m)
}

func runCase095PeekAfterCloseWorks() error {
	m := testMap()
	m.Store(S("a"), 1)
	m.Close()
	v, ok := m.Peek(S("a"))
	if !ok || v != 1 {
		return fmt.Errorf("Peek(a) after Close = %v, %v", v, ok)
	}
	if !m.Has(S("a")) {
		return fmt.Errorf("Has(a) after Close = false")
	}
	previous, ok := m.Delete(S("a"))
	if !ok || previous != 1 {
		return fmt.Errorf("Delete(a) after Close = %v, %v", previous, ok)
	}
	if m.Has(S("a")) || m.Len() != 0 {
		return fmt.Errorf("a should be gone after Delete, Has=%v Len=%d", m.Has(S("a")), m.Len())
	}
	return assertStableViews("peek after close", m)
}

func runCase116PointerAtomicInt64Add() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	v, _ := m.Load(S("ip:1"))
	v.(*atomicStats).Count.Add(5)
	if s.Count.Load() != 5 {
		return fmt.Errorf("Count = %d, want 5", s.Count.Load())
	}
	return nil
}

func runCase117PointerAtomicInt64Read() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Count.Store(42)
	m.Store(S("ip:1"), s)
	v, _ := m.Load(S("ip:1"))
	if v.(*atomicStats).Count.Load() != 42 {
		return fmt.Errorf("Count = %d, want 42", v.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase118PointerAtomicValueStore() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	v, _ := m.Load(S("ip:1"))
	v.(*atomicStats).Name.Store("john")
	if s.Name.Load() != "john" {
		return fmt.Errorf("Name = %v, want john", s.Name.Load())
	}
	return nil
}

func runCase119PointerAtomicValueRead() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Name.Store("alyx")
	m.Store(S("ip:1"), s)
	v, _ := m.Load(S("ip:1"))
	if v.(*atomicStats).Name.Load() != "alyx" {
		return fmt.Errorf("Name = %v, want alyx", v.(*atomicStats).Name.Load())
	}
	return nil
}

func runCase120PointerMutexMapConcurrent() error {
	m := testMap()
	defer m.Close()
	ms := &mutexStats{Tags: []string{}}
	m.Store(S("sess"), ms)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load(S("sess"))
			s := v.(*mutexStats)
			s.mu.Lock()
			s.Tags = append(s.Tags, fmt.Sprintf("tag-%d", i))
			s.mu.Unlock()
		}()
	}
	wg.Wait()
	if len(ms.Tags) != 8 {
		return fmt.Errorf("Tags length = %d, want 8", len(ms.Tags))
	}
	return nil
}

func runCase121PointerMutexSliceConcurrent() error {
	m := testMap()
	defer m.Close()
	ms := &mutexStats{Tags: []string{}}
	m.Store(S("sess"), ms)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load(S("sess"))
			s := v.(*mutexStats)
			for j := 0; j < 100; j++ {
				s.mu.Lock()
				s.Hits++
				s.mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ms.Hits != 800 {
		return fmt.Errorf("Hits = %d, want 800", ms.Hits)
	}
	return nil
}

func runCase122PointerLoadReturnsSameAddress() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	v1, _ := m.Load(S("ip:1"))
	v2, _ := m.Load(S("ip:1"))
	if v1.(*atomicStats) != v2.(*atomicStats) {
		return fmt.Errorf("different addresses from two Loads")
	}
	return nil
}

func runCase123PointerDeleteRestore() error {
	m := testMap()
	defer m.Close()
	s1 := &atomicStats{}
	s1.Count.Store(1)
	m.Store(S("ip:1"), s1)
	m.Delete(S("ip:1"))
	s2 := &atomicStats{}
	s2.Count.Store(2)
	m.Store(S("ip:1"), s2)
	v, _ := m.Load(S("ip:1"))
	if v.(*atomicStats).Count.Load() != 2 {
		return fmt.Errorf("Count = %d, want 2", v.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase124PointerCompareAndSwapSuccess() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	s2 := &atomicStats{}
	if !m.CompareAndSwap(S("ip:1"), s, s2) {
		return fmt.Errorf("CompareAndSwap failed")
	}
	return nil
}

func runCase125PointerCompareAndSwapFailure() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	wrong := &atomicStats{}
	if m.CompareAndSwap(S("ip:1"), wrong, &atomicStats{}) {
		return fmt.Errorf("CompareAndSwap should have failed")
	}
	return nil
}

func runCase126PointerSwapOldStillValid() error {
	m := testMap()
	defer m.Close()
	old := &atomicStats{}
	old.Count.Store(100)
	m.Store(S("ip:1"), old)
	prev, _ := m.Swap(S("ip:1"), &atomicStats{})
	if prev.(*atomicStats).Count.Load() != 100 {
		return fmt.Errorf("old pointer Count = %d", prev.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase127PointerTTLExpiryMakesInaccessible() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.StoreWithTTL(S("ip:1"), s, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Load(S("ip:1")); ok {
		return fmt.Errorf("pointer should be inaccessible after TTL")
	}
	return nil
}

func runCase128PointerHitLimitedAccessible() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.StoreWithHits(S("ip:1"), s, 2)
	v1, ok1 := m.Load(S("ip:1"))
	v2, ok2 := m.Load(S("ip:1"))
	if !ok1 || !ok2 {
		return fmt.Errorf("pointer loads = %v, %v", ok1, ok2)
	}
	if v1.(*atomicStats) != s || v2.(*atomicStats) != s {
		return fmt.Errorf("pointer mismatch")
	}
	if _, ok := m.Load(S("ip:1")); ok {
		return fmt.Errorf("should be dead after 2 hits")
	}
	return nil
}

func runCase129PointerRangeCorrect() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Count.Store(77)
	m.Store(S("ip:1"), s)
	found := false
	m.Range(func(k Key, v any) bool {
		if k == S("ip:1") && v.(*atomicStats).Count.Load() == 77 {
			found = true
		}
		return true
	})
	if !found {
		return fmt.Errorf("Range did not find pointer with Count=77")
	}
	return nil
}

func runCase130PointerSnapshotCaptures() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Bytes.Store(4096)
	m.Store(S("ip:1"), s)
	snap := m.Snapshot()
	if len(snap) != 1 {
		return fmt.Errorf("Snapshot length = %d", len(snap))
	}
	if snap[0].Value.(*atomicStats).Bytes.Load() != 4096 {
		return fmt.Errorf("Snapshot pointer Bytes = %d", snap[0].Value.(*atomicStats).Bytes.Load())
	}
	return nil
}

func runCase131MultipleKeysDifferentPointerTypes() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	ms := &mutexStats{}
	m.Store(S("stats"), s)
	m.Store(S("mutex"), ms)
	v1, _ := m.Load(S("stats"))
	v2, _ := m.Load(S("mutex"))
	if _, ok := v1.(*atomicStats); !ok {
		return fmt.Errorf("stats type mismatch")
	}
	if _, ok := v2.(*mutexStats); !ok {
		return fmt.Errorf("mutex type mismatch")
	}
	return nil
}

func runCase132LargeStructPointerManyAtomics() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	v, _ := m.Load(S("ip:1"))
	p := v.(*atomicStats)
	p.Count.Add(1)
	p.Bytes.Add(4096)
	p.Blocked.Add(1)
	p.Name.Store("test")
	if p.Count.Load() != 1 || p.Bytes.Load() != 4096 || p.Blocked.Load() != 1 || p.Name.Load() != "test" {
		return fmt.Errorf("multi-field assertion failed")
	}
	return nil
}

func runCase133LoadOrStorePointerSingleInit() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	v, loaded := m.LoadOrStore(S("ip:1"), s)
	if loaded {
		return fmt.Errorf("should not be loaded on first call")
	}
	v2, loaded2 := m.LoadOrStore(S("ip:1"), &atomicStats{})
	if !loaded2 {
		return fmt.Errorf("should be loaded on second call")
	}
	if v.(*atomicStats) != s || v2.(*atomicStats) != s {
		return fmt.Errorf("pointer mismatch")
	}
	return nil
}

func runCase134InterfaceWrappingPointer() error {
	m := testMap()
	defer m.Close()
	var iface any = &atomicStats{}
	m.Store(S("ip:1"), iface)
	v, _ := m.Load(S("ip:1"))
	v.(*atomicStats).Count.Add(10)
	if iface.(*atomicStats).Count.Load() != 10 {
		return fmt.Errorf("Count = %d, want 10", iface.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase135ConcurrentLoadsSameAddress() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load(S("ip:1"))
			if !ok || v.(*atomicStats) != s {
				errs <- fmt.Errorf("pointer mismatch in goroutine")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		return e
	}
	return nil
}

func runCase136ClearMapPointerStillValidInUserCode() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Count.Store(42)
	m.Store(S("ip:1"), s)
	m.Clear()
	if s.Count.Load() != 42 {
		return fmt.Errorf("pointer Count changed after Clear")
	}
	return nil
}

func runCase137TTLExpiryPointerRemainsInUserCode() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Count.Store(99)
	m.StoreWithTTL(S("ip:1"), s, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if s.Count.Load() != 99 {
		return fmt.Errorf("pointer Count changed after TTL expiry")
	}
	return nil
}

func runCase138StoreNilPointer() error {
	m := testMap()
	defer m.Close()
	m.Store(S("ip:1"), (*atomicStats)(nil))
	v, ok := m.Load(S("ip:1"))
	if !ok {
		return fmt.Errorf("Load(ip:1) missing")
	}
	if v != (*atomicStats)(nil) {
		return fmt.Errorf("Load(ip:1) = %v, want nil pointer", v)
	}
	return nil
}

func runCase139RangePointerMutateDuringIteration() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	m.Range(func(k Key, v any) bool {
		v.(*atomicStats).Count.Add(1)
		return true
	})
	if s.Count.Load() != 1 {
		return fmt.Errorf("Count = %d, want 1", s.Count.Load())
	}
	return nil
}

func runCase140SnapshotPointerMutateAfter() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	snap := m.Snapshot()
	snap[0].Value.(*atomicStats).Count.Add(1)
	if s.Count.Load() != 1 {
		return fmt.Errorf("mutation through snapshot did not affect original")
	}
	return nil
}

func runCase141ConcurrentStoreSameKey() error {
	m := testMap(WithCapacity(64), WithShardCount(4))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.Store(S("k"), w*500+i)
			}
		}()
	}
	wg.Wait()
	value, ok := m.Load(S("k"))
	if !ok {
		return fmt.Errorf("key missing after concurrent stores")
	}
	if _, ok := value.(int); !ok {
		return fmt.Errorf("Load(k) type = %T, want int", value)
	}
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return assertStableViews("concurrent store same key", m)
}

func runCase142ConcurrentLoadSameKey() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), 42)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if v, ok := m.Load(S("k")); !ok || v != 42 {
					errs <- fmt.Errorf("Load = %v, %v", v, ok)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		return e
	}
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return assertStableViews("concurrent load same key", m)
}

func runCase143ConcurrentStoreDifferentKeys() error {
	m := testMap(WithCapacity(4096), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.Store(S(testKey(w*200+i)), i)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	for i := 0; i < 1600; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i%200 {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent store different keys", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent store different keys", m, "string")
}

func runCase144ConcurrentLoadDifferentKeys() error {
	m := testMap(WithCapacity(1024), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				value, ok := m.Load(S(testKey(i)))
				if !ok || value != i {
					errs <- fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	if err := assertStableViews("concurrent load different keys", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent load different keys", m, "string")
}

func runCase145ConcurrentMixed5050() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 128; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := uint64(1)
			for i := 0; i < 1000; i++ {
				seed = xorshift(seed)
				k := S(testKey(int(seed % 128)))
				if seed&1 == 0 {
					m.Store(k, int(seed))
				} else {
					m.Load(k)
				}
			}
		}()
	}
	wg.Wait()
	if m.Len() != 128 {
		return fmt.Errorf("Len = %d, want 128", m.Len())
	}
	for i := 0; i < 128; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok {
			return fmt.Errorf("Load(%s) missing", testKey(i))
		}
		if _, ok := value.(int); !ok {
			return fmt.Errorf("Load(%s) type = %T, want int", testKey(i), value)
		}
	}
	if err := assertStableViews("concurrent mixed 50/50", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent mixed 50/50", m, "string")
}

func runCase146ConcurrentMixedStoreLoadDelete() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 128; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := uint64(w + 1)
			for i := 0; i < 2000; i++ {
				seed = xorshift(seed)
				k := S(testKey(int(seed % 128)))
				switch seed & 3 {
				case 0:
					m.Store(k, int(seed))
				case 1:
					m.Load(k)
				case 2:
					m.LoadOrStore(k, int(seed))
				default:
					m.Delete(k)
				}
			}
		}()
	}
	wg.Wait()
	if m.Len() > 128 {
		return fmt.Errorf("Len = %d, want 0..128", m.Len())
	}
	if err := assertStableViews("concurrent mixed store load delete", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent mixed store load delete", m, "string")
}

func runCase147ConcurrentLoadOrStoreSameKey() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	var first atomic.Int64
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, loaded := m.LoadOrStore(S("k"), 1)
			if !loaded {
				first.Add(1)
			}
		}()
	}
	wg.Wait()
	if first.Load() != 1 {
		return fmt.Errorf("expected exactly 1 first writer, got %d", first.Load())
	}
	value, ok := m.Load(S("k"))
	if !ok || value != 1 {
		return fmt.Errorf("Load(k) = %v, %v", value, ok)
	}
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return assertStableViews("concurrent LoadOrStore same key", m)
}

func runCase148ConcurrentLoadOrStoreDifferentKeys() error {
	m := testMap(WithCapacity(1024), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.LoadOrStore(S(testKey(w*100+i)), i)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 800 {
		return fmt.Errorf("Len = %d, want 800", m.Len())
	}
	for i := 0; i < 800; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i%100 {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent LoadOrStore different keys", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent LoadOrStore different keys", m, "string")
}

func runCase149ConcurrentSwapSameKey() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), 0)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.Swap(S("k"), w*200+i)
			}
		}()
	}
	wg.Wait()
	value, ok := m.Load(S("k"))
	if !ok {
		return fmt.Errorf("key missing after concurrent swaps")
	}
	if _, ok := value.(int); !ok {
		return fmt.Errorf("Load(k) type = %T, want int", value)
	}
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return assertStableViews("concurrent swap same key", m)
}

func runCase150ConcurrentDeleteSameKey() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.Store(S("k"), i)
				m.Delete(S("k"))
			}
		}()
	}
	wg.Wait()
	if _, ok := m.Load(S("k")); ok {
		return fmt.Errorf("Load(k) should be absent after concurrent deletes")
	}
	if m.Len() != 0 {
		return fmt.Errorf("Len = %d, want 0", m.Len())
	}
	return assertStableViews("concurrent delete same key", m)
}

func runCase151ConcurrentCAS() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), 0)
	var wg sync.WaitGroup
	var successes atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if m.CompareAndSwap(S("k"), 0, 1) {
					successes.Add(1)
				}
				m.Store(S("k"), 0)
			}
		}()
	}
	wg.Wait()
	if successes.Load() == 0 {
		return fmt.Errorf("no CAS succeeded")
	}
	value, ok := m.Load(S("k"))
	if !ok {
		return fmt.Errorf("Load(k) missing after concurrent CAS")
	}
	intValue, ok := value.(int)
	if !ok || (intValue != 0 && intValue != 1) {
		return fmt.Errorf("Load(k) = %v, want 0 or 1", value)
	}
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return assertStableViews("concurrent CAS", m)
}

func runCase152ConcurrentStoreWithTTL() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.StoreWithTTL(S(testKey(w*200+i)), i, time.Minute)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	for i := 0; i < 1600; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i%200 {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent store with TTL", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent store with TTL", m, "string")
}

func runCase153ConcurrentStoreWithHits() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.StoreWithHits(S(testKey(w*200+i)), i, 10)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	for i := 0; i < 1600; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i%200 {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent store with hits", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent store with hits", m, "string")
}

func runCase154ConcurrentStoreWithTTLAndHits() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.StoreWithTTLAndHits(S(testKey(w*200+i)), i, time.Minute, 10)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	for i := 0; i < 1600; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i%200 {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent store with TTL and hits", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent store with TTL and hits", m, "string")
}

func runCase155ConcurrentRangeWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	rangeCount := 0
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			m.Store(S(testKey(i)), i)
		}
	}()
	go func() {
		defer wg.Done()
		localCount := 0
		m.Range(func(_ Key, _ any) bool {
			localCount++
			return true
		})
		rangeCount = localCount
	}()
	wg.Wait()
	if rangeCount < 100 || rangeCount > 200 {
		return fmt.Errorf("concurrent Range count = %d, want 100..200", rangeCount)
	}
	if m.Len() != 200 {
		return fmt.Errorf("Len = %d, want 200", m.Len())
	}
	for i := 0; i < 200; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent Range while writing", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent Range while writing", m, "string")
}

func runCase156ConcurrentSnapshotWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	var snapshot []Pair
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			m.Store(S(testKey(i)), i)
		}
	}()
	go func() {
		defer wg.Done()
		snapshot = m.Snapshot()
	}()
	wg.Wait()
	if len(snapshot) < 100 || len(snapshot) > 200 {
		return fmt.Errorf("concurrent Snapshot length = %d, want 100..200", len(snapshot))
	}
	if m.Len() != 200 {
		return fmt.Errorf("Len = %d, want 200", m.Len())
	}
	for i := 0; i < 200; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent Snapshot while writing", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent Snapshot while writing", m, "string")
}

func runCase157ConcurrentClearWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Store(S(testKey(i)), i)
		}
	}()
	go func() { defer wg.Done(); m.Clear() }()
	wg.Wait()
	if m.Len() > 200 {
		return fmt.Errorf("Len = %d, want 0..200", m.Len())
	}
	if err := assertStableViews("concurrent Clear while writing", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent Clear while writing", m, "string")
}

func runCase158ConcurrentCleanupWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.StoreWithTTL(S(testKey(i)), i, time.Minute)
		}
	}()
	go func() { defer wg.Done(); m.CleanupNow() }()
	wg.Wait()
	if m.Len() != 200 {
		return fmt.Errorf("Len = %d, want 200", m.Len())
	}
	for i := 0; i < 200; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("concurrent Cleanup while writing", m); err != nil {
		return err
	}
	return assertLiveIntEntries("concurrent Cleanup while writing", m, "string")
}

func runCase159HotSetContention() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 128; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := uint64(w + 1)
			for i := 0; i < 2000; i++ {
				seed = xorshift(seed)
				k := S(testKey(int(seed % 128)))
				switch seed & 3 {
				case 0:
					m.Store(k, int(seed))
				case 1:
					m.Load(k)
				case 2:
					m.LoadOrStore(k, int(seed))
				default:
					m.Delete(k)
				}
			}
		}()
	}
	wg.Wait()
	if m.Len() > 128 {
		return fmt.Errorf("Len = %d, want 0..128", m.Len())
	}
	if err := assertStableViews("hot set contention", m); err != nil {
		return err
	}
	return assertLiveIntEntries("hot set contention", m, "string")
}

func runCase160LargeFanOut() error {
	m := testMap(WithCapacity(8192), WithShardCount(16))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.Store(S(testKey(w*100+i)), i)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 3200 {
		return fmt.Errorf("Len = %d, want 3200", m.Len())
	}
	for i := 0; i < 3200; i++ {
		value, ok := m.Load(S(testKey(i)))
		if !ok || value != i%100 {
			return fmt.Errorf("Load(%s) = %v, %v", testKey(i), value, ok)
		}
	}
	if err := assertStableViews("large fan out", m); err != nil {
		return err
	}
	return assertLiveIntEntries("large fan out", m, "string")
}

func runCase166ConcurrentAtomicInt64Add() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load(S("ip:1"))
			for i := 0; i < 100; i++ {
				v.(*atomicStats).Count.Add(1)
			}
		}()
	}
	wg.Wait()
	if s.Count.Load() != 1600 {
		return fmt.Errorf("Count = %d, want 1600", s.Count.Load())
	}
	return nil
}

func runCase167ConcurrentAtomicValueStore() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Name.Store("")
	m.Store(S("ip:1"), s)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load(S("ip:1"))
			for i := 0; i < 100; i++ {
				v.(*atomicStats).Name.Store(fmt.Sprintf("w%d-i%d", w, i))
			}
		}()
	}
	wg.Wait()
	name := s.Name.Load().(string)
	if name == "" {
		return fmt.Errorf("Name should not be empty after 1600 stores")
	}
	return nil
}

func runCase168ConcurrentMutexMapWrites() error {
	m := testMap()
	defer m.Close()
	ms := &mutexStats{Tags: []string{}}
	m.Store(S("sess"), ms)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load(S("sess"))
			s := v.(*mutexStats)
			for i := 0; i < 100; i++ {
				s.mu.Lock()
				s.Tags = append(s.Tags, "t")
				s.mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(ms.Tags) != 800 {
		return fmt.Errorf("Tags = %d, want 800", len(ms.Tags))
	}
	return nil
}

func runCase169ConcurrentMutexSliceAppends() error {
	m := testMap()
	defer m.Close()
	ms := &mutexStats{}
	m.Store(S("sess"), ms)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load(S("sess"))
			s := v.(*mutexStats)
			for i := 0; i < 100; i++ {
				s.mu.Lock()
				s.Hits++
				s.mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ms.Hits != 800 {
		return fmt.Errorf("Hits = %d, want 800", ms.Hits)
	}
	return nil
}

func runCase170MixedPointerReadsWrites() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load(S("ip:1"))
			p := v.(*atomicStats)
			for i := 0; i < 500; i++ {
				p.Count.Add(1)
				p.Bytes.Add(int64(i))
				_ = p.Count.Load()
				_ = p.Bytes.Load()
			}
		}()
	}
	wg.Wait()
	if s.Count.Load() != 8000 {
		return fmt.Errorf("Count = %d, want 8000", s.Count.Load())
	}
	return nil
}

func runCase171ConcurrentLoadAndAtomicMutation() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			s.Count.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			m.Load(S("ip:1"))
		}
	}()
	wg.Wait()
	if s.Count.Load() != 1000 {
		return fmt.Errorf("Count = %d", s.Count.Load())
	}
	return nil
}

func runCase172StoreNPointersIncrementAll() error {
	m := testMap(WithCapacity(256), WithShardCount(8))
	defer m.Close()
	ptrs := make([]*atomicStats, 50)
	for i := range ptrs {
		ptrs[i] = &atomicStats{}
		m.Store(S(testKey(i)), ptrs[i])
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				v, _ := m.Load(S(testKey(i)))
				v.(*atomicStats).Count.Add(1)
			}
		}()
	}
	wg.Wait()
	for i, p := range ptrs {
		if p.Count.Load() != 8 {
			return fmt.Errorf("key-%d Count = %d", i, p.Count.Load())
		}
	}
	return nil
}

func runCase173ConcurrentLoadPointerSameAddress() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load(S("ip:1"))
			if !ok || v.(*atomicStats) != s {
				errs <- fmt.Errorf("mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		return e
	}
	return nil
}

func runCase174LoadOrStorePointerParallelMutation() error {
	m := testMap()
	defer m.Close()
	init := &atomicStats{}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.LoadOrStore(S("ip:1"), init)
			v.(*atomicStats).Count.Add(1)
		}()
	}
	wg.Wait()
	v, _ := m.Load(S("ip:1"))
	if v.(*atomicStats).Count.Load() != 16 {
		return fmt.Errorf("Count = %d", v.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase175PointerMutationWithTTL() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.StoreWithTTL(S("ip:1"), s, time.Minute)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load(S("ip:1"))
			if ok {
				v.(*atomicStats).Count.Add(1)
			}
		}()
	}
	wg.Wait()
	if s.Count.Load() != 8 {
		return fmt.Errorf("Count = %d", s.Count.Load())
	}
	return nil
}

func runCase176PointerMutationWithHitLimited() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.StoreWithHits(S("ip:1"), s, 8)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load(S("ip:1"))
			if ok {
				v.(*atomicStats).Count.Add(1)
			}
		}()
	}
	wg.Wait()
	if s.Count.Load() < 1 || s.Count.Load() > 8 {
		return fmt.Errorf("Count = %d, want 1..8", s.Count.Load())
	}
	return nil
}

func runCase177ConcurrentRangeReadsPointerFields() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Count.Store(42)
	m.Store(S("ip:1"), s)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Range(func(_ Key, v any) bool { _ = v.(*atomicStats).Count.Load(); return true })
		}()
	}
	wg.Wait()
	return nil
}

func runCase178ConcurrentSnapshotMutateAfter() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store(S("ip:1"), s)
	snap := m.Snapshot()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() { defer wg.Done(); snap[0].Value.(*atomicStats).Count.Add(1) }()
	}
	wg.Wait()
	if s.Count.Load() != 8 {
		return fmt.Errorf("Count = %d", s.Count.Load())
	}
	return nil
}

func runCase179HighContentionFewKeysPointer() error {
	m := testMap(WithCapacity(16), WithShardCount(4))
	defer m.Close()
	for i := 0; i < 4; i++ {
		m.Store(S(testKey(i)), &atomicStats{})
	}
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := uint64(1)
			for i := 0; i < 500; i++ {
				seed = xorshift(seed)
				k := S(testKey(int(seed % 4)))
				v, ok := m.Load(k)
				if ok {
					v.(*atomicStats).Count.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func runCase180ProducerConsumerPointer() error {
	m := testMap(WithCapacity(256), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.Store(S(testKey(i)), &atomicStats{})
		}
	}()
	wg.Wait()
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if v, ok := m.Load(S(testKey(i))); ok {
					v.(*atomicStats).Count.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func runCase181StoreDeleteRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				m.Store(S("k"), i)
				m.Delete(S("k"))
			}
		}()
	}
	wg.Wait()
	if _, ok := m.Load(S("k")); ok {
		return fmt.Errorf("Load(k) should be absent after store/delete race")
	}
	if m.Len() != 0 {
		return fmt.Errorf("Len = %d, want 0", m.Len())
	}
	return assertStableViews("store delete race", m)
}

func runCase182StoreClearRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Store(S(testKey(i%50)), i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			m.Clear()
		}
	}()
	wg.Wait()
	if m.Len() > 50 {
		return fmt.Errorf("Len = %d, want 0..50", m.Len())
	}
	if err := assertStableViews("store clear race", m); err != nil {
		return err
	}
	return assertLiveIntEntries("store clear race", m, "string")
}

func runCase183LoadOrStoreDeleteRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.LoadOrStore(S("k"), i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Delete(S("k"))
		}
	}()
	wg.Wait()
	if m.Len() > 1 {
		return fmt.Errorf("Len = %d, want 0..1", m.Len())
	}
	if value, ok := m.Load(S("k")); ok {
		if _, ok := value.(int); !ok {
			return fmt.Errorf("Load(k) type = %T, want int", value)
		}
	}
	return assertStableViews("LoadOrStore delete race", m)
}

func runCase184SwapDeleteRace() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Swap(S("k"), i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Delete(S("k"))
		}
	}()
	wg.Wait()
	if m.Len() > 1 {
		return fmt.Errorf("Len = %d, want 0..1", m.Len())
	}
	if value, ok := m.Load(S("k")); ok {
		if _, ok := value.(int); !ok {
			return fmt.Errorf("Load(k) type = %T, want int", value)
		}
	}
	return assertStableViews("swap delete race", m)
}

func runCase185CASStoreRace() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.CompareAndSwap(S("k"), i, i+1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Store(S("k"), i)
		}
	}()
	wg.Wait()
	value, ok := m.Load(S("k"))
	if !ok {
		return fmt.Errorf("Load(k) missing after CAS/store race")
	}
	intValue, ok := value.(int)
	if !ok || intValue < 0 || intValue > 200 {
		return fmt.Errorf("Load(k) = %v, want int in 0..200", value)
	}
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return assertStableViews("CAS store race", m)
}

func runCase186RangeDeleteRace() error {
	m := testMap(WithCapacity(256), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(S(testKey(i)), i)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			m.Range(func(_ Key, _ any) bool { return true })
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.Delete(S(testKey(i)))
		}
	}()
	wg.Wait()
	if m.Len() != 0 {
		return fmt.Errorf("Len = %d, want 0", m.Len())
	}
	return assertStableViews("range delete race", m)
}

func runCase187TTLExpiryLoadRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.StoreWithTTL(S("k"), i, time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Load(S("k"))
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	time.Sleep(5 * time.Millisecond)
	m.CleanupNow()
	if _, ok := m.Load(S("k")); ok {
		return fmt.Errorf("Load(k) should be absent after TTL expiry race")
	}
	if m.Len() != 0 {
		return fmt.Errorf("Len = %d, want 0", m.Len())
	}
	return assertStableViews("TTL expiry load race", m)
}

func runCase188HitExhaustionLoadRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.StoreWithHits(S("k"), i, 3)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			m.Load(S("k"))
		}
	}()
	wg.Wait()
	extraLoads := int64(0)
	for i := 0; i < 4; i++ {
		value, ok := m.Load(S("k"))
		if ok {
			extraLoads++
			if _, ok := value.(int); !ok {
				return fmt.Errorf("Load(k) type = %T, want int", value)
			}
		}
	}
	if extraLoads > 3 {
		return fmt.Errorf("extra successful loads = %d, want <= 3", extraLoads)
	}
	if _, ok := m.Load(S("k")); ok {
		return fmt.Errorf("Load(k) should be exhausted after extra reads")
	}
	if m.Len() != 0 {
		return fmt.Errorf("Len = %d, want 0", m.Len())
	}
	return assertStableViews("hit exhaustion load race", m)
}

func runCase189CleanupLoadRace() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 50; i++ {
		m.StoreWithTTL(S(testKey(i)), i, 5*time.Millisecond)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			m.CleanupNow()
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Load(S(testKey(i % 50)))
		}
	}()
	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	m.CleanupNow()
	if m.Len() != 0 {
		return fmt.Errorf("Len = %d, want 0", m.Len())
	}
	return assertStableViews("cleanup load race", m)
}

func runCase190BackgroundCleanupStoreRace() error {
	m := testMap(WithCleanupInterval(2 * time.Millisecond))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.StoreWithTTL(S(testKey(i)), i, time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if !waitUntil(100*time.Millisecond, func() bool {
		return m.Len() == 0
	}) {
		return fmt.Errorf("background cleanup did not clear expired entries: Len=%d", m.Len())
	}
	return assertStableViews("background cleanup store race", m)
}

func runCase191VeryLongKey() error {
	m := testMap()
	defer m.Close()
	key := ""
	for i := 0; i < 1000; i++ {
		key += "k"
	}
	m.Store(S(key), 42)
	v, ok := m.Load(S(key))
	if !ok || v.(int) != 42 {
		return fmt.Errorf("long key: ok=%v, v=%v", ok, v)
	}
	return nil
}

func runCase192NilValue() error {
	m := testMap()
	defer m.Close()
	m.Store(S("k"), nil)
	v, ok := m.Load(S("k"))
	if !ok {
		return fmt.Errorf("nil value not stored")
	}
	if v != nil {
		return fmt.Errorf("expected nil, got %v", v)
	}
	return nil
}

func runCase193BooleanValue() error {
	m := testMap()
	defer m.Close()
	m.Store(S("flag"), true)
	v, ok := m.Load(S("flag"))
	if !ok || v.(bool) != true {
		return fmt.Errorf("bool: ok=%v, v=%v", ok, v)
	}
	return nil
}

func runCase194ChannelValue() error {
	m := testMap()
	defer m.Close()
	ch := make(chan int, 1)
	m.Store(S("ch"), ch)
	v, _ := m.Load(S("ch"))
	v.(chan int) <- 99
	got := <-ch
	if got != 99 {
		return fmt.Errorf("channel: got %d", got)
	}
	return nil
}

func runCase195MapValue() error {
	m := testMap()
	defer m.Close()
	inner := map[string]int{"a": 1}
	m.Store(S("inner"), inner)
	v, _ := m.Load(S("inner"))
	v.(map[string]int)["b"] = 2
	if inner["b"] != 2 {
		return fmt.Errorf("map mutation not visible")
	}
	return nil
}

func runCase196FuncValue() error {
	m := testMap()
	defer m.Close()
	fn := func() int { return 42 }
	m.Store(S("fn"), fn)
	v, _ := m.Load(S("fn"))
	if v.(func() int)() != 42 {
		return fmt.Errorf("func: wrong return")
	}
	return nil
}

func runCase197InterfaceWrapping() error {
	m := testMap()
	defer m.Close()
	var iface any = &atomicStats{}
	m.Store(S("iface"), iface)
	v, _ := m.Load(S("iface"))
	v.(*atomicStats).Count.Add(5)
	if iface.(*atomicStats).Count.Load() != 5 {
		return fmt.Errorf("interface wrap mismatch")
	}
	return nil
}

func runCase198LargeEntryCount() error {
	m := testMap(WithCapacity(16384), WithShardCount(64))
	defer m.Close()
	for i := 0; i < 1000; i++ {
		m.Store(S(testKey(i)), i)
	}
	if m.Len() != 1000 {
		return fmt.Errorf("Len = %d", m.Len())
	}
	for i := 0; i < 1000; i++ {
		v, ok := m.Load(S(testKey(i)))
		if !ok || v.(int) != i {
			return fmt.Errorf("key-%d: ok=%v v=%v", i, ok, v)
		}
	}
	return nil
}

func runCase199RapidStoreDeleteCycle() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 1000; i++ {
		m.Store(S("k"), i)
		m.Delete(S("k"))
	}
	_, ok := m.Load(S("k"))
	if ok {
		return fmt.Errorf("key should not exist after delete cycle")
	}
	return nil
}

func runCase200StatsConsistencyAfterConcurrentWorkload() error {
	m := testMap(WithCapacity(512), WithShardCount(16))
	defer m.Close()
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.Store(S(testKey(i)), i)
			}
		}()
	}
	wg.Wait()
	live := m.Stats().LiveEntries
	length := m.Len()
	if live != int64(length) {
		return fmt.Errorf("Stats.LiveEntries=%d != Len=%d", live, length)
	}
	if live != 500 {
		return fmt.Errorf("expected 500 live, got %d", live)
	}
	return nil
}

func buildAdditionalTestCasesPart1() []TestCase {
	tests := make([]TestCase, 0, 50)

	makeRoundTripCase := func(id int, name string, key Key, value any) TestCase {
		return TestCase{
			ID:   id,
			Name: name,
			Fn: func() error {
				m := testMap()
				defer m.Close()
				m.Store(key, value)
				got, ok := m.Load(key)
				if !ok || !valuesEqual(got, value) {
					return fmt.Errorf(`Load(%s) = %v, %v`, key.String(), got, ok)
				}
				return nil
			},
		}
	}

	roundTripSpecs := []struct {
		id    int
		name  string
		key   Key
		value any
	}{
		{201, `Int64 Store and Load positive key`, I(201), `alpha`},
		{202, `Int64 Store and Load zero key`, I(0), 202},
		{203, `Int64 Store and Load negative key`, I(-203), true},
		{204, `Int64 Store and Load large positive key`, I(1 << 40), int64(204)},
		{205, `Int64 Store and Load large negative key`, I(-(1 << 40)), exampleUser{Name: `Ivy`, Age: 41}},
		{206, `Int64 Store and Load int slice`, I(206), []int{1, 2, 3}},
		{207, `Int64 Store and Load string-int map`, I(207), map[string]int{`a`: 1, `b`: 2}},
		{208, `Int64 Store and Load nil value`, I(208), nil},
		{209, `Int64 Store and Load string slice`, I(209), []string{`red`, `blue`}},
		{210, `Int64 Store and Load pointer value`, I(210), func() any {
			stats := &atomicStats{}
			stats.Count.Store(210)
			return stats
		}()},
	}
	for _, spec := range roundTripSpecs {
		spec := spec
		tests = append(tests, makeRoundTripCase(spec.id, spec.name, spec.key, spec.value))
	}

	tests = append(tests,
		TestCase{211, `Int64 LoadOrStore missing stores`, func() error {
			m := testMap()
			defer m.Close()
			value, loaded := m.LoadOrStore(I(211), `fresh`)
			if loaded || value != `fresh` {
				return fmt.Errorf(`LoadOrStore(211) = %v, %v`, value, loaded)
			}
			return nil
		}},
		TestCase{212, `Int64 LoadOrStore existing returns old`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(212), `first`)
			value, loaded := m.LoadOrStore(I(212), `second`)
			if !loaded || value != `first` {
				return fmt.Errorf(`LoadOrStore(212) = %v, %v`, value, loaded)
			}
			return nil
		}},
		TestCase{213, `Int64 LoadOrStoreWithOptions respects hit limits`, func() error {
			m := testMap()
			defer m.Close()
			value, loaded := m.LoadOrStoreWithOptions(I(213), `budget`, EntryOptions{Hits: 2})
			if loaded || value != `budget` {
				return fmt.Errorf(`LoadOrStoreWithOptions(213) = %v, %v`, value, loaded)
			}
			if _, ok := m.Load(I(213)); !ok {
				return fmt.Errorf(`first Load(213) missing`)
			}
			if _, ok := m.Load(I(213)); !ok {
				return fmt.Errorf(`second Load(213) missing`)
			}
			if _, ok := m.Load(I(213)); ok {
				return fmt.Errorf(`Load(213) should be exhausted`)
			}
			return nil
		}},
		TestCase{214, `Int64 Swap existing returns previous`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(214), `old`)
			previous, loaded := m.Swap(I(214), `new`)
			if !loaded || previous != `old` {
				return fmt.Errorf(`Swap(214) = %v, %v`, previous, loaded)
			}
			return nil
		}},
		TestCase{215, `Int64 SwapWithOptions applies replacement TTL`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(215), `old`)
			previous, loaded := m.SwapWithOptions(I(215), `new`, EntryOptions{TTL: 5 * time.Millisecond})
			if !loaded || previous != `old` {
				return fmt.Errorf(`SwapWithOptions(215) = %v, %v`, previous, loaded)
			}
			time.Sleep(10 * time.Millisecond)
			if _, ok := m.Load(I(215)); ok {
				return fmt.Errorf(`Load(215) should be expired`)
			}
			return nil
		}},
		TestCase{216, `Int64 CompareAndSwap succeeds for byte slices`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(216), []byte(`alpha`))
			if !m.CompareAndSwap(I(216), []byte(`alpha`), []byte(`beta`)) {
				return fmt.Errorf(`CompareAndSwap(216) failed`)
			}
			got, ok := m.Load(I(216))
			if !ok || string(got.([]byte)) != `beta` {
				return fmt.Errorf(`Load(216) = %v, %v`, got, ok)
			}
			return nil
		}},
		TestCase{217, `Int64 CompareAndSwap fails for wrong old value`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(217), []byte(`alpha`))
			if m.CompareAndSwap(I(217), []byte(`wrong`), []byte(`beta`)) {
				return fmt.Errorf(`CompareAndSwap(217) unexpectedly succeeded`)
			}
			return nil
		}},
		TestCase{218, `Int64 CompareAndDelete succeeds for map values`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(218), map[string]int{`a`: 1})
			if !m.CompareAndDelete(I(218), map[string]int{`a`: 1}) {
				return fmt.Errorf(`CompareAndDelete(218) failed`)
			}
			return nil
		}},
		TestCase{219, `Int64 CompareAndDelete fails for wrong old value`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(219), map[string]int{`a`: 1})
			if m.CompareAndDelete(I(219), map[string]int{`a`: 2}) {
				return fmt.Errorf(`CompareAndDelete(219) unexpectedly succeeded`)
			}
			return nil
		}},
		TestCase{220, `Int64 Delete missing returns false`, func() error {
			m := testMap()
			defer m.Close()
			previous, ok := m.Delete(I(220))
			if ok || previous != nil {
				return fmt.Errorf(`Delete(220) = %v, %v`, previous, ok)
			}
			return nil
		}},
		TestCase{221, `Int64 StoreWithTTL expires entry`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithTTL(I(221), `ttl`, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			if _, ok := m.Load(I(221)); ok {
				return fmt.Errorf(`Load(221) should be expired`)
			}
			return nil
		}},
		TestCase{222, `Int64 StoreWithHits exhausts entry`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithHits(I(222), `once`, 1)
			if _, ok := m.Load(I(222)); !ok {
				return fmt.Errorf(`first Load(222) missing`)
			}
			if _, ok := m.Load(I(222)); ok {
				return fmt.Errorf(`Load(222) should be exhausted`)
			}
			return nil
		}},
		TestCase{223, `Int64 StoreWithTTLAndHits expires before hits finish`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithTTLAndHits(I(223), `combo`, 5*time.Millisecond, 5)
			time.Sleep(10 * time.Millisecond)
			if _, ok := m.Load(I(223)); ok {
				return fmt.Errorf(`Load(223) should be expired`)
			}
			return nil
		}},
		TestCase{224, `Int64 SetWithTTLAndHits exhausts on hits`, func() error {
			m := testMap()
			defer m.Close()
			m.SetWithTTLAndHits(I(224), `combo`, time.Minute, 2)
			m.Load(I(224))
			m.Load(I(224))
			if _, ok := m.Load(I(224)); ok {
				return fmt.Errorf(`Load(224) should be exhausted`)
			}
			return nil
		}},
		TestCase{225, `Int64 CleanupNow removes expired entries`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithTTL(I(225), `soon-gone`, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			m.CleanupNow()
			if _, ok := m.Peek(I(225)); ok {
				return fmt.Errorf(`Peek(225) should be empty after CleanupNow`)
			}
			return nil
		}},
		TestCase{226, `Int64 Range skips expired entries`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(226), 1)
			m.StoreWithTTL(I(1226), 2, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			count := 0
			m.Range(func(_ Key, _ any) bool {
				count++
				return true
			})
			if count != 1 {
				return fmt.Errorf(`Range count = %d, want 1`, count)
			}
			return nil
		}},
		TestCase{227, `Int64 Snapshot skips hit exhausted entries`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(227), `live`)
			m.StoreWithHits(I(1227), `gone`, 1)
			m.Load(I(1227))
			snapshot := m.Snapshot()
			if len(snapshot) != 1 || snapshot[0].Key != I(227) {
				return fmt.Errorf(`Snapshot = %#v`, snapshot)
			}
			return nil
		}},
		TestCase{228, `Int64 Has reports false after expiry`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithTTL(I(228), `gone`, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			if m.Has(I(228)) {
				return fmt.Errorf(`Has(228) should be false after expiry`)
			}
			return nil
		}},
		TestCase{229, `Int64 Peek preserves later hit-limited load`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithHits(I(229), `ticket`, 1)
			if _, ok := m.Peek(I(229)); !ok {
				return fmt.Errorf(`Peek(229) missing`)
			}
			if _, ok := m.Load(I(229)); !ok {
				return fmt.Errorf(`Load(229) missing after Peek`)
			}
			if _, ok := m.Load(I(229)); ok {
				return fmt.Errorf(`Load(229) should be exhausted`)
			}
			return nil
		}},
		TestCase{230, `Int64 Len reflects cleanup of expired entries`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(230), 1)
			m.Store(I(1230), 2)
			m.StoreWithTTL(I(2230), 3, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			m.CleanupNow()
			if m.Len() != 2 {
				return fmt.Errorf(`Len() = %d, want 2`, m.Len())
			}
			return nil
		}},
		TestCase{231, `String key helper methods round trip`, func() error {
			key := S(`helper`)
			if !key.IsString() || key.IsInt64() || key.String() != `helper` || key.StringVal() != `helper` || key.Int64Val() != 0 {
				return fmt.Errorf(`string key helper mismatch: %+v`, key)
			}
			return nil
		}},
		TestCase{232, `Int64 key helper methods round trip`, func() error {
			key := I(-232)
			if key.IsString() || !key.IsInt64() || key.String() != `-232` || key.StringVal() != `` || key.Int64Val() != -232 {
				return fmt.Errorf(`int64 key helper mismatch: %+v`, key)
			}
			return nil
		}},
		TestCase{233, `String key Raw returns string`, func() error {
			if raw, ok := S(`raw-string`).Raw().(string); !ok || raw != `raw-string` {
				return fmt.Errorf(`Raw() string = %v, %v`, raw, ok)
			}
			return nil
		}},
		TestCase{234, `Int64 key Raw returns int64`, func() error {
			if raw, ok := I(234).Raw().(int64); !ok || raw != 234 {
				return fmt.Errorf(`Raw() int64 = %v, %v`, raw, ok)
			}
			return nil
		}},
		TestCase{235, `String and int64 keys with same text stay distinct`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`42`), `string`)
			m.Store(I(42), `int64`)
			stringValue, _ := m.Load(S(`42`))
			intValue, _ := m.Load(I(42))
			if stringValue != `string` || intValue != `int64` {
				return fmt.Errorf(`mixed key values = %v, %v`, stringValue, intValue)
			}
			return nil
		}},
		TestCase{236, `Int64 key String formats negatives`, func() error {
			if I(-236).String() != `-236` {
				return fmt.Errorf(`I(-236).String() = %s`, I(-236).String())
			}
			return nil
		}},
		TestCase{237, `Mismatched helper accessors return zero values`, func() error {
			if S(`x`).Int64Val() != 0 || I(237).StringVal() != `` {
				return fmt.Errorf(`mismatched helper accessors returned non-zero values`)
			}
			return nil
		}},
		TestCase{238, `keysEqual matches identical string keys`, func() error {
			if !keysEqual(S(`same`), S(`same`)) || keysEqual(S(`same`), S(`other`)) {
				return fmt.Errorf(`keysEqual string comparison failed`)
			}
			return nil
		}},
		TestCase{239, `keysEqual matches identical int64 keys`, func() error {
			if !keysEqual(I(239), I(239)) || keysEqual(I(239), I(240)) {
				return fmt.Errorf(`keysEqual int64 comparison failed`)
			}
			return nil
		}},
		TestCase{240, `keysEqual rejects mixed key kinds`, func() error {
			if keysEqual(S(`240`), I(240)) {
				return fmt.Errorf(`keysEqual should reject mixed key kinds`)
			}
			return nil
		}},
		TestCase{241, `RecommendedShardCount returns positive power of two`, func() error {
			count := RecommendedShardCount(0)
			if !isPowerOfTwo(count) {
				return fmt.Errorf(`RecommendedShardCount(0) = %d`, count)
			}
			return nil
		}},
		TestCase{242, `RecommendedShardCount stays power of two for large capacity`, func() error {
			count := RecommendedShardCount(1_000_000)
			if !isPowerOfTwo(count) || count > 1024 {
				return fmt.Errorf(`RecommendedShardCount(1_000_000) = %d`, count)
			}
			return nil
		}},
		TestCase{243, `WithShardCount rounds to next power of two`, func() error {
			m := New(WithCapacity(64), WithShardCount(3), WithCleanupInterval(0))
			defer m.Close()
			if m.Stats().Shards != 4 {
				return fmt.Errorf(`Shards = %d, want 4`, m.Stats().Shards)
			}
			return nil
		}},
		TestCase{244, `Builder Shards rounds to next power of two`, func() error {
			m := NewBuilder().Capacity(64).Shards(6).CleanupInterval(0).Build()
			defer m.Close()
			if m.Stats().Shards != 8 {
				return fmt.Errorf(`Shards = %d, want 8`, m.Stats().Shards)
			}
			return nil
		}},
		TestCase{245, `WithLoadFactor clamps low values`, func() error {
			m := New(WithCapacity(64), WithShardCount(4), WithCleanupInterval(0), WithLoadFactor(0.10))
			defer m.Close()
			if m.shards[0].loadFactor != 0.50 {
				return fmt.Errorf(`loadFactor = %.2f, want 0.50`, m.shards[0].loadFactor)
			}
			return nil
		}},
		TestCase{246, `WithLoadFactor clamps high values`, func() error {
			m := New(WithCapacity(64), WithShardCount(4), WithCleanupInterval(0), WithLoadFactor(1.50))
			defer m.Close()
			if m.shards[0].loadFactor != 0.90 {
				return fmt.Errorf(`loadFactor = %.2f, want 0.90`, m.shards[0].loadFactor)
			}
			return nil
		}},
		TestCase{247, `Negative cleanup interval disables background cleanup`, func() error {
			m := New(WithCapacity(64), WithShardCount(4), WithCleanupInterval(-time.Second))
			defer m.Close()
			if m.stopCleanup != nil {
				return fmt.Errorf(`stopCleanup should be nil when cleanup is disabled`)
			}
			return nil
		}},
		TestCase{248, `WithoutCleanup disables background cleanup`, func() error {
			m := New(WithCapacity(64), WithShardCount(4), WithoutCleanup())
			defer m.Close()
			if m.stopCleanup != nil {
				return fmt.Errorf(`stopCleanup should be nil when WithoutCleanup is used`)
			}
			return nil
		}},
		TestCase{249, `WithValueCloner exposes tracked bytes`, func() error {
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				text := value.(string)
				return text, int64(len(text))
			}))
			defer m.Close()
			m.Store(S(`tracked`), `tracked`)
			if m.Stats().TrackedValueBytes != int64(len(`tracked`)) {
				return fmt.Errorf(`TrackedValueBytes = %d`, m.Stats().TrackedValueBytes)
			}
			return nil
		}},
		TestCase{250, `Builder ValueCloner exposes tracked bytes`, func() error {
			m := NewBuilder().Capacity(64).Shards(4).CleanupInterval(0).ValueCloner(func(value any) (any, int64) {
				text := value.(string)
				return text, int64(len(text))
			}).Build()
			defer m.Close()
			m.Store(S(`tracked`), `builder`)
			if m.Stats().TrackedValueBytes != int64(len(`builder`)) {
				return fmt.Errorf(`TrackedValueBytes = %d`, m.Stats().TrackedValueBytes)
			}
			return nil
		}},
	)

	if len(tests) != 50 {
		panic(fmt.Sprintf(`buildAdditionalTestCasesPart1 generated %d tests, want 50`, len(tests)))
	}
	return tests
}

func buildAdditionalTestCasesPart2() []TestCase {
	tests := make([]TestCase, 0, 50)

	tests = append(tests,
		TestCase{251, `MapCloner copies stored values`, func() error {
			m := testMap(WithValueCloner(cloneFromInterfaces))
			defer m.Close()
			user := cloneableUser{Name: `nova`, Labels: []string{`red`, `fast`}}
			m.Store(S(`user`), user)
			user.Labels[0] = `mutated`
			stored, _ := m.Load(S(`user`))
			if stored.(cloneableUser).Labels[0] != `red` {
				return fmt.Errorf(`stored clone = %#v`, stored)
			}
			return nil
		}},
		TestCase{252, `SizedMapCloner copies values and tracks bytes`, func() error {
			m := testMap(WithValueCloner(cloneFromInterfaces))
			defer m.Close()
			user := sizedCloneableUser{Name: `sized`, Labels: []string{`blue`, `beta`}}
			m.Store(S(`user`), user)
			user.Labels[0] = `mutated`
			stored, _ := m.Load(S(`user`))
			if stored.(sizedCloneableUser).Labels[0] != `blue` {
				return fmt.Errorf(`stored clone = %#v`, stored)
			}
			if m.Stats().TrackedValueBytes <= 0 {
				return fmt.Errorf(`TrackedValueBytes = %d`, m.Stats().TrackedValueBytes)
			}
			return nil
		}},
		TestCase{253, `Nil ValueCloner falls back to direct reference`, func() error {
			m := testMap(WithValueCloner(nil))
			defer m.Close()
			payload := []string{`one`, `two`}
			m.Store(S(`slice`), payload)
			payload[0] = `changed`
			stored, _ := m.Load(S(`slice`))
			if stored.([]string)[0] != `changed` {
				return fmt.Errorf(`stored slice = %#v`, stored)
			}
			return nil
		}},
		TestCase{254, `Custom ValueCloner copies byte slices`, func() error {
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				data := append([]byte(nil), value.([]byte)...)
				return data, int64(len(data))
			}))
			defer m.Close()
			payload := []byte(`alpha`)
			m.Store(S(`blob`), payload)
			payload[0] = 'o'
			stored, _ := m.Load(S(`blob`))
			if string(stored.([]byte)) != `alpha` {
				return fmt.Errorf(`stored blob = %q`, string(stored.([]byte)))
			}
			return nil
		}},
		TestCase{255, `Custom ValueCloner copies maps`, func() error {
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				original := value.(map[string]int)
				clone := make(map[string]int, len(original))
				for key, entry := range original {
					clone[key] = entry
				}
				return clone, int64(len(clone) * 8)
			}))
			defer m.Close()
			payload := map[string]int{`a`: 1}
			m.Store(S(`map`), payload)
			payload[`a`] = 99
			stored, _ := m.Load(S(`map`))
			if stored.(map[string]int)[`a`] != 1 {
				return fmt.Errorf(`stored map = %#v`, stored)
			}
			return nil
		}},
		TestCase{256, `Custom ValueCloner runs on Store`, func() error {
			var calls atomic.Int64
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				calls.Add(1)
				return value, 1
			}))
			defer m.Close()
			m.Store(S(`k`), `value`)
			if calls.Load() != 1 {
				return fmt.Errorf(`clone calls = %d`, calls.Load())
			}
			return nil
		}},
		TestCase{257, `Custom ValueCloner runs on Swap`, func() error {
			var calls atomic.Int64
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				calls.Add(1)
				return value, 1
			}))
			defer m.Close()
			m.Store(S(`k`), `first`)
			_, _ = m.Swap(S(`k`), `second`)
			if calls.Load() != 2 {
				return fmt.Errorf(`clone calls = %d, want 2`, calls.Load())
			}
			return nil
		}},
		TestCase{258, `LoadOrStore existing skips unused clone`, func() error {
			var calls atomic.Int64
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				calls.Add(1)
				return value, 1
			}))
			defer m.Close()
			m.Store(S(`k`), `live`)
			calls.Store(0)
			value, loaded := m.LoadOrStore(S(`k`), `shadow`)
			if !loaded || value != `live` {
				return fmt.Errorf(`LoadOrStore(k) = %v, %v`, value, loaded)
			}
			if calls.Load() != 0 {
				return fmt.Errorf(`clone calls = %d, want 0`, calls.Load())
			}
			return nil
		}},
		TestCase{259, `LoadOrStoreWithOptions missing uses cloner`, func() error {
			var calls atomic.Int64
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				calls.Add(1)
				return value, 7
			}))
			defer m.Close()
			_, loaded := m.LoadOrStoreWithOptions(S(`k`), `value`, EntryOptions{TTL: time.Minute})
			if loaded || calls.Load() != 1 {
				return fmt.Errorf(`loaded=%v calls=%d`, loaded, calls.Load())
			}
			return nil
		}},
		TestCase{260, `SwapWithOptions updates tracked value bytes`, func() error {
			m := testMap(WithValueCloner(func(value any) (any, int64) {
				text := value.(string)
				return text, int64(len(text))
			}))
			defer m.Close()
			m.Store(S(`k`), `one`)
			_, _ = m.SwapWithOptions(S(`k`), `seventh`, EntryOptions{TTL: time.Minute})
			if m.Stats().TrackedValueBytes != int64(len(`seventh`)) {
				return fmt.Errorf(`TrackedValueBytes = %d`, m.Stats().TrackedValueBytes)
			}
			return nil
		}},
		TestCase{261, `CompareAndSwap supports nested []any equality`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`nested`), []any{`a`, []int{1, 2}})
			if !m.CompareAndSwap(S(`nested`), []any{`a`, []int{1, 2}}, []any{`b`, []int{3, 4}}) {
				return fmt.Errorf(`CompareAndSwap(nested) failed`)
			}
			return nil
		}},
		TestCase{262, `CompareAndDelete supports nested map equality`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`nested`), map[string]any{`labels`: []string{`x`, `y`}})
			if !m.CompareAndDelete(S(`nested`), map[string]any{`labels`: []string{`x`, `y`}}) {
				return fmt.Errorf(`CompareAndDelete(nested) failed`)
			}
			return nil
		}},
		TestCase{263, `CompareAndSwap supports []byte equality`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`bytes`), []byte(`abc`))
			if !m.CompareAndSwap(S(`bytes`), []byte(`abc`), []byte(`xyz`)) {
				return fmt.Errorf(`CompareAndSwap(bytes) failed`)
			}
			return nil
		}},
		TestCase{264, `CompareAndSwap supports []string equality`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`strings`), []string{`a`, `b`})
			if !m.CompareAndSwap(S(`strings`), []string{`a`, `b`}, []string{`b`, `c`}) {
				return fmt.Errorf(`CompareAndSwap(strings) failed`)
			}
			return nil
		}},
		TestCase{265, `CompareAndSwap supports []int64 equality`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`ints`), []int64{1, 2, 3})
			if !m.CompareAndSwap(S(`ints`), []int64{1, 2, 3}, []int64{3, 2, 1}) {
				return fmt.Errorf(`CompareAndSwap(ints) failed`)
			}
			return nil
		}},
		TestCase{266, `CompareAndSwap supports map string-int equality`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`map`), map[string]int{`a`: 1, `b`: 2})
			if !m.CompareAndSwap(S(`map`), map[string]int{`a`: 1, `b`: 2}, map[string]int{`b`: 3}) {
				return fmt.Errorf(`CompareAndSwap(map) failed`)
			}
			return nil
		}},
		TestCase{267, `CompareAndSwap supports MapEqualer`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`version`), equalerVersion{Generation: 1, Label: `first`})
			if !m.CompareAndSwap(S(`version`), equalerVersion{Generation: 1, Label: `shadow`}, equalerVersion{Generation: 2, Label: `second`}) {
				return fmt.Errorf(`CompareAndSwap(version) failed`)
			}
			return nil
		}},
		TestCase{268, `CompareAndDelete supports MapEqualer`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`version`), equalerVersion{Generation: 2, Label: `live`})
			if !m.CompareAndDelete(S(`version`), equalerVersion{Generation: 2, Label: `shadow`}) {
				return fmt.Errorf(`CompareAndDelete(version) failed`)
			}
			return nil
		}},
		TestCase{269, `CompareAndSwap returns false for unequal nested maps`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`nested`), map[string]any{`count`: 1})
			if m.CompareAndSwap(S(`nested`), map[string]any{`count`: 2}, `new`) {
				return fmt.Errorf(`CompareAndSwap(nested) unexpectedly succeeded`)
			}
			return nil
		}},
		TestCase{270, `CompareAndDelete supports nil values`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`nil`), nil)
			if !m.CompareAndDelete(S(`nil`), nil) {
				return fmt.Errorf(`CompareAndDelete(nil) failed`)
			}
			return nil
		}},
		TestCase{271, `StringSet works through pointer stored at int64 key`, func() error {
			m := testMap()
			defer m.Close()
			stats := &atomicStats{}
			m.Store(I(271), stats)
			value, _ := m.Load(I(271))
			StringSet(&value.(*atomicStats).Name, `updated`)
			if stats.Name.Load() != `updated` {
				return fmt.Errorf(`Name = %v`, stats.Name.Load())
			}
			return nil
		}},
		TestCase{272, `Int64 key pointer loads return same address`, func() error {
			m := testMap()
			defer m.Close()
			stats := &atomicStats{}
			m.Store(I(272), stats)
			left, _ := m.Load(I(272))
			right, _ := m.Load(I(272))
			if left.(*atomicStats) != right.(*atomicStats) {
				return fmt.Errorf(`different pointers loaded for same int64 key`)
			}
			return nil
		}},
		TestCase{273, `Int64 key pointer TTL expiry hides entry`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithTTL(I(273), &atomicStats{}, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			if _, ok := m.Load(I(273)); ok {
				return fmt.Errorf(`Load(273) should be expired`)
			}
			return nil
		}},
		TestCase{274, `Int64 key pointer hit limit exhausts entry`, func() error {
			m := testMap()
			defer m.Close()
			stats := &atomicStats{}
			m.StoreWithHits(I(274), stats, 1)
			if _, ok := m.Load(I(274)); !ok {
				return fmt.Errorf(`first Load(274) missing`)
			}
			if _, ok := m.Load(I(274)); ok {
				return fmt.Errorf(`Load(274) should be exhausted`)
			}
			return nil
		}},
		TestCase{275, `Int64 key pointer Range reads live fields`, func() error {
			m := testMap()
			defer m.Close()
			stats := &atomicStats{}
			stats.Count.Store(275)
			m.Store(I(275), stats)
			found := false
			m.Range(func(key Key, value any) bool {
				found = key == I(275) && value.(*atomicStats).Count.Load() == 275
				return true
			})
			if !found {
				return fmt.Errorf(`Range did not find int64 pointer entry`)
			}
			return nil
		}},
		TestCase{276, `Int64 key pointer Snapshot reads live fields`, func() error {
			m := testMap()
			defer m.Close()
			stats := &atomicStats{}
			stats.Bytes.Store(276)
			m.Store(I(276), stats)
			snapshot := m.Snapshot()
			if len(snapshot) != 1 || snapshot[0].Value.(*atomicStats).Bytes.Load() != 276 {
				return fmt.Errorf(`Snapshot = %#v`, snapshot)
			}
			return nil
		}},
		TestCase{277, `Concurrent pointer mutation works for int64 keys`, func() error {
			m := testMap()
			defer m.Close()
			stats := &atomicStats{}
			m.Store(I(277), stats)
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					value, _ := m.Load(I(277))
					for index := 0; index < 100; index++ {
						value.(*atomicStats).Count.Add(1)
					}
				}()
			}
			wg.Wait()
			if stats.Count.Load() != 800 {
				return fmt.Errorf(`Count = %d, want 800`, stats.Count.Load())
			}
			return nil
		}},
		TestCase{278, `Int64 key pointer CompareAndSwap succeeds`, func() error {
			m := testMap()
			defer m.Close()
			oldStats := &atomicStats{}
			newStats := &atomicStats{}
			m.Store(I(278), oldStats)
			if !m.CompareAndSwap(I(278), oldStats, newStats) {
				return fmt.Errorf(`CompareAndSwap(278) failed`)
			}
			return nil
		}},
		TestCase{279, `Int64 key pointer Swap returns previous pointer`, func() error {
			m := testMap()
			defer m.Close()
			oldStats := &atomicStats{}
			oldStats.Count.Store(279)
			m.Store(I(279), oldStats)
			previous, loaded := m.Swap(I(279), &atomicStats{})
			if !loaded || previous.(*atomicStats).Count.Load() != 279 {
				return fmt.Errorf(`Swap(279) = %v, %v`, previous, loaded)
			}
			return nil
		}},
		TestCase{280, `Int64 key pointer Delete returns pointer`, func() error {
			m := testMap()
			defer m.Close()
			stats := &atomicStats{}
			stats.Count.Store(280)
			m.Store(I(280), stats)
			previous, ok := m.Delete(I(280))
			if !ok || previous.(*atomicStats).Count.Load() != 280 {
				return fmt.Errorf(`Delete(280) = %v, %v`, previous, ok)
			}
			return nil
		}},
		TestCase{281, `Range counts mixed string and int64 keys`, func() error {
			m := testMap()
			defer m.Close()
			for index := 0; index < 3; index++ {
				m.Store(S(testKey(index)), index)
				m.Store(I(int64(index)), index)
			}
			count := 0
			m.Range(func(_ Key, _ any) bool {
				count++
				return true
			})
			if count != 6 {
				return fmt.Errorf(`Range count = %d, want 6`, count)
			}
			return nil
		}},
		TestCase{282, `Snapshot captures mixed string and int64 keys`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`a`), 1)
			m.Store(I(282), 2)
			snapshot := m.Snapshot()
			if len(snapshot) != 2 {
				return fmt.Errorf(`Snapshot length = %d`, len(snapshot))
			}
			return nil
		}},
		TestCase{283, `Clear removes mixed key sets`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`a`), 1)
			m.Store(I(283), 2)
			m.Clear()
			if m.Len() != 0 {
				return fmt.Errorf(`Len() = %d, want 0`, m.Len())
			}
			return nil
		}},
		TestCase{284, `Len updates after deleting mixed keys`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`a`), 1)
			m.Store(I(284), 2)
			m.Delete(I(284))
			if m.Len() != 1 {
				return fmt.Errorf(`Len() = %d, want 1`, m.Len())
			}
			return nil
		}},
		TestCase{285, `Stats live entries matches len for mixed keys`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`a`), 1)
			m.Store(I(285), 2)
			stats := m.Stats()
			if stats.LiveEntries != int64(m.Len()) {
				return fmt.Errorf(`LiveEntries=%d Len=%d`, stats.LiveEntries, m.Len())
			}
			return nil
		}},
		TestCase{286, `CleanupNow keeps live mixed entries`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`live`), 1)
			m.StoreWithTTL(I(286), 2, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			m.CleanupNow()
			if !m.Has(S(`live`)) || m.Has(I(286)) {
				return fmt.Errorf(`CleanupNow mixed state mismatch`)
			}
			return nil
		}},
		TestCase{287, `Range early stop works for mixed keys`, func() error {
			m := testMap()
			defer m.Close()
			for index := 0; index < 5; index++ {
				m.Store(S(testKey(index)), index)
				m.Store(I(int64(index+100)), index)
			}
			count := 0
			m.Range(func(_ Key, _ any) bool {
				count++
				return count < 2
			})
			if count != 2 {
				return fmt.Errorf(`Range count = %d, want 2`, count)
			}
			return nil
		}},
		TestCase{288, `Snapshot is point in time for mixed keys`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`a`), 1)
			m.Store(I(288), 2)
			snapshot := m.Snapshot()
			m.Store(S(`a`), 3)
			m.Delete(I(288))
			if len(snapshot) != 2 {
				return fmt.Errorf(`Snapshot length = %d, want 2`, len(snapshot))
			}
			return nil
		}},
		TestCase{289, `SwapWithOptions keeps mixed key live`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(S(`swap`), `old`)
			_, _ = m.SwapWithOptions(S(`swap`), `new`, EntryOptions{TTL: time.Minute})
			if !m.Has(S(`swap`)) {
				return fmt.Errorf(`Has(swap) = false`)
			}
			return nil
		}},
		TestCase{290, `Delete after TTL expiry returns false`, func() error {
			m := testMap()
			defer m.Close()
			m.StoreWithTTL(S(`gone`), `value`, 5*time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			previous, ok := m.Delete(S(`gone`))
			if ok || previous != nil {
				return fmt.Errorf(`Delete(gone) = %v, %v`, previous, ok)
			}
			return nil
		}},
		TestCase{291, `Concurrent int64 store-load distinct keys`, func() error {
			m := testMap(WithCapacity(512), WithShardCount(8))
			defer m.Close()
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				worker := worker
				wg.Add(1)
				go func() {
					defer wg.Done()
					for index := 0; index < 20; index++ {
						key := I(int64(worker*20 + index))
						m.Store(key, index)
						m.Load(key)
					}
				}()
			}
			wg.Wait()
			if m.Len() != 160 {
				return fmt.Errorf(`Len() = %d, want 160`, m.Len())
			}
			for index := 0; index < 160; index++ {
				value, ok := m.Load(I(int64(index)))
				if !ok || value != index%20 {
					return fmt.Errorf(`Load(%d) = %v, %v`, index, value, ok)
				}
			}
			if err := assertStableViews(`concurrent int64 store-load distinct keys`, m); err != nil {
				return err
			}
			return assertLiveIntEntries(`concurrent int64 store-load distinct keys`, m, `int64`)
		}},
		TestCase{292, `Concurrent int64 LoadOrStore picks one first writer`, func() error {
			m := testMap()
			defer m.Close()
			var wg sync.WaitGroup
			var first atomic.Int64
			for worker := 0; worker < 16; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, loaded := m.LoadOrStore(I(292), `shared`)
					if !loaded {
						first.Add(1)
					}
				}()
			}
			wg.Wait()
			if first.Load() != 1 {
				return fmt.Errorf(`first writers = %d, want 1`, first.Load())
			}
			value, ok := m.Load(I(292))
			if !ok || value != `shared` {
				return fmt.Errorf(`Load(292) = %v, %v`, value, ok)
			}
			if m.Len() != 1 {
				return fmt.Errorf(`Len() = %d, want 1`, m.Len())
			}
			return assertStableViews(`concurrent int64 LoadOrStore`, m)
		}},
		TestCase{293, `Concurrent int64 Swap keeps key present`, func() error {
			m := testMap()
			defer m.Close()
			m.Store(I(293), 0)
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				worker := worker
				wg.Add(1)
				go func() {
					defer wg.Done()
					for index := 0; index < 50; index++ {
						m.Swap(I(293), worker*50+index)
					}
				}()
			}
			wg.Wait()
			value, ok := m.Load(I(293))
			if !ok {
				return fmt.Errorf(`Load(293) missing`)
			}
			if _, ok := value.(int); !ok {
				return fmt.Errorf(`Load(293) type = %T, want int`, value)
			}
			if m.Len() != 1 {
				return fmt.Errorf(`Len() = %d, want 1`, m.Len())
			}
			return assertStableViews(`concurrent int64 Swap`, m)
		}},
		TestCase{294, `Concurrent int64 Delete leaves key absent`, func() error {
			m := testMap()
			defer m.Close()
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for index := 0; index < 50; index++ {
						m.Store(I(294), index)
						m.Delete(I(294))
					}
				}()
			}
			wg.Wait()
			if _, ok := m.Load(I(294)); ok {
				return fmt.Errorf(`Load(294) should be absent`)
			}
			if m.Len() != 0 {
				return fmt.Errorf(`Len() = %d, want 0`, m.Len())
			}
			return assertStableViews(`concurrent int64 Delete`, m)
		}},
		TestCase{295, `Concurrent Range while writing int64 keys`, func() error {
			m := testMap(WithCapacity(256), WithShardCount(8))
			defer m.Close()
			for index := 0; index < 100; index++ {
				m.Store(I(int64(index)), index)
			}
			var wg sync.WaitGroup
			rangeCount := 0
			wg.Add(2)
			go func() {
				defer wg.Done()
				for index := 100; index < 200; index++ {
					m.Store(I(int64(index)), index)
				}
			}()
			go func() {
				defer wg.Done()
				localCount := 0
				m.Range(func(_ Key, _ any) bool {
					localCount++
					return true
				})
				rangeCount = localCount
			}()
			wg.Wait()
			if rangeCount < 100 || rangeCount > 200 {
				return fmt.Errorf(`concurrent Range count = %d, want 100..200`, rangeCount)
			}
			if m.Len() != 200 {
				return fmt.Errorf(`Len() = %d, want 200`, m.Len())
			}
			for index := 0; index < 200; index++ {
				value, ok := m.Load(I(int64(index)))
				if !ok || value != index {
					return fmt.Errorf(`Load(%d) = %v, %v`, index, value, ok)
				}
			}
			if err := assertStableViews(`concurrent int64 Range while writing`, m); err != nil {
				return err
			}
			return assertLiveIntEntries(`concurrent int64 Range while writing`, m, `int64`)
		}},
		TestCase{296, `Concurrent Snapshot while writing int64 keys`, func() error {
			m := testMap(WithCapacity(256), WithShardCount(8))
			defer m.Close()
			for index := 0; index < 100; index++ {
				m.Store(I(int64(index)), index)
			}
			var wg sync.WaitGroup
			var snapshot []Pair
			wg.Add(2)
			go func() {
				defer wg.Done()
				for index := 100; index < 200; index++ {
					m.Store(I(int64(index)), index)
				}
			}()
			go func() {
				defer wg.Done()
				snapshot = m.Snapshot()
			}()
			wg.Wait()
			if len(snapshot) < 100 || len(snapshot) > 200 {
				return fmt.Errorf(`concurrent Snapshot length = %d, want 100..200`, len(snapshot))
			}
			if m.Len() != 200 {
				return fmt.Errorf(`Len() = %d, want 200`, m.Len())
			}
			for index := 0; index < 200; index++ {
				value, ok := m.Load(I(int64(index)))
				if !ok || value != index {
					return fmt.Errorf(`Load(%d) = %v, %v`, index, value, ok)
				}
			}
			if err := assertStableViews(`concurrent int64 Snapshot while writing`, m); err != nil {
				return err
			}
			return assertLiveIntEntries(`concurrent int64 Snapshot while writing`, m, `int64`)
		}},
		TestCase{297, `Concurrent CleanupNow with int64 TTL keys`, func() error {
			m := testMap(WithCapacity(256), WithShardCount(8))
			defer m.Close()
			for index := 0; index < 50; index++ {
				m.StoreWithTTL(I(int64(index)), index, 5*time.Millisecond)
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				for index := 0; index < 20; index++ {
					m.CleanupNow()
				}
			}()
			go func() {
				defer wg.Done()
				for index := 0; index < 100; index++ {
					m.Load(I(int64(index % 50)))
				}
			}()
			wg.Wait()
			time.Sleep(10 * time.Millisecond)
			m.CleanupNow()
			if m.Len() != 0 {
				return fmt.Errorf(`Len() = %d, want 0`, m.Len())
			}
			return assertStableViews(`concurrent int64 CleanupNow`, m)
		}},
		TestCase{298, `Concurrent int64 hit-limited writes and reads`, func() error {
			m := testMap(WithCapacity(256), WithShardCount(8))
			defer m.Close()
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				worker := worker
				wg.Add(1)
				go func() {
					defer wg.Done()
					for index := 0; index < 20; index++ {
						key := I(int64(worker*20 + index))
						m.StoreWithHits(key, index, 3)
						m.Load(key)
					}
				}()
			}
			wg.Wait()
			for worker := 0; worker < 8; worker++ {
				for index := 0; index < 20; index++ {
					key := I(int64(worker*20 + index))
					for attempt := 0; attempt < 2; attempt++ {
						value, ok := m.Load(key)
						if !ok || value != index {
							return fmt.Errorf(`Load(%s) = %v, %v`, key.String(), value, ok)
						}
					}
					if _, ok := m.Load(key); ok {
						return fmt.Errorf(`Load(%s) should be exhausted after 3 hits`, key.String())
					}
				}
			}
			if m.Len() != 0 {
				return fmt.Errorf(`Len() = %d, want 0`, m.Len())
			}
			return assertStableViews(`concurrent int64 hit-limited`, m)
		}},
		TestCase{299, `Large negative int64 keys round trip in a loop`, func() error {
			m := testMap()
			defer m.Close()
			keys := []int64{-1, -(1 << 20), -(1 << 40), -9_000_000_000_000_000_000}
			for index, key := range keys {
				m.Store(I(key), index)
				value, ok := m.Load(I(key))
				if !ok || value != index {
					return fmt.Errorf(`Load(%d) = %v, %v`, key, value, ok)
				}
			}
			return nil
		}},
		TestCase{300, `Close keeps int64-key map usable`, func() error {
			m := New(WithCapacity(64), WithShardCount(4), WithCleanupInterval(2*time.Millisecond))
			m.StoreWithTTL(I(300), `ephemeral`, time.Minute)
			if !waitUntil(50*time.Millisecond, func() bool {
				return m.Stats().CleanupRuns > 0
			}) {
				return fmt.Errorf(`background cleanup never ran before Close`)
			}
			m.Close()
			runsAfterClose := m.Stats().CleanupRuns
			time.Sleep(15 * time.Millisecond)
			if runsLater := m.Stats().CleanupRuns; runsLater != runsAfterClose {
				return fmt.Errorf(`CleanupRuns changed after Close: after=%d later=%d`, runsAfterClose, runsLater)
			}
			if value, ok := m.Load(I(300)); !ok || value != `ephemeral` {
				return fmt.Errorf(`Load(300) = %v, %v`, value, ok)
			}
			m.Store(I(1300), `after-close`)
			previous, loaded := m.Swap(I(1300), `swapped`)
			if !loaded || previous != `after-close` {
				return fmt.Errorf(`Swap(1300) = %v, %v`, previous, loaded)
			}
			value, ok := m.Load(I(1300))
			if !ok || value != `swapped` {
				return fmt.Errorf(`Load(1300) = %v, %v`, value, ok)
			}
			deleted, ok := m.Delete(I(1300))
			if !ok || deleted != `swapped` {
				return fmt.Errorf(`Delete(1300) = %v, %v`, deleted, ok)
			}
			if m.Has(I(1300)) {
				return fmt.Errorf(`Has(1300) = true after Delete`)
			}
			if m.Len() != 1 {
				return fmt.Errorf(`Len() = %d, want 1`, m.Len())
			}
			if err := assertStableViews(`close keeps int64-key map usable`, m); err != nil {
				return err
			}
			value, ok = m.Load(I(1300))
			if ok || value != nil {
				return fmt.Errorf(`Load(1300) should be absent after Delete: %v, %v`, value, ok)
			}
			return nil
		}},
	)

	if len(tests) != 50 {
		panic(fmt.Sprintf(`buildAdditionalTestCasesPart2 generated %d tests, want 50`, len(tests)))
	}
	return tests
}

var AllTests = []TestCase{
	{1, "Store and Load string", runCase001StoreLoadString},
	{2, "Store and Load int", runCase002StoreLoadInt},
	{3, "Store and Load struct", runCase003StoreLoadStruct},
	{4, "Store pointer Load same pointer", runCase004StorePointerLoadSamePointer},
	{5, "Store byte slice direct reference", runCase005StoreByteSliceDirectReference},
	{6, "Store string slice direct reference", runCase006StoreStringSliceDirectReference},
	{7, "Short-lived substring key round-trip", runCase007KeyCloneOnInsert},
	{8, "LoadOrStore missing stores", runCase008LoadOrStoreMissingStores},
	{9, "LoadOrStore existing returns", runCase009LoadOrStoreExistingReturns},
	{10, "Swap missing stores", runCase010SwapMissingStores},
	{11, "Swap existing returns previous", runCase011SwapExistingReturnsPrevious},
	{12, "Delete existing returns previous", runCase012DeleteExistingReturnsPrevious},
	{13, "Delete missing returns false", runCase013DeleteMissingReturnsFalse},
	{14, "CompareAndSwap success", runCase014CompareAndSwapSuccess},
	{15, "CompareAndSwap failure", runCase015CompareAndSwapFailure},
	{16, "CompareAndDelete success", runCase016CompareAndDeleteSuccess},
	{17, "CompareAndDelete failure", runCase017CompareAndDeleteFailure},
	{18, "CompareAndSwap nil old", runCase018CompareAndSwapNilOld},
	{19, "CompareAndSwap pointers", runCase019CompareAndSwapPointers},
	{20, "Swap replaces pointer old still usable", runCase020SwapReplacesPointerOldStillUsable},
	{21, "Store overwrites existing", runCase021StoreOverwritesExisting},
	{22, "Store nil value", runCase022StoreNilValue},
	{23, "Delete returns pointer", runCase023DeleteReturnsPointer},
	{24, "Multiple stores only latest visible", runCase024MultipleStoresOnlyLatestVisible},
	{25, "Store empty string key", runCase025StoreEmptyStringKey},
	{26, "Has does not consume hits", runCase026HasDoesNotConsumeHits},
	{27, "Peek does not consume hits", runCase027PeekDoesNotConsumeHits},
	{28, "Has missing key returns false", runCase028HasMissingKeyReturnsFalse},
	{29, "Peek missing key returns false", runCase029PeekMissingKeyReturnsFalse},
	{30, "Get returns value", runCase030GetReturnsValue},
	{31, "Get missing returns false", runCase031GetMissingReturnsFalse},
	{32, "Get consumes hits", runCase032GetConsumesHits},
	{33, "Peek returns correct value", runCase033PeekReturnsCorrectValue},
	{34, "Has after delete returns false", runCase034HasAfterDeleteReturnsFalse},
	{35, "Peek after store returns value", runCase035PeekAfterStoreReturnsValue},
	{36, "TTL before expiry still present", runCase036TTLBeforeExpiryStillPresent},
	{37, "TTL expiry on Load", runCase037TTLExpiryOnLoad},
	{38, "Cleanup now removes expired", runCase038CleanupNowRemovesExpired},
	{39, "Background cleanup removes expired", runCase039BackgroundCleanupRemovesExpired},
	{40, "TTL Peek before expiry", runCase040TTLPeekBeforeExpiry},
	{41, "TTL Peek after expiry", runCase041TTLPeekAfterExpiry},
	{42, "Short TTL expires quickly", runCase042ShortTTLExpiresQuickly},
	{43, "Store overwrite resets TTL", runCase043StoreOverwriteResetsTTL},
	{44, "TTL expires before hits", runCase044TTLExpiresBeforeHits},
	{45, "Hits exhaust before TTL", runCase045HitsExhaustBeforeTTL},
	{46, "Expired invisible to Range", runCase046ExpiredInvisibleToRange},
	{47, "Expired invisible to Snapshot", runCase047ExpiredInvisibleToSnapshot},
	{48, "Expired invisible to Has", runCase048ExpiredInvisibleToHas},
	{49, "Cleanup now on empty map", runCase049CleanupNowOnEmptyMap},
	{50, "Multiple TTL entries", runCase050MultipleTTLEntries},
	{51, "Hits one returns once then gone", runCase051HitsOneReturnsOnceThenGone},
	{52, "Load consumes hits until dead", runCase052LoadConsumesHitsUntilDead},
	{53, "Zero hits immediately dead", runCase053ZeroHitsImmediatelyDead},
	{54, "Negative hits unlimited", runCase054NegativeHitsUnlimited},
	{55, "TTL and hits preserved together", runCase055TTLAndHitsPreservedTogether},
	{56, "Hit exhausted invisible to Range", runCase056HitExhaustedInvisibleToRange},
	{57, "Hit exhausted invisible to Snapshot", runCase057HitExhaustedInvisibleToSnapshot},
	{58, "Hit exhausted invisible to Has", runCase058HitExhaustedInvisibleToHas},
	{59, "Peek does not decrement hits", runCase059PeekDoesNotDecrementHits},
	{60, "Load then Peek on hit limited", runCase060LoadThenPeekOnHitLimited},
	{61, "Range visits all live", runCase061RangeVisitsAllLive},
	{62, "Range early stop", runCase062RangeEarlyStop},
	{63, "Range skips expired and dead", runCase063RangeSkipsExpiredAndDead},
	{64, "Snapshot returns all live", runCase064SnapshotReturnsAllLive},
	{65, "Snapshot skips expired and dead", runCase065SnapshotSkipsExpiredAndDead},
	{66, "Snapshot after delete excludes", runCase066SnapshotAfterDeleteExcludes},
	{67, "Range on empty map", runCase067RangeOnEmptyMap},
	{68, "Snapshot on empty map", runCase068SnapshotOnEmptyMap},
	{69, "Range with concurrent modifications", runCase069RangeWithConcurrentModifications},
	{70, "Snapshot point in time", runCase070SnapshotPointInTime},
	{71, "Clear empties map", runCase071ClearEmptiesMap},
	{72, "Clear shrinks table", runCase072ClearShrinksTable},
	{73, "Len tracks store count", runCase073LenTracksStoreCount},
	{74, "Len decrements on delete", runCase074LenDecrementsOnDelete},
	{75, "Len after clear is zero", runCase075LenAfterClearIsZero},
	{76, "Len tracks concurrent distinct keys", runCase076LenTracksConcurrentDistinctKeys},
	{77, "Len with hit exhausted", runCase077LenWithHitExhausted},
	{78, "Clear after TTL entries", runCase078ClearAfterTTLEntries},
	{79, "Len with mixed store delete", runCase079LenWithMixedStoreDelete},
	{80, "Clear resets stats", runCase080ClearResetsStats},
	{81, "Stats LiveEntries matches Len", runCase081StatsLiveEntriesMatchesLen},
	{82, "Stats track key bytes", runCase082StatsTrackKeyBytes},
	{83, "Stats ValueBytes zero no clone", runCase083StatsValueBytesZeroNoClone},
	{84, "Memory scale down after delete cleanup", runCase084MemoryScaleDownAfterDeleteCleanup},
	{85, "Stats slot capacity grows", runCase085StatsSlotCapacityGrows},
	{86, "Cleanup shrinks oversized table", runCase086CleanupShrinksOversizedTable},
	{87, "Stats after clear", runCase087StatsAfterClear},
	{88, "Stats tombstones after deletes", runCase088StatsTombstonesAfterDeletes},
	{89, "Estimated resident bytes positive", runCase089EstimatedResidentBytesPositive},
	{90, "Stats shards match config", runCase090StatsShardsMatchConfig},
	{91, "Close is idempotent", runCase091CloseIsIdempotent},
	{92, "Close stops background cleanup", runCase092CloseStopsBackgroundCleanup},
	{93, "Multiple close no panic", runCase093MultipleCloseNoPanic},
	{94, "Store after close no panic", runCase094StoreAfterCloseNoPanic},
	{95, "Peek after close works", runCase095PeekAfterCloseWorks},
	{116, "Pointer atomic Int64 Add", runCase116PointerAtomicInt64Add},
	{117, "Pointer atomic Int64 Read", runCase117PointerAtomicInt64Read},
	{118, "Pointer atomic Value Store", runCase118PointerAtomicValueStore},
	{119, "Pointer atomic Value Read", runCase119PointerAtomicValueRead},
	{120, "Pointer mutex map concurrent", runCase120PointerMutexMapConcurrent},
	{121, "Pointer mutex slice concurrent", runCase121PointerMutexSliceConcurrent},
	{122, "Pointer Load returns same address", runCase122PointerLoadReturnsSameAddress},
	{123, "Pointer delete restore", runCase123PointerDeleteRestore},
	{124, "Pointer CompareAndSwap success", runCase124PointerCompareAndSwapSuccess},
	{125, "Pointer CompareAndSwap failure", runCase125PointerCompareAndSwapFailure},
	{126, "Pointer Swap old still valid", runCase126PointerSwapOldStillValid},
	{127, "Pointer TTL expiry makes inaccessible", runCase127PointerTTLExpiryMakesInaccessible},
	{128, "Pointer hit limited accessible", runCase128PointerHitLimitedAccessible},
	{129, "Pointer Range correct", runCase129PointerRangeCorrect},
	{130, "Pointer Snapshot captures", runCase130PointerSnapshotCaptures},
	{131, "Multiple keys different pointer types", runCase131MultipleKeysDifferentPointerTypes},
	{132, "Large struct pointer many atomics", runCase132LargeStructPointerManyAtomics},
	{133, "LoadOrStore pointer single init", runCase133LoadOrStorePointerSingleInit},
	{134, "Interface wrapping pointer", runCase134InterfaceWrappingPointer},
	{135, "Concurrent loads same address", runCase135ConcurrentLoadsSameAddress},
	{136, "Clear map pointer still valid", runCase136ClearMapPointerStillValidInUserCode},
	{137, "TTL expiry pointer remains in user code", runCase137TTLExpiryPointerRemainsInUserCode},
	{138, "Store nil pointer", runCase138StoreNilPointer},
	{139, "Range pointer mutate during iteration", runCase139RangePointerMutateDuringIteration},
	{140, "Snapshot pointer mutate after", runCase140SnapshotPointerMutateAfter},
	{141, "Concurrent store same key", runCase141ConcurrentStoreSameKey},
	{142, "Concurrent load same key", runCase142ConcurrentLoadSameKey},
	{143, "Concurrent store different keys", runCase143ConcurrentStoreDifferentKeys},
	{144, "Concurrent load different keys", runCase144ConcurrentLoadDifferentKeys},
	{145, "Concurrent mixed 50/50", runCase145ConcurrentMixed5050},
	{146, "Concurrent mixed store load delete", runCase146ConcurrentMixedStoreLoadDelete},
	{147, "Concurrent LoadOrStore same key", runCase147ConcurrentLoadOrStoreSameKey},
	{148, "Concurrent LoadOrStore different keys", runCase148ConcurrentLoadOrStoreDifferentKeys},
	{149, "Concurrent Swap same key", runCase149ConcurrentSwapSameKey},
	{150, "Concurrent Delete same key", runCase150ConcurrentDeleteSameKey},
	{151, "Concurrent CAS", runCase151ConcurrentCAS},
	{152, "Concurrent Store with TTL", runCase152ConcurrentStoreWithTTL},
	{153, "Concurrent Store with hits", runCase153ConcurrentStoreWithHits},
	{154, "Concurrent Store with TTL and hits", runCase154ConcurrentStoreWithTTLAndHits},
	{155, "Concurrent Range while writing", runCase155ConcurrentRangeWhileWriting},
	{156, "Concurrent Snapshot while writing", runCase156ConcurrentSnapshotWhileWriting},
	{157, "Concurrent Clear while writing", runCase157ConcurrentClearWhileWriting},
	{158, "Concurrent Cleanup while writing", runCase158ConcurrentCleanupWhileWriting},
	{159, "Hot set contention", runCase159HotSetContention},
	{160, "Large fan out", runCase160LargeFanOut},
	{166, "Concurrent atomic Int64 Add 16 goroutines", runCase166ConcurrentAtomicInt64Add},
	{167, "Concurrent atomic Value Store", runCase167ConcurrentAtomicValueStore},
	{168, "Concurrent mutex map writes", runCase168ConcurrentMutexMapWrites},
	{169, "Concurrent mutex slice appends", runCase169ConcurrentMutexSliceAppends},
	{170, "Mixed pointer reads and writes", runCase170MixedPointerReadsWrites},
	{171, "Concurrent Load and atomic mutation", runCase171ConcurrentLoadAndAtomicMutation},
	{172, "Store N pointers increment all", runCase172StoreNPointersIncrementAll},
	{173, "Concurrent Load pointer same address", runCase173ConcurrentLoadPointerSameAddress},
	{174, "LoadOrStore pointer parallel mutation", runCase174LoadOrStorePointerParallelMutation},
	{175, "Pointer mutation with TTL", runCase175PointerMutationWithTTL},
	{176, "Pointer mutation with hit limited", runCase176PointerMutationWithHitLimited},
	{177, "Concurrent Range reads pointer fields", runCase177ConcurrentRangeReadsPointerFields},
	{178, "Concurrent Snapshot mutate after", runCase178ConcurrentSnapshotMutateAfter},
	{179, "High contention few keys pointer", runCase179HighContentionFewKeysPointer},
	{180, "Producer consumer pointer pattern", runCase180ProducerConsumerPointer},
	{181, "Store and Delete race", runCase181StoreDeleteRace},
	{182, "Store and Clear race", runCase182StoreClearRace},
	{183, "LoadOrStore and Delete race", runCase183LoadOrStoreDeleteRace},
	{184, "Swap and Delete race", runCase184SwapDeleteRace},
	{185, "CAS and Store race", runCase185CASStoreRace},
	{186, "Range and Delete race", runCase186RangeDeleteRace},
	{187, "TTL expiry and Load race", runCase187TTLExpiryLoadRace},
	{188, "Hit exhaustion and Load race", runCase188HitExhaustionLoadRace},
	{189, "Cleanup and Load race", runCase189CleanupLoadRace},
	{190, "Background cleanup and Store race", runCase190BackgroundCleanupStoreRace},
	{191, "Very long key", runCase191VeryLongKey},
	{192, "Nil value store and load", runCase192NilValue},
	{193, "Boolean value", runCase193BooleanValue},
	{194, "Channel value", runCase194ChannelValue},
	{195, "Map value mutation visible", runCase195MapValue},
	{196, "Func value callable", runCase196FuncValue},
	{197, "Interface wrapping pointer", runCase197InterfaceWrapping},
	{198, "Large entry count 10K", runCase198LargeEntryCount},
	{199, "Rapid store-delete cycle", runCase199RapidStoreDeleteCycle},
	{200, "Stats consistency after concurrent workload", runCase200StatsConsistencyAfterConcurrentWorkload},
}

func init() {
	AllTests = append(AllTests, buildAdditionalTestCasesPart1()...)
	AllTests = append(AllTests, buildAdditionalTestCasesPart2()...)
}
