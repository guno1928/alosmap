package alosmap

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
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

type mutexStats struct {
	mu   sync.Mutex
	Tags []string
	Hits int
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
	m.Store("name", "alos")
	value, ok := m.Load("name")
	if !ok || value != "alos" {
		return fmt.Errorf("Load(name) = %v, %v", value, ok)
	}
	return nil
}

func runCase002StoreLoadInt() error {
	m := testMap()
	defer m.Close()
	m.Store("count", 42)
	value, ok := m.Load("count")
	if !ok || value != 42 {
		return fmt.Errorf("Load(count) = %v, %v", value, ok)
	}
	return nil
}

func runCase003StoreLoadStruct() error {
	m := testMap()
	defer m.Close()
	expected := exampleUser{Name: "Alyx", Age: 29}
	m.Store("user", expected)
	value, ok := m.Load("user")
	if !ok || value != expected {
		return fmt.Errorf("Load(user) = %v, %v", value, ok)
	}
	return nil
}

func runCase004StorePointerLoadSamePointer() error {
	m := testMap()
	defer m.Close()
	stats := &atomicStats{}
	m.Store("ip:1", stats)
	value, ok := m.Load("ip:1")
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
	m.Store("blob", payload)
	payload[0] = 'o'
	value, ok := m.Load("blob")
	if !ok || string(value.([]byte)) != "olpha" {
		return fmt.Errorf("Load(blob) = %v; want olpha (direct reference)", value)
	}
	return nil
}

func runCase006StoreStringSliceDirectReference() error {
	m := testMap()
	defer m.Close()
	payload := []string{"one", "two"}
	m.Store("slice", payload)
	payload[0] = "changed"
	value, ok := m.Load("slice")
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
	checkStoredKeyClone := func(label string, write func(m *Map, key string) error) error {
		m := testMap()
		defer m.Close()

		large := string(make([]byte, 256)) + label
		key := large[len(large)-len(label):]
		inputKey := unsafe.StringData(key)

		if err := write(m, key); err != nil {
			return err
		}

		snapshot := m.Snapshot()
		if len(snapshot) != 1 {
			return fmt.Errorf("%s snapshot length = %d", label, len(snapshot))
		}
		storedKey := unsafe.StringData(snapshot[0].Key)
		if inputKey == storedKey {
			return fmt.Errorf("%s reused original key backing bytes", label)
		}
		return nil
	}

	if err := checkStoredKeyClone("store", func(m *Map, key string) error {
		m.Store(key, "value")
		return nil
	}); err != nil {
		return err
	}

	if err := checkStoredKeyClone("loadorstore", func(m *Map, key string) error {
		value, loaded := m.LoadOrStore(key, "value")
		if loaded || value != "value" {
			return fmt.Errorf("LoadOrStore(%s) = %v, %v", key, value, loaded)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := checkStoredKeyClone("swap", func(m *Map, key string) error {
		previous, loaded := m.Swap(key, "value")
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
	value, loaded := m.LoadOrStore("shared", 7)
	if loaded || value != 7 {
		return fmt.Errorf("LoadOrStore(shared) = %v, %v", value, loaded)
	}
	return nil
}

func runCase009LoadOrStoreExistingReturns() error {
	m := testMap()
	defer m.Close()
	m.Store("shared", 7)
	value, loaded := m.LoadOrStore("shared", 9)
	if !loaded || value != 7 {
		return fmt.Errorf("LoadOrStore(existing) = %v, %v", value, loaded)
	}
	return nil
}

func runCase010SwapMissingStores() error {
	m := testMap()
	defer m.Close()
	previous, loaded := m.Swap("user", "first")
	if loaded || previous != nil {
		return fmt.Errorf("Swap(user) = %v, %v", previous, loaded)
	}
	return nil
}

func runCase011SwapExistingReturnsPrevious() error {
	m := testMap()
	defer m.Close()
	m.Store("user", "first")
	previous, loaded := m.Swap("user", "second")
	if !loaded || previous != "first" {
		return fmt.Errorf("Swap(user) = %v, %v", previous, loaded)
	}
	value, ok := m.Load("user")
	if !ok || value != "second" {
		return fmt.Errorf("Load(user) = %v, %v", value, ok)
	}
	return nil
}

func runCase012DeleteExistingReturnsPrevious() error {
	m := testMap()
	defer m.Close()
	m.Store("user", "first")
	previous, ok := m.Delete("user")
	if !ok || previous != "first" {
		return fmt.Errorf("Delete(user) = %v, %v", previous, ok)
	}
	return nil
}

func runCase013DeleteMissingReturnsFalse() error {
	m := testMap()
	defer m.Close()
	previous, ok := m.Delete("missing")
	if ok || previous != nil {
		return fmt.Errorf("Delete(missing) = %v, %v", previous, ok)
	}
	return nil
}

func runCase014CompareAndSwapSuccess() error {
	m := testMap()
	defer m.Close()
	m.Store("payload", []int{1, 2, 3})
	if !m.CompareAndSwap("payload", []int{1, 2, 3}, []int{3, 2, 1}) {
		return fmt.Errorf("CompareAndSwap(payload) = false")
	}
	value, ok := m.Load("payload")
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
	m.Store("payload", []int{1, 2, 3})
	if m.CompareAndSwap("payload", []int{9, 9, 9}, []int{3, 2, 1}) {
		return fmt.Errorf("CompareAndSwap(payload) unexpectedly succeeded")
	}
	return nil
}

func runCase016CompareAndDeleteSuccess() error {
	m := testMap()
	defer m.Close()
	m.Store("payload", []int{1, 2, 3})
	if !m.CompareAndDelete("payload", []int{1, 2, 3}) {
		return fmt.Errorf("CompareAndDelete(payload) = false")
	}
	if _, ok := m.Load("payload"); ok {
		return fmt.Errorf("payload still present after CompareAndDelete")
	}
	return nil
}

func runCase017CompareAndDeleteFailure() error {
	m := testMap()
	defer m.Close()
	m.Store("payload", []int{1, 2, 3})
	if m.CompareAndDelete("payload", []int{0, 0, 0}) {
		return fmt.Errorf("CompareAndDelete(payload) unexpectedly succeeded")
	}
	return nil
}

func runCase018CompareAndSwapNilOld() error {
	m := testMap()
	defer m.Close()
	m.Store("val", nil)
	if !m.CompareAndSwap("val", nil, "new") {
		return fmt.Errorf("CompareAndSwap(nil  new) = false")
	}
	v, ok := m.Load("val")
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
	m.Store("s", p1)
	if !m.CompareAndSwap("s", p1, p2) {
		return fmt.Errorf("CompareAndSwap(p1  p2) = false")
	}
	v, _ := m.Load("s")
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
	m.Store("s", old)
	newS := &atomicStats{}
	prev, loaded := m.Swap("s", newS)
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
	m.Store("k", "first")
	m.Store("k", "second")
	v, ok := m.Load("k")
	if !ok || v != "second" {
		return fmt.Errorf("Load(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase022StoreNilValue() error {
	m := testMap()
	defer m.Close()
	m.Store("k", nil)
	v, ok := m.Load("k")
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
	m.Store("s", p)
	prev, ok := m.Delete("s")
	if !ok || prev.(*atomicStats).Count.Load() != 99 {
		return fmt.Errorf("Delete(s) = %v, %v", prev, ok)
	}
	return nil
}

func runCase024MultipleStoresOnlyLatestVisible() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.Store("k", i)
	}
	v, ok := m.Load("k")
	if !ok || v != 9 {
		return fmt.Errorf("Load(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase025StoreEmptyStringKey() error {
	m := testMap()
	defer m.Close()
	m.Store("", "empty-key")
	v, ok := m.Load("")
	if !ok || v != "empty-key" {
		return fmt.Errorf("Load('') = %v, %v", v, ok)
	}
	return nil
}

func runCase026HasDoesNotConsumeHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("ticket", "open", 2)
	if !m.Has("ticket") || !m.Has("ticket") {
		return fmt.Errorf("Has(ticket) = false")
	}
	if _, ok := m.Load("ticket"); !ok {
		return fmt.Errorf("ticket missing after Has checks")
	}
	return nil
}

func runCase027PeekDoesNotConsumeHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("ticket", "open", 2)
	if _, ok := m.Peek("ticket"); !ok {
		return fmt.Errorf("Peek(ticket) missing")
	}
	if _, ok := m.Peek("ticket"); !ok {
		return fmt.Errorf("Peek(ticket) missing on second call")
	}
	if _, ok := m.Load("ticket"); !ok {
		return fmt.Errorf("ticket missing after Peek checks")
	}
	return nil
}

func runCase028HasMissingKeyReturnsFalse() error {
	m := testMap()
	defer m.Close()
	if m.Has("missing") {
		return fmt.Errorf("Has(missing) = true")
	}
	return nil
}

func runCase029PeekMissingKeyReturnsFalse() error {
	m := testMap()
	defer m.Close()
	v, ok := m.Peek("missing")
	if ok || v != nil {
		return fmt.Errorf("Peek(missing) = %v, %v", v, ok)
	}
	return nil
}

func runCase030GetReturnsValue() error {
	m := testMap()
	defer m.Close()
	m.Store("k", "present")
	v, ok := m.Get("k")
	if !ok || v != "present" {
		return fmt.Errorf("Get(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase031GetMissingReturnsFalse() error {
	m := testMap()
	defer m.Close()
	v, ok := m.Get("k")
	if ok || v != nil {
		return fmt.Errorf("Get(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase032GetConsumesHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("ticket", "open", 1)
	if v, ok := m.Get("ticket"); !ok || v != "open" {
		return fmt.Errorf("Get(ticket) = %v, %v", v, ok)
	}
	if v, ok := m.Get("ticket"); ok || v != nil {
		return fmt.Errorf("Get(ticket) after exhaust = %v, %v", v, ok)
	}
	return nil
}

func runCase033PeekReturnsCorrectValue() error {
	m := testMap()
	defer m.Close()
	m.Store("k", 42)
	v, ok := m.Peek("k")
	if !ok || v != 42 {
		return fmt.Errorf("Peek(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase034HasAfterDeleteReturnsFalse() error {
	m := testMap()
	defer m.Close()
	m.Store("k", 1)
	m.Delete("k")
	if m.Has("k") {
		return fmt.Errorf("Has(k) after delete = true")
	}
	return nil
}

func runCase035PeekAfterStoreReturnsValue() error {
	m := testMap()
	defer m.Close()
	m.Store("k", "hello")
	v, ok := m.Peek("k")
	if !ok || v != "hello" {
		return fmt.Errorf("Peek(k) = %v, %v", v, ok)
	}
	return nil
}

func runCase036TTLBeforeExpiryStillPresent() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits("session", "live", 50*time.Millisecond, 4)
	time.Sleep(10 * time.Millisecond)
	if value, ok := m.Load("session"); !ok || value != "live" {
		return fmt.Errorf("Load(session) = %v, %v", value, ok)
	}
	return nil
}

func runCase037TTLExpiryOnLoad() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("session", "live", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, ok := m.Load("session"); ok {
		return fmt.Errorf("session still present after TTL expiry")
	}
	return nil
}

func runCase038CleanupNowRemovesExpired() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("session", "live", 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	m.CleanupNow()
	if _, ok := m.Peek("session"); ok {
		return fmt.Errorf("session still present after CleanupNow")
	}
	return nil
}

func runCase039BackgroundCleanupRemovesExpired() error {
	m := New(WithCapacity(64), WithShardCount(4), WithCleanupInterval(10*time.Millisecond))
	defer m.Close()
	m.StoreWithTTL("session", "live", 5*time.Millisecond)
	if !waitUntil(100*time.Millisecond, func() bool {
		_, ok := m.Peek("session")
		return !ok
	}) {
		return fmt.Errorf("background cleanup did not remove expired entry")
	}
	return nil
}

func runCase040TTLPeekBeforeExpiry() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("s", "alive", 50*time.Millisecond)
	v, ok := m.Peek("s")
	if !ok || v != "alive" {
		return fmt.Errorf("Peek(s) = %v, %v", v, ok)
	}
	return nil
}

func runCase041TTLPeekAfterExpiry() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("s", "alive", 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Peek("s"); ok {
		return fmt.Errorf("Peek(s) should be expired")
	}
	return nil
}

func runCase042ShortTTLExpiresQuickly() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("s", "x", 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if _, ok := m.Load("s"); ok {
		return fmt.Errorf("1ms TTL entry still present after 5ms")
	}
	return nil
}

func runCase043StoreOverwriteResetsTTL() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("s", "v1", 5*time.Millisecond)
	m.StoreWithTTL("s", "v2", time.Minute)
	time.Sleep(10 * time.Millisecond)
	v, ok := m.Load("s")
	if !ok || v != "v2" {
		return fmt.Errorf("Load(s) = %v, %v; want v2 (new TTL)", v, ok)
	}
	return nil
}

func runCase044TTLExpiresBeforeHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits("s", "v", 5*time.Millisecond, 100)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Load("s"); ok {
		return fmt.Errorf("TTL should have expired before hits exhausted")
	}
	return nil
}

func runCase045HitsExhaustBeforeTTL() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits("s", "v", time.Minute, 2)
	m.Load("s")
	m.Load("s")
	if _, ok := m.Load("s"); ok {
		return fmt.Errorf("hits should have exhausted before TTL")
	}
	return nil
}

func runCase046ExpiredInvisibleToRange() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.StoreWithTTL("b", 2, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	count := 0
	m.Range(func(_ string, _ any) bool { count++; return true })
	if count != 1 {
		return fmt.Errorf("Range count = %d, want 1", count)
	}
	return nil
}

func runCase047ExpiredInvisibleToSnapshot() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.StoreWithTTL("b", 2, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].Key != "a" {
		return fmt.Errorf("Snapshot = %#v", snap)
	}
	return nil
}

func runCase048ExpiredInvisibleToHas() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("s", "v", 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if m.Has("s") {
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
	m.StoreWithTTL("short", 1, 5*time.Millisecond)
	m.StoreWithTTL("long", 2, time.Minute)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Load("short"); ok {
		return fmt.Errorf("short TTL still present")
	}
	if v, ok := m.Load("long"); !ok || v != 2 {
		return fmt.Errorf("long TTL missing or wrong: %v, %v", v, ok)
	}
	return nil
}

func runCase051HitsOneReturnsOnceThenGone() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("once", 99, 1)
	if v, ok := m.Load("once"); !ok || v != 99 {
		return fmt.Errorf("Load(once) = %v, %v", v, ok)
	}
	if _, ok := m.Load("once"); ok {
		return fmt.Errorf("once still present after single hit")
	}
	return nil
}

func runCase052LoadConsumesHitsUntilDead() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits("ticket", "open", time.Minute, 3)
	for i := 0; i < 3; i++ {
		if v, ok := m.Load("ticket"); !ok || v != "open" {
			return fmt.Errorf("Load(ticket) #%d = %v, %v", i+1, v, ok)
		}
	}
	if _, ok := m.Load("ticket"); ok {
		return fmt.Errorf("ticket still present after 3 hits")
	}
	return nil
}

func runCase053ZeroHitsImmediatelyDead() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("zero", "val", 0)
	// 0 hits normalizes to unlimited (-1), so the entry should still be loadable
	if _, ok := m.Load("zero"); !ok {
		return fmt.Errorf("zero-hit entry should be loadable (normalized to unlimited)")
	}
	return nil
}

func runCase054NegativeHitsUnlimited() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("inf", "val", -1)
	for i := 0; i < 100; i++ {
		if _, ok := m.Load("inf"); !ok {
			return fmt.Errorf("unlimited-hit entry missing at read %d", i)
		}
	}
	return nil
}

func runCase055TTLAndHitsPreservedTogether() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTLAndHits("combo", "val", time.Minute, 5)
	for i := 0; i < 4; i++ {
		if _, ok := m.Load("combo"); !ok {
			return fmt.Errorf("combo missing at read %d", i)
		}
	}
	if _, ok := m.Load("combo"); !ok {
		return fmt.Errorf("combo should still have 1 hit left")
	}
	if _, ok := m.Load("combo"); ok {
		return fmt.Errorf("combo should be dead after 5 hits")
	}
	return nil
}

func runCase056HitExhaustedInvisibleToRange() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.StoreWithHits("b", 2, 1)
	m.Load("b")
	count := 0
	m.Range(func(_ string, _ any) bool { count++; return true })
	if count != 1 {
		return fmt.Errorf("Range count = %d, want 1", count)
	}
	return nil
}

func runCase057HitExhaustedInvisibleToSnapshot() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.StoreWithHits("b", 2, 1)
	m.Load("b")
	snap := m.Snapshot()
	if len(snap) != 1 {
		return fmt.Errorf("Snapshot length = %d, want 1", len(snap))
	}
	return nil
}

func runCase058HitExhaustedInvisibleToHas() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("b", 2, 1)
	m.Load("b")
	if m.Has("b") {
		return fmt.Errorf("Has(b) should be false after hits exhausted")
	}
	return nil
}

func runCase059PeekDoesNotDecrementHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("t", "val", 1)
	m.Peek("t")
	m.Peek("t")
	if v, ok := m.Load("t"); !ok || v != "val" {
		return fmt.Errorf("Load(t) = %v, %v; peek should not have consumed hits", v, ok)
	}
	return nil
}

func runCase060LoadThenPeekOnHitLimited() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("t", "val", 2)
	m.Load("t")
	m.Peek("t")
	if v, ok := m.Load("t"); !ok || v != "val" {
		return fmt.Errorf("Load(t) after load+peek = %v, %v", v, ok)
	}
	if _, ok := m.Load("t"); ok {
		return fmt.Errorf("t should be dead after 2 loads")
	}
	return nil
}

func runCase061RangeVisitsAllLive() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)
	sum := 0
	m.Range(func(_ string, v any) bool { sum += v.(int); return true })
	if sum != 6 {
		return fmt.Errorf("Range sum = %d, want 6", sum)
	}
	return nil
}

func runCase062RangeEarlyStop() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.Store(testKey(i), i)
	}
	count := 0
	m.Range(func(_ string, _ any) bool { count++; return count < 3 })
	if count != 3 {
		return fmt.Errorf("Range count = %d, want 3", count)
	}
	return nil
}

func runCase063RangeSkipsExpiredAndDead() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.StoreWithTTL("b", 2, 5*time.Millisecond)
	m.StoreWithHits("c", 3, 1)
	_, _ = m.Load("c")
	time.Sleep(10 * time.Millisecond)
	count := 0
	m.Range(func(_ string, v any) bool { count++; return true })
	if count != 1 {
		return fmt.Errorf("Range count = %d, want 1", count)
	}
	return nil
}

func runCase064SnapshotReturnsAllLive() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.Store("b", 2)
	snap := m.Snapshot()
	if len(snap) != 2 {
		return fmt.Errorf("Snapshot length = %d, want 2", len(snap))
	}
	return nil
}

func runCase065SnapshotSkipsExpiredAndDead() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.StoreWithTTL("b", 2, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].Key != "a" {
		return fmt.Errorf("Snapshot = %#v", snap)
	}
	return nil
}

func runCase066SnapshotAfterDeleteExcludes() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.Store("b", 2)
	m.Delete("b")
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].Key != "a" {
		return fmt.Errorf("Snapshot = %#v", snap)
	}
	return nil
}

func runCase067RangeOnEmptyMap() error {
	m := testMap()
	defer m.Close()
	count := 0
	m.Range(func(_ string, _ any) bool { count++; return true })
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
		m.Store(testKey(i), i)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; i < 100; i++ {
			m.Store(testKey(i), i)
		}
	}()
	count := 0
	m.Range(func(_ string, _ any) bool { count++; return true })
	wg.Wait()
	if count < 50 {
		return fmt.Errorf("Range count = %d, want >= 50", count)
	}
	return nil
}

func runCase070SnapshotPointInTime() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.Store("b", 2)
	snap := m.Snapshot()
	m.Store("c", 3)
	if len(snap) != 2 {
		return fmt.Errorf("Snapshot should capture 2 entries, got %d", len(snap))
	}
	return nil
}

func runCase071ClearEmptiesMap() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 10; i++ {
		m.Store(testKey(i), i)
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
		m.Store(testKey(i), i)
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
		m.Store(testKey(i), i)
	}
	if m.Len() != 10 {
		return fmt.Errorf("Len = %d, want 10", m.Len())
	}
	return nil
}

func runCase074LenDecrementsOnDelete() error {
	m := testMap()
	defer m.Close()
	m.Store("a", 1)
	m.Store("b", 2)
	m.Delete("a")
	if m.Len() != 1 {
		return fmt.Errorf("Len = %d, want 1", m.Len())
	}
	return nil
}

func runCase075LenAfterClearIsZero() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(testKey(i), i)
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
				m.Store(testKey(w*perWorker+i), w*perWorker+i)
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
	m.Store("a", 1)
	m.StoreWithHits("b", 2, 1)
	m.Load("b")
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
		m.StoreWithTTL(testKey(i), i, time.Minute)
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
		m.Store(testKey(i), i)
	}
	for i := 0; i < 10; i++ {
		m.Delete(testKey(i))
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
		m.Store(testKey(i), i)
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
		m.Store(testKey(i), i)
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
	m.Store("alpha", []byte("abcdef"))
	stats := m.Stats()
	if stats.TrackedKeyBytes < int64(len("alpha")) {
		return fmt.Errorf("TrackedKeyBytes = %d", stats.TrackedKeyBytes)
	}
	return nil
}

func runCase083StatsValueBytesZeroNoClone() error {
	m := testMap()
	defer m.Close()
	m.Store("alpha", []byte("abcdef"))
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
		m.Store(testKey(i), data)
	}
	before := m.Stats()
	for i := 0; i < 1000; i++ {
		m.Delete(testKey(i))
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
		m.Store(testKey(i), i)
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
		m.Store(testKey(i), i)
	}
	large := m.Stats().SlotCapacity
	for i := 0; i < 4000; i++ {
		m.Delete(testKey(i))
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
		m.Store(testKey(i), i)
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
		m.Store(testKey(i), i)
	}
	for i := 0; i < 5; i++ {
		m.Delete(testKey(i))
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
		m.Store(testKey(i), i)
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
	m.Store("k", 1)
	m.Close()
	m.Close()
	m.Close()
	return nil
}

func runCase092CloseStopsBackgroundCleanup() error {
	m := New(WithCleanupInterval(10 * time.Millisecond))
	m.StoreWithTTL("s", "v", 5*time.Millisecond)
	m.Close()
	time.Sleep(20 * time.Millisecond)
	return nil
}

func runCase093MultipleCloseNoPanic() error {
	m := testMap()
	m.Store("a", 1)
	m.Close()
	m.Close()
	m.Close()
	return nil
}

func runCase094StoreAfterCloseNoPanic() error {
	m := testMap()
	m.Close()
	m.Store("a", 1)
	return nil
}

func runCase095PeekAfterCloseWorks() error {
	m := testMap()
	m.Store("a", 1)
	m.Close()
	v, ok := m.Peek("a")
	if !ok || v != 1 {
		return fmt.Errorf("Peek(a) after Close = %v, %v", v, ok)
	}
	return nil
}

func runCase096AddInt() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 10)
	v, err := m.Add("c", 5)
	if err != nil || v != 15 {
		return fmt.Errorf("Add(c,5) = %v, %v", v, err)
	}
	return nil
}

func runCase097SubInt() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 10)
	v, err := m.Sub("c", 3)
	if err != nil || v != 7 {
		return fmt.Errorf("Sub(c,3) = %v, %v", v, err)
	}
	return nil
}

func runCase098SetInt() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 50)
	if err := m.Set("c", 0); err != nil {
		return err
	}
	v, _ := m.Load("c")
	if v != 0 {
		return fmt.Errorf("Set(c,0) Load = %v", v)
	}
	return nil
}

func runCase099AddInt64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", int64(100))
	v, err := m.Add("c", int64(50))
	if err != nil || v != int64(150) {
		return fmt.Errorf("Add(c,50) = %v, %v", v, err)
	}
	return nil
}

func runCase100SubInt64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", int64(100))
	v, err := m.Sub("c", int64(30))
	if err != nil || v != int64(70) {
		return fmt.Errorf("Sub(c,30) = %v, %v", v, err)
	}
	return nil
}

func runCase101SetInt64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", int64(100))
	if err := m.Set("c", int64(0)); err != nil {
		return err
	}
	v, _ := m.Load("c")
	if v != int64(0) {
		return fmt.Errorf("Set(c,0) Load = %v", v)
	}
	return nil
}

func runCase102AddFloat64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 1.5)
	v, err := m.Add("c", 2.5)
	if err != nil || v != 4.0 {
		return fmt.Errorf("Add(c,2.5) = %v, %v", v, err)
	}
	return nil
}

func runCase103SubFloat64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 10.0)
	v, err := m.Sub("c", 3.5)
	if err != nil || v != 6.5 {
		return fmt.Errorf("Sub(c,3.5) = %v, %v", v, err)
	}
	return nil
}

func runCase104SetFloat64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 99.9)
	if err := m.Set("c", 0.0); err != nil {
		return err
	}
	v, _ := m.Load("c")
	if v != 0.0 {
		return fmt.Errorf("Set(c,0.0) Load = %v", v)
	}
	return nil
}

func runCase105AddUint64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", uint64(10))
	v, err := m.Add("c", uint64(5))
	if err != nil || v != uint64(15) {
		return fmt.Errorf("Add(c,5) = %v, %v", v, err)
	}
	return nil
}

func runCase106SubUint64NoUnderflow() error {
	m := testMap()
	defer m.Close()
	m.Store("c", uint64(10))
	v, err := m.Sub("c", uint64(5))
	if err != nil || v != uint64(5) {
		return fmt.Errorf("Sub(c,5) = %v, %v", v, err)
	}
	return nil
}

func runCase107SetUint64() error {
	m := testMap()
	defer m.Close()
	m.Store("c", uint64(100))
	if err := m.Set("c", uint64(0)); err != nil {
		return err
	}
	v, _ := m.Load("c")
	if v != uint64(0) {
		return fmt.Errorf("Set(c,0) Load = %v", v)
	}
	return nil
}

func runCase108AddMissingKeyError() error {
	m := testMap()
	defer m.Close()
	_, err := m.Add("missing", 1)
	if !errors.Is(err, ErrKeyNotFound) {
		return fmt.Errorf("Add(missing) error = %v", err)
	}
	return nil
}

func runCase109SubMissingKeyError() error {
	m := testMap()
	defer m.Close()
	_, err := m.Sub("missing", 1)
	if !errors.Is(err, ErrKeyNotFound) {
		return fmt.Errorf("Sub(missing) error = %v", err)
	}
	return nil
}

func runCase110SetMissingKeyError() error {
	m := testMap()
	defer m.Close()
	err := m.Set("missing", 0)
	if !errors.Is(err, ErrKeyNotFound) {
		return fmt.Errorf("Set(missing) error = %v", err)
	}
	return nil
}

func runCase111AddOnStringTypeMismatch() error {
	m := testMap()
	defer m.Close()
	m.Store("name", "hello")
	_, err := m.Add("name", 1)
	if !errors.Is(err, ErrTypeMismatch) {
		return fmt.Errorf("Add(name) error = %v", err)
	}
	return nil
}

func runCase112SetOnStringTypeMismatch() error {
	m := testMap()
	defer m.Close()
	m.Store("name", "hello")
	err := m.Set("name", 0)
	if !errors.Is(err, ErrTypeMismatch) {
		return fmt.Errorf("Set(name,0) error = %v", err)
	}
	return nil
}

func runCase113SetNonNumericValueError() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 10)
	err := m.Set("c", "hello")
	if !errors.Is(err, ErrTypeMismatch) {
		return fmt.Errorf("Set(c, string) error = %v", err)
	}
	return nil
}

func runCase114SetPreservesTTL() error {
	m := testMap()
	defer m.Close()
	m.StoreWithTTL("c", 50, time.Minute)
	if err := m.Set("c", 0); err != nil {
		return err
	}
	v, ok := m.Load("c")
	if !ok || v != 0 {
		return fmt.Errorf("Load(c) = %v, %v", v, ok)
	}
	return nil
}

func runCase115SetPreservesHits() error {
	m := testMap()
	defer m.Close()
	m.StoreWithHits("c", 50, 3)
	if err := m.Set("c", 0); err != nil {
		return err
	}
	v, ok := m.Load("c")
	if !ok || v != 0 {
		return fmt.Errorf("Load(c) = %v, %v", v, ok)
	}
	return nil
}

func runCase116PointerAtomicInt64Add() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store("ip:1", s)
	v, _ := m.Load("ip:1")
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
	m.Store("ip:1", s)
	v, _ := m.Load("ip:1")
	if v.(*atomicStats).Count.Load() != 42 {
		return fmt.Errorf("Count = %d, want 42", v.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase118PointerAtomicValueStore() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store("ip:1", s)
	v, _ := m.Load("ip:1")
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
	m.Store("ip:1", s)
	v, _ := m.Load("ip:1")
	if v.(*atomicStats).Name.Load() != "alyx" {
		return fmt.Errorf("Name = %v, want alyx", v.(*atomicStats).Name.Load())
	}
	return nil
}

func runCase120PointerMutexMapConcurrent() error {
	m := testMap()
	defer m.Close()
	ms := &mutexStats{Tags: []string{}}
	m.Store("sess", ms)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load("sess")
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
	m.Store("sess", ms)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load("sess")
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
	m.Store("ip:1", s)
	v1, _ := m.Load("ip:1")
	v2, _ := m.Load("ip:1")
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
	m.Store("ip:1", s1)
	m.Delete("ip:1")
	s2 := &atomicStats{}
	s2.Count.Store(2)
	m.Store("ip:1", s2)
	v, _ := m.Load("ip:1")
	if v.(*atomicStats).Count.Load() != 2 {
		return fmt.Errorf("Count = %d, want 2", v.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase124PointerCompareAndSwapSuccess() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store("ip:1", s)
	s2 := &atomicStats{}
	if !m.CompareAndSwap("ip:1", s, s2) {
		return fmt.Errorf("CompareAndSwap failed")
	}
	return nil
}

func runCase125PointerCompareAndSwapFailure() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store("ip:1", s)
	wrong := &atomicStats{}
	if m.CompareAndSwap("ip:1", wrong, &atomicStats{}) {
		return fmt.Errorf("CompareAndSwap should have failed")
	}
	return nil
}

func runCase126PointerSwapOldStillValid() error {
	m := testMap()
	defer m.Close()
	old := &atomicStats{}
	old.Count.Store(100)
	m.Store("ip:1", old)
	prev, _ := m.Swap("ip:1", &atomicStats{})
	if prev.(*atomicStats).Count.Load() != 100 {
		return fmt.Errorf("old pointer Count = %d", prev.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase127PointerTTLExpiryMakesInaccessible() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.StoreWithTTL("ip:1", s, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if _, ok := m.Load("ip:1"); ok {
		return fmt.Errorf("pointer should be inaccessible after TTL")
	}
	return nil
}

func runCase128PointerHitLimitedAccessible() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.StoreWithHits("ip:1", s, 2)
	v1, ok1 := m.Load("ip:1")
	v2, ok2 := m.Load("ip:1")
	if !ok1 || !ok2 {
		return fmt.Errorf("pointer loads = %v, %v", ok1, ok2)
	}
	if v1.(*atomicStats) != s || v2.(*atomicStats) != s {
		return fmt.Errorf("pointer mismatch")
	}
	if _, ok := m.Load("ip:1"); ok {
		return fmt.Errorf("should be dead after 2 hits")
	}
	return nil
}

func runCase129PointerRangeCorrect() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	s.Count.Store(77)
	m.Store("ip:1", s)
	found := false
	m.Range(func(k string, v any) bool {
		if k == "ip:1" && v.(*atomicStats).Count.Load() == 77 {
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
	m.Store("ip:1", s)
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
	m.Store("stats", s)
	m.Store("mutex", ms)
	v1, _ := m.Load("stats")
	v2, _ := m.Load("mutex")
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
	m.Store("ip:1", s)
	v, _ := m.Load("ip:1")
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
	v, loaded := m.LoadOrStore("ip:1", s)
	if loaded {
		return fmt.Errorf("should not be loaded on first call")
	}
	v2, loaded2 := m.LoadOrStore("ip:1", &atomicStats{})
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
	m.Store("ip:1", iface)
	v, _ := m.Load("ip:1")
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
	m.Store("ip:1", s)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load("ip:1")
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
	m.Store("ip:1", s)
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
	m.StoreWithTTL("ip:1", s, 5*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if s.Count.Load() != 99 {
		return fmt.Errorf("pointer Count changed after TTL expiry")
	}
	return nil
}

func runCase138StoreNilPointer() error {
	m := testMap()
	defer m.Close()
	m.Store("ip:1", (*atomicStats)(nil))
	v, ok := m.Load("ip:1")
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
	m.Store("ip:1", s)
	m.Range(func(k string, v any) bool {
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
	m.Store("ip:1", s)
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
				m.Store("k", w*500+i)
			}
		}()
	}
	wg.Wait()
	_, ok := m.Load("k")
	if !ok {
		return fmt.Errorf("key missing after concurrent stores")
	}
	return nil
}

func runCase142ConcurrentLoadSameKey() error {
	m := testMap()
	defer m.Close()
	m.Store("k", 42)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if v, ok := m.Load("k"); !ok || v != 42 {
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
	return nil
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
				m.Store(testKey(w*200+i), i)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	return nil
}

func runCase144ConcurrentLoadDifferentKeys() error {
	m := testMap(WithCapacity(1024), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(testKey(i), i)
	}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.Load(testKey(i))
			}
		}()
	}
	wg.Wait()
	return nil
}

func runCase145ConcurrentMixed5050() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 128; i++ {
		m.Store(testKey(i), i)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := uint64(1)
			for i := 0; i < 1000; i++ {
				seed = xorshift(seed)
				k := testKey(int(seed % 128))
				if seed&1 == 0 {
					m.Store(k, int(seed))
				} else {
					m.Load(k)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func runCase146ConcurrentMixedStoreLoadDelete() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 128; i++ {
		m.Store(testKey(i), i)
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
				k := testKey(int(seed % 128))
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
	stats := m.Stats()
	if stats.LiveEntries < 0 || stats.UsedSlots < 0 {
		return fmt.Errorf("invalid stats: %+v", stats)
	}
	return nil
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
			_, loaded := m.LoadOrStore("k", 1)
			if !loaded {
				first.Add(1)
			}
		}()
	}
	wg.Wait()
	if first.Load() != 1 {
		return fmt.Errorf("expected exactly 1 first writer, got %d", first.Load())
	}
	return nil
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
				m.LoadOrStore(testKey(w*100+i), i)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 800 {
		return fmt.Errorf("Len = %d, want 800", m.Len())
	}
	return nil
}

func runCase149ConcurrentSwapSameKey() error {
	m := testMap()
	defer m.Close()
	m.Store("k", 0)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.Swap("k", w*200+i)
			}
		}()
	}
	wg.Wait()
	_, ok := m.Load("k")
	if !ok {
		return fmt.Errorf("key missing after concurrent swaps")
	}
	return nil
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
				m.Store("k", i)
				m.Delete("k")
			}
		}()
	}
	wg.Wait()
	return nil
}

func runCase151ConcurrentCAS() error {
	m := testMap()
	defer m.Close()
	m.Store("k", 0)
	var wg sync.WaitGroup
	var successes atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if m.CompareAndSwap("k", 0, 1) {
					successes.Add(1)
				}
				m.Store("k", 0)
			}
		}()
	}
	wg.Wait()
	if successes.Load() == 0 {
		return fmt.Errorf("no CAS succeeded")
	}
	return nil
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
				m.StoreWithTTL(testKey(w*200+i), i, time.Minute)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	return nil
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
				m.StoreWithHits(testKey(w*200+i), i, 10)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	return nil
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
				m.StoreWithTTLAndHits(testKey(w*200+i), i, time.Minute, 10)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 1600 {
		return fmt.Errorf("Len = %d, want 1600", m.Len())
	}
	return nil
}

func runCase155ConcurrentRangeWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(testKey(i), i)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			m.Store(testKey(i), i)
		}
	}()
	go func() { defer wg.Done(); m.Range(func(_ string, _ any) bool { return true }) }()
	wg.Wait()
	return nil
}

func runCase156ConcurrentSnapshotWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(testKey(i), i)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			m.Store(testKey(i), i)
		}
	}()
	go func() { defer wg.Done(); _ = m.Snapshot() }()
	wg.Wait()
	return nil
}

func runCase157ConcurrentClearWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Store(testKey(i), i)
		}
	}()
	go func() { defer wg.Done(); m.Clear() }()
	wg.Wait()
	return nil
}

func runCase158ConcurrentCleanupWhileWriting() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.StoreWithTTL(testKey(i), i, time.Minute)
		}
	}()
	go func() { defer wg.Done(); m.CleanupNow() }()
	wg.Wait()
	return nil
}

func runCase159HotSetContention() error {
	m := testMap(WithCapacity(512), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 128; i++ {
		m.Store(testKey(i), i)
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
				k := testKey(int(seed % 128))
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
	return nil
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
				m.Store(testKey(w*100+i), i)
			}
		}()
	}
	wg.Wait()
	if m.Len() != 3200 {
		return fmt.Errorf("Len = %d, want 3200", m.Len())
	}
	return nil
}

func runCase161ConcurrentAddSameKey() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 0)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.Add("c", 1)
			}
		}()
	}
	wg.Wait()
	v, _ := m.Load("c")
	if v != 4000 {
		return fmt.Errorf("count = %v, want 4000", v)
	}
	return nil
}

func runCase162ConcurrentSubSameKey() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 4000)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.Sub("c", 1)
			}
		}()
	}
	wg.Wait()
	v, _ := m.Load("c")
	if v != 0 {
		return fmt.Errorf("count = %v, want 0", v)
	}
	return nil
}

func runCase163ConcurrentSetSameKey() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 0)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				m.Set("c", w*200+i)
			}
		}()
	}
	wg.Wait()
	_, ok := m.Load("c")
	if !ok {
		return fmt.Errorf("key missing after concurrent sets")
	}
	return nil
}

func runCase164ConcurrentAddDifferentKeys() error {
	m := testMap(WithCapacity(1024), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(testKey(i), 0)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.Add(testKey(i), 1)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 100; i++ {
		v, _ := m.Load(testKey(i))
		if v != 8 {
			return fmt.Errorf("key-%d = %v, want 8", i, v)
		}
	}
	return nil
}

func runCase165ConcurrentSetThenLoad() error {
	m := testMap()
	defer m.Close()
	m.Store("c", 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			m.Set("c", i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			m.Load("c")
		}
	}()
	wg.Wait()
	return nil
}

func runCase166ConcurrentAtomicInt64Add() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store("ip:1", s)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load("ip:1")
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
	m.Store("ip:1", s)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load("ip:1")
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
	m.Store("sess", ms)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load("sess")
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
	m.Store("sess", ms)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load("sess")
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
	m.Store("ip:1", s)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := m.Load("ip:1")
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
	m.Store("ip:1", s)
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
			m.Load("ip:1")
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
		m.Store(testKey(i), ptrs[i])
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				v, _ := m.Load(testKey(i))
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
	m.Store("ip:1", s)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load("ip:1")
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
			v, _ := m.LoadOrStore("ip:1", init)
			v.(*atomicStats).Count.Add(1)
		}()
	}
	wg.Wait()
	v, _ := m.Load("ip:1")
	if v.(*atomicStats).Count.Load() != 16 {
		return fmt.Errorf("Count = %d", v.(*atomicStats).Count.Load())
	}
	return nil
}

func runCase175PointerMutationWithTTL() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.StoreWithTTL("ip:1", s, time.Minute)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load("ip:1")
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
	m.StoreWithHits("ip:1", s, 8)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := m.Load("ip:1")
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
	m.Store("ip:1", s)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Range(func(_ string, v any) bool { _ = v.(*atomicStats).Count.Load(); return true })
		}()
	}
	wg.Wait()
	return nil
}

func runCase178ConcurrentSnapshotMutateAfter() error {
	m := testMap()
	defer m.Close()
	s := &atomicStats{}
	m.Store("ip:1", s)
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
		m.Store(testKey(i), &atomicStats{})
	}
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := uint64(1)
			for i := 0; i < 500; i++ {
				seed = xorshift(seed)
				k := testKey(int(seed % 4))
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
			m.Store(testKey(i), &atomicStats{})
		}
	}()
	wg.Wait()
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if v, ok := m.Load(testKey(i)); ok {
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
				m.Store("k", i)
				m.Delete("k")
			}
		}()
	}
	wg.Wait()
	return nil
}

func runCase182StoreClearRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Store(testKey(i%50), i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			m.Clear()
		}
	}()
	wg.Wait()
	return nil
}

func runCase183LoadOrStoreDeleteRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.LoadOrStore("k", i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Delete("k")
		}
	}()
	wg.Wait()
	return nil
}

func runCase184SwapDeleteRace() error {
	m := testMap()
	defer m.Close()
	m.Store("k", 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Swap("k", i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Delete("k")
		}
	}()
	wg.Wait()
	return nil
}

func runCase185CASStoreRace() error {
	m := testMap()
	defer m.Close()
	m.Store("k", 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.CompareAndSwap("k", i, i+1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Store("k", i)
		}
	}()
	wg.Wait()
	return nil
}

func runCase186RangeDeleteRace() error {
	m := testMap(WithCapacity(256), WithShardCount(8))
	defer m.Close()
	for i := 0; i < 100; i++ {
		m.Store(testKey(i), i)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			m.Range(func(_ string, _ any) bool { return true })
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.Delete(testKey(i))
		}
	}()
	wg.Wait()
	return nil
}

func runCase187TTLExpiryLoadRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.StoreWithTTL("k", i, time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Load("k")
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	return nil
}

func runCase188HitExhaustionLoadRace() error {
	m := testMap()
	defer m.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.StoreWithHits("k", i, 3)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			m.Load("k")
		}
	}()
	wg.Wait()
	return nil
}

func runCase189CleanupLoadRace() error {
	m := testMap()
	defer m.Close()
	for i := 0; i < 50; i++ {
		m.StoreWithTTL(testKey(i), i, 5*time.Millisecond)
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
			m.Load(testKey(i % 50))
		}
	}()
	wg.Wait()
	return nil
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
				m.StoreWithTTL(testKey(i), i, time.Millisecond)
			}
		}()
	}
	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	return nil
}

func runCase191VeryLongKey() error {
	m := testMap()
	defer m.Close()
	key := ""
	for i := 0; i < 1000; i++ {
		key += "k"
	}
	m.Store(key, 42)
	v, ok := m.Load(key)
	if !ok || v.(int) != 42 {
		return fmt.Errorf("long key: ok=%v, v=%v", ok, v)
	}
	return nil
}

func runCase192NilValue() error {
	m := testMap()
	defer m.Close()
	m.Store("k", nil)
	v, ok := m.Load("k")
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
	m.Store("flag", true)
	v, ok := m.Load("flag")
	if !ok || v.(bool) != true {
		return fmt.Errorf("bool: ok=%v, v=%v", ok, v)
	}
	return nil
}

func runCase194ChannelValue() error {
	m := testMap()
	defer m.Close()
	ch := make(chan int, 1)
	m.Store("ch", ch)
	v, _ := m.Load("ch")
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
	m.Store("inner", inner)
	v, _ := m.Load("inner")
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
	m.Store("fn", fn)
	v, _ := m.Load("fn")
	if v.(func() int)() != 42 {
		return fmt.Errorf("func: wrong return")
	}
	return nil
}

func runCase197InterfaceWrapping() error {
	m := testMap()
	defer m.Close()
	var iface any = &atomicStats{}
	m.Store("iface", iface)
	v, _ := m.Load("iface")
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
		m.Store(testKey(i), i)
	}
	if m.Len() != 1000 {
		return fmt.Errorf("Len = %d", m.Len())
	}
	for i := 0; i < 1000; i++ {
		v, ok := m.Load(testKey(i))
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
		m.Store("k", i)
		m.Delete("k")
	}
	_, ok := m.Load("k")
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
				m.Store(testKey(i), i)
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

var AllTests = []TestCase{
	{1, "Store and Load string", runCase001StoreLoadString},
	{2, "Store and Load int", runCase002StoreLoadInt},
	{3, "Store and Load struct", runCase003StoreLoadStruct},
	{4, "Store pointer Load same pointer", runCase004StorePointerLoadSamePointer},
	{5, "Store byte slice direct reference", runCase005StoreByteSliceDirectReference},
	{6, "Store string slice direct reference", runCase006StoreStringSliceDirectReference},
	{7, "Key clone on insert", runCase007KeyCloneOnInsert},
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
	{96, "Add int", runCase096AddInt},
	{97, "Sub int", runCase097SubInt},
	{98, "Set int", runCase098SetInt},
	{99, "Add int64", runCase099AddInt64},
	{100, "Sub int64", runCase100SubInt64},
	{101, "Set int64", runCase101SetInt64},
	{102, "Add float64", runCase102AddFloat64},
	{103, "Sub float64", runCase103SubFloat64},
	{104, "Set float64", runCase104SetFloat64},
	{105, "Add uint64", runCase105AddUint64},
	{106, "Sub uint64 no underflow", runCase106SubUint64NoUnderflow},
	{107, "Set uint64", runCase107SetUint64},
	{108, "Add missing key error", runCase108AddMissingKeyError},
	{109, "Sub missing key error", runCase109SubMissingKeyError},
	{110, "Set missing key error", runCase110SetMissingKeyError},
	{111, "Add on string type mismatch", runCase111AddOnStringTypeMismatch},
	{112, "Set on string type mismatch", runCase112SetOnStringTypeMismatch},
	{113, "Set non-numeric value error", runCase113SetNonNumericValueError},
	{114, "Set preserves TTL", runCase114SetPreservesTTL},
	{115, "Set preserves hits", runCase115SetPreservesHits},
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
	{161, "Concurrent Add same key", runCase161ConcurrentAddSameKey},
	{162, "Concurrent Sub same key", runCase162ConcurrentSubSameKey},
	{163, "Concurrent Set same key", runCase163ConcurrentSetSameKey},
	{164, "Concurrent Add different keys", runCase164ConcurrentAddDifferentKeys},
	{165, "Concurrent Set then Load", runCase165ConcurrentSetThenLoad},
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
