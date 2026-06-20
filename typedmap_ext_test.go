package alosmap

import (
	"sync"
	"testing"
	"time"
)

func TestTypedLoadOrStore(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	if v, loaded := m.LoadOrStore("a", 1); loaded || v != 1 {
		t.Fatalf("LoadOrStore miss = %d,%v", v, loaded)
	}
	if v, loaded := m.LoadOrStore("a", 99); !loaded || v != 1 {
		t.Fatalf("LoadOrStore hit = %d,%v", v, loaded)
	}
	if v, _ := m.Load("a"); v != 1 {
		t.Fatalf("Load after LoadOrStore = %d", v)
	}
}

func TestTypedSwap(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	if prev, loaded := m.Swap("a", 1); loaded || prev != 0 {
		t.Fatalf("Swap miss = %d,%v", prev, loaded)
	}
	if prev, loaded := m.Swap("a", 2); !loaded || prev != 1 {
		t.Fatalf("Swap existing = %d,%v", prev, loaded)
	}
	if v, _ := m.Load("a"); v != 2 {
		t.Fatalf("Load after Swap = %d", v)
	}
}

func TestTypedCompareAndSwap(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.Store("a", 10)
	if m.CompareAndSwap("a", 99, 20) {
		t.Fatal("CAS wrong old succeeded")
	}
	if !m.CompareAndSwap("a", 10, 20) {
		t.Fatal("CAS correct old failed")
	}
	if v, _ := m.Load("a"); v != 20 {
		t.Fatalf("Load after CAS = %d", v)
	}
	if m.CompareAndSwap("missing", 0, 1) {
		t.Fatal("CAS on missing key succeeded")
	}
}

func TestTypedCompareAndDelete(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.Store("a", 10)
	if m.CompareAndDelete("a", 99) {
		t.Fatal("CompareAndDelete wrong old succeeded")
	}
	if !m.CompareAndDelete("a", 10) {
		t.Fatal("CompareAndDelete correct old failed")
	}
	if _, ok := m.Load("a"); ok {
		t.Fatal("key present after CompareAndDelete")
	}
}

func TestTypedHasPeek(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.Store("a", 7)
	if !m.Has("a") {
		t.Fatal("Has(a) = false")
	}
	if v, ok := m.Peek("a"); !ok || v != 7 {
		t.Fatalf("Peek(a) = %d,%v", v, ok)
	}
	if m.Has("missing") {
		t.Fatal("Has(missing) = true")
	}
	m.Delete("a")
	if m.Has("a") {
		t.Fatal("Has(a) after delete = true")
	}
}

func TestTypedTTLExpiry(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.StoreWithTTL("s", 1, 10*time.Millisecond)
	if v, ok := m.Load("s"); !ok || v != 1 {
		t.Fatalf("Load before expiry = %d,%v", v, ok)
	}
	time.Sleep(20 * time.Millisecond)
	if _, ok := m.Load("s"); ok {
		t.Fatal("entry present after TTL expiry")
	}
	if m.Has("s") {
		t.Fatal("Has true after TTL expiry")
	}
	if m.Len() != 0 {
		t.Fatalf("Len = %d after expiry, want 0", m.Len())
	}
}

func TestTypedHitsExhaust(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.StoreWithHits("s", 5, 2)
	if v, ok := m.Load("s"); !ok || v != 5 {
		t.Fatalf("Load #1 = %d,%v", v, ok)
	}
	if _, ok := m.Load("s"); !ok {
		t.Fatal("Load #2 missing")
	}
	if _, ok := m.Load("s"); ok {
		t.Fatal("Load #3 should be exhausted")
	}
	if m.Has("s") {
		t.Fatal("Has true after hits exhausted")
	}
}

func TestTypedPeekDoesNotConsumeHits(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.StoreWithHits("s", 5, 1)
	m.Peek("s")
	m.Peek("s")
	if v, ok := m.Load("s"); !ok || v != 5 {
		t.Fatalf("Load after peeks = %d,%v (peek consumed a hit)", v, ok)
	}
	if _, ok := m.Load("s"); ok {
		t.Fatal("hit not exhausted after the single allowed Load")
	}
}

func TestTypedTTLAndHits(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.StoreWithTTLAndHits("s", 1, 5*time.Millisecond, 100)
	time.Sleep(15 * time.Millisecond)
	if _, ok := m.Load("s"); ok {
		t.Fatal("TTL should expire before hits exhausted")
	}

	m.StoreWithTTLAndHits("h", 1, time.Minute, 1)
	m.Load("h")
	if _, ok := m.Load("h"); ok {
		t.Fatal("hits should exhaust before TTL")
	}
}

func TestTypedHitsZeroUnlimited(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.StoreWithHits("s", 1, 0)
	for i := 0; i < 100; i++ {
		if _, ok := m.Load("s"); !ok {
			t.Fatalf("zero-hit entry missing at read %d (should be unlimited)", i)
		}
	}
}

func TestTypedSnapshot(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.Store("a", 1)
	m.Store("b", 2)
	m.StoreWithTTL("c", 3, 5*time.Millisecond)
	time.Sleep(15 * time.Millisecond)
	snap := m.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2 (expired excluded)", len(snap))
	}
	seen := map[string]int64{}
	for _, p := range snap {
		seen[p.Key] = p.Value
	}
	if seen["a"] != 1 || seen["b"] != 2 {
		t.Fatalf("Snapshot contents = %v", seen)
	}
}

func TestTypedClear(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	for i := int64(0); i < 100; i++ {
		m.Store("k"+string(rune('a'+i%26)), i)
	}
	m.Clear()
	if m.Len() != 0 {
		t.Fatalf("Len after Clear = %d", m.Len())
	}
	m.Store("new", 5)
	if v, ok := m.Load("new"); !ok || v != 5 {
		t.Fatalf("map unusable after Clear: %d,%v", v, ok)
	}
}

func TestTypedCleanupNow(t *testing.T) {
	m := NewTypedSized[string, int64](256, 0)
	m.Store("keep", 1)
	for i := 0; i < 50; i++ {
		m.StoreWithTTL("t"+string(rune(i)), int64(i), 5*time.Millisecond)
	}
	time.Sleep(15 * time.Millisecond)
	m.CleanupNow()
	if m.Len() != 1 {
		t.Fatalf("Len after CleanupNow = %d, want 1", m.Len())
	}
	if v, ok := m.Load("keep"); !ok || v != 1 {
		t.Fatalf("keep lost after CleanupNow: %d,%v", v, ok)
	}
}

func TestTypedStats(t *testing.T) {
	m := NewTypedSized[string, int64](256, 0)
	for i := int64(0); i < 100; i++ {
		m.Store("k"+string(rune(i)), i)
	}
	st := m.Stats()
	if st.LiveEntries != int64(m.Len()) {
		t.Fatalf("Stats.LiveEntries=%d Len=%d", st.LiveEntries, m.Len())
	}
	if st.Shards == 0 || st.SlotCapacity == 0 {
		t.Fatalf("Stats shards/capacity zero: %+v", st)
	}
}

func TestTypedRangePar(t *testing.T) {
	m := NewTypedSized[int64, int64](4096, 16)
	for i := int64(0); i < 1000; i++ {
		m.Store(i, i*2)
	}
	var mu sync.Mutex
	seen := make(map[int64]int64, 1000)
	m.RangePar(func(k, v int64) bool {
		mu.Lock()
		seen[k] = v
		mu.Unlock()
		return true
	})
	if len(seen) != 1000 {
		t.Fatalf("RangePar visited %d, want 1000", len(seen))
	}
	for i := int64(0); i < 1000; i++ {
		if seen[i] != i*2 {
			t.Fatalf("RangePar k=%d got %d want %d", i, seen[i], i*2)
		}
	}
}

func TestTypedCloseStillUsable(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.Store("a", 1)
	m.Close()
	m.Close()
	m.Store("b", 2)
	if v, ok := m.Load("a"); !ok || v != 1 {
		t.Fatalf("Load(a) after Close = %d,%v", v, ok)
	}
	if v, ok := m.Load("b"); !ok || v != 2 {
		t.Fatalf("Load(b) after Close = %d,%v", v, ok)
	}
}

func TestTypedWithOptions(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.StoreWithOptions("a", 1, EntryOptions{TTL: 5 * time.Millisecond})
	time.Sleep(15 * time.Millisecond)
	if _, ok := m.Load("a"); ok {
		t.Fatal("StoreWithOptions TTL did not expire")
	}

	if v, loaded := m.LoadOrStoreWithOptions("b", 2, EntryOptions{Hits: 1}); loaded || v != 2 {
		t.Fatalf("LoadOrStoreWithOptions = %d,%v", v, loaded)
	}
	m.Load("b")
	if _, ok := m.Load("b"); ok {
		t.Fatal("LoadOrStoreWithOptions hits not applied")
	}

	m.Store("c", 1)
	if prev, loaded := m.SwapWithOptions("c", 2, EntryOptions{Hits: 1}); !loaded || prev != 1 {
		t.Fatalf("SwapWithOptions = %d,%v", prev, loaded)
	}
	m.Load("c")
	if _, ok := m.Load("c"); ok {
		t.Fatal("SwapWithOptions hits not applied")
	}

	m.Store("d", 1)
	if !m.CompareAndSwapWithOptions("d", 1, 2, EntryOptions{TTL: 5 * time.Millisecond}) {
		t.Fatal("CompareAndSwapWithOptions failed")
	}
	time.Sleep(15 * time.Millisecond)
	if _, ok := m.Load("d"); ok {
		t.Fatal("CompareAndSwapWithOptions TTL not applied")
	}

	m.SetWithTTLAndHits("e", 1, 5*time.Millisecond, 100)
	time.Sleep(15 * time.Millisecond)
	if _, ok := m.Load("e"); ok {
		t.Fatal("SetWithTTLAndHits TTL not applied")
	}
}

func TestTypedPlainStoreResetsTTL(t *testing.T) {
	m := NewTypedSized[string, int64](64, 0)
	m.StoreWithTTL("a", 1, 5*time.Millisecond)
	m.Store("a", 2)
	time.Sleep(15 * time.Millisecond)
	if v, ok := m.Load("a"); !ok || v != 2 {
		t.Fatalf("plain Store did not clear TTL: %d,%v", v, ok)
	}
}
