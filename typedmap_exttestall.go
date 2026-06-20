package alosmap

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

func tmStr(capacity int) *TypedMap[string, int64] {
	return NewTypedSized[string, int64](capacity, 0, WithoutCleanup())
}

func tmI64(capacity int) *TypedMap[int64, int64] {
	return NewTypedSized[int64, int64](capacity, 0, WithoutCleanup())
}

func buildTypedMapExtTestCases() []TestCase {
	tests := make([]TestCase, 0, 200)
	id := 601
	add := func(name string, fn func() error) {
		tests = append(tests, TestCase{id, name, fn})
		id++
	}

	for i := 0; i < 30; i++ {
		n := (i + 1) * 10
		add(fmt.Sprintf("TypedExt LoadOrStore hit/miss %d", n), func() error {
			m := tmStr(n)
			defer m.Close()
			for j := 0; j < n; j++ {
				if v, loaded := m.LoadOrStore("k"+strconv.Itoa(j), int64(j)); loaded || v != int64(j) {
					return fmt.Errorf("LoadOrStore miss k%d = %d,%v", j, v, loaded)
				}
			}
			for j := 0; j < n; j++ {
				if v, loaded := m.LoadOrStore("k"+strconv.Itoa(j), -1); !loaded || v != int64(j) {
					return fmt.Errorf("LoadOrStore hit k%d = %d,%v", j, v, loaded)
				}
			}
			if m.Len() != n {
				return fmt.Errorf("Len = %d, want %d", m.Len(), n)
			}
			return nil
		})
	}

	for i := 0; i < 25; i++ {
		n := (i + 1) * 8
		add(fmt.Sprintf("TypedExt Swap %d", n), func() error {
			m := tmI64(n)
			defer m.Close()
			for j := 0; j < n; j++ {
				if prev, loaded := m.Swap(int64(j), int64(j)); loaded || prev != 0 {
					return fmt.Errorf("Swap miss %d = %d,%v", j, prev, loaded)
				}
			}
			for j := 0; j < n; j++ {
				if prev, loaded := m.Swap(int64(j), int64(j)*2); !loaded || prev != int64(j) {
					return fmt.Errorf("Swap existing %d = %d,%v", j, prev, loaded)
				}
			}
			for j := 0; j < n; j++ {
				if v, _ := m.Load(int64(j)); v != int64(j)*2 {
					return fmt.Errorf("Load %d = %d", j, v)
				}
			}
			return nil
		})
	}

	for i := 0; i < 25; i++ {
		n := (i + 1) * 8
		add(fmt.Sprintf("TypedExt CompareAndSwap %d", n), func() error {
			m := tmI64(n)
			defer m.Close()
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j))
			}
			for j := 0; j < n; j++ {
				if m.CompareAndSwap(int64(j), int64(j)+999, int64(j)*2) {
					return fmt.Errorf("CAS wrong old %d succeeded", j)
				}
				if !m.CompareAndSwap(int64(j), int64(j), int64(j)*2) {
					return fmt.Errorf("CAS correct old %d failed", j)
				}
			}
			if m.CompareAndSwap(int64(n+1), 0, 1) {
				return fmt.Errorf("CAS missing key succeeded")
			}
			for j := 0; j < n; j++ {
				if v, _ := m.Load(int64(j)); v != int64(j)*2 {
					return fmt.Errorf("Load %d = %d after CAS", j, v)
				}
			}
			return nil
		})
	}

	for i := 0; i < 20; i++ {
		n := (i + 1) * 8
		add(fmt.Sprintf("TypedExt CompareAndDelete %d", n), func() error {
			m := tmI64(n)
			defer m.Close()
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j))
			}
			for j := 0; j < n; j++ {
				if m.CompareAndDelete(int64(j), int64(j)+999) {
					return fmt.Errorf("CAD wrong old %d succeeded", j)
				}
				if !m.CompareAndDelete(int64(j), int64(j)) {
					return fmt.Errorf("CAD correct old %d failed", j)
				}
			}
			if m.Len() != 0 {
				return fmt.Errorf("Len = %d after deleting all", m.Len())
			}
			return nil
		})
	}

	for i := 0; i < 20; i++ {
		n := (i + 1) * 10
		add(fmt.Sprintf("TypedExt Has/Peek/Get %d", n), func() error {
			m := tmStr(n)
			defer m.Close()
			for j := 0; j < n; j++ {
				m.Store("k"+strconv.Itoa(j), int64(j))
			}
			for j := 0; j < n; j++ {
				k := "k" + strconv.Itoa(j)
				if !m.Has(k) {
					return fmt.Errorf("Has(%s) = false", k)
				}
				if v, ok := m.Peek(k); !ok || v != int64(j) {
					return fmt.Errorf("Peek(%s) = %d,%v", k, v, ok)
				}
				if v, ok := m.Get(k); !ok || v != int64(j) {
					return fmt.Errorf("Get(%s) = %d,%v", k, v, ok)
				}
			}
			if m.Has("absent") {
				return fmt.Errorf("Has(absent) = true")
			}
			return nil
		})
	}

	for i := 0; i < 20; i++ {
		n := (i + 1) * 5
		add(fmt.Sprintf("TypedExt StoreWithTTL expiry %d", n), func() error {
			m := tmStr(n)
			defer m.Close()
			for j := 0; j < n; j++ {
				m.StoreWithTTL("k"+strconv.Itoa(j), int64(j), 10*time.Millisecond)
			}
			for j := 0; j < n; j++ {
				if v, ok := m.Load("k" + strconv.Itoa(j)); !ok || v != int64(j) {
					return fmt.Errorf("before expiry k%d = %d,%v", j, v, ok)
				}
			}
			time.Sleep(25 * time.Millisecond)
			for j := 0; j < n; j++ {
				if _, ok := m.Load("k" + strconv.Itoa(j)); ok {
					return fmt.Errorf("k%d present after TTL", j)
				}
			}
			if m.Len() != 0 {
				return fmt.Errorf("Len = %d after expiry", m.Len())
			}
			return nil
		})
	}

	for i := 0; i < 15; i++ {
		hits := int64(i + 1)
		add(fmt.Sprintf("TypedExt StoreWithHits exhaust %d", hits), func() error {
			m := tmStr(16)
			defer m.Close()
			m.StoreWithHits("k", 7, hits)
			for h := int64(0); h < hits; h++ {
				if v, ok := m.Load("k"); !ok || v != 7 {
					return fmt.Errorf("Load #%d = %d,%v", h, v, ok)
				}
			}
			if _, ok := m.Load("k"); ok {
				return fmt.Errorf("k present after %d hits", hits)
			}
			m.StoreWithHits("u", 9, 0)
			for h := 0; h < 50; h++ {
				if _, ok := m.Load("u"); !ok {
					return fmt.Errorf("unlimited entry gone at %d", h)
				}
			}
			return nil
		})
	}

	for i := 0; i < 15; i++ {
		hits := int64(i + 2)
		add(fmt.Sprintf("TypedExt StoreWithTTLAndHits %d", hits), func() error {
			m := tmStr(16)
			defer m.Close()
			m.StoreWithTTLAndHits("t", 1, 8*time.Millisecond, 1000)
			time.Sleep(20 * time.Millisecond)
			if _, ok := m.Load("t"); ok {
				return fmt.Errorf("TTL should expire first")
			}
			m.StoreWithTTLAndHits("h", 1, time.Minute, hits)
			for k := int64(0); k < hits; k++ {
				if _, ok := m.Load("h"); !ok {
					return fmt.Errorf("h missing at %d", k)
				}
			}
			if _, ok := m.Load("h"); ok {
				return fmt.Errorf("h present after %d hits", hits)
			}
			return nil
		})
	}

	for i := 0; i < 15; i++ {
		n := (i + 1) * 6
		add(fmt.Sprintf("TypedExt Snapshot excludes expired %d", n), func() error {
			m := tmStr(n + 4)
			defer m.Close()
			for j := 0; j < n; j++ {
				m.Store("a"+strconv.Itoa(j), int64(j))
			}
			m.StoreWithTTL("dead", 1, 5*time.Millisecond)
			time.Sleep(15 * time.Millisecond)
			snap := m.Snapshot()
			if len(snap) != n {
				return fmt.Errorf("Snapshot len = %d, want %d", len(snap), n)
			}
			seen := make(map[string]int64, len(snap))
			for _, p := range snap {
				seen[p.Key] = p.Value
			}
			for j := 0; j < n; j++ {
				if seen["a"+strconv.Itoa(j)] != int64(j) {
					return fmt.Errorf("snapshot missing a%d", j)
				}
			}
			if _, ok := seen["dead"]; ok {
				return fmt.Errorf("snapshot included expired entry")
			}
			return nil
		})
	}

	for i := 0; i < 12; i++ {
		n := (i + 1) * 20
		add(fmt.Sprintf("TypedExt Clear/Cleanup/Stats/RangePar %d", n), func() error {
			m := tmI64(n)
			defer m.Close()
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j))
			}
			st := m.Stats()
			if st.LiveEntries != int64(m.Len()) || st.LiveEntries != int64(n) {
				return fmt.Errorf("Stats.LiveEntries=%d Len=%d n=%d", st.LiveEntries, m.Len(), n)
			}
			var mu sync.Mutex
			cnt := 0
			m.RangePar(func(_, _ int64) bool {
				mu.Lock()
				cnt++
				mu.Unlock()
				return true
			})
			if cnt != n {
				return fmt.Errorf("RangePar visited %d, want %d", cnt, n)
			}
			m.CleanupNow()
			if m.Len() != n {
				return fmt.Errorf("Len = %d after CleanupNow", m.Len())
			}
			m.Clear()
			if m.Len() != 0 || len(m.Snapshot()) != 0 {
				return fmt.Errorf("not empty after Clear: Len=%d", m.Len())
			}
			return nil
		})
	}

	add("TypedExt WithOptions variants", func() error {
		m := tmStr(16)
		defer m.Close()
		m.StoreWithOptions("a", 1, EntryOptions{TTL: 5 * time.Millisecond})
		time.Sleep(15 * time.Millisecond)
		if _, ok := m.Load("a"); ok {
			return fmt.Errorf("StoreWithOptions TTL not applied")
		}
		if v, loaded := m.LoadOrStoreWithOptions("b", 2, EntryOptions{Hits: 1}); loaded || v != 2 {
			return fmt.Errorf("LoadOrStoreWithOptions = %d,%v", v, loaded)
		}
		m.Load("b")
		if _, ok := m.Load("b"); ok {
			return fmt.Errorf("LoadOrStoreWithOptions hits not applied")
		}
		m.Store("c", 1)
		if prev, loaded := m.SwapWithOptions("c", 2, EntryOptions{Hits: 1}); !loaded || prev != 1 {
			return fmt.Errorf("SwapWithOptions = %d,%v", prev, loaded)
		}
		m.Load("c")
		if _, ok := m.Load("c"); ok {
			return fmt.Errorf("SwapWithOptions hits not applied")
		}
		m.Store("d", 1)
		if !m.CompareAndSwapWithOptions("d", 1, 2, EntryOptions{TTL: 5 * time.Millisecond}) {
			return fmt.Errorf("CompareAndSwapWithOptions failed")
		}
		time.Sleep(15 * time.Millisecond)
		if _, ok := m.Load("d"); ok {
			return fmt.Errorf("CompareAndSwapWithOptions TTL not applied")
		}
		return nil
	})

	add("TypedExt background cleanup reclaims expired", func() error {
		m := NewTypedSized[string, int64](64, 0, WithCleanupInterval(5*time.Millisecond))
		defer m.Close()
		m.StoreWithTTL("a", 1, 5*time.Millisecond)
		if used := m.Stats().UsedSlots; used != 1 {
			return fmt.Errorf("UsedSlots = %d before expiry, want 1", used)
		}
		reclaimed := false
		for k := 0; k < 200; k++ {
			if m.Stats().UsedSlots == 0 {
				reclaimed = true
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		if !reclaimed {
			return fmt.Errorf("background cleanup did not reclaim expired slot")
		}
		return nil
	})

	add("TypedExt Close stops background cleanup", func() error {
		m := NewTypedSized[string, int64](64, 0, WithCleanupInterval(5*time.Millisecond))
		m.StoreWithTTL("live", 1, time.Minute)
		time.Sleep(15 * time.Millisecond)
		m.Close()
		m.StoreWithTTL("dead", 1, 5*time.Millisecond)
		time.Sleep(40 * time.Millisecond)
		if used := m.Stats().UsedSlots; used < 2 {
			return fmt.Errorf("UsedSlots = %d; cleanup ran after Close", used)
		}
		if _, ok := m.Load("dead"); ok {
			return fmt.Errorf("expired entry still loadable")
		}
		if v, ok := m.Load("live"); !ok || v != 1 {
			return fmt.Errorf("live entry lost = %d,%v", v, ok)
		}
		return nil
	})

	if len(tests) != 200 {
		panic(fmt.Sprintf("buildTypedMapExtTestCases generated %d tests, want 200", len(tests)))
	}
	return tests
}

func init() {
	AllTests = append(AllTests, buildTypedMapExtTestCases()...)
}
