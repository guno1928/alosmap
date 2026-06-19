package alosmap

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)

type tmPoint struct {
	X, Y int32
}

type tmBig struct {
	A, B, C, D int64
}

func buildTypedMapTestCases() []TestCase {
	tests := make([]TestCase, 0, 300)
	id := 301
	add := func(name string, fn func() error) {
		tests = append(tests, TestCase{id, name, fn})
		id++
	}

	for i := 0; i < 40; i++ {
		n := (i + 1) * 25
		add(fmt.Sprintf("Typed string→int64 round-trip %d entries", n), func() error {
			m := NewTypedSized[string, int64](n, 0)
			for j := 0; j < n; j++ {
				m.Store("k"+strconv.Itoa(j), int64(j*3))
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len = %d, want %d", got, n)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load("k" + strconv.Itoa(j))
				if !ok || v != int64(j*3) {
					return fmt.Errorf("Load k%d = %d,%v", j, v, ok)
				}
			}
			if _, ok := m.Load("absent"); ok {
				return fmt.Errorf("absent key reported present")
			}
			return nil
		})
	}

	for i := 0; i < 30; i++ {
		n := (i + 1) * 20
		add(fmt.Sprintf("Typed int64→int64 round-trip %d entries", n), func() error {
			m := NewTypedSized[int64, int64](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j)*int64(j))
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != int64(j)*int64(j) {
					return fmt.Errorf("Load %d = %d,%v", j, v, ok)
				}
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len = %d, want %d", got, n)
			}
			return nil
		})
	}

	for i := 0; i < 30; i++ {
		n := (i + 1) * 20
		add(fmt.Sprintf("Typed delete keeps probe chain intact %d entries", n), func() error {
			m := NewTypedSized[int64, int64](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j+1))
			}
			for j := 0; j < n; j += 2 {
				v, ok := m.Delete(int64(j))
				if !ok || v != int64(j+1) {
					return fmt.Errorf("Delete %d = %d,%v", j, v, ok)
				}
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if j%2 == 0 {
					if ok {
						return fmt.Errorf("deleted key %d still present", j)
					}
				} else if !ok || v != int64(j+1) {
					return fmt.Errorf("surviving key %d = %d,%v", j, v, ok)
				}
			}
			remaining := n - (n+1)/2
			if got := m.Len(); got != remaining {
				return fmt.Errorf("Len = %d, want %d", got, remaining)
			}
			return nil
		})
	}

	for i := 0; i < 30; i++ {
		n := (i + 1) * 15
		add(fmt.Sprintf("Typed Range visits all then early-stops %d entries", n), func() error {
			m := NewTypedSized[int64, int64](n, 0)
			want := int64(0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j))
				want += int64(j)
			}
			got := int64(0)
			cnt := 0
			m.Range(func(_, v int64) bool {
				got += v
				cnt++
				return true
			})
			if cnt != n || got != want {
				return fmt.Errorf("Range cnt=%d sum=%d, want cnt=%d sum=%d", cnt, got, n, want)
			}
			limit := n / 2
			if limit < 1 {
				limit = 1
			}
			stop := 0
			m.Range(func(_, _ int64) bool {
				stop++
				return stop < limit
			})
			if stop != limit {
				return fmt.Errorf("early stop visited %d, want %d", stop, limit)
			}
			return nil
		})
	}

	for i := 0; i < 30; i++ {
		n := (i + 1) * 20
		add(fmt.Sprintf("Typed Len tracks mixed store/delete %d entries", n), func() error {
			m := NewTypedSized[string, int64](n, 0)
			for j := 0; j < n; j++ {
				m.Store("e"+strconv.Itoa(j), int64(j))
			}
			del := n / 3
			for j := 0; j < del; j++ {
				m.Delete("e" + strconv.Itoa(j))
			}
			if got := m.Len(); got != n-del {
				return fmt.Errorf("Len after delete = %d, want %d", got, n-del)
			}
			for j := 0; j < del; j++ {
				m.Store("e"+strconv.Itoa(j), int64((j+1)*100))
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len after re-add = %d, want %d", got, n)
			}
			if v, ok := m.Load("e0"); !ok || v != 100 {
				return fmt.Errorf("Load e0 = %d,%v, want 100", v, ok)
			}
			return nil
		})
	}

	for i := 0; i < 30; i++ {
		n := (i + 1) * 10
		add(fmt.Sprintf("Typed pointer value identity %d entries", n), func() error {
			m := NewTypedSized[int64, *tmBig](n, 0)
			ptrs := make([]*tmBig, n)
			for j := 0; j < n; j++ {
				ptrs[j] = &tmBig{A: int64(j), D: int64(-j)}
				m.Store(int64(j), ptrs[j])
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != ptrs[j] {
					return fmt.Errorf("pointer identity broke at %d", j)
				}
				if v.A != int64(j) || v.D != int64(-j) {
					return fmt.Errorf("payload at %d = %+v", j, *v)
				}
			}
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed float64 value round-trip %d entries", n), func() error {
			m := NewTypedSized[int64, float64](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), float64(j)+0.5)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != float64(j)+0.5 {
					return fmt.Errorf("Load %d = %v,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed int value round-trip %d entries", n), func() error {
			m := NewTypedSized[int64, int](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), j*7)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != j*7 {
					return fmt.Errorf("Load %d = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed uint64 value round-trip %d entries", n), func() error {
			m := NewTypedSized[int64, uint64](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), uint64(j)<<20)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != uint64(j)<<20 {
					return fmt.Errorf("Load %d = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed 8-byte struct value round-trip %d entries", n), func() error {
			m := NewTypedSized[int64, tmPoint](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), tmPoint{X: int32(j), Y: int32(-j)})
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v.X != int32(j) || v.Y != int32(-j) {
					return fmt.Errorf("Load %d = %+v,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed uint32 value round-trip %d entries", n), func() error {
			m := NewTypedSized[int64, uint32](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), uint32(j*5))
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != uint32(j*5) {
					return fmt.Errorf("Load %d = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed int8 value round-trip %d entries", n), func() error {
			m := NewTypedSized[int64, int8](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), int8(j%127))
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != int8(j%127) {
					return fmt.Errorf("Load %d = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 10; i++ {
		n := (i + 1) * 50
		chunk := (i + 1) * 16
		add(fmt.Sprintf("Typed Prealloc(%d) round-trip %d entries", chunk, n), func() error {
			m := NewTypedSized[string, int64](n, 0).Prealloc(chunk)
			for j := 0; j < n; j++ {
				m.Store("p"+strconv.Itoa(j), int64(j+1))
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len = %d, want %d", got, n)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load("p" + strconv.Itoa(j))
				if !ok || v != int64(j+1) {
					return fmt.Errorf("Load p%d = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 10; i++ {
		n := (i + 1) * 40
		add(fmt.Sprintf("Typed NewTyped default growth %d entries", n), func() error {
			m := NewTyped[int64, int64]()
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j)+1000)
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len = %d, want %d", got, n)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != int64(j)+1000 {
					return fmt.Errorf("Load %d = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 10; i++ {
		workers := 8
		perWorker := (i + 1) * 50
		total := workers * perWorker
		add(fmt.Sprintf("Typed concurrent disjoint writers %d entries", total), func() error {
			m := NewTypedSized[int64, int64](total, 16)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for k := 0; k < perWorker; k++ {
						key := int64(w*perWorker + k)
						m.Store(key, key*2)
					}
				}(w)
			}
			wg.Wait()
			if got := m.Len(); got != total {
				return fmt.Errorf("Len = %d, want %d", got, total)
			}
			for k := 0; k < total; k++ {
				v, ok := m.Load(int64(k))
				if !ok || v != int64(k)*2 {
					return fmt.Errorf("Load %d = %d,%v", k, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 10; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed overwrite in place %d entries", n), func() error {
			m := NewTypedSized[int64, int64](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j))
			}
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j)+1)
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len changed after overwrite = %d, want %d", got, n)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != int64(j)+1 {
					return fmt.Errorf("Load %d = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 10; i++ {
		n := (i + 1) * 80
		add(fmt.Sprintf("Typed tiny capacity forces growth %d entries", n), func() error {
			m := NewTypedSized[int64, int64](4, 1)
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j)*int64(j))
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len = %d, want %d", got, n)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != int64(j)*int64(j) {
					return fmt.Errorf("Load %d after growth = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 10; i++ {
		n := (i + 1) * 30
		add(fmt.Sprintf("Typed tombstone reuse on delete+reinsert %d entries", n), func() error {
			m := NewTypedSized[int64, int64](n, 0)
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j))
			}
			for j := 0; j < n; j++ {
				m.Delete(int64(j))
			}
			if got := m.Len(); got != 0 {
				return fmt.Errorf("Len after delete-all = %d, want 0", got)
			}
			for j := 0; j < n; j++ {
				m.Store(int64(j), int64(j)+7)
			}
			if got := m.Len(); got != n {
				return fmt.Errorf("Len after reinsert = %d, want %d", got, n)
			}
			for j := 0; j < n; j++ {
				v, ok := m.Load(int64(j))
				if !ok || v != int64(j)+7 {
					return fmt.Errorf("Load %d after reinsert = %d,%v", j, v, ok)
				}
			}
			return nil
		})
	}

	for i := 0; i < 20; i++ {
		workers := 16
		iters := (i + 1) * 1000
		keyspace := int64((i + 1) * 100)
		add(fmt.Sprintf("Typed concurrent torn-read stress %d iters", iters), func() error {
			m := NewTypedSized[int64, int64](int(keyspace)*2, 16)
			var wg sync.WaitGroup
			var failed atomic.Int64
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for it := 0; it < iters; it++ {
						k := int64((it + w)) % keyspace
						m.Store(k, k*10)
						if v, ok := m.Load(k); ok && v != k*10 && v != 0 {
							failed.Add(1)
						}
						if it%7 == 0 {
							m.Delete(k)
						}
					}
				}(w)
			}
			wg.Wait()
			if n := failed.Load(); n > 0 {
				return fmt.Errorf("observed %d torn reads", n)
			}
			return nil
		})
	}

	if len(tests) != 300 {
		panic(fmt.Sprintf("buildTypedMapTestCases generated %d tests, want 300", len(tests)))
	}
	return tests
}

func init() {
	AllTests = append(AllTests, buildTypedMapTestCases()...)
}
