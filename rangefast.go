package alosmap

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// parallelRangeThreshold is the minimum total live entry count at which
// Range fans out per-shard workers. Below this the goroutine spawn cost
// outweighs the parallelism win.
const parallelRangeThreshold = 1024

// rangePair holds a single (key, value) snapshot for the visitor.
type rangePair struct {
	key   Key
	value any
}

// rangeSlabs is a per-Range workspace: one slab per worker.
type rangeSlabs [][]rangePair

// slabPool recycles per-Range slab arrays so the parallel path is alloc-free
// in steady state.
var slabPool = sync.Pool{
	New: func() any {
		s := make(rangeSlabs, 0, 64)
		return &s
	},
}

func acquireRangeSlabs(n int) rangeSlabs {
	ptr := slabPool.Get().(*rangeSlabs)
	s := *ptr
	if cap(s) < n {
		s = make(rangeSlabs, n)
	} else {
		s = s[:n]
		for i := range s {
			s[i] = s[i][:0]
		}
	}
	*ptr = s
	return s
}

func releaseRangeSlabs(s rangeSlabs) {
	for i := range s {
		s[i] = s[i][:0]
	}
	slabPool.Put(&s)
}

// readyPool recycles the per-worker atomic.Bool array used to signal that
// a worker has finished filling its slab.
var readyPool = sync.Pool{
	New: func() any {
		r := make([]atomic.Bool, 0, 64)
		return &r
	},
}

func acquireRangeReady(n int) []atomic.Bool {
	ptr := readyPool.Get().(*[]atomic.Bool)
	r := *ptr
	if cap(r) < n {
		r = make([]atomic.Bool, n)
	} else {
		r = r[:n]
		for i := range r {
			r[i].Store(false)
		}
	}
	*ptr = r
	return r
}

func releaseRangeReady(r []atomic.Bool) {
	readyPool.Put(&r)
}

// rangeWorkerPipeline fills the worker's slab, flips its ready flag, then
// signals completion via wg. ready.Store(true) wakes the main goroutine to
// begin visiting this slab while later workers are still scanning theirs;
// wg.Done is called last so wg.Wait() guarantees every worker has fully
// exited.
func rangeWorkerPipeline(shards []shard, out *[]rangePair, ready *atomic.Bool, wg *sync.WaitGroup) {
	rangeShardsCollect(shards, out)
	ready.Store(true)
	wg.Done()
}


// rangeWorkerDirect is the worker body of RangePar: it scans a slice of
// shards and invokes visitor directly. No slab is built. visitor must be
// safe to call concurrently from up to GOMAXPROCS*4 goroutines.
func rangeWorkerDirect(shards []shard, visitor func(key Key, value any) bool, stopped *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	for s := range shards {
		if stopped.Load() {
			return
		}
		shardPtr := &shards[s]
		currentTable := shardPtr.table.Load()
		ctrl := currentTable.ctrl
		slots := currentTable.slots
		nSlots := len(slots)
		i := 0
		for ; i+8 <= nSlots; i += 8 {
			chunk := *(*uint64)(unsafe.Pointer(&ctrl[i]))
			if chunk == 0 {
				continue
			}
			if i+16 <= nSlots {
				prefetchT0(unsafe.Pointer(&slots[i+8]))
			}
			for j := 0; j < 8; j++ {
				if byte(chunk>>(uint(j)*8)) == 0 {
					continue
				}
				current := slots[i+j].entry.Load()
				if current == nil {
					continue
				}
				if t := current.cellTyp.Load(); t != 0 {
					d := current.cellData.Load()
					if !visitor(current.key, ifaceFromWords(t, unsafe.Pointer(d))) {
						stopped.Store(true)
						return
					}
					continue
				}
				v, ok := shardPtr.readEntry(current, false)
				if !ok {
					continue
				}
				if !visitor(current.key, v) {
					stopped.Store(true)
					return
				}
			}
		}
		for ; i < nSlots; i++ {
			if ctrl[i] == 0 {
				continue
			}
			current := slots[i].entry.Load()
			if current == nil {
				continue
			}
			if t := current.cellTyp.Load(); t != 0 {
				d := current.cellData.Load()
				if !visitor(current.key, ifaceFromWords(t, unsafe.Pointer(d))) {
					stopped.Store(true)
					return
				}
				continue
			}
			v, ok := shardPtr.readEntry(current, false)
			if !ok {
				continue
			}
			if !visitor(current.key, v) {
				stopped.Store(true)
				return
			}
		}
	}
}

// rangeShardSequential walks the shard's slots and invokes visitor inline.
// Returns false when the visitor returned false (stop the outer iteration).
// Reads ctrl bytes 8 at a time so whole runs of empty slots are skipped.
func rangeShardSequential(s *shard, visitor func(key Key, value any) bool) bool {
	currentTable := s.table.Load()
	ctrl := currentTable.ctrl
	slots := currentTable.slots
	nSlots := len(slots)
	i := 0
	for ; i+8 <= nSlots; i += 8 {
		chunk := *(*uint64)(unsafe.Pointer(&ctrl[i]))
		if chunk == 0 {
			continue
		}
		for j := 0; j < 8; j++ {
			if ctrl[i+j] == 0 {
				continue
			}
			current := slots[i+j].entry.Load()
			if current == nil {
				continue
			}
			if t := current.cellTyp.Load(); t != 0 {
				d := current.cellData.Load()
				if !visitor(current.key, ifaceFromWords(t, unsafe.Pointer(d))) {
					return false
				}
				continue
			}
			v, ok := s.readEntry(current, false)
			if !ok {
				continue
			}
			if !visitor(current.key, v) {
				return false
			}
		}
	}
	for ; i < nSlots; i++ {
		if ctrl[i] == 0 {
			continue
		}
		current := slots[i].entry.Load()
		if current == nil {
			continue
		}
		if t := current.cellTyp.Load(); t != 0 {
			d := current.cellData.Load()
			if !visitor(current.key, ifaceFromWords(t, unsafe.Pointer(d))) {
				return false
			}
			continue
		}
		v, ok := s.readEntry(current, false)
		if !ok {
			continue
		}
		if !visitor(current.key, v) {
			return false
		}
	}
	return true
}

// rangeShardsCollect collects entries from a slice of shards into a single
// per-worker slab. Bounded-worker fanout calls this once per worker so the
// total goroutine count stays bounded regardless of shard count.
//
// Each shard is scanned by reading ctrl bytes 8 at a time as a uint64 and
// skipping whole runs of empty slots when the chunk is zero. This pays off
// for sparse tables (high shard count, low fill) without hurting dense ones.
func rangeShardsCollect(shards []shard, out *[]rangePair) {
	// Pre-size to the sum of live entries across the worker's shards.
	var liveTotal int64
	for i := range shards {
		liveTotal += shards[i].live.Load()
	}
	if liveTotal < 0 {
		liveTotal = 0
	}

	buf := *out
	if int64(cap(buf)) < liveTotal {
		buf = make([]rangePair, 0, liveTotal+8)
	} else {
		buf = buf[:0]
	}

	for s := range shards {
		shardPtr := &shards[s]
		currentTable := shardPtr.table.Load()
		ctrl := currentTable.ctrl
		slots := currentTable.slots
		nSlots := len(slots)
		// Scan ctrl in 8-byte chunks; skip whole chunks when all slots empty.
		i := 0
		for ; i+8 <= nSlots; i += 8 {
			chunk := *(*uint64)(unsafe.Pointer(&ctrl[i]))
			if chunk == 0 {
				continue
			}
			// Prefetch the NEXT chunk's worth of slot data so the cache lines
			// that hold the entry pointers are warm by the time we get there.
			if i+16 <= nSlots {
				prefetchT0(unsafe.Pointer(&slots[i+8]))
			}
			// Walk the 8 slots, prefetching each non-empty entry struct one
			// step ahead of when we actually dereference it.
			for j := 0; j < 8; j++ {
				if byte(chunk>>(uint(j)*8)) == 0 {
					continue
				}
				// One-step lookahead prefetch on the next live entry struct.
				for k := j + 1; k < 8; k++ {
					if byte(chunk>>(uint(k)*8)) == 0 {
						continue
					}
					if nxt := slots[i+k].entry.Load(); nxt != nil {
						prefetchT0(unsafe.Pointer(nxt))
					}
					break
				}
				current := slots[i+j].entry.Load()
				if current == nil {
					continue
				}
				if t := current.cellTyp.Load(); t != 0 {
					d := current.cellData.Load()
					buf = append(buf, rangePair{key: current.key, value: ifaceFromWords(t, unsafe.Pointer(d))})
					continue
				}
				v, ok := shardPtr.readEntry(current, false)
				if !ok {
					continue
				}
				buf = append(buf, rangePair{key: current.key, value: v})
			}
		}
		// Tail (final < 8 slots)
		for ; i < nSlots; i++ {
			if ctrl[i] == 0 {
				continue
			}
			current := slots[i].entry.Load()
			if current == nil {
				continue
			}
			if t := current.cellTyp.Load(); t != 0 {
				d := current.cellData.Load()
				buf = append(buf, rangePair{key: current.key, value: ifaceFromWords(t, unsafe.Pointer(d))})
				continue
			}
			v, ok := shardPtr.readEntry(current, false)
			if !ok {
				continue
			}
			buf = append(buf, rangePair{key: current.key, value: v})
		}
	}
	*out = buf
}
