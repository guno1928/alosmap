//go:build amd64

package alosmap

import "unsafe"

// prefetchT0 issues a PREFETCHT0 instruction targeting the cache line
// containing *addr. It is a hint; the CPU may ignore it. Costs roughly
// one cycle, vs. ~100 cycles for an unprefetched cache miss.
//
//go:noescape
//go:nosplit
func prefetchT0(addr unsafe.Pointer)

//go:noescape
//go:nosplit
func prefetchT2(addr unsafe.Pointer)
