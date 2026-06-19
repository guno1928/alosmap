//go:build amd64

#include "textflag.h"

// func prefetchT0(addr unsafe.Pointer)
// Hints the CPU to bring the cache line containing *addr into L1.
TEXT ·prefetchT0(SB), NOSPLIT, $0-8
    MOVQ addr+0(FP), AX
    PREFETCHT0 (AX)
    RET

// func prefetchT2(addr unsafe.Pointer)
// Same idea, hint into L2 instead of L1 (for things touched once).
TEXT ·prefetchT2(SB), NOSPLIT, $0-8
    MOVQ addr+0(FP), AX
    PREFETCHT2 (AX)
    RET
