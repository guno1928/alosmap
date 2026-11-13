//go:build amd64

#include "textflag.h"

// func prefetchT0(addr unsafe.Pointer)
// Hints the CPU to bring the cache line containing *addr into L1.
TEXT ·prefetchT0(SB), NOSPLIT, $0-8
    MOVQ addr+0(FP), AX
    PREFETCHT0 (AX)
    RET

TEXT ·prefetchGroupSlots(SB), NOSPLIT, $0-8
    MOVQ group+0(FP), AX
    MOVQ 8(AX), CX
    PREFETCHT0 (CX)
    MOVQ 16(AX), CX
    PREFETCHT0 (CX)
    MOVQ 24(AX), CX
    PREFETCHT0 (CX)
    MOVQ 32(AX), CX
    PREFETCHT0 (CX)
    MOVQ 40(AX), CX
    PREFETCHT0 (CX)
    MOVQ 48(AX), CX
    PREFETCHT0 (CX)
    MOVQ 56(AX), CX
    PREFETCHT0 (CX)
    RET
