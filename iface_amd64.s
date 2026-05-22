// 2-word atomic interface ops via LOCK CMPXCHG16B.
// Requires 16-byte aligned memory.
// CPU support: x86_64 with CX16 (all amd64 since Nehalem 2008).
//go:build amd64

#include "textflag.h"

// func atomicStoreIface(addr unsafe.Pointer, typ uintptr, data unsafe.Pointer)
// Stores (typ, data) into *addr atomically (16 bytes).
TEXT ·atomicStoreIface(SB), NOSPLIT, $0-24
    MOVQ addr+0(FP), DI
    MOVQ typ+8(FP), BX
    MOVQ data+16(FP), CX
loop_store:
    MOVQ 0(DI), AX
    MOVQ 8(DI), DX
    LOCK
    CMPXCHG16B (DI)
    JNE  loop_store
    RET

// func atomicLoadIface(addr unsafe.Pointer) (typ uintptr, data unsafe.Pointer)
// Atomically loads the 16 bytes at *addr as (typ, data).
TEXT ·atomicLoadIface(SB), NOSPLIT, $0-24
    MOVQ addr+0(FP), DI
    XORQ AX, AX
    XORQ DX, DX
    XORQ BX, BX
    XORQ CX, CX
    LOCK
    CMPXCHG16B (DI)
    MOVQ AX, typ+8(FP)
    MOVQ DX, data+16(FP)
    RET
