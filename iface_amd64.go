//go:build amd64

package alosmap

import "unsafe"

// atomicStoreIface atomically writes (typ, data) into the 16 bytes at addr.
// The memory at addr MUST be 16-byte aligned.
//
//go:noescape
func atomicStoreIface(addr unsafe.Pointer, typ uintptr, data unsafe.Pointer)

// atomicLoadIface atomically reads the 16 bytes at addr as (typ, data).
// The memory at addr MUST be 16-byte aligned.
//
//go:noescape
func atomicLoadIface(addr unsafe.Pointer) (typ uintptr, data unsafe.Pointer)
