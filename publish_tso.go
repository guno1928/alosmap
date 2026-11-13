//go:build amd64 && !race

package alosmap

import (
	"sync/atomic"
	"unsafe"
)

func publishPointer(slot *unsafe.Pointer, p unsafe.Pointer) {
	*slot = p
}

func publishWord(word *atomic.Uint64, value uint64) {
	*(*uint64)(unsafe.Pointer(word)) = value
}

func publishCount(count *atomic.Int64, delta int64) {
	*(*int64)(unsafe.Pointer(count)) += delta
}

func touchLine(line unsafe.Pointer) {
	_ = atomic.LoadUintptr((*uintptr)(line))
}
