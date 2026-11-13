//go:build !amd64 || race

package alosmap

import (
	"sync/atomic"
	"unsafe"
)

func publishPointer(slot *unsafe.Pointer, p unsafe.Pointer) {
	atomic.StorePointer(slot, p)
}

func publishWord(word *atomic.Uint64, value uint64) {
	word.Store(value)
}

func publishCount(count *atomic.Int64, delta int64) {
	count.Add(delta)
}

func touchLine(unsafe.Pointer) {}
