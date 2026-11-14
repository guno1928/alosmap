//go:build amd64

package alosmap

import (
	"sync/atomic"
	"unsafe"
)

func touchLine(line unsafe.Pointer) {
	_ = (*atomic.Uint64)(unsafe.Add(line, typedFlatTouchOffset)).Load()
}
