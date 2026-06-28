//go:build !amd64

package alosmap

import "unsafe"

// prefetchT0 falls back to a no-op on non-amd64 architectures.
func prefetchT0(addr unsafe.Pointer) {}
