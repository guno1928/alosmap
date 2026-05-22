//go:build !amd64

package alosmap

import "unsafe"

// prefetchT0 / prefetchT2 fall back to no-ops on non-amd64 architectures.
func prefetchT0(addr unsafe.Pointer) {}
func prefetchT2(addr unsafe.Pointer) {}
