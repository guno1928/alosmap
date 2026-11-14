//go:build !amd64

package alosmap

import "unsafe"

func touchLine(unsafe.Pointer) {}
