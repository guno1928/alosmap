package alosmap

import (
	"strings"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestStringSetStoresStringInAtomicValue(t *testing.T) {
	var value atomic.Value

	StringSet(&value, "john")

	stored := value.Load()
	if stored == nil {
		t.Fatal("Load() = nil, want stored string")
	}
	if got := stored.(string); got != "john" {
		t.Fatalf("Load() = %q, want john", got)
	}
}

func TestStringSetOverwritesExistingString(t *testing.T) {
	var value atomic.Value

	value.Store("before")
	StringSet(&value, "after")

	if got := value.Load().(string); got != "after" {
		t.Fatalf("Load() = %q, want after", got)
	}
}

func TestStringSetClonesStoredString(t *testing.T) {
	var value atomic.Value

	large := strings.Repeat("x", 256) + "john"
	input := large[len(large)-4:]
	inputPtr := unsafe.StringData(input)

	StringSet(&value, input)

	stored := value.Load().(string)
	storedPtr := unsafe.StringData(stored)
	if storedPtr == inputPtr {
		t.Fatalf("stored string reused original backing bytes")
	}
	if stored != "john" {
		t.Fatalf("stored string = %q, want john", stored)
	}
}
