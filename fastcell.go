package alosmap

import "unsafe"

// ifaceWords extracts the (type, data) words of an interface value.
// The result is suitable for atomic publication into a valueBox.
func ifaceWords(v any) (uintptr, unsafe.Pointer) {
	type eface struct {
		typ  uintptr
		data unsafe.Pointer
	}
	e := (*eface)(unsafe.Pointer(&v))
	return e.typ, e.data
}

// ifaceFromWords reconstructs an interface value from its (type, data) words.
func ifaceFromWords(typ uintptr, data unsafe.Pointer) any {
	type eface struct {
		typ  uintptr
		data unsafe.Pointer
	}
	var v any
	e := (*eface)(unsafe.Pointer(&v))
	e.typ = typ
	e.data = data
	return v
}

// deadMarker is the data word a remover installs to claim a box. It is an
// unexported package address that never escapes, so no stored value can ever
// present it as its own data word.
var deadMarker byte

// liveVal reconstructs the stored value from the box's interface words, and
// reports false once a remover has claimed the box. typ is fixed for a box's
// lifetime while data is updated atomically by same-type repeat stores, so
// (typ, data.Load()) is always a consistent pair — a reader can never observe a
// type-confused interface.
func (b *valueBox) liveVal() (any, bool) {
	_, value, live := b.liveData()
	return value, live
}

// liveData returns the data word alongside the reconstructed value so that a
// caller can compare the value and then claim the box at exactly the word it
// compared.
func (b *valueBox) liveData() (*byte, any, bool) {
	data := b.data.Load()
	if data == &deadMarker {
		return nil, nil, false
	}
	return data, ifaceFromWords(b.typ, unsafe.Pointer(data)), true
}

func (b *valueBox) claimData(data *byte) bool {
	return b.data.CompareAndSwap(data, &deadMarker)
}

// replaceData publishes other's value at exactly the data word the caller
// compared, so a conditional update linearizes on the same word as an in-place
// store. Both boxes must be simple and share a type.
func (b *valueBox) replaceData(data *byte, other *valueBox) bool {
	return b.data.CompareAndSwap(data, other.data.Load())
}

func (b *valueBox) sameShape(other *valueBox) bool {
	return b.isSimple() && other.isSimple() && b.typ == other.typ
}

// swapData publishes newData into the box and returns the data word it replaced.
// It reports false once a remover has claimed the box, in which case nothing was
// published and the caller must not treat its write as visible. The box's data
// word is the single linearization point for both in-place updates and removal,
// which is what keeps a repeat store from being swallowed by a concurrent
// delete.
func (b *valueBox) swapData(newData *byte) (*byte, bool) {
	for {
		data := b.data.Load()
		if data == &deadMarker {
			return nil, false
		}
		if b.data.CompareAndSwap(data, newData) {
			return data, true
		}
	}
}

// claim takes exclusive ownership of the box for removal and returns the value
// it held. It reports false when another remover claimed it first.
func (b *valueBox) claim() (any, bool) {
	data, ok := b.swapData(&deadMarker)
	if !ok {
		return nil, false
	}
	return ifaceFromWords(b.typ, unsafe.Pointer(data)), true
}

func (b *valueBox) claimed() bool {
	return b.data.Load() == &deadMarker
}

// setVal initializes the box's interface words from value. Used only before the
// box is published (no concurrent readers yet).
func (b *valueBox) setVal(value any) {
	typ, data := ifaceWords(value)
	b.typ = typ
	b.data.Store((*byte)(data))
}

// loadSimple returns the entry's value when it is a live simple box (no
// TTL/hits). It is the fast read path used by Range; callers fall back to the
// full readEntry path when it returns false.
func (e *entry) loadSimple() (any, bool) {
	if b := e.value.Load(); b != nil && b.isSimple() {
		return b.liveVal()
	}
	return nil, false
}
