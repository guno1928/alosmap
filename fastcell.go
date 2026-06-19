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

// val reconstructs the stored value from the box's interface words. typ is fixed
// for a box's lifetime while data is updated atomically by same-type repeat
// stores, so (typ, data.Load()) is always a consistent pair — a reader can never
// observe a type-confused interface.
func (b *valueBox) val() any {
	return ifaceFromWords(b.typ, unsafe.Pointer(b.data.Load()))
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
		return b.val(), true
	}
	return nil, false
}
