package alosmap

import "unsafe"

// ifaceWords extracts the (type, data) words of an interface value.
// The result is suitable for atomic publication into a cell.
func ifaceWords(v any) (uintptr, unsafe.Pointer) {
	type eface struct {
		typ  uintptr
		data unsafe.Pointer
	}
	e := (*eface)(unsafe.Pointer(&v))
	return e.typ, e.data
}

// ifaceFromWords reconstructs an interface value from its (type, data) words.
// Caller MUST ensure typ != 0 (otherwise the returned eface is malformed).
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

// primeCell publishes boxed's value into the entry's cell so that subsequent
// reads can return the value without consulting valueBox at all. Order matters:
// cellData is stored first (release), then cellTyp (release) — a reader that
// observes the new cellTyp via an acquire load is guaranteed to see the matching
// cellData. Boxes carrying TTL, hit limits, or cloned-byte tracking are NOT
// primed; the cell stays invalidated (cellTyp == 0) so readers consult the
// valueBox.
func (e *entry) primeCell(boxed *valueBox) {
	if boxed == nil || boxed.expiresAt != 0 || boxed.clonedBytes != 0 || boxed.hits.Load() != 0 {
		return
	}
	typ, data := ifaceWords(boxed.value)
	if typ == 0 {
		return
	}
	e.cellData.Store((*byte)(data))
	e.cellTyp.Store(typ)
}

// invalidateCell clears the cell so readers fall back to the valueBox path.
// Used by Delete and by every path that publishes a non-simple valueBox.
func (e *entry) invalidateCell() {
	e.cellTyp.Store(0)
}
