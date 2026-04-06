package alosmap

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrKeyNotFound  = errors.New("alosmap: key not found")
	ErrTypeMismatch = errors.New("alosmap: modifier type mismatch")
	ErrOverflow     = errors.New("alosmap: numeric overflow")
)

// StringSet clones value and stores it in target.
//
// Use StringSet with an atomic.Value field inside a pointer stored in the map when
// you want a small string-specific helper without introducing a custom wrapper type.
// The stored string is cloned first so short-lived substrings do not retain the
// caller's original backing bytes.
func StringSet(target *atomic.Value, value string) {
	cloned := strings.Clone(value)
	target.Store(cloned)
}

// Add atomically adds delta to the numeric value stored at key and returns the updated value.
//
// Add is intended for top-level numeric values stored directly in the map. The key must
// already exist and the current value must be numeric. TTL and hit-limit settings are
// preserved across the update. Add returns ErrKeyNotFound when the key does not resolve
// to a live entry, ErrTypeMismatch when the current value or delta type is incompatible,
// and ErrOverflow when the requested update cannot be represented safely.
func (m *Map) Add(key string, delta any) (any, error) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).modifyNumericDirect(hash, key, m.cloneValue, delta, false)
}

// Sub atomically subtracts delta from the numeric value stored at key and returns the updated value.
//
// Sub follows the same rules as Add: the key must already resolve to a live numeric
// value, TTL and hit-limit settings are preserved, and errors report missing keys,
// type mismatches, or arithmetic overflow.
func (m *Map) Sub(key string, delta any) (any, error) {
	hash := hashString(m.seed, key)
	return m.pickShard(hash).modifyNumericDirect(hash, key, m.cloneValue, delta, true)
}

// Set atomically replaces the numeric value stored at key while preserving the entry's TTL and hit limit.
//
// Set is the numeric-update counterpart to Store. It requires an existing live numeric
// entry and a numeric replacement value. Unlike Store, it keeps the existing entry's
// lifetime rules intact. Set returns ErrKeyNotFound for missing keys and ErrTypeMismatch
// when either the current value or the new value is not numeric.
func (m *Map) Set(key string, value any) error {
	if !isNumeric(value) {
		return fmt.Errorf("%w: Set value must be numeric, got %T", ErrTypeMismatch, value)
	}
	hash := hashString(m.seed, key)
	return m.pickShard(hash).setNumericDirect(hash, key, value)
}

func isNumeric(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64:
		return true
	}
	return false
}

func (s *shard) setNumericDirect(hash uint64, key string, value any) error {
	for {
		s.beginWrite()
		t := s.table.Load()
		e := findEntry(t, hash, key)
		if e == nil {
			s.endWrite()
			return ErrKeyNotFound
		}

		nb := new(valueBox)
		for {
			old := e.value.Load()
			if old == nil {
				s.endWrite()
				return ErrKeyNotFound
			}

			if old.expiresAt > 0 && time.Now().UnixNano() >= old.expiresAt {
				if s.clearIfMatch(e, old, true, false) {
					s.endWrite()
					return ErrKeyNotFound
				}
				continue
			}

			hits := old.hits.Load()
			if hits < 0 {
				if s.clearIfMatch(e, old, false, true) {
					s.endWrite()
					return ErrKeyNotFound
				}
				continue
			}

			if !isNumeric(old.value) {
				s.endWrite()
				return fmt.Errorf("%w: current value must be numeric, got %T", ErrTypeMismatch, old.value)
			}

			nb.value = value
			nb.clonedBytes = 0
			nb.expiresAt = old.expiresAt
			nb.hits.Store(hits)
			if e.value.CompareAndSwap(old, nb) {
				s.valueBytes.Add(-old.clonedBytes)
				if old.expiresAt > 0 || hits != 0 {
					s.needsCleanup.Store(true)
				}
				s.endWrite()
				return nil
			}
		}
	}
}

func (s *shard) modifyNumericDirect(hash uint64, key string, cloneValue ValueCloneFunc, delta any, subtract bool) (any, error) {
	for {
		s.beginWrite()
		t := s.table.Load()
		e := findEntry(t, hash, key)
		if e == nil {
			s.endWrite()
			return nil, ErrKeyNotFound
		}

		nb := new(valueBox)
		for {
			old := e.value.Load()
			if old == nil {
				s.endWrite()
				return nil, ErrKeyNotFound
			}

			if old.expiresAt > 0 && time.Now().UnixNano() >= old.expiresAt {
				if s.clearIfMatch(e, old, true, false) {
					s.endWrite()
					return nil, ErrKeyNotFound
				}
				continue
			}

			hits := old.hits.Load()
			if hits < 0 {
				if s.clearIfMatch(e, old, false, true) {
					s.endWrite()
					return nil, ErrKeyNotFound
				}
				continue
			}

			if cv, cvOK := old.value.(int); cvOK {
				if d, dOK := delta.(int); dOK {
					var v int
					if subtract {
						v = cv - d
						if (d > 0 && v > cv) || (d < 0 && v < cv) {
							s.endWrite()
							return nil, ErrOverflow
						}
					} else {
						v = cv + d
						if (d > 0 && v < cv) || (d < 0 && v > cv) {
							s.endWrite()
							return nil, ErrOverflow
						}
					}
					nb.value = v
					nb.clonedBytes = 0
					nb.expiresAt = old.expiresAt
					nb.hits.Store(hits)
					if e.value.CompareAndSwap(old, nb) {
						s.valueBytes.Add(-old.clonedBytes)
						if old.expiresAt > 0 || hits != 0 {
							s.needsCleanup.Store(true)
						}
						s.endWrite()
						return v, nil
					}
					continue
				}
			}

			if cv, cvOK := old.value.(int64); cvOK {
				if d, dOK := deltaAsInt64(delta); dOK {
					if subtract {
						if d == math.MinInt64 {
							s.endWrite()
							return nil, ErrOverflow
						}
						d = -d
					}
					v := cv + d
					if (d > 0 && v < cv) || (d < 0 && v > cv) {
						s.endWrite()
						return nil, ErrOverflow
					}
					nb.value = v
					nb.clonedBytes = 0
					nb.expiresAt = old.expiresAt
					nb.hits.Store(hits)
					if e.value.CompareAndSwap(old, nb) {
						s.valueBytes.Add(-old.clonedBytes)
						if old.expiresAt > 0 || hits != 0 {
							s.needsCleanup.Store(true)
						}
						s.endWrite()
						return v, nil
					}
					continue
				}
			}

			if cv, cvOK := old.value.(float64); cvOK {
				if d, dOK := deltaAsFloat64(delta); dOK {
					if subtract {
						d = -d
					}
					v := cv + d
					if !math.IsNaN(v) && !math.IsInf(v, 0) {
						nb.value = v
						nb.clonedBytes = 0
						nb.expiresAt = old.expiresAt
						nb.hits.Store(hits)
						if e.value.CompareAndSwap(old, nb) {
							s.valueBytes.Add(-old.clonedBytes)
							if old.expiresAt > 0 || hits != 0 {
								s.needsCleanup.Store(true)
							}
							s.endWrite()
							return v, nil
						}
						continue
					}
				}
			}

			updated, result, ok := fastNumericOp(old.value, delta, subtract)
			if !ok {
				uv, rv, err := numericFallback(old.value, delta, subtract)
				if err != nil {
					s.endWrite()
					return nil, err
				}
				updated, result = uv, rv
			}
			nb.value = updated
			nb.clonedBytes = 0
			nb.expiresAt = old.expiresAt
			nb.hits.Store(hits)
			if e.value.CompareAndSwap(old, nb) {
				s.valueBytes.Add(-old.clonedBytes)
				if old.expiresAt > 0 || hits != 0 {
					s.needsCleanup.Store(true)
				}
				s.endWrite()
				return result, nil
			}
		}
	}
}
func fastNumericOp(current any, delta any, subtract bool) (updated any, result any, ok bool) {
	switch cv := current.(type) {
	case int:
		d, dok := deltaAsInt64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if d == math.MinInt64 {
				return nil, nil, false
			}
			d = -d
		}
		r := int64(cv) + d
		if signedAddOverflows(int64(cv), d, r, 64) {
			return nil, nil, false
		}
		v := int(r)
		return v, v, true
	case int64:
		d, dok := deltaAsInt64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if d == math.MinInt64 {
				return nil, nil, false
			}
			d = -d
		}
		r := cv + d
		if signedAddOverflows(cv, d, r, 64) {
			return nil, nil, false
		}
		return r, r, true
	case int32:
		d, dok := deltaAsInt64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if d == math.MinInt64 {
				return nil, nil, false
			}
			d = -d
		}
		r := int64(cv) + d
		if r < math.MinInt32 || r > math.MaxInt32 {
			return nil, nil, false
		}
		v := int32(r)
		return v, v, true
	case int16:
		d, dok := deltaAsInt64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if d == math.MinInt64 {
				return nil, nil, false
			}
			d = -d
		}
		r := int64(cv) + d
		if r < math.MinInt16 || r > math.MaxInt16 {
			return nil, nil, false
		}
		v := int16(r)
		return v, v, true
	case int8:
		d, dok := deltaAsInt64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if d == math.MinInt64 {
				return nil, nil, false
			}
			d = -d
		}
		r := int64(cv) + d
		if r < math.MinInt8 || r > math.MaxInt8 {
			return nil, nil, false
		}
		v := int8(r)
		return v, v, true
	case uint:
		d, dok := deltaAsUint64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if uint64(cv) < d {
				return nil, nil, false
			}
			v := cv - uint(d)
			return v, v, true
		}
		v := cv + uint(d)
		if uint64(v) < uint64(cv) {
			return nil, nil, false
		}
		return v, v, true
	case uint64:
		d, dok := deltaAsUint64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if cv < d {
				return nil, nil, false
			}
			v := cv - d
			return v, v, true
		}
		v := cv + d
		if v < cv {
			return nil, nil, false
		}
		return v, v, true
	case uint32:
		d, dok := deltaAsUint64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if uint64(cv) < d {
				return nil, nil, false
			}
			r := uint64(cv) - d
			if r > math.MaxUint32 {
				return nil, nil, false
			}
			v := uint32(r)
			return v, v, true
		}
		r := uint64(cv) + d
		if r > math.MaxUint32 {
			return nil, nil, false
		}
		v := uint32(r)
		return v, v, true
	case uint16:
		d, dok := deltaAsUint64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if uint64(cv) < d {
				return nil, nil, false
			}
			r := uint64(cv) - d
			if r > math.MaxUint16 {
				return nil, nil, false
			}
			v := uint16(r)
			return v, v, true
		}
		r := uint64(cv) + d
		if r > math.MaxUint16 {
			return nil, nil, false
		}
		v := uint16(r)
		return v, v, true
	case uint8:
		d, dok := deltaAsUint64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			if uint64(cv) < d {
				return nil, nil, false
			}
			r := uint64(cv) - d
			if r > math.MaxUint8 {
				return nil, nil, false
			}
			v := uint8(r)
			return v, v, true
		}
		r := uint64(cv) + d
		if r > math.MaxUint8 {
			return nil, nil, false
		}
		v := uint8(r)
		return v, v, true
	case float64:
		d, dok := deltaAsFloat64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			d = -d
		}
		r := cv + d
		if math.IsNaN(r) || math.IsInf(r, 0) {
			return nil, nil, false
		}
		return r, r, true
	case float32:
		d, dok := deltaAsFloat64(delta)
		if !dok {
			return nil, nil, false
		}
		if subtract {
			d = -d
		}
		r := float64(cv) + d
		v := float32(r)
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, nil, false
		}
		return v, v, true
	}
	return nil, nil, false
}

func numericFallback(current any, delta any, subtract bool) (any, any, error) {
	if current == nil {
		return nil, nil, fmt.Errorf("%w: nil value", ErrTypeMismatch)
	}
	rv := reflect.ValueOf(current)
	if !rv.IsValid() {
		return nil, nil, fmt.Errorf("%w: invalid value", ErrTypeMismatch)
	}
	updated, result, err := numericLeaf(rv, delta, subtract)
	if err != nil {
		return nil, nil, err
	}
	return updated.Interface(), result, nil
}

func numericLeaf(target reflect.Value, delta any, subtract bool) (reflect.Value, any, error) {
	switch target.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return signedNumericLeaf(target, delta, subtract)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return unsignedNumericLeaf(target, delta, subtract)
	case reflect.Float32, reflect.Float64:
		return floatNumericLeaf(target, delta, subtract)
	default:
		return reflect.Value{}, nil, fmt.Errorf("%w: want numeric target, have %s", ErrTypeMismatch, target.Type())
	}
}

func signedNumericLeaf(target reflect.Value, delta any, subtract bool) (reflect.Value, any, error) {
	deltaInt, ok := deltaAsInt64(delta)
	if !ok {
		return reflect.Value{}, nil, fmt.Errorf("%w: want integer delta for %s", ErrTypeMismatch, target.Type())
	}
	if subtract {
		if deltaInt == math.MinInt64 {
			return reflect.Value{}, nil, ErrOverflow
		}
		deltaInt = -deltaInt
	}
	current := target.Int()
	updated := current + deltaInt
	if signedAddOverflows(current, deltaInt, updated, target.Type().Bits()) {
		return reflect.Value{}, nil, ErrOverflow
	}
	clone := reflect.New(target.Type()).Elem()
	clone.SetInt(updated)
	return clone, clone.Interface(), nil
}

func unsignedNumericLeaf(target reflect.Value, delta any, subtract bool) (reflect.Value, any, error) {
	deltaUint, ok := deltaAsUint64(delta)
	if !ok {
		return reflect.Value{}, nil, fmt.Errorf("%w: want unsigned delta for %s", ErrTypeMismatch, target.Type())
	}
	current := target.Uint()
	var updated uint64
	if subtract {
		if current < deltaUint {
			return reflect.Value{}, nil, ErrOverflow
		}
		updated = current - deltaUint
	} else {
		updated = current + deltaUint
		if updated < current {
			return reflect.Value{}, nil, ErrOverflow
		}
	}
	bits := target.Type().Bits()
	if bits < 64 {
		maxValue := (uint64(1) << bits) - 1
		if updated > maxValue {
			return reflect.Value{}, nil, ErrOverflow
		}
	}
	clone := reflect.New(target.Type()).Elem()
	clone.SetUint(updated)
	return clone, clone.Interface(), nil
}

func floatNumericLeaf(target reflect.Value, delta any, subtract bool) (reflect.Value, any, error) {
	deltaFloat, ok := deltaAsFloat64(delta)
	if !ok {
		return reflect.Value{}, nil, fmt.Errorf("%w: want numeric delta for %s", ErrTypeMismatch, target.Type())
	}
	if subtract {
		deltaFloat = -deltaFloat
	}
	updated := target.Float() + deltaFloat
	if math.IsNaN(updated) || math.IsInf(updated, 0) {
		return reflect.Value{}, nil, ErrOverflow
	}
	if target.Kind() == reflect.Float32 {
		cast := float32(updated)
		if math.IsNaN(float64(cast)) || math.IsInf(float64(cast), 0) {
			return reflect.Value{}, nil, ErrOverflow
		}
		updated = float64(cast)
	}
	clone := reflect.New(target.Type()).Elem()
	clone.SetFloat(updated)
	return clone, clone.Interface(), nil
}

func signedAddOverflows(current int64, delta int64, updated int64, bits int) bool {
	if (delta > 0 && updated < current) || (delta < 0 && updated > current) {
		return true
	}
	if bits >= 64 {
		return false
	}
	minValue := int64(-1) << (bits - 1)
	maxValue := ^minValue
	return updated < minValue || updated > maxValue
}

func deltaAsInt64(delta any) (int64, bool) {
	switch typed := delta.(type) {
	case int:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case uint8:
		return int64(typed), true
	case uint16:
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint64:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case uintptr:
		if uint64(typed) > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	default:
		return 0, false
	}
}

func deltaAsUint64(delta any) (uint64, bool) {
	switch typed := delta.(type) {
	case int:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int8:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int16:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int32:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int64:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case uint:
		return uint64(typed), true
	case uint8:
		return uint64(typed), true
	case uint16:
		return uint64(typed), true
	case uint32:
		return uint64(typed), true
	case uint64:
		return typed, true
	case uintptr:
		return uint64(typed), true
	default:
		return 0, false
	}
}

func deltaAsFloat64(delta any) (float64, bool) {
	switch typed := delta.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case uintptr:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}
