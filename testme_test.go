package alosmap

import (
	"fmt"
	"testing"
)

func TestExternalMutations(t *testing.T) {
	m := New()

	// === Test 1: Reassign string key variable ===
	sessionKey := "session:42"
	m.Store(S(sessionKey), "original-value")

	sessionKey = "i-changed"

	v, ok := m.Load(S("session:42"))
	fmt.Printf("Test 1 - Reassign key variable:\n")
	fmt.Printf("  Load with original key 'session:42': ok=%v value=%v\n", ok, v)

	v2, ok2 := m.Load(S(sessionKey))
	fmt.Printf("  Load with new key '%s': ok=%v value=%v\n", sessionKey, ok2, v2)

	if !ok || v != "original-value" {
		t.Errorf("Test 1 failed: expected to find 'session:42' with value 'original-value', got ok=%v value=%v", ok, v)
	}
	if ok2 {
		t.Errorf("Test 1 failed: expected to NOT find '%s', but got ok=true", sessionKey)
	}

	// === Test 2: Mutate slice value ===
	payload := []byte{1, 2, 3}
	m.Store(S("config"), payload)

	payload[0] = 99

	v3, ok3 := m.Load(S("config"))
	fmt.Printf("\nTest 2 - Mutate slice value after Store:\n")
	fmt.Printf("  Original slice after mutation: %v\n", payload)
	fmt.Printf("  Value from map: ok=%v value=%v\n", ok3, v3)

	if !ok3 {
		t.Fatalf("Test 2 failed: expected to find 'config'")
	}
	mapSlice := v3.([]byte)
	if mapSlice[0] != 99 {
		t.Errorf("Test 2 failed: expected map value[0]=99 (shared), got %d", mapSlice[0])
	}

	// === Test 3: Mutate map value ===
	configMap := map[string]int{"a": 1}
	m.Store(S("mapval"), configMap)

	configMap["a"] = 999
	configMap["b"] = 2

	v4, ok4 := m.Load(S("mapval"))
	fmt.Printf("\nTest 3 - Mutate map value after Store:\n")
	fmt.Printf("  Original map after mutation: %v\n", configMap)
	fmt.Printf("  Value from map: ok=%v value=%v\n", ok4, v4)

	if !ok4 {
		t.Fatalf("Test 3 failed: expected to find 'mapval'")
	}
	mapMap := v4.(map[string]int)
	if mapMap["a"] != 999 {
		t.Errorf("Test 3 failed: expected map value['a']=999 (shared), got %d", mapMap["a"])
	}
	if _, exists := mapMap["b"]; !exists {
		t.Errorf("Test 3 failed: expected map value to have key 'b' (shared)")
	}

	// === Test 4: Mutate struct via pointer ===
	type Item struct {
		Name  string
		Count int
	}
	item := &Item{Name: "widget", Count: 5}
	m.Store(S("ptr"), item)

	item.Count = 42

	v5, ok5 := m.Load(S("ptr"))
	fmt.Printf("\nTest 4 - Mutate struct via pointer after Store:\n")
	fmt.Printf("  Original struct after mutation: %+v\n", item)
	fmt.Printf("  Value from map: ok=%v value=%+v\n", ok5, v5)

	if !ok5 {
		t.Fatalf("Test 4 failed: expected to find 'ptr'")
	}
	mapItem := v5.(*Item)
	if mapItem.Count != 42 {
		t.Errorf("Test 4 failed: expected map value.Count=42 (shared), got %d", mapItem.Count)
	}

	// === Test 5: Reassign int64 key variable ===
	numKey := int64(12345)
	m.Store(I(numKey), "number-value")

	numKey = 99999

	v6, ok6 := m.Load(I(12345))
	fmt.Printf("\nTest 5 - Reassign int64 key variable:\n")
	fmt.Printf("  Load with original key 12345: ok=%v value=%v\n", ok6, v6)

	v7, ok7 := m.Load(I(numKey))
	fmt.Printf("  Load with new key %d: ok=%v value=%v\n", numKey, ok7, v7)

	if !ok6 || v6 != "number-value" {
		t.Errorf("Test 5 failed: expected to find 12345 with value 'number-value', got ok=%v value=%v", ok6, v6)
	}
	if ok7 {
		t.Errorf("Test 5 failed: expected to NOT find %d, but got ok=true", numKey)
	}
}
