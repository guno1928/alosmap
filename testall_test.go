package alosmap

import (
	"fmt"
	"testing"
)

func TestAllCases(t *testing.T) {
	if len(AllTests) == 0 {
		t.Fatal("AllTests is empty")
	}

	seenIDs := make(map[int]struct{}, len(AllTests))
	for _, tc := range AllTests {
		tc := tc
		if tc.Fn == nil {
			t.Fatalf("test case %d (%s) has a nil function", tc.ID, tc.Name)
		}
		if _, exists := seenIDs[tc.ID]; exists {
			t.Fatalf("duplicate test case id %d", tc.ID)
		}
		seenIDs[tc.ID] = struct{}{}

		t.Run(fmt.Sprintf("%03d_%s", tc.ID, tc.Name), func(t *testing.T) {
			if err := tc.Fn(); err != nil {
				t.Fatal(err)
			}
		})
	}

	t.Logf("executed %d testall cases", len(AllTests))
}
