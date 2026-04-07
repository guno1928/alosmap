package alosmap

import (
	"strings"
	"sync/atomic"
)

// StringSet clones value and stores the cloned string in target.
//
// This helper is intended for callers that keep mutable state in a pointer stored
// inside Map and want a small, string-specific write helper for an atomic.Value
// field. StringSet always calls strings.Clone before publishing the value, which
// avoids retaining the backing array of a larger source string or short-lived
// substring.
//
// The target argument must be a non-nil pointer to an atomic.Value that will hold
// string values. The function does not perform type assertions on previous contents;
// it simply stores the cloned string atomically.
func StringSet(target *atomic.Value, value string) {
	cloned := strings.Clone(value)
	target.Store(cloned)
}
