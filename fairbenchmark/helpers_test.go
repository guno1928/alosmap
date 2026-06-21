package benchmarks

import "strconv"

func stringKeys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = "key:" + strconv.Itoa(i)
	}
	return ks
}
