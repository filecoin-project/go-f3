package test

var ballast []byte

func init() {
	// Allocate 200MiB memory ballast to reduce the frequency of GC, which reduces
	// runtime of tests by ~20%.
	ballast = make([]byte, 200<<20)
}
