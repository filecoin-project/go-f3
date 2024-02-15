package test

var ballast []byte

func init() {
	// 100MiB memory ballast to reduce the frequency of GC
	// reduces runtime of tests by 20%
	ballast = make([]byte, 100<<20)
}
