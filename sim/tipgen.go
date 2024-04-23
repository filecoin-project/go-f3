package sim

import "github.com/filecoin-project/go-f3/gpbft"

var alphanum = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// A tipset generator.
// This uses a fast xorshift PRNG to generate random tipset IDs.
// The statistical properties of these are not important to correctness.
type TipGen struct {
	xorshiftState uint64
}

func NewTipGen(seed uint64) *TipGen {
	return &TipGen{seed}
}

func (c *TipGen) Sample() gpbft.TipSet {
	b := make([]byte, 8)
	for i := range b {
		b[i] = alphanum[c.nextN(len(alphanum))]
	}
	return b
}

func (c *TipGen) nextN(n int) uint64 {
	bucketSize := uint64(1<<63) / uint64(n)
	limit := bucketSize * uint64(n)
	for {
		x := c.next()
		if x < limit {
			return x / bucketSize
		}
	}
}

func (c *TipGen) next() uint64 {
	x := c.xorshiftState
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	c.xorshiftState = x
	return x
}
