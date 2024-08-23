package gpbft

import (
	"bytes"
	"math/big"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTQ_BigLog2_Table(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		integer int64
		fract   float64
	}{
		{"0.(9)", "ffffffffffffffffffffffffffffffff", -1, 0.9999999999999999},
		{"0.(9)8", "fffffffffffff8000000000000000000", -1, 0.9999999999999999},
		{"0.(9)7", "fffffffffffff7000000000000000000", -1, 0.9999999999999997},
		{"0.5", "80000000000000000000000000000000", -1, 0.0},
		{"2^-128", "1", -128, 0.0},
		{"2^-127", "2", -127, 0.0},
		{"2^-127 + eps", "3", -127, 0.5849625007211563},
		{"zero", "0", -129, 0.0},
		{"medium", "10020000000000000", -64, 0.0007042690112466499},
		{"medium2", "1000000000020000000000000", -32, 1.6409096303959814e-13},
		{"2^(53-128)", "20000000000000", -75, 0.0},
		{"2^(53-128)+eps", "20000000000001", -75, 0.0},
		{"2^(53-128)-eps", "1fffffffffffff", -76, 0.9999999999999999},
		{"2^(53-128)-2eps", "1ffffffffffff3", -76, 0.9999999999999979},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bigInt, ok := new(big.Int).SetString(test.input, 16)
			require.True(t, ok, "parsing int")
			integer, fract := bigLog2(bigInt)
			assert.EqualValues(t, test.integer, integer, "wrong integer part")
			assert.EqualValues(t, test.fract, fract, "wrong fractional part")
		})
	}
}

func FuzzTQ_linearToExp(f *testing.F) {
	f.Add(make([]byte, 16))
	f.Add(bytes.Repeat([]byte{0xff}, 16))
	f.Add(bytes.Repeat([]byte{0xa0}, 16))
	f.Fuzz(func(t *testing.T, ticket []byte) {
		if len(ticket) != 16 {
			return
		}
		q := linearToExpDist(ticket)
		runtime.KeepAlive(q)
	})
}
