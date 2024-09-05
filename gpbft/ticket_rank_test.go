package gpbft

import (
	"bytes"
	"math"
	"math/big"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestTicketRank_BigLog2(t *testing.T) {
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
		{"2^-129", "0", -129, 0.0},
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
			assert.Equal(t, test.integer, integer, "wrong integer part")
			assert.InDelta(t, test.fract, fract, 1e-15, "wrong fractional delta")
			if test.fract != 0.0 {
				assert.InEpsilon(t, test.fract, fract, 1e-03, "wrong fractional epsilon")
			} else {
				assert.Equal(t, test.fract, fract, "wrong fractional epsilon")
			}
		})
	}
}

func FuzzTQ_LinearToExp(f *testing.F) {
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

func TestComputeTicketRank(t *testing.T) {
	t.Run("Non-zero for non-zero power", func(t *testing.T) {
		ticket := generateTicket(t)
		power := int64(10)
		rank := ComputeTicketRank(ticket, power)
		require.Greater(t, rank, 0.0, "Expected positive rank value, got %f", rank)
	})

	t.Run("Weighed by power", func(t *testing.T) {
		ticket := generateTicket(t)
		rank1 := ComputeTicketRank(ticket, 10)
		rank2 := ComputeTicketRank(ticket, 11)
		require.Less(t, rank2, rank1, "Expected rank2 to be less than rank1 due to weight by power, got rank1=%f, rank2=%f", rank1, rank2)
	})

	t.Run("Zero power is handled gracefully", func(t *testing.T) {
		ticket := generateTicket(t)
		rank := ComputeTicketRank(ticket, 0)
		require.True(t, math.IsInf(rank, 1), "Expected rank to be infinity with power 0, got %f", rank)
	})

	t.Run("Negative power is handled gracefully", func(t *testing.T) {
		ticket := generateTicket(t)
		rank := ComputeTicketRank(ticket, -5)
		require.True(t, math.IsInf(rank, 1), "Expected rank to be infinity for negative power, got %f", rank)
	})

	t.Run("Different tickets should have different qualities", func(t *testing.T) {
		rank1 := ComputeTicketRank(generateTicket(t), 1413)
		rank2 := ComputeTicketRank(generateTicket(t), 1413)
		require.NotEqual(t, rank1, rank2, "Expected different qualities for different tickets, got rank1=%f, rank2=%f", rank1, rank2)
	})

	t.Run("Tickets with same 16 byte prefix should different rank", func(t *testing.T) {
		prefix := generateTicket(t)
		ticket1 := append(prefix, 14)
		ticket2 := append(prefix, 13)
		require.NotEqual(t, ticket1, ticket2)

		rank1 := ComputeTicketRank(ticket1, 1413)
		rank2 := ComputeTicketRank(ticket2, 1413)
		require.NotEqual(t, rank1, rank2, "Expected different qualities for different tickets with the same 16 byte prefix, got rank1=%f, rank2=%f", rank1, rank2)
	})
}

func generateTicket(t *testing.T) []byte {
	var ticket [16]byte
	n, err := rand.Read(ticket[:])
	require.NoError(t, err)
	require.Equal(t, 16, n)
	return ticket[:]
}
