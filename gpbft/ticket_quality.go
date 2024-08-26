package gpbft

import (
	"fmt"
	"math"
	"math/big"

	"golang.org/x/crypto/blake2b"
)

// ComputeTicketQuality computes the quality of the ticket.
// The lower the resulting quality the better.
// We take the ticket, hash it using Blake2b256, take the low 128 bits, interpret them as a Q.128
// fixed point number in range of [0, 1). Then we convert this uniform distribution into exponential one,
// using -log(x) inverse distribution function.
// The exponential distribution has a property where minimum of two exponentially distributed random
// variables is itself a exponentially distributed.
// This allows us to use the rate parameter to weight across different participants according to there power.
// This ends up being `-log(ticket) / power` where ticket is [0, 1).
// We additionally use log-base-2 instead of natural logarithm as it is easier to implement,
// and it is just a linear factor on all tickets, meaning it does not influence their ordering.
func ComputeTicketQuality(ticket []byte, power int64) float64 {
	// we could use Blake2b-128 but 256 is more common and more widely supported
	ticketHash := blake2b.Sum256(ticket)
	quality := linearToExpDist(ticketHash[:16])
	return quality / float64(power)
}

// ticket should be 16 bytes
func linearToExpDist(ticket []byte) float64 {
	// we are interpreting the ticket as fixed-point number with 128 fractional bits
	// and adjusting using exponential distribution inverse function, -log(x)
	// we are computing Log2 of it with the adjustment that Log2(0) == -129
	// we can use Log2 instead of Ln as the difference is linear transform between them which
	// has no relative effect
	asInt := new(big.Int).SetBytes(ticket) // interpret at Q.128
	log2Int, log2Frac := bigLog2(asInt)
	// combine integer and fractional parts, in theory we could operate on them separately
	// but the 7bit gain on top of 52bits is minor
	log2 := float64(log2Int) + log2Frac
	return -log2
}

// bigLog2 takes an approximate logarithm of the big integer interpreted as Q.128
// If the input is zero, the output is [-129, 0.f).
// The result is an integer and fraction, where fraction is in [0, 1)
func bigLog2(asInt *big.Int) (int64, float64) {
	bitLen := uint(asInt.BitLen())
	if bitLen == 0 {
		return -129, 0.
	}
	log2Int := -int64(128 - bitLen + 1) //integer part of the Log2
	// now that we saved the integer part, we want to interpret it as [1,2)
	// so it will be Q.(bitlen-1)
	// to convert to float exactly, we need to bring it down to 53 bits
	if bitLen > 53 {
		asInt = asInt.Rsh(asInt, bitLen-53)
	} else if bitLen < 53 {
		asInt = asInt.Lsh(asInt, 53-bitLen)
	}
	if asInt.BitLen() != 53 {
		panic(fmt.Sprintf("wrong bitlen: %v", asInt.BitLen()))
	}
	asFloat := float64(asInt.Uint64()) / (1 << 52)
	if asFloat < 1 || asFloat >= 2 {
		panic("wrong range")
	}
	return log2Int, math.Log2(asFloat)
}
