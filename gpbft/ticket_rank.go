package gpbft

import (
	"fmt"
	"math"
	"math/big"

	"golang.org/x/crypto/blake2b"
)

// ComputeTicketRank computes a rank for the given ticket, weighted by a
// participant's power. A lower rank value indicates a better ranking. The
// process involves the following phases:
//
//  1. Hash the ticket using the Blake2b256 hash function.
//  2. Extract the low 128 bits from the hash and interpret them as a Q.128
//     fixed-point number in the range [0, 1).
//  3. Convert this uniform distribution to an exponential distribution using
//     the inverse distribution function -log(x).
//
// The exponential distribution has the property that the minimum of two
// exponentially distributed random variables is also exponentially distributed.
// This allows us to weight ranks according to the participant's power by using
// the formula: `-log(ticket) / power`, where `ticket` is in the range [0, 1).
//
// We use a base-2 logarithm instead of a natural logarithm for ease of
// implementation. The choice of logarithm base only affects all ranks linearly
// and does not alter their order.
func ComputeTicketRank(ticket Ticket, scaledPower int64) float64 {
	if scaledPower <= 0 {
		return math.Inf(1)
	}
	// we could use Blake2b-128 but 256 is more common and more widely supported
	ticketHash := blake2b.Sum256(ticket)
	rank := linearToExpDist(ticketHash[:16])
	return rank / float64(scaledPower)
}

// if ticket length is not 16, linearToExpDist will panic
func linearToExpDist(ticket []byte) float64 {
	if len(ticket) != 16 {
		panic(fmt.Sprintf("expected ticket to be 16 bytes, got: %d", len(ticket)))
	}
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
