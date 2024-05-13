package gpbft

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
)

// Opaque type representing a tipset.
// This is expected to be:
// - a canonical sequence of CIDs of block headers identifying a tipset,
// - a commitment to the resulting power table,
// - a commitment to additional derived values.
// However, GossipPBFT doesn't need to know anything about that structure.
type TipSet = []byte

// A chain of tipsets comprising a base (the last finalised tipset from which the chain extends).
// and (possibly empty) suffix.
// Tipsets are assumed to be built contiguously on each other,
// though epochs may be missing due to null rounds.
// The zero value is not a valid chain, and represents a "bottom" value
// when used in a Granite message.
type ECChain []TipSet

// A map key for a chain. The zero value means "bottom".
type ChainKey string

// Maximum length of a chain value.
const CHAIN_MAX_LEN = 100

// Creates a new chain.
func NewChain(base TipSet, suffix ...TipSet) (ECChain, error) {
	var chain ECChain = []TipSet{base}
	chain = append(chain, suffix...)
	if err := chain.Validate(); err != nil {
		return nil, err
	}
	return chain, nil
}

func (c ECChain) IsZero() bool {
	return len(c) == 0
}

// Returns the base tipset.
func (c ECChain) Base() TipSet {
	return c[0]
}

// Returns the suffix of the chain after the base.
// An empty slice for a zero value.
func (c ECChain) Suffix() []TipSet {
	if c.IsZero() {
		return nil
	}
	return c[1:]
}

// Returns the last tipset in the chain.
// This could be the base tipset if there is no suffix.
// This will panic on a zero value.
func (c ECChain) Head() TipSet {
	return c[len(c)-1]
}

// Returns a new chain with the same base and no suffix.
// Invalid for a zero value.
func (c ECChain) BaseChain() ECChain {
	return ECChain{c[0]}
}

func (c ECChain) Extend(tip ...TipSet) ECChain {
	return append(c, tip...)
}

// Returns a chain with suffix (after the base) truncated to a maximum length.
// Prefix(0) returns the base chain.
// Invalid for a zero value.
func (c ECChain) Prefix(to int) ECChain {
	return c[:to+1]
}

// Compares two ECChains for equality.
func (c ECChain) Eq(other ECChain) bool {
	if len(c) != len(other) {
		return false
	}
	for i := range c {
		if !bytes.Equal(c[i], other[i]) {
			return false
		}
	}
	return true
}

// Checks whether two chains have the same base.
// Always false for a zero value.
func (c ECChain) SameBase(other ECChain) bool {
	if c.IsZero() || other.IsZero() {
		return false
	}
	return bytes.Equal(c.Base(), other.Base())
}

// Check whether a chain has a specific base tipset.
// Always false for a zero value.
func (c ECChain) HasBase(t TipSet) bool {
	if c.IsZero() || len(t) == 0 {
		return false
	}
	return bytes.Equal(c[0], t)
}

// Checks whether a chain has some prefix (including the base).
// Always false for a zero value.
func (c ECChain) HasPrefix(other ECChain) bool {
	if c.IsZero() || other.IsZero() {
		return false
	}
	if len(other) > len(c) {
		return false
	}
	for i := range other {
		if !bytes.Equal(c[i], other[i]) {
			return false
		}
	}
	return true
}

// Checks whether a chain has some tipset (including as its base).
func (c ECChain) HasTipset(t TipSet) bool {
	if len(t) == 0 {
		// Chain can never contain zero-valued TipSet.
		return false
	}
	for _, t2 := range c {
		if bytes.Equal(t, t2) {
			return true
		}
	}
	return false
}

// Validates a chain value, returning an error if it finds any issues.
// A chain is valid if it meets the following criteria:
// 1) All contained tipsets are non-empty.
// 2) The chain is not longer than CHAIN_MAX_LEN.
// An entirely zero-valued chain itself is deemed valid. See ECChain.IsZero.
func (c ECChain) Validate() error {
	if c.IsZero() {
		return nil
	}
	if len(c) > CHAIN_MAX_LEN {
		return errors.New("chain too long")
	}
	for _, tipSet := range c {
		if len(tipSet) == 0 {
			return errors.New("chain cannot contain zero-valued tip sets")
		}
	}
	return nil
}

// Returns an identifier for the chain suitable for use as a map key.
// This must completely determine the sequence of tipsets in the chain.
func (c ECChain) Key() ChainKey {
	var ln int
	for _, t := range c {
		ln += 4      // for length
		ln += len(t) // for data
	}
	var buf bytes.Buffer
	buf.Grow(ln)
	for _, t := range c {
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(t)))
		buf.Write(t)
	}
	return ChainKey(buf.String())
}

func (c ECChain) String() string {
	var b strings.Builder
	b.WriteString("[")
	for i, t := range c {
		b.WriteString(hex.EncodeToString(t))
		if i < len(c)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	return b.String()
}
