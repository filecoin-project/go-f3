package gpbft

import (
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"
)

// Information about a tipset that is relevant to the F3 protocol.
// This is a lightweight value type comprising 3 machine words.
// Fields are exported for CBOR generation, but are opqaue and should not be accessed
// within the protocol implementation.
type TipSet struct {
	// The epoch of the blocks in the tipset.
	Epoch int64
	// The CID of the tipset.
	CID TipSetID
}

// Creates a new tipset.
func NewTipSet(epoch int64, cid TipSetID) TipSet {
	return TipSet{
		Epoch: epoch,
		CID:   cid,
	}
}

// Returns a zero value tipset.
// The zero value is not a meaningful tipset and may be used to represent bottom.
func ZeroTipSet() TipSet {
	return TipSet{}
}

func (t TipSet) IsZero() bool {
	return t.Epoch == 0 && t.CID.IsZero()
}

func (t TipSet) String() string {
	var b strings.Builder
	b.Write(t.CID.Bytes())
	b.WriteString("@")
	b.WriteString(strconv.FormatInt(t.Epoch, 10))
	return b.String()
}

func (t TipSet) MarshalForSigning(w io.Writer) {
	_ = binary.Write(w, binary.BigEndian, t.Epoch)
	_, _ = w.Write(t.CID.Bytes())
}

// A chain of tipsets comprising a base (the last finalised tipset from which the chain extends).
// and (possibly empty) suffix.
// Tipsets are assumed to be built contiguously on each other, though epochs may be missing due to null rounds.
// The zero value is not a valid chain, and represents a "bottom" value when used in a Granite message.
type ECChain []TipSet

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

// Returns the CID of the head tipset, or empty string for a zero value
func (c ECChain) HeadOrZero() TipSet {
	if c.IsZero() {
		return ZeroTipSet()
	}
	return c.Head()
}

// Returns a new chain with the same base and no suffix.
// Invalid for a zero value.
func (c ECChain) BaseChain() ECChain {
	return ECChain{c[0]}
}

// Returns a new chain extending this chain with one tipset.
// The new tipset is given an epoch and weight one greater than the previous head.
func (c ECChain) Extend(cid TipSetID) ECChain {
	return append(c, TipSet{
		Epoch: c.Head().Epoch + 1,
		CID:   cid,
	})
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
		if c[i] != other[i] {
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
	return c.Base() == other.Base()
}

// Check whether a chain has a specific base tipset.
// Always false for a zero value.
func (c ECChain) HasBase(t TipSet) bool {
	if c.IsZero() || t.IsZero() {
		return false
	}
	return c[0] == t
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
		if c[i] != other[i] {
			return false
		}
	}
	return true
}

// Checks whether a chain has some tipset (including as its base).
func (c ECChain) HasTipset(t TipSet) bool {
	if t.IsZero() {
		// Chain can never contain zero-valued TipSet.
		return false
	}
	for _, t2 := range c {
		if t2 == t {
			return true
		}
	}
	return false
}

// Validate verifies the integrity of the chain, returning an error if it finds any issues.
// A chain is valid if it meets the following criteria:
// 1) All contained elements have non-zero values. Use TipSet.IsZero to check for zero-valued elements.
// 2) TipSets are arranged in strictly increasing order by their epochs, without any repetitions.
// An entirely zero-valued chain itself is deemed valid. See ECChain.IsZero.
func (c ECChain) Validate() error {
	if c.IsZero() {
		return nil
	}
	var epochSoFar int64
	for _, tipSet := range c {
		switch {
		case tipSet.IsZero():
			return errors.New("chain cannot contain zero-valued tip sets")
		case tipSet.Epoch <= epochSoFar:
			return errors.New("chain epoch must be in order and unique")
		default:
			epochSoFar = tipSet.Epoch
		}
	}
	return nil
}

func (c ECChain) String() string {
	var b strings.Builder
	b.WriteString("[")
	for i, t := range c {
		b.WriteString(t.String())
		if i < len(c)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	return b.String()
}
