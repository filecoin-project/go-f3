package f3

import (
	"reflect"
	"strconv"
	"strings"
)

// Information about a tipset that is relevant to the F3 protocol.
type TipSet struct {
	// The epoch of the blocks in the tipset.
	Epoch int
	// The CID of the tipset.
	CID TipSetID
	// The EC consensus weight of the tipset.
	Weight uint
}

// Creates a new tipset.
func NewTipSet(epoch int, cid TipSetID, weight uint) TipSet {
	return TipSet{
		Epoch:  epoch,
		CID:    cid,
		Weight: weight,
	}
}

// Compares two tipsets by weight, breaking ties with CID.
// Note that the real weight function breaks ties with VRF tickets.
func (t *TipSet) Compare(other *TipSet) int {
	if t.Weight == other.Weight {
		return t.CID.Compare(other.CID)
	} else if t.Weight < other.Weight {
		return -1
	}
	return 1
}

// Compares tipsets for equality.
func (t *TipSet) Eq(other *TipSet) bool {
	return t.Epoch == other.Epoch &&
		t.CID == other.CID &&
		t.Weight == other.Weight
}

func (t *TipSet) String() string {
	var b strings.Builder
	b.Write(t.CID.Bytes())
	b.WriteString("@")
	b.WriteString(strconv.Itoa(t.Epoch))
	return b.String()
}

// A chain of tipsets comprising a base (the last finalised tipset from which the chain extends).
// and (possibly empty) suffix.
// Tipsets are assumed to be built contiguously on each other, though epochs may be missing due to null rounds.
// The zero value is not a valid chain, and represents a "bottom" value when used in a Granite message.
type ECChain []TipSet

// Creates a new chain.
func NewChain(base TipSet, suffix ...TipSet) ECChain {
	epoch := base.Epoch
	for _, t := range suffix {
		if t.Epoch <= epoch {
			panic("tipsets not in order")
		}
		epoch = t.Epoch
	}
	return append([]TipSet{base}, suffix...)
}

func (c ECChain) IsZero() bool {
	return len(c) == 0
}

// Returns a pointer to the base tipset.
func (c ECChain) Base() *TipSet {
	return &c[0]
}

// Returns the suffix of the chain after the base.
// An empty slice for a zero value.
func (c ECChain) Suffix() []TipSet {
	if c.IsZero() {
		return nil
	}
	return c[1:]
}

// Returns a pointer to the last tipset in the chain.
// This could be the base tipset if there is no suffix.
// This will panic on a zero value.
func (c ECChain) Head() *TipSet {
	return &c[len(c)-1]
}

// Returns the CID of the head tipset, or empty string for a zero value
func (c ECChain) HeadCIDOrZero() TipSetID {
	if c.IsZero() {
		return ZeroTipSetID()
	}
	return c.Head().CID
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
		Epoch:  c[0].Epoch + 1,
		CID:    cid,
		Weight: c[0].Weight + 1,
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
	return reflect.DeepEqual(c, other)
}

// Checks whether two chains have the same base.
// Always false for a zero value.
func (c ECChain) SameBase(other ECChain) bool {
	if c.IsZero() {
		return false
	}
	return c[0].Eq(&other[0])
}

// Check whether a chain has a specific base tipset.
// Always false for a zero value.
func (c ECChain) HasBase(t *TipSet) bool {
	if c.IsZero() {
		return false
	}
	return c[0].Eq(t)
}

// Checks whether a chain has some prefix (including the base).
// Always false for a zero value.
func (c ECChain) HasPrefix(other ECChain) bool {
	if c.IsZero() {
		return false
	}
	if len(other) > len(c) {
		return false
	}
	for i := range other {
		if !c[i].Eq(&other[i]) {
			return false
		}
	}
	return true
}

// Checks whether a chain has some tipset (including as its base).
func (c ECChain) HasTipset(t *TipSet) bool {
	for _, t2 := range c {
		if t2.Eq(t) {
			return true
		}
	}
	return false
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
