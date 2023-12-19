package f3

import (
	"strings"
)

type ActorID uint64

// Opaque type identifying a tipset.
// This is expected to be a concatenation of CIDs of block headers identifying a tipset.
// GossipPBFT doesn't need to know anything about CID format or behaviour, so adapts to this simple type
// rather than import all the dependencies of the CID package.
type TipSetID struct {
	// The inner value is a string so that this type can be used as a map key (like the go-cid package).
	value string
}

// Creates a new TipSetID from a byte array.
func NewTipSetID(b []byte) TipSetID {
	return TipSetID{string(b)}
}

// Creates a new TipSetID from a string.
func NewTipSetIDFromString(s string) TipSetID {
	return TipSetID{s}
}

// Returns a zero-value TipSetID.
func ZeroTipSetID() TipSetID {
	return TipSetID{}
}

// Checks whether a tipset ID is zero.
func (t TipSetID) IsZero() bool {
	return t.value == ""
}

// Orders two tipset IDs.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// This is a lexicographic ordering of the bytes of the IDs.
func (t TipSetID) Compare(o TipSetID) int {
	return strings.Compare(t.value, o.value)
}

func (t TipSetID) Bytes() []byte {
	return []byte(t.value)
}
