package f3

import (
	"math/big"
	"strings"
)

type ActorID uint64

type StoragePower big.Int

// Creates a new StoragePower struct with a specific value and returns the result
func NewStoragePower(value int64) *StoragePower {
	return (*StoragePower)(new(big.Int).SetInt64(value))
}

// Add sets sp to the sum x+y and returns sp.
func (sp *StoragePower) Add(x, y *StoragePower) *StoragePower {
	return (*StoragePower)((*big.Int)(sp).Add((*big.Int)(x), (*big.Int)(y)))
}

// Mul sets z to the product of x and y and returns z.
func (z *StoragePower) Mul(x, y *StoragePower) *StoragePower {
	return (*StoragePower)((*big.Int)(z).Mul((*big.Int)(x), (*big.Int)(y)))
}

// Div sets z to the quotient x/y and returns z.
func (z *StoragePower) Div(x, y *StoragePower) *StoragePower {
	return (*StoragePower)((*big.Int)(z).Div((*big.Int)(x), (*big.Int)(y)))
}

// Copy creates a new StoragePower with the same value as the original.
func (sp StoragePower) Copy() *StoragePower {
	var copy StoragePower
	(*big.Int)(&copy).Set((*big.Int)(&sp))
	return &copy
}

// Cmp compares sp with other and returns:
// -1 if sp <  other
//
//	0 if sp == other
//	1 if sp >  other
func (sp *StoragePower) Cmp(other *StoragePower) int {
	return (*big.Int)(sp).Cmp((*big.Int)(other))
}

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
