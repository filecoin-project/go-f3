package gpbft

import (
	"bytes"
	"github.com/ipfs/go-datastore"
	"math/big"
)

type ActorID uint64

type StoragePower = big.Int

type PubKey []byte

// NetworkName provides separation between different networks
// it is implicitly included in all signatures and VRFs
type NetworkName string

func (nn NetworkName) DatastorePrefix() datastore.Key {
	return datastore.NewKey("/f3/" + string(nn))
}
func (nn NetworkName) PubSubTopic() string {
	return "/f3/granite/0.0.1/" + string(nn)
}

// Creates a new StoragePower struct with a specific value and returns the result
func NewStoragePower(value int64) *StoragePower {
	return new(big.Int).SetInt64(value)
}

// Opaque type identifying a tipset.
// This is expected to be a canonical sequence of CIDs of block headers identifying a tipset.
// GossipPBFT doesn't need to know anything about CID format or behaviour, so adapts to this simple type
// rather than import all the dependencies of the CID package.
// This type is intentionally unsuitable for use as a map key.
type TipSetID [][]byte

// Creates a new TipSetID from a slice of block CIDs.
func NewTipSetID(blocks [][]byte) TipSetID {
	return blocks
}

// Creates a new singleton TipSetID from a string, treated as a CID.
func NewTipSetIDFromString(s string) TipSetID {
	return TipSetID{[]byte(s)}
}

// Checks whether a tipset ID is zero.
func (t TipSetID) IsZero() bool {
	return len(t) == 0
}

func (t TipSetID) Eq(other TipSetID) bool {
	if len(t) != len(other) {
		return false
	}
	for i, b := range t {
		if !bytes.Equal(b, other[i]) {
			return false
		}
	}
	return true
}

// FIXME this is not unique without a delimiter
func (t TipSetID) Bytes() []byte {
	buf := bytes.Buffer{}
	for _, b := range t {
		buf.Write(b)
	}
	return buf.Bytes()
}
