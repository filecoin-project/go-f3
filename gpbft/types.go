package gpbft

import "github.com/filecoin-project/go-f3/big"

type ActorID uint64

type StoragePower = big.Int

type PubKey []byte

// NetworkName provides separation between different networks
// it is implicitly included in all signatures and VRFs
type NetworkName string

// Creates a new StoragePower struct with a specific value and returns the result
func NewStoragePower(value int64) *StoragePower {
	return big.NewInt(value)
}
