package gpbft

import (
	"math/big"

	"github.com/ipfs/go-datastore"
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
