package net

import "reflect"

// A tipset CID is represented by an opaque string.
type TipSet = string

// An EC chain suffix.
type ECChain struct {
	// The last finalised tipset on which this suffix is based.
	Base TipSet
	// The epoch of the base tipset.
	BaseEpoch int64
	// Chain of tipsets after base, one per epoch.
	// (Note a real implementation will have to handle empty tipsets somehow).
	Suffix []TipSet
}

// Returns the base of a chain with no suffix.
func (c *ECChain) BaseChain() ECChain {
	return ECChain{
		Base:      c.Base,
		BaseEpoch: c.BaseEpoch,
		Suffix:    nil,
	}
}

// Compares two ECChains for equality
func (c *ECChain) Eq(other *ECChain) bool {
	return reflect.DeepEqual(c, other)
}

// Receives an updated EC chain.
type ECReceiver interface {
	ReceiveCanonicalChain(chain ECChain)
}
