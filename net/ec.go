package net

import (
	"reflect"
	"strconv"
	"strings"
)

type CID = string

// A power table maps participant IDs to power values.
type PowerTable struct {
	Entries map[string]uint64
	Total   uint64
}

func NewPowerTable() PowerTable {
	return PowerTable{
		Entries: map[string]uint64{},
		Total:   0,
	}
}

func (p *PowerTable) Add(id string, power uint64) {
	if p.Entries[id] != 0 {
		panic("duplicate power entry")
	}
	p.Entries[id] = power
	p.Total += power
}

type TipSet struct {
	Epoch      int64
	CID        CID
	Weight     uint64
	PowerTable PowerTable
}

// Creates a new tipset.
func NewTipSet(epoch int64, cid CID, weight uint64, powerTable PowerTable) TipSet {
	return TipSet{
		Epoch:      epoch,
		CID:        cid,
		Weight:     weight,
		PowerTable: powerTable,
	}
}

// Compares two tipsets by weight, breaking ties with CID.
// Note that the real weight function breaks ties with VRF tickets.
func (t *TipSet) Compare(other *TipSet) int {
	if t.Weight == other.Weight {
		return strings.Compare(t.CID, other.CID)
	} else if t.Weight < other.Weight {
		return -1
	}
	return 1
}

// Compares tipsets for equality.
func (t *TipSet) Eq(other *TipSet) bool {
	return reflect.DeepEqual(t, other)
}

func (t *TipSet) String() string {
	var b strings.Builder
	b.WriteString(t.CID)
	b.WriteString("@")
	b.WriteString(strconv.FormatInt(t.Epoch, 10))
	return b.String()
}

// An EC chain suffix.
type ECChain struct {
	// The last finalised tipset on which this suffix is based.
	Base TipSet
	// Chain of tipsets after base, one per epoch.
	// (Note a real implementation will have to handle empty tipsets somehow).
	Suffix []TipSet
}

// Returns the base of a chain with no suffix.
func (c *ECChain) BaseChain() ECChain {
	return ECChain{
		Base:   c.Base,
		Suffix: nil,
	}
}

// Returns the last tipset in the chain.
func (c *ECChain) Head() TipSet {
	if len(c.Suffix) == 0 {
		return c.Base
	}
	return c.Suffix[len(c.Suffix)-1]
}

// Returns a chain with suffix truncated to a maximum length.
func (c *ECChain) Prefix(to int) ECChain {
	return ECChain{
		Base:   c.Base,
		Suffix: c.Suffix[:to],
	}
}

// Compares two ECChains for equality.
func (c *ECChain) Eq(other *ECChain) bool {
	return reflect.DeepEqual(c, other)
}

// Checks whether two chains have the same base.
func (c *ECChain) SameBase(other *ECChain) bool {
	return c.Base.Eq(&other.Base)
}

// Checks whether a chain suffix has some prefix.
// Note this doesn't check the base.
func (c *ECChain) HasPrefix(prefix []TipSet) bool {
	if len(prefix) > len(c.Suffix) {
		return false
	}
	for i := range prefix {
		if !c.Suffix[i].Eq(&prefix[i]) {
			return false
		}
	}
	return true
}

func (c *ECChain) String() string {
	var b strings.Builder
	b.WriteString("{")
	b.WriteString(c.Base.String())
	b.WriteString(" [")
	for i, t := range c.Suffix {
		b.WriteString(t.String())
		if i < len(c.Suffix)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	b.WriteString("}")
	return b.String()
}

// Receives an updated EC chain.
type ECReceiver interface {
	ReceiveCanonicalChain(chain ECChain)
}
