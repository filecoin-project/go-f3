package net

import (
	"reflect"
	"strconv"
	"strings"
)

type CID = string

// A power table maps participant IDs to power values.
type PowerTable struct {
	Entries map[string]uint
	Total   uint
}

func NewPowerTable() PowerTable {
	return PowerTable{
		Entries: map[string]uint{},
		Total:   0,
	}
}

func (p *PowerTable) Add(id string, power uint) {
	if p.Entries[id] != 0 {
		panic("duplicate power entry")
	}
	p.Entries[id] = power
	p.Total += power
}

type TipSet struct {
	Epoch      int
	CID        CID
	Weight     uint
	PowerTable PowerTable
}

// Creates a new tipset.
func NewTipSet(epoch int, cid CID, weight uint, powerTable PowerTable) TipSet {
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
	b.WriteString(strconv.Itoa(t.Epoch))
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

func NewChain(base TipSet, suffix ...TipSet) *ECChain {
	return &ECChain{
		Base:   base,
		Suffix: suffix,
	}
}

func (c *ECChain) IsZero() bool {
	return c.Base.Eq(&TipSet{}) && len(c.Suffix) == 0
}

// Returns a copy of the base of a chain with no suffix.
func (c *ECChain) BaseChain() *ECChain {
	return &ECChain{
		Base:   c.Base,
		Suffix: nil,
	}
}

// Returns a pointer to the last tipset in the chain.
func (c *ECChain) Head() *TipSet {
	if len(c.Suffix) == 0 {
		return &c.Base
	}
	return &c.Suffix[len(c.Suffix)-1]
}

// Returns a new chain extending this chain with one tipset and the same head power table.
func (c *ECChain) Extend(cid CID) *ECChain {
	head := c.Head()
	return &ECChain{
		Base: c.Base,
		Suffix: append(c.Suffix, TipSet{
			Epoch:      head.Epoch + 1,
			CID:        cid,
			Weight:     head.Weight + 1,
			PowerTable: head.PowerTable,
		}),
	}
}

func (c *ECChain) ExtendWith(cid CID, weight uint, powerTable PowerTable) *ECChain {
	head := c.Head()
	if weight < head.Weight {
		panic("new tipset weight must be greater than current head weight")
	}
	return &ECChain{
		Base: c.Base,
		Suffix: append(c.Suffix, TipSet{
			Epoch:      head.Epoch + 1,
			CID:        cid,
			Weight:     weight,
			PowerTable: powerTable,
		}),
	}
}

// Returns a chain with suffix truncated to a maximum length.
func (c *ECChain) Prefix(to int) *ECChain {
	return &ECChain{
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
func (c *ECChain) HasPrefix(other *ECChain) bool {
	if !c.Base.Eq(&other.Base) {
		return false
	}
	if len(other.Suffix) > len(c.Suffix) {
		return false
	}
	for i := range other.Suffix {
		if !c.Suffix[i].Eq(&other.Suffix[i]) {
			return false
		}
	}
	return true
}

// Checks whether a chain has some tipset (including as its base).
func (c *ECChain) HasTipset(t *TipSet) bool {
	if t.Eq(&c.Base) {
		return true
	}
	for _, t2 := range c.Suffix {
		if t2.Eq(t) {
			return true
		}
	}
	return false
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
