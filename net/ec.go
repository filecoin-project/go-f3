package net

import (
	"reflect"
	"strconv"
	"strings"
)

type ActorID uint64
type CID = string

// A power table maps participant IDs to power values.
type PowerTable struct {
	Entries map[ActorID]uint
	Total   uint
}

func NewPowerTable() PowerTable {
	return PowerTable{
		Entries: map[ActorID]uint{},
		Total:   0,
	}
}

func (p *PowerTable) Add(id ActorID, power uint) {
	if p.Entries[id] != 0 {
		panic("duplicate power entry")
	}
	p.Entries[id] = power
	p.Total += power
}

type TipSet struct {
	Epoch  int
	CID    CID
	Weight uint
}

// Creates a new tipset.
func NewTipSet(epoch int, cid CID, weight uint) TipSet {
	return TipSet{
		Epoch:  epoch,
		CID:    cid,
		Weight: weight,
	}
}

// Checks whether a tipset is the zero value.
func (t *TipSet) IsZero() bool {
	return t.Epoch == 0 && t.CID == "" && t.Weight == 0
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
	return t.Epoch == other.Epoch &&
		t.CID == other.CID &&
		t.Weight == other.Weight
}

func (t *TipSet) String() string {
	var b strings.Builder
	b.WriteString(t.CID)
	b.WriteString("@")
	b.WriteString(strconv.Itoa(t.Epoch))
	return b.String()
}

// An EC chain comprising a base tipset and (possibly empty) suffix.
// The power table and beacon derived from the base tipset are included.
type ECChain struct {
	// The last finalised tipset on which this suffix is based.
	Base TipSet
	// Power table from the base tipset.
	BasePowerTable PowerTable
	// Random beacon from the base tipset.
	BaseBeacon []byte

	// Chain of tipsets after base, one per epoch.
	// (Note a real implementation will have to handle empty tipsets somehow).
	Suffix []TipSet
}

func NewChain(base TipSet, power PowerTable, beacon []byte, suffix ...TipSet) *ECChain {
	return &ECChain{
		Base:           base,
		BasePowerTable: power,
		BaseBeacon:     beacon,
		Suffix:         suffix,
	}
}

func (c *ECChain) IsZero() bool {
	return c.Base.IsZero() && len(c.Suffix) == 0
}

// Returns a copy of the base of a chain with no suffix.
// Note that the power table and beacon are shallow copies.
func (c *ECChain) BaseChain() *ECChain {
	base := *c
	base.Suffix = nil
	return &base
}

// Returns a pointer to the last tipset in the chain.
func (c *ECChain) Head() *TipSet {
	if len(c.Suffix) == 0 {
		return &c.Base
	}
	return &c.Suffix[len(c.Suffix)-1]
}

// Returns a new chain extending this chain with one tipset.
func (c *ECChain) Extend(cid CID) *ECChain {
	base := *c
	head := base.Head()
	base.Suffix = append(base.Suffix, TipSet{
		Epoch:  head.Epoch + 1,
		CID:    cid,
		Weight: head.Weight + 1,
	})
	return &base
}

// Returns a chain with suffix truncated to a maximum length.
func (c *ECChain) Prefix(to int) *ECChain {
	base := *c
	base.Suffix = base.Suffix[:to]
	return &base
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
