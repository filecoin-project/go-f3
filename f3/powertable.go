package f3

import (
	"fmt"
	"math/big"
	"sort"
)

// PowerEntry represents a single entry in the PowerTable.
// It includes an ActorID and its Weight
type PowerEntry struct {
	ID     ActorID
	Power  *StoragePower
	PubKey PubKey
}

// PowerTable maps ActorID to a unique index in the range [0, len(powerTable.Entries)).
// Entries is the reverse mapping to a PowerEntry.
type PowerTable struct {
	Entries []PowerEntry    // Slice to maintain the order. Meant to be maintained in order in order by (Power descending, ID ascending)
	Lookup  map[ActorID]int // Maps ActorID to the index of the associated entry in Entries
	Total   *StoragePower
}

// NewPowerTable creates a new PowerTable from a slice of PowerEntry .
// It is more efficient than Add, as it only needs to sort the entries once.
// Note that the function takes ownership of the slice - it must not be modified afterwards.
func NewPowerTable(entries []PowerEntry) *PowerTable {
	sort.Slice(entries, func(i, j int) bool {
		return comparePowerEntries(entries[i], entries[j])
	})

	lookup := make(map[ActorID]int, len(entries))
	total := NewStoragePower(0)
	for i, entry := range entries {
		lookup[entry.ID] = i
		total.Add(total, entry.Power)
	}

	if len(entries) != len(lookup) {
		panic("duplicate power entries")
	}
	return &PowerTable{
		Entries: entries,
		Lookup:  lookup,
		Total:   total,
	}
}

// Add adds a new entry to the PowerTable.
// This is linear in the table size, so adding many entries this way is inefficient.
func (p *PowerTable) Add(id ActorID, power *StoragePower, pubKey []byte) error {
	if _, ok := p.Lookup[id]; ok {
		return fmt.Errorf("duplicate power entry")
	}
	entry := PowerEntry{ID: id, Power: power, PubKey: pubKey}
	index := sort.Search(len(p.Entries), func(i int) bool {
		return comparePowerEntries(p.Entries[i], entry)
	})

	p.Entries = append(p.Entries, PowerEntry{})
	copy(p.Entries[index+1:], p.Entries[index:])
	p.Entries[index] = entry

	p.Lookup[id] = index
	for i := index + 1; i < len(p.Entries); i++ {
		p.Lookup[p.Entries[i].ID] = i
	}
	p.Total.Add(p.Total, power)

	return nil
}

func (p *PowerTable) Get(id ActorID) (*StoragePower, []byte) {
	if index, ok := p.Lookup[id]; ok {
		powerCopy := new(big.Int)
		powerCopy.Set(p.Entries[index].Power)
		return powerCopy, p.Entries[index].PubKey
	}
	return nil, nil
}

// Has returns true iff the ActorID is part of the power table with positive power
func (p *PowerTable) Has(id ActorID) bool {
	index, ok := p.Lookup[id]
	return ok && p.Entries[index].Power.Cmp(NewStoragePower(0)) > 0
}

///// General helpers /////

// comparePowerEntries compares two PowerEntry elements.
// It returns true if the first entry should be sorted before the second.
func comparePowerEntries(a, b PowerEntry) bool {
	cmp := a.Power.Cmp(b.Power)
	if cmp > 0 {
		return true
	}
	if cmp == 0 {
		return a.ID < b.ID
	}
	return false
}

func (p *PowerTable) GetPower(id ActorID) uint {
	if index, ok := p.Lookup[id]; ok {
		return p.Entries[index].Power
	}
	return 0
}
