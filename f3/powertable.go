package f3

import (
	"sort"
)

// PowerEntry represents a single entry in the PowerTable.
// It includes an ActorID and its Weight
type PowerEntry struct {
	ID     ActorID
	Power  *StoragePower
	PubKey []byte
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
func NewPowerTable(entries []PowerEntry) *PowerTable {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Power.Cmp(entries[j].Power) > 0 ||
			(entries[i].Power.Cmp(entries[j].Power) == 0 && entries[i].ID < entries[j].ID)
	})

	lookup := make(map[ActorID]int, len(entries))
	total := NewStoragePower(0)
	for i, entry := range entries {
		lookup[entry.ID] = i
		total.Add(total, entry.Power)
	}

	return &PowerTable{
		Entries: entries,
		Lookup:  lookup,
		Total:   total,
	}
}

// Add adds a new entry to the PowerTable.
// This is linear in the table size, so adding many entries this way is inefficient.
func (p *PowerTable) Add(id ActorID, power *StoragePower, pubKey []byte) {
	entry := PowerEntry{ID: id, Power: power, PubKey: pubKey}
	index := sort.Search(len(p.Entries), func(i int) bool {
		return p.Entries[i].Power.Cmp(power) > 0 ||
			(p.Entries[i].Power.Cmp(power) == 0 && p.Entries[i].ID < id)
	})

	// Check for duplication at the found index or adjacent entries
	if index < len(p.Entries) && (p.Entries[index].ID == id || (index > 0 && p.Entries[index-1].ID == id)) {
		panic("duplicate power entry")
	}

	p.Entries = append(p.Entries, PowerEntry{})
	copy(p.Entries[index+1:], p.Entries[index:])
	p.Entries[index] = entry

	p.Lookup[id] = index
	for i := index + 1; i < len(p.Entries); i++ {
		p.Lookup[p.Entries[i].ID] = i
	}
	p.Total.Add(p.Total, power)
}

func (p *PowerTable) Get(id ActorID) (*StoragePower, []byte, bool) {
	if index, ok := p.Lookup[id]; ok {
		return p.Entries[index].Power.Copy(), p.Entries[index].PubKey, true
	}
	return nil, nil, false
}
