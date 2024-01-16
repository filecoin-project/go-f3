package f3

import "sort"

// PowerEntry represents a single entry in the PowerTable.
// It includes an ActorID and its Weight
type PowerEntry struct {
	ID    ActorID
	Power uint
}

// PowerTable maps ActorID to a unique index in the range [0, len(powerTable.Entries)).
// Entries is the reverse mapping to a PowerEntry.
type PowerTable struct {
	Entries []PowerEntry    // Slice to maintain the order
	Lookup  map[ActorID]int // Map for quick index lookups
	Total   uint
}

func NewPowerTable() PowerTable {
	return PowerTable{
		Entries: make([]PowerEntry, 0),
		Lookup:  make(map[ActorID]int),
	}
}

// NewPowerTableFromMap creates a new PowerTable from a map of ActorID to weight.
// It is more efficient than Add, as it only needs to sort the entries once.
func NewPowerTableFromMap(entriesMap map[ActorID]uint) PowerTable {
	entries := make([]PowerEntry, 0, len(entriesMap))

	for id, power := range entriesMap {
		entries = append(entries, PowerEntry{ID: id, Power: power})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Power > entries[j].Power || (entries[i].Power == entries[j].Power && entries[i].ID < entries[j].ID)
	})

	lookup := make(map[ActorID]int, len(entriesMap))
	var total uint = 0
	for i, entry := range entries {
		lookup[entry.ID] = i
		total += entry.Power
	}

	return PowerTable{
		Entries: entries,
		Lookup:  lookup,
		Total:   total,
	}
}

// Add adds a new entry to the PowerTable. 
// This is linear in the table size, so adding many entries this way is inefficient.
func (p *PowerTable) Add(id ActorID, power uint) {
	if _, ok := p.Lookup[id]; ok {
		panic("duplicate power entry")
	}
	entry := PowerEntry{ID: id, Power: power}
	index := sort.Search(len(p.Entries), func(i int) bool {
		return p.Entries[i].Power > power || (p.Entries[i].Power == power && p.Entries[i].ID < id)
	})

	p.Entries = append(p.Entries, PowerEntry{})
	copy(p.Entries[index+1:], p.Entries[index:])
	p.Entries[index] = entry

	p.Lookup[id] = index
	for i := index + 1; i < len(p.Entries); i++ {
		p.Lookup[p.Entries[i].ID] = i
	}
	p.Total += power
}

func (p *PowerTable) GetPower(id ActorID) uint {
	if index, ok := p.Lookup[id]; ok {
		return p.Entries[index].Power
	}
	return 0
}
