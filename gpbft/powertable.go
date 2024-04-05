package gpbft

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
)

var _ sort.Interface = (*PowerTable)(nil)

// PowerEntry represents a single entry in the PowerTable, including ActorID and its StoragePower and PubKey.
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
func NewPowerTable() *PowerTable {
	return &PowerTable{
		Lookup: make(map[ActorID]int),
		Total:  NewStoragePower(0),
	}
}

// Add inserts one or more entries to this PowerTable.
//
// Each inserted entry must meet the following criteria:
// * It must not already be present int the PowerTable.
// * It must have StoragePower larger than zero.
// * It must have a non-zero length public key.
func (p *PowerTable) Add(entries ...PowerEntry) error {
	for _, entry := range entries {
		switch {
		case len(entry.PubKey) == 0:
			return fmt.Errorf("unspecified public key for actor ID: %d", entry.ID)
		case p.Has(entry.ID):
			return fmt.Errorf("power entry already exists for actor ID: %d", entry.ID)
		case entry.Power.Sign() <= 0:
			return fmt.Errorf("zero power for actor ID: %d", entry.ID)
		default:
			p.Total.Add(p.Total, entry.Power)
			p.Entries = append(p.Entries, entry)
			p.Lookup[entry.ID] = len(p.Entries) - 1
		}
	}
	sort.Sort(p)
	return nil
}

// Get retrieves the StoragePower and PubKey for the given id, if present in the table.
// Otherwise, returns nil.
func (p *PowerTable) Get(id ActorID) (*StoragePower, PubKey) {
	if index, ok := p.Lookup[id]; ok {
		entry := p.Entries[index]

		powerCopy := new(big.Int)
		powerCopy.Set(entry.Power)
		return powerCopy, entry.PubKey
	}
	return nil, nil
}

// Has check whether this PowerTable contains an entry for the given id.
func (p *PowerTable) Has(id ActorID) bool {
	_, found := p.Lookup[id]
	return found
}

// Copy creates a deep copy of this PowerTable.
func (p *PowerTable) Copy() *PowerTable {
	replica := NewPowerTable()
	if p.Len() != 0 {
		replica.Entries = make([]PowerEntry, p.Len())
		copy(replica.Entries, p.Entries)
	}
	for k, v := range p.Lookup {
		replica.Lookup[k] = v
	}
	replica.Total.Add(replica.Total, p.Total)
	return replica
}

// Len returns the number of entries in this PowerTable.
func (p *PowerTable) Len() int {
	return len(p.Entries)
}

// Less determines if the entry at index i should be sorted before the entry at index j.
// Entries are sorted descending order of their power, where entries with equal power are
// sorted by ascending order of their ID.
// This ordering is guaranteed to be stable, since a valid PowerTable cannot contain entries with duplicate IDs; see Validate.
func (p *PowerTable) Less(i, j int) bool {
	one, other := p.Entries[i], p.Entries[j]
	switch cmp := one.Power.Cmp(other.Power); {
	case cmp > 0:
		return true
	case cmp == 0:
		return one.ID < other.ID
	default:
		return false
	}
}

// Swap swaps the entry at index i with the entry at index j.
// This function must not be called directly since it is used as part of sort.Interface.
func (p *PowerTable) Swap(i, j int) {
	p.Entries[i], p.Entries[j] = p.Entries[j], p.Entries[i]
	p.Lookup[p.Entries[i].ID], p.Lookup[p.Entries[j].ID] = i, j
}

// Validate checks the validity of this PowerTable.
// Such table must meet the following criteria:
// * Its entries must be in order as defined by Less.
// * It must not contain any entries with duplicate ID.
// * All entries must have power larger than zero
// * All entries must have non-zero public key.
// * PowerTable.Total must correspond to the total aggregated power of entries.
// * PowerTable.Lookup must contain the expected mapping of entry actor ID to index.
func (p *PowerTable) Validate() error {
	if len(p.Entries) != len(p.Lookup) {
		return errors.New("inconsistent entries and lookup map")
	}
	total := NewStoragePower(0)
	var previous *PowerEntry
	for index, entry := range p.Entries {
		if lookupIndex, found := p.Lookup[entry.ID]; !found || index != lookupIndex {
			return fmt.Errorf("lookup index does not match entries for actor ID: %d", entry.ID)
		}
		if len(entry.PubKey) == 0 {
			return fmt.Errorf("unspecified public key for actor ID: %d", entry.ID)
		}
		if entry.Power.Sign() <= 0 {
			return fmt.Errorf("zero power for entry with actor ID: %d", entry.ID)
		}
		if previous != nil && !p.Less(index-1, index) {
			return fmt.Errorf("entry not in order at index: %d", index)
		}
		total.Add(total, entry.Power)
		previous = &entry
	}
	if total.Cmp(p.Total) != 0 {
		return errors.New("total power does not match entries")
	}
	return nil
}
