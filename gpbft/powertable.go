package gpbft

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sort"

	xerrors "golang.org/x/xerrors"
)

var _ sort.Interface = (*PowerTable)(nil)
var _ sort.Interface = (PowerEntries)(nil)

// PowerEntry represents a single entry in the PowerTable, including ActorID and its StoragePower and PubKey.
type PowerEntry struct {
	ID     ActorID
	Power  *StoragePower
	PubKey PubKey
}

type PowerEntries []PowerEntry

// PowerTable maps ActorID to a unique index in the range [0, len(powerTable.Entries)).
// Entries is the reverse mapping to a PowerEntry.
type PowerTable struct {
	Entries     PowerEntries // Slice to maintain the order. Meant to be maintained in order in order by (Power descending, ID ascending)
	ScaledPower []uint16
	Lookup      map[ActorID]int // Maps ActorID to the index of the associated entry in Entries
	Total       *StoragePower
	ScaledTotal uint16
}

// Len returns the number of entries in this PowerTable.
func (p PowerEntries) Len() int {
	return len(p)
}

// Less determines if the entry at index i should be sorted before the entry at index j.
// Entries are sorted descending order of their power, where entries with equal power are
// sorted by ascending order of their ID.
// This ordering is guaranteed to be stable, since a valid PowerTable cannot contain entries with duplicate IDs; see Validate.
func (p PowerEntries) Less(i, j int) bool {
	one, other := p[i], p[j]
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
func (p PowerEntries) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p PowerEntries) Scaled() (scaled []uint16, total uint16, err error) {
	totalUnscaled := new(StoragePower)
	for i := range p {
		pwr := p[i].Power
		if pwr.Sign() <= 0 {
			return nil, 0, fmt.Errorf("invalid non-positive power %s for participant %d", pwr, p[i].ID)
		}
		totalUnscaled = totalUnscaled.Add(totalUnscaled, pwr)
	}
	scaled = make([]uint16, len(p))
	for i := range p {
		p, err := scalePower(p[i].Power, totalUnscaled)
		if err != nil {
			// We just summed the total power, this operation can't fail.
			panic(err)
		}
		scaled[i] = p
		total += p
	}
	return scaled, total, nil
}

// NewPowerTable creates a new PowerTable from a slice of PowerEntry .
// It is more efficient than Add, as it only needs to sort the entries once.
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
			p.ScaledPower = append(p.ScaledPower, 0)
			p.Lookup[entry.ID] = len(p.Entries) - 1
		}
	}
	sort.Sort(p)
	return p.rescale()
}

func (p *PowerTable) rescale() error {
	p.ScaledTotal = 0
	for i := range p.Entries {
		scaled, err := scalePower(p.Entries[i].Power, p.Total)
		if err != nil {
			return err
		}
		p.ScaledPower[i] = scaled
		p.ScaledTotal += scaled
	}
	return nil
}

// Get retrieves the scaled power, unscaled StoragePower and PubKey for the given id, if present in
// the table. Otherwise, returns 0/nil.
func (p *PowerTable) Get(id ActorID) (uint16, PubKey) {
	if index, ok := p.Lookup[id]; ok {
		key := p.Entries[index].PubKey
		scaledPower := p.ScaledPower[index]
		return scaledPower, key
	}
	return 0, nil
}

// Has check whether this PowerTable contains an entry for the given id.
func (p *PowerTable) Has(id ActorID) bool {
	_, found := p.Lookup[id]
	return found
}

// Copy creates a deep copy of this PowerTable.
func (p *PowerTable) Copy() *PowerTable {
	replica := NewPowerTable()
	replica.Entries = slices.Clone(p.Entries)
	replica.ScaledPower = slices.Clone(p.ScaledPower)
	replica.Lookup = maps.Clone(p.Lookup)
	replica.ScaledTotal = p.ScaledTotal
	replica.Total.Add(replica.Total, p.Total)
	return replica
}

// Len returns the number of entries in this PowerTable.
func (p *PowerTable) Len() int {
	return p.Entries.Len()
}

// Less determines if the entry at index i should be sorted before the entry at index j.
// Entries are sorted descending order of their power, where entries with equal power are
// sorted by ascending order of their ID.
// This ordering is guaranteed to be stable, since a valid PowerTable cannot contain entries with duplicate IDs; see Validate.
func (p *PowerTable) Less(i, j int) bool {
	return p.Entries.Less(i, j)
}

// Swap swaps the entry at index i with the entry at index j.
// This function must not be called directly since it is used as part of sort.Interface.
func (p *PowerTable) Swap(i, j int) {
	p.Entries.Swap(i, j)
	p.ScaledPower[i], p.ScaledPower[j] = p.ScaledPower[j], p.ScaledPower[i]
	p.Lookup[p.Entries[i].ID], p.Lookup[p.Entries[j].ID] = i, j
}

// Validate checks the validity of this PowerTable.
// Such table must meet the following criteria:
// * Its entries must be in order as defined by Less.
// * It must not contain any entries with duplicate ID.
// * All entries must have power larger than zero
// * All entries must have non-zero public key.
// * All entries must match their scaled powers.
// * PowerTable.Total must correspond to the total aggregated power of entries.
// * PowerTable.ScaledTotal must correspond to the total aggregated scaled power.
// * PowerTable.Lookup must contain the expected mapping of entry actor ID to index.
func (p *PowerTable) Validate() error {
	if len(p.Entries) != len(p.Lookup) {
		return errors.New("inconsistent entries and lookup map")
	}
	if len(p.Entries) != len(p.ScaledPower) {
		return errors.New("inconsistent entries and scaled power")
	}
	total := NewStoragePower(0)
	totalScaled := 0 // int instead of uint16 to detect overflow
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

		scaledPower, err := scalePower(entry.Power, p.Total)
		if err != nil {
			return fmt.Errorf("failed to scale power at index %d: %w", index, err)
		}
		if scaledPower != p.ScaledPower[index] {
			return fmt.Errorf("incorrect scaled power at index: %d", index)
		}

		total.Add(total, entry.Power)
		totalScaled += int(scaledPower)
		previous = &entry
	}
	if total.Cmp(p.Total) != 0 {
		return errors.New("total power does not match entries")
	}
	if int(p.ScaledTotal) != totalScaled {
		return errors.New("scaled total power does not match entries")
	}
	return nil
}

func scalePower(power, total *StoragePower) (uint16, error) {
	const maxPower = 0xffff
	if power.Cmp(total) > 0 {
		return 0, xerrors.Errorf("total power %d is less than the power of a single participant %d", total, power)
	}
	scaled := big.NewInt(maxPower)
	scaled = scaled.Mul(scaled, (*big.Int)(power))
	scaled = scaled.Div(scaled, (*big.Int)(total))
	return uint16(scaled.Uint64()), nil
}
