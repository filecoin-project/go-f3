package net

type ActorID uint64

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
