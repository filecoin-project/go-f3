package sim

import (
	"github.com/filecoin-project/go-f3/gpbft"
)

// Simulated EC state.
type EC struct {
	// The base of all chains, which participants must agree on.
	Base gpbft.ECChain
	// EC chains visible to participants.
	Chains map[gpbft.ActorID]gpbft.ECChain
	// The power table at the base chain head.
	PowerTable *gpbft.PowerTable
	// The beacon value of the base chain head.
	Beacon []byte
}

func NewEC(base gpbft.ECChain, beacon []byte) *EC {
	return &EC{
		Base:       base,
		Chains:     make(map[gpbft.ActorID]gpbft.ECChain),
		PowerTable: gpbft.NewPowerTable(nil),
		Beacon:     beacon,
	}
}

func (ec *EC) AddParticipant(id gpbft.ActorID, power *gpbft.StoragePower, pubkey []byte) {
	if err := ec.PowerTable.Add(id, power, pubkey); err != nil {
		panic("failed to add participant")
	}
}
