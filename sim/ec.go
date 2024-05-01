package sim

import (
	"github.com/filecoin-project/go-f3/gpbft"
)

// Simulated EC state for each protocol instance.
type EC struct {
	Instances []*ECInstance
}

type ECInstance struct {
	// The base of all chains, which participants must agree on.
	Base gpbft.ECChain
	// EC chains visible to participants.
	Chains map[gpbft.ActorID]gpbft.ECChain
	// The power table at the base chain head.
	PowerTable *gpbft.PowerTable
	// The beacon value of the base chain head.
	Beacon []byte
}

func newEC(opts *options) *EC {
	return &EC{
		Instances: []*ECInstance{
			{
				Base:       *opts.baseChain,
				Chains:     make(map[gpbft.ActorID]gpbft.ECChain),
				PowerTable: gpbft.NewPowerTable(),
				Beacon:     opts.beacon,
			},
		},
	}
}

// Adds a participant to the first instance.
func (ec *EC) AddParticipant(id gpbft.ActorID, power *gpbft.StoragePower, pubkey []byte) {
	entry := gpbft.PowerEntry{ID: id, Power: power, PubKey: pubkey}
	if err := ec.Instances[0].PowerTable.Add(entry); err != nil {
		panic("failed to add participant")
	}
}

// Adds a new instance to the EC state, with a new chain shared by all participants.
// The power table and beacon correspond to the base of the new chain.
func (ec *EC) AddInstance(chain gpbft.ECChain, power *gpbft.PowerTable, beacon []byte) {
	newInstance := &ECInstance{
		Base:       chain.BaseChain(),
		Chains:     make(map[gpbft.ActorID]gpbft.ECChain),
		PowerTable: power,
		Beacon:     beacon,
	}
	// Set the chain for each participant with power.
	for _, entry := range power.Entries {
		newInstance.Chains[entry.ID] = chain
	}
	ec.Instances = append(ec.Instances, newInstance)
}
