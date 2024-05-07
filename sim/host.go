package sim

import (
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
)

var _ gpbft.Host = (*simHost)(nil)
var _ adversary.Host = (*simHost)(nil)

// One participant's host
// This provides methods that know the caller's participant ID and can provide its view of the world.
type simHost struct {
	SimNetwork
	gpbft.Signer
	gpbft.Verifier
	gpbft.Clock

	sim *Simulation
	id  gpbft.ActorID
}

type SimNetwork interface {
	gpbft.Network
	BroadcastSynchronous(sender gpbft.ActorID, msg gpbft.GMessage)
	// Sends a message to all other participants.
	Broadcast(*gpbft.GMessage)
}

func newHost(id gpbft.ActorID, sim *Simulation) *simHost {
	return &simHost{
		SimNetwork: sim.network.NetworkFor(sim.signingBacked, id),
		Verifier:   sim.signingBacked,
		Signer:     sim.signingBacked,
		sim:        sim,
		id:         id,
	}
}

func (v *simHost) GetCanonicalChain() (chain gpbft.ECChain, power gpbft.PowerTable, beacon []byte) {
	// Find the instance after the last instance finalised by the participant.
	var instance uint64
	decisions := v.sim.decisions.Decisions
	for i := len(decisions) - 1; i >= 0; i-- {
		if decisions[i][v.id] != nil {
			instance = uint64(i + 1)
			break
		}
	}
	i := v.sim.ec.Instances[instance]
	chain = i.Chains[v.id]
	power = *i.PowerTable
	beacon = i.Beacon
	return
}

func (v *simHost) SetAlarm(at time.Time) {
	v.sim.network.SetAlarm(v.id, at)
}

func (v *simHost) Time() time.Time {
	return v.sim.network.Time()
}

func (v *simHost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	firstForInstance := v.sim.decisions.ReceiveDecision(v.id, decision)
	if firstForInstance {
		// When the first valid decision is received for an instance, prepare for the next one.
		nextBase := decision.Vote.Value.Head()
		// Copy the previous instance power table.
		// The simulator doesn't have any facility to evolve the power table.
		// See https://github.com/filecoin-project/go-f3/issues/114.
		nextPowerTable := v.sim.ec.Instances[decision.Vote.Instance].PowerTable.Copy()
		nextBeacon := []byte(fmt.Sprintf("beacon %d", decision.Vote.Instance+1))
		// Create a new chain for all participants.
		// There's no facility yet for them to observe different chains after the first instance.
		// See https://github.com/filecoin-project/go-f3/issues/115.
		newTip := v.sim.tipSetGenerator.Sample()
		nextChain, _ := gpbft.NewChain(nextBase, newTip)

		v.sim.ec.AddInstance(nextChain, nextPowerTable, nextBeacon)
		v.sim.decisions.BeginInstance(decision.Vote.Instance+1, nextBase, nextPowerTable)
	}
	// Next instance starts some fixed time after the next EC epoch is due.
	nextInstanceStart := v.Time().Add(v.sim.ecEpochDuration).Add(v.sim.ecStabilisationDelay)
	return nextInstanceStart
}
