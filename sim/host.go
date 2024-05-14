package sim

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
)

var _ gpbft.Host = (*simHost)(nil)
var _ adversary.Host = (*simHost)(nil)

// One participant's host
// This provides methods that know the caller's participant ID and can provide its view of the world.
type simHost struct {
	gpbft.Network
	gpbft.Signer
	gpbft.Verifier
	gpbft.Clock

	id     gpbft.ActorID
	sim    *Simulation
	pubkey gpbft.PubKey

	// The simulation package always starts at instance zero.
	// TODO: https://github.com/filecoin-project/go-f3/issues/195
	instance uint64
	ecChain  gpbft.ECChain
	ecg      ECChainGenerator
	spg      StoragePowerGenerator
}

func newHost(id gpbft.ActorID, sim *Simulation, ecg ECChainGenerator, spg StoragePowerGenerator) *simHost {
	pubKey, _ := sim.signingBacked.GenerateKey()
	return &simHost{
		Network:  sim.network,
		Verifier: sim.signingBacked,
		Signer:   sim.signingBacked,
		sim:      sim,
		id:       id,
		ecg:      ecg,
		spg:      spg,
		pubkey:   pubKey,
		ecChain:  *sim.baseChain,
	}
}

func (v *simHost) GetCanonicalChain() (gpbft.ECChain, gpbft.PowerTable, []byte) {
	i := v.sim.ec.GetInstance(v.instance)
	// Use the head of latest agreement chain as the base of next.
	chain := v.ecg.GenerateECChain(v.instance, *v.ecChain.Head(), v.id)
	return chain, *i.PowerTable, i.Beacon
}

func (v *simHost) SetAlarm(at time.Time) {
	v.sim.network.SetAlarm(v.id, at)
}

func (v *simHost) Time() time.Time {
	return v.sim.network.Time()
}

func (v *simHost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	v.sim.ec.NotifyDecision(v.id, decision)
	v.instance = decision.Vote.Instance + 1
	v.ecChain = decision.Vote.Value
	return v.Time().Add(v.sim.ecEpochDuration).Add(v.sim.ecStabilisationDelay)
}

func (v *simHost) BroadcastSynchronous(sender gpbft.ActorID, msg gpbft.GMessage) {
	v.sim.network.BroadcastSynchronous(sender, msg)
}

func (v *simHost) StoragePower() *gpbft.StoragePower {
	return v.spg(v.instance, v.id)
}

func (v *simHost) PublicKey() gpbft.PubKey {
	return v.pubkey
}

func (v *simHost) ID() gpbft.ActorID {
	return v.id
}
