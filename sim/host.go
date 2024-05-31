package sim

import (
	"errors"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
)

// An error to be returned when a chain or committee is not available for an instance.
var ErrInstanceUnavailable = errors.New("instance not available")

var _ gpbft.Host = (*simHost)(nil)
var _ adversary.Host = (*simHost)(nil)

// One participant's host
// This provides methods that know the caller's participant ID and can provide its view of the world.
type simHost struct {
	SimNetwork
	gpbft.Signer
	gpbft.Verifier
	gpbft.SigningMarshaler
	gpbft.Clock

	id     gpbft.ActorID
	sim    *Simulation
	pubkey gpbft.PubKey

	ecChain gpbft.ECChain
	ecg     ECChainGenerator
	spg     StoragePowerGenerator
}

type SimNetwork interface {
	gpbft.Network
	// sends a message to all other participants immediately.
	BroadcastSynchronous(msg *gpbft.GMessage)
}

func newHost(id gpbft.ActorID, sim *Simulation, ecg ECChainGenerator, spg StoragePowerGenerator) *simHost {
	pubKey, _ := sim.signingBacked.GenerateKey()
	return &simHost{
		SimNetwork:       sim.network.NetworkFor(sim.signingBacked, id),
		Verifier:         sim.signingBacked,
		Signer:           sim.signingBacked,
		SigningMarshaler: sim.signingBacked,
		sim:              sim,
		id:               id,
		ecg:              ecg,
		spg:              spg,
		pubkey:           pubKey,
		ecChain:          *sim.baseChain,
	}
}

func (v *simHost) GetProposalForInstance(instance uint64) (*gpbft.SupplementalData, gpbft.ECChain, error) {
	// Use the head of latest agreement chain as the base of next.
	// TODO: use lookback to return the correct next power table commitment and commitments hash.
	chain := v.ecg.GenerateECChain(instance, *v.ecChain.Head(), v.id)
	return new(gpbft.SupplementalData), chain, nil
}

func (v *simHost) GetCommitteeForInstance(instance uint64) (power *gpbft.PowerTable, beacon []byte, err error) {
	i := v.sim.ec.GetInstance(instance)
	if i == nil {
		return nil, nil, ErrInstanceUnavailable
	}
	return i.PowerTable, i.Beacon, nil
}

func (v *simHost) SetAlarm(at time.Time) {
	v.sim.network.SetAlarm(v.id, at)
}

func (v *simHost) Time() time.Time {
	return v.sim.network.Time()
}

func (v *simHost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	v.sim.ec.NotifyDecision(v.id, decision)
	v.ecChain = decision.Vote.Value
	return v.Time().Add(v.sim.ecEpochDuration).Add(v.sim.ecStabilisationDelay)
}

func (v *simHost) ReceiveFinalityCertificate(uint64, gpbft.FinalityInfo) error {
	panic("not implemented")
}

func (v *simHost) StoragePower(instance uint64) *gpbft.StoragePower {
	return v.spg(instance, v.id)
}

func (v *simHost) PublicKey( /*instance */ uint64) gpbft.PubKey {
	return v.pubkey
}

func (v *simHost) ID() gpbft.ActorID {
	return v.id
}
