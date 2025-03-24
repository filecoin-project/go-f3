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
	gpbft.Clock

	id     gpbft.ActorID
	sim    *Simulation
	pubkey gpbft.PubKey

	ecChain *gpbft.ECChain
	ecg     ECChainGenerator
	spg     StoragePowerGenerator
}

func (v *simHost) RequestSynchronousBroadcast(mb *gpbft.MessageBuilder) error {
	return v.SimNetwork.RequestSynchronousBroadcast(mb)
}

type SimNetwork interface {
	gpbft.Network
	gpbft.Tracer
	// sends a message to all other participants immediately.
	RequestSynchronousBroadcast(mb *gpbft.MessageBuilder) error
}

func newHost(id gpbft.ActorID, sim *Simulation, ecg ECChainGenerator, spg StoragePowerGenerator, isAdversary bool) *simHost {
	pubKey, _ := sim.signingBacked.GenerateKey()
	return &simHost{
		SimNetwork: sim.network.networkFor(sim.signingBacked, id, isAdversary),
		Verifier:   sim.signingBacked,
		Signer:     sim.signingBacked,
		sim:        sim,
		id:         id,
		ecg:        ecg,
		spg:        spg,
		pubkey:     pubKey,
		ecChain:    sim.baseChain,
	}
}

func (v *simHost) GetProposal(instance uint64) (*gpbft.SupplementalData, *gpbft.ECChain, error) {
	// Use the head of latest agreement chain as the base of next.
	// TODO: use lookback to return the correct next power table commitment and commitments hash.
	chain := v.ecg.GenerateECChain(instance, v.ecChain.Head(), v.id)
	i := v.sim.ec.GetInstance(instance)
	if i == nil {
		// It is possible for one node to start the next instance before others have
		// completed theirs, e.g. in the case of partial connectivity across nodes.
		//
		// Check for errors and if there isn't any start the next instance early. Use the
		// decision of this participant as the base of next instance. sim.Run will check
		// if the base is inconsistent with the decision of the rest of the network.
		// Therefore, it is safe to optimistically start the next instance here.
		if err := v.sim.ec.Err(); err != nil {
			return nil, nil, err
		}
		table, err := v.sim.getPowerTable(instance)
		if err != nil {
			return nil, nil, err
		}
		i = v.sim.ec.BeginInstance(v.ecChain, table)
	}
	return i.SupplementalData, chain, nil
}

func (v *simHost) GetCommittee(instance uint64) (*gpbft.Committee, error) {
	i := v.sim.ec.GetInstance(instance)
	if i == nil {
		return nil, ErrInstanceUnavailable
	}
	return &gpbft.Committee{
		PowerTable:        i.PowerTable,
		Beacon:            i.Beacon,
		AggregateVerifier: i.aggregateVerifier,
	}, nil
}

func (v *simHost) SetAlarm(at time.Time) {
	v.sim.network.SetAlarm(v.id, at)
}

func (v *simHost) Time() time.Time {
	return v.sim.network.Time()
}

func (v *simHost) ReceiveDecision(decision *gpbft.Justification) (time.Time, error) {
	v.sim.ec.NotifyDecision(v.id, decision)
	v.ecChain = decision.Vote.Value
	return v.Time().Add(v.sim.ecEpochDuration).Add(v.sim.ecStabilisationDelay), nil
}

func (v *simHost) StoragePower(instance uint64) gpbft.StoragePower {
	return v.spg(instance, v.id)
}

func (v *simHost) PublicKey( /*instance */ uint64) gpbft.PubKey {
	return v.pubkey
}

func (v *simHost) ID() gpbft.ActorID {
	return v.id
}

func (v *simHost) Log(format string, args ...any) {
	v.SimNetwork.Log(format, args...)
}
