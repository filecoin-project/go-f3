package adversary

import (
	"context"
	"time"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*ImmediateDecide)(nil)

// / An "adversary" that immediately sends a DECIDE message, justified by its own COMMIT.
type ImmediateDecide struct {
	id    gpbft.ActorID
	host  Host
	value gpbft.ECChain
}

func NewImmediateDecide(id gpbft.ActorID, host Host, value gpbft.ECChain) *ImmediateDecide {
	return &ImmediateDecide{
		id:    id,
		host:  host,
		value: value,
	}
}

func NewImmediateDecideGenerator(value gpbft.ECChain, power *gpbft.StoragePower) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewImmediateDecide(id, host, value),
			Power:    power,
		}
	}
}

func (i *ImmediateDecide) ID() gpbft.ActorID {
	return i.id
}

func (i *ImmediateDecide) StartInstanceAt(instance uint64, _when time.Time) error {
	supplementalData, _, err := i.host.GetProposalForInstance(instance)
	if err != nil {
		panic(err)
	}
	powertable, _, err := i.host.GetCommitteeForInstance(instance)
	if err != nil {
		panic(err)
	}
	justificationPayload := gpbft.Payload{
		Instance:         instance,
		Round:            0,
		Step:             gpbft.COMMIT_PHASE,
		Value:            i.value,
		SupplementalData: *supplementalData,
	}
	sigPayload := i.host.MarshalPayloadForSigning(i.host.NetworkName(), &justificationPayload)
	_, pubkey := powertable.Get(i.id)
	sig, err := i.host.Sign(context.Background(), pubkey, sigPayload)
	if err != nil {
		panic(err)
	}

	signers := bitfield.New()
	signers.Set(uint64(powertable.Lookup[i.id]))
	aggregatedSig, err := i.host.Aggregate([]gpbft.PubKey{pubkey}, [][]byte{sig})
	if err != nil {
		panic(err)
	}

	// Immediately send a DECIDE message
	mb := &gpbft.MessageBuilder{
		NetworkName:       i.host.NetworkName(),
		PowerTable:        powertable,
		SigningMarshaller: i.host,
		Payload: gpbft.Payload{
			Instance:         instance,
			Round:            0,
			Step:             gpbft.DECIDE_PHASE,
			Value:            i.value,
			SupplementalData: *supplementalData,
		},
		Justification: &gpbft.Justification{
			Vote:      justificationPayload,
			Signers:   signers,
			Signature: aggregatedSig,
		},
	}

	if err := i.host.RequestSynchronousBroadcast(mb); err != nil {
		panic(err)
	}
	return nil
}

func (*ImmediateDecide) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}

func (*ImmediateDecide) ReceiveMessage(_ gpbft.ValidatedMessage) error {
	return nil
}

func (*ImmediateDecide) ReceiveAlarm() error {
	return nil
}

func (*ImmediateDecide) AllowMessage(_ gpbft.ActorID, _ gpbft.ActorID, _ gpbft.GMessage) bool {
	// Allow all messages
	return true
}
