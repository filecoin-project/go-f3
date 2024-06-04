package adversary

import (
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

func (i *ImmediateDecide) Start() error {
	powertable, _, _ := i.host.GetCommitteeForInstance(0)
	// Immediately send a DECIDE message
	payload := gpbft.Payload{
		Instance: 0,
		Round:    0,
		Step:     gpbft.DECIDE_PHASE,
		Value:    i.value,
	}
	justificationPayload := gpbft.Payload{
		Instance: 0,
		Round:    0,
		Step:     gpbft.COMMIT_PHASE,
		Value:    i.value,
	}
	sigPayload := i.host.MarshalPayloadForSigning(i.host.NetworkName(), &justificationPayload)
	_, pubkey := powertable.Get(i.id)
	sig, err := i.host.Sign(pubkey, sigPayload)
	if err != nil {
		panic(err)
	}

	signers := bitfield.New()
	signers.Set(uint64(powertable.Lookup[i.id]))
	aggregatedSig, err := i.host.Aggregate([]gpbft.PubKey{pubkey}, [][]byte{sig})
	if err != nil {
		panic(err)
	}

	justification := gpbft.Justification{
		Vote:      justificationPayload,
		Signers:   signers,
		Signature: aggregatedSig,
	}

	i.broadcast(payload, &justification, powertable)
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

func (*ImmediateDecide) SkipToInstance(_ uint64) error {
	return nil
}

func (*ImmediateDecide) AllowMessage(_ gpbft.ActorID, _ gpbft.ActorID, _ gpbft.GMessage) bool {
	// Allow all messages
	return true
}

func (i *ImmediateDecide) broadcast(payload gpbft.Payload, justification *gpbft.Justification, powertable *gpbft.PowerTable) {

	pS := i.host.MarshalPayloadForSigning(i.host.NetworkName(), &payload)
	_, pubkey := powertable.Get(i.id)
	sig, err := i.host.Sign(pubkey, pS)
	if err != nil {
		panic(err)
	}

	i.host.BroadcastSynchronous(&gpbft.GMessage{
		Sender:        i.id,
		Vote:          payload,
		Signature:     sig,
		Justification: justification,
	})
}
