package adversary

import (
	"fmt"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
)

type ImmediateDecide struct {
	id         gpbft.ActorID
	host       sim.AdversaryHost
	powertable *gpbft.PowerTable
	value      gpbft.ECChain
}

func NewImmediateDecide(id gpbft.ActorID, host sim.AdversaryHost, powertable *gpbft.PowerTable, value gpbft.ECChain) *ImmediateDecide {
	return &ImmediateDecide{
		id:         id,
		host:       host,
		powertable: powertable,
		value:      value,
	}
}

func (i *ImmediateDecide) ID() gpbft.ActorID {
	return i.id
}

func (i *ImmediateDecide) ReceiveCanonicalChain(_ gpbft.ECChain, _ gpbft.PowerTable, _ []byte) error {
	return nil
}

func (i *ImmediateDecide) ReceiveECChain(_ gpbft.ECChain) error {
	return nil
}

func (i *ImmediateDecide) ValidateMessage(_ *gpbft.GMessage) (bool, error) {
	return true, nil
}

func (i *ImmediateDecide) ReceiveMessage(_ *gpbft.GMessage) (bool, error) {
	return true, nil
}

func (i *ImmediateDecide) ReceiveAlarm() error {
	return nil
}

func (i *ImmediateDecide) Begin() {
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
	pS := justificationPayload.MarshalForSigning(i.host.NetworkName())

	_, pubkey := i.powertable.Get(i.id)
	sig, err := i.host.Sign(pubkey, pS)
	if err != nil {
		panic(err)
	}

	signers := bitfield.New()
	signers.Set(uint64(i.powertable.Lookup[i.id]))

	aggregatedSig, err := i.host.Aggregate([]gpbft.PubKey{pubkey}, [][]byte{sig})
	if err != nil {
		panic(err)
	}

	justification := gpbft.Justification{
		Vote:      justificationPayload,
		Signers:   signers,
		Signature: aggregatedSig,
	}

	fmt.Printf("Sending message %v\n", payload)
	i.broadcast(payload, &justification)
}

func (i *ImmediateDecide) AllowMessage(_ gpbft.ActorID, _ gpbft.ActorID, _ gpbft.GMessage) bool {
	// Allow all messages
	return true
}

func (i *ImmediateDecide) broadcast(payload gpbft.Payload, justification *gpbft.Justification) {
	pS := payload.MarshalForSigning(i.host.NetworkName())
	_, pubkey := i.powertable.Get(i.id)
	sig, err := i.host.Sign(pubkey, pS)
	if err != nil {
		panic(err)
	}

	i.host.BroadcastSynchronous(i.id, gpbft.GMessage{
		Sender:        i.id,
		Vote:          payload,
		Signature:     sig,
		Justification: justification,
	})
}
