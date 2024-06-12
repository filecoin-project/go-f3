package adversary

import (
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*ImmediateDecide)(nil)

type ImmediateDecideOption func(*ImmediateDecide)

func ImmediateDecideWithNthParticipant(n uint64) ImmediateDecideOption {
	return func(i *ImmediateDecide) {
		i.additionalParticipant = &n
	}
}

// / An "adversary" that immediately sends a DECIDE message, justified by its own COMMIT.
type ImmediateDecide struct {
	id    gpbft.ActorID
	host  Host
	value gpbft.ECChain

	additionalParticipant *uint64
}

func NewImmediateDecide(id gpbft.ActorID, host Host, value gpbft.ECChain, opts ...ImmediateDecideOption) *ImmediateDecide {
	i := &ImmediateDecide{
		id:    id,
		host:  host,
		value: value,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

func NewImmediateDecideGenerator(value gpbft.ECChain, power *gpbft.StoragePower, opts ...ImmediateDecideOption) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewImmediateDecide(id, host, value, opts...),
			Power:    power,
		}
	}
}

func (i *ImmediateDecide) ID() gpbft.ActorID {
	return i.id
}

func (i *ImmediateDecide) StartInstance(instance uint64) error {
	supplementalData, _, err := i.host.GetProposalForInstance(instance)
	if err != nil {
		panic(err)
	}
	powertable, _, err := i.host.GetCommitteeForInstance(instance)
	if err != nil {
		panic(err)
	}
	// Immediately send a DECIDE message
	payload := gpbft.Payload{
		Instance:         instance,
		Round:            0,
		Step:             gpbft.DECIDE_PHASE,
		Value:            i.value,
		SupplementalData: *supplementalData,
	}
	justificationPayload := gpbft.Payload{
		Instance:         instance,
		Round:            0,
		Step:             gpbft.COMMIT_PHASE,
		Value:            i.value,
		SupplementalData: *supplementalData,
	}
	sigPayload := i.host.MarshalPayloadForSigning(i.host.NetworkName(), &justificationPayload)
	signers := bitfield.New()

	signers.Set(uint64(powertable.Lookup[i.id]))

	if i.additionalParticipant != nil {
		signers.Set(*i.additionalParticipant)
	}

	var (
		pubkeys []gpbft.PubKey
		sigs    [][]byte
	)

	if err := signers.ForEach(func(j uint64) error {
		pubkey := gpbft.PubKey("fake pubkey")
		sig := []byte("fake sig")
		if j < uint64(len(powertable.Entries)) {
			pubkey = powertable.Entries[j].PubKey
			var err error
			sig, err = i.host.Sign(pubkey, sigPayload)
			if err != nil {
				return err
			}
		}

		pubkeys = append(pubkeys, pubkey)
		sigs = append(sigs, sig)
		return nil
	}); err != nil {
		panic(err)
	}

	aggregatedSig, err := i.host.Aggregate(pubkeys, sigs)
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

func (*ImmediateDecide) AllowMessage(_ gpbft.ActorID, _ gpbft.ActorID, _ gpbft.GMessage) bool {
	// Allow all messages
	return true
}

func (i *ImmediateDecide) broadcast(payload gpbft.Payload, justification *gpbft.Justification, powertable *gpbft.PowerTable) {

	pS := i.host.MarshalPayloadForSigning(i.host.NetworkName(), &payload)
	_, _, pubkey := powertable.Get(i.id)
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
