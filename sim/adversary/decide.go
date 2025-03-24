package adversary

import (
	"context"
	"time"

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

// Immediately decide with a value that may or may not match the justification.
func ImmediateDecideWithJustifiedValue(value *gpbft.ECChain) ImmediateDecideOption {
	return func(i *ImmediateDecide) {
		i.jValue = value
	}
}

// Immediately decide with a value that may or may not match the justification.
func ImmediateDecideWithJustifiedSupplementalData(data gpbft.SupplementalData) ImmediateDecideOption {
	return func(i *ImmediateDecide) {
		i.supplementalData = &data
	}
}

// / An "adversary" that immediately sends a DECIDE message, justified by its own COMMIT.
type ImmediateDecide struct {
	id            gpbft.ActorID
	host          Host
	value, jValue *gpbft.ECChain

	additionalParticipant *uint64
	supplementalData      *gpbft.SupplementalData
}

func NewImmediateDecide(id gpbft.ActorID, host Host, value *gpbft.ECChain, opts ...ImmediateDecideOption) *ImmediateDecide {
	i := &ImmediateDecide{
		id:     id,
		host:   host,
		value:  value,
		jValue: value,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

func NewImmediateDecideGenerator(value *gpbft.ECChain, power gpbft.StoragePower, opts ...ImmediateDecideOption) Generator {
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

func (i *ImmediateDecide) StartInstanceAt(instance uint64, _when time.Time) error {
	supplementalData, _, err := i.host.GetProposal(instance)
	if err != nil {
		panic(err)
	}
	committee, err := i.host.GetCommittee(instance)
	if err != nil {
		panic(err)
	}
	justificationPayload := gpbft.Payload{
		Instance:         instance,
		Round:            0,
		Phase:            gpbft.COMMIT_PHASE,
		Value:            i.jValue,
		SupplementalData: *supplementalData,
	}
	if i.supplementalData != nil {
		justificationPayload.SupplementalData = *i.supplementalData
	}
	sigPayload := justificationPayload.MarshalForSigning(i.host.NetworkName())
	signers := bitfield.New()

	signers.Set(uint64(committee.PowerTable.Lookup[i.id]))

	if i.additionalParticipant != nil {
		signers.Set(*i.additionalParticipant)
	}

	var (
		mask []int
		sigs [][]byte
	)

	if err := signers.ForEach(func(j uint64) error {
		if j >= uint64(len(committee.PowerTable.Entries)) {
			return nil
		}
		pubkey := committee.PowerTable.Entries[j].PubKey
		sig, err := i.host.Sign(context.Background(), pubkey, sigPayload)
		if err != nil {
			return err
		}

		mask = append(mask, int(j))
		sigs = append(sigs, sig)
		return nil
	}); err != nil {
		panic(err)
	}

	agg, err := i.host.Aggregate(committee.PowerTable.Entries.PublicKeys())
	if err != nil {
		panic(err)
	}
	aggregatedSig, err := agg.Aggregate(mask, sigs)
	if err != nil {
		panic(err)
	}

	// Immediately send a DECIDE message
	mb := &gpbft.MessageBuilder{
		NetworkName: i.host.NetworkName(),
		PowerTable:  committee.PowerTable,
		Payload: gpbft.Payload{
			Instance:         instance,
			Round:            0,
			Phase:            gpbft.DECIDE_PHASE,
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
