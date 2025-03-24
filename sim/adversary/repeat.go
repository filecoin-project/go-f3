package adversary

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*Repeat)(nil)

// Repeat is a type of adversary in the gpbft consensus protocol that intercepts and rebroadcasts messages.
// It is designed to test the resilience of the consensus mechanism against Byzantine faults, specifically through
// message repetition attacks.
//
// The number of times each message is repeated can be configured using RepetitionSampler. This allows simulating fixed
// or varying degrees of repetition throughout a simulation. The repeated messages are resigned by the adversary with
// other fields left unmodified. This results in a mimicking behaviour, where the adversary broadcasts any message it
// receives as its own.
//
// By repeating messages, this adversary can simulate a variety of fault conditions, including the creation of
// equivocations. Equivocations occur when a node (or in this case, an adversary) sends conflicting information to
// different parts of the network, which can potentially mislead other nodes about the state of consensus. This behavior
// effectively amplifies the likelihood of network partitions in cases where there are diverging base chains across the
// participants. More importantly, this adversary may momentarily appear to be part of one or other network partitions.
//
// See RepetitionSampler.
type Repeat struct {
	id   gpbft.ActorID
	host Host

	// repetitionSampler determines the number of times each message is echoed by this adversary.
	repetitionSampler RepetitionSampler
}

// RepetitionSampler returns the number of times each message is repeated by Repeat adversary.
// The sampler may implement a fixed or random sampling, and can return different values over time.
// This allows an implementer to program scenarios where messages are repeated probabilistically or
// at certain stage through the experiment.
//
// The number of times a message s repeated is dynamically configurable based on the original message itself.
// For example, an implementer may choose to only repeat messages in a certain phase or from a certain participants.
// A repetition count of less than or equal to zero signals that the adversary should not rebroadcast the message at
// all.
type RepetitionSampler func(*gpbft.GMessage) int

// NewRepeat creates a new instance of the Repeat adversary using the provided id, host, and repetitionSampler.
// The RepetitionSampler is used to determine the number of times each message received by this adversary should be
// repeated and retransmitted to other nodes in the network. This repetition could potentially disrupt or manipulate
// the consensus process depending on how the repetitionSampler is configured (e.g., fixed or probabilistic
// repetitions), and can lead to equivocating messages across the network.
//
// The primary role of the Repeat adversary is to intercept messages and then broadcast them multiple times.
// The number of repetitions for each message is determined by the RepetitionSampler. Each echoed message is
// signed and optionally includes a new ticket if the original message had one. Note, this adversary does not modify the
// received messages. Instead, it resigns them as its own and broadcasts them effectively mimicking the behavior of other
// participants in the network.
func NewRepeat(id gpbft.ActorID, host Host, sampler RepetitionSampler) *Repeat {
	return &Repeat{
		id:                id,
		host:              host,
		repetitionSampler: sampler,
	}
}

func NewRepeatGenerator(power gpbft.StoragePower, sampler RepetitionSampler) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewRepeat(id, host, sampler),
			Power:    power,
		}
	}
}

func (*Repeat) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}

func (r *Repeat) ReceiveMessage(vmsg gpbft.ValidatedMessage) error {
	msg := vmsg.Message()
	echoCount := r.repetitionSampler(msg)
	if echoCount <= 0 {
		return nil
	}
	instance := msg.Vote.Instance
	supplementalData, _, err := r.host.GetProposal(instance)
	if err != nil {
		panic(err)
	}
	committee, _ := r.host.GetCommittee(instance)
	p := gpbft.Payload{
		Instance:         instance,
		Round:            msg.Vote.Round,
		Phase:            msg.Vote.Phase,
		SupplementalData: *supplementalData,
		Value:            msg.Vote.Value,
	}
	mt := &gpbft.MessageBuilder{
		NetworkName:   r.host.NetworkName(),
		PowerTable:    committee.PowerTable,
		Payload:       p,
		Justification: msg.Justification,
	}
	if len(msg.Ticket) > 0 {
		mt.BeaconForTicket = committee.Beacon
	}
	for i := 0; i < echoCount; i++ {
		if msg.Sender != r.ID() {
			if err := r.host.RequestBroadcast(mt); err != nil {
				panic(err)
			}
		}
	}
	return nil
}

func (r *Repeat) ID() gpbft.ActorID                                              { return r.id }
func (r *Repeat) StartInstanceAt(uint64, time.Time) error                        { return nil }
func (r *Repeat) ReceiveAlarm() error                                            { return nil }
func (r *Repeat) AllowMessage(gpbft.ActorID, gpbft.ActorID, gpbft.GMessage) bool { return true }
