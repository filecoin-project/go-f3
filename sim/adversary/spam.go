package adversary

import (
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*Spam)(nil)

// Spam is an adversary that propagates COMMIT messages for bottom for a
// configured number of future rounds.
type Spam struct {
	id                     gpbft.ActorID
	host                   Host
	roundsAhead            uint64
	latestObservedInstance uint64
}

// NewSpam instantiates a new Spam adversary that spams the network with
// spammable messages (i.e. COMMIT for bottom) for the configured number of
// roundsAhead via either synchronous or regular broadcast. This adversary
// resigns the spammable messages as its own to mimic messages with valid
// signature but for future rounds.
func NewSpam(id gpbft.ActorID, host Host, roundsAhead uint64) *Spam {
	return &Spam{
		id:          id,
		host:        host,
		roundsAhead: roundsAhead,
	}
}

func NewSpamGenerator(power *gpbft.StoragePower, roundsAhead uint64) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewSpam(id, host, roundsAhead),
			Power:    power,
		}
	}
}

func (s *Spam) Start() error {
	// Immediately start spamming the network.
	s.spamAtInstance(s.latestObservedInstance)
	return nil
}

func (*Spam) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}

func (s *Spam) ReceiveMessage(vmsg gpbft.ValidatedMessage) error {
	msg := vmsg.Message()
	// Watch for increase in instance, and when increased spam again.
	if msg.Vote.Instance > s.latestObservedInstance {
		s.spamAtInstance(msg.Vote.Instance)
		s.latestObservedInstance = msg.Vote.Instance
	}
	return nil
}

func (s *Spam) spamAtInstance(instance uint64) {
	// Spam the network with COMMIT messages by incrementing rounds up to
	// roundsAhead.
	for spamRound := uint64(0); spamRound < s.roundsAhead; spamRound++ {
		spamVote := gpbft.Payload{
			Instance: instance,
			Round:    spamRound,
			Step:     gpbft.COMMIT_PHASE,
		}
		sigPayload := s.host.MarshalPayloadForSigning(&spamVote)
		power, _, err := s.host.GetCommitteeForInstance(instance)
		if err != nil {
			panic(err)
		}
		_, pubkey := power.Get(s.id)
		sig, err := s.host.Sign(pubkey, sigPayload)
		if err != nil {
			panic(err)
		}
		s.host.RequestBroadcast(&gpbft.GMessage{
			Sender:    s.ID(),
			Vote:      spamVote,
			Signature: sig,
		})
	}
}

func (s *Spam) ID() gpbft.ActorID                                              { return s.id }
func (s *Spam) ReceiveAlarm() error                                            { return nil }
func (s *Spam) AllowMessage(gpbft.ActorID, gpbft.ActorID, gpbft.GMessage) bool { return true }
