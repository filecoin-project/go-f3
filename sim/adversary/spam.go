package adversary

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*Spam)(nil)

// Spam is an adversary that propagates COMMIT messages for bottom for a
// configured number of future rounds.
type Spam struct {
	host                   Host
	roundsAhead            uint64
	latestObservedInstance uint64

	Absent
	allowAll
}

// NewSpam instantiates a new Spam adversary that spams the network with
// spammable messages (i.e. COMMIT for bottom) for the configured number of
// roundsAhead via either synchronous or regular broadcast. This adversary
// resigns the spammable messages as its own to mimic messages with valid
// signature but for future rounds.
func NewSpam(host Host, roundsAhead uint64) *Spam {
	return &Spam{
		host:        host,
		roundsAhead: roundsAhead,
	}
}

func NewSpamGenerator(power gpbft.StoragePower, roundsAhead uint64) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewSpam(host, roundsAhead),
			Power:    power,
			ID:       id,
		}
	}
}

func (s *Spam) StartInstanceAt(instance uint64, _when time.Time) error {
	// Immediately start spamming the network.
	s.latestObservedInstance = instance
	s.spamAtInstance(context.Background(), s.latestObservedInstance)
	return nil
}

func (s *Spam) ReceiveMessage(ctx context.Context, vmsg gpbft.ValidatedMessage) error {
	msg := vmsg.Message()
	// Watch for increase in instance, and when increased spam again.
	if msg.Vote.Instance > s.latestObservedInstance {
		s.spamAtInstance(ctx, msg.Vote.Instance)
		s.latestObservedInstance = msg.Vote.Instance
	}
	return nil
}

func (s *Spam) spamAtInstance(ctx context.Context, instance uint64) {
	// Spam the network with COMMIT messages by incrementing rounds up to
	// roundsAhead.
	supplementalData, _, err := s.host.GetProposal(ctx, instance)
	if err != nil {
		panic(err)
	}
	committee, err := s.host.GetCommittee(ctx, instance)
	if err != nil {
		panic(err)
	}
	for spamRound := uint64(0); spamRound < s.roundsAhead; spamRound++ {
		p := gpbft.Payload{
			Instance:         instance,
			Round:            spamRound,
			SupplementalData: *supplementalData,
			Phase:            gpbft.COMMIT_PHASE,
		}
		mt := &gpbft.MessageBuilder{
			NetworkName: s.host.NetworkName(),
			PowerTable:  committee.PowerTable,
			Payload:     p,
		}
		if err := s.host.RequestBroadcast(mt); err != nil {
			panic(err)
		}
	}
}
