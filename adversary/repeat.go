package adversary

import (
	"github.com/filecoin-project/go-f3/gpbft"
)

type (
	Repeat struct {
		id            gpbft.ActorID
		host          gpbft.Host
		echoCountDist CountSampler
	}
	CountSampler interface {
		// TODO replace with math/rand/v2 rand.Source once upgraded to go 1.22.
		Uint64() uint64
	}
)

func NewRepeat(id gpbft.ActorID, host gpbft.Host, echoCountDist CountSampler) *Repeat {
	return &Repeat{
		id:            id,
		host:          host,
		echoCountDist: echoCountDist,
	}
}

func (r *Repeat) ReceiveMessage(msg *gpbft.GMessage, _ bool) (bool, error) {
	sigPayload := msg.Vote.MarshalForSigning(r.host.NetworkName())
	_, power, beacon := r.host.GetCanonicalChain()
	_, pubkey := power.Get(r.id)

	sig, err := r.host.Sign(pubkey, sigPayload)
	if err != nil {
		panic(err)
	}

	var ticket gpbft.Ticket
	if len(msg.Ticket) != 0 {
		var err error
		ticket, err = gpbft.MakeTicket(beacon, msg.Vote.Instance, msg.Vote.Round, pubkey, r.host)
		if err != nil {
			panic(err)
		}
	}
	echo := &gpbft.GMessage{
		Sender:        r.ID(),
		Vote:          msg.Vote,
		Signature:     sig,
		Justification: msg.Justification,
		Ticket:        ticket,
	}
	echoCount := int(r.echoCountDist.Uint64())
	for i := 0; i < echoCount; i++ {
		r.host.Broadcast(echo)
	}
	return true, nil
}

func (r *Repeat) ID() gpbft.ActorID                                              { return r.id }
func (r *Repeat) Start() error                                                   { return nil }
func (r *Repeat) ReceiveECChain(gpbft.ECChain) error                             { return nil }
func (r *Repeat) ValidateMessage(*gpbft.GMessage) (bool, error)                  { return true, nil }
func (r *Repeat) ReceiveAlarm() error                                            { return nil }
func (r *Repeat) AllowMessage(gpbft.ActorID, gpbft.ActorID, gpbft.GMessage) bool { return true }
