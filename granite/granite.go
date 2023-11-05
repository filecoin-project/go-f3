package granite

import (
	"fmt"
	"github.com/anorth/f3sim/net"
)

type Participant struct {
	id    string
	ntwk  net.NetworkSink
	mpool []GraniteMessage

	instance     *instance
	nextChain    net.ECChain
	nextInstance int
}

func NewParticipant(id string, ntwk net.NetworkSink) *Participant {
	return &Participant{id: id, ntwk: ntwk}
}

func (p *Participant) ID() string {
	return p.id
}

// Receives a new canonical EC chain for the instance.
// This becomes the instance's preferred value to finalise.
func (p *Participant) ReceiveCanonicalChain(chain net.ECChain) {
	p.nextChain = chain
	if p.instance == nil {
		p.instance = newInstance(p.ntwk, p.id, p.nextInstance, p.nextChain)
		p.instance.start()
		p.ntwk.SetAlarm(p.id, p.ntwk.Time()+10)
		p.nextInstance += 1
	}
}

// Receives a Granite message from some other participant.
func (p *Participant) ReceiveMessage(sender string, msg net.Message) {
	gmsg := msg.(GraniteMessage)
	if gmsg.Instance == p.nextInstance && p.instance != nil {
		p.instance.receive(sender, gmsg)
	} else if gmsg.Instance > p.nextInstance {
		// Queue messages for later instances
		p.mpool = append(p.mpool, gmsg)
	}
}

func (p *Participant) ReceiveAlarm() {
	if !p.decided() {
		p.instance.decide(p.nextChain)
	}
	// Decided, terminate instance.
	p.instance = nil
}

const SHARE = "SHARE"
const DECIDE = "DECIDE"

type GraniteMessage struct {
	Instance int
	Step     string
	Value    net.ECChain
}

// A single Granite consensus instance.
type instance struct {
	ntwk          net.NetworkSink
	participantID string
	instanceID    int
	// This instance's preferred value to finalise.
	input net.ECChain
	// The value this instance has decided on (zero until a decision).
	decision net.ECChain
}

func newInstance(ntwk net.NetworkSink, participantID string, instanceID int, input net.ECChain) *instance {
	return &instance{ntwk: ntwk, instanceID: instanceID, participantID: participantID, input: input}
}

func (i *instance) start() {
	i.broadcast(SHARE, i.input)
}

func (i *instance) receive(sender string, msg GraniteMessage) {
	// Placeholder algorithm: require all peers to agree, else decide current input.
	switch msg.Step {
	case SHARE:
		if !msg.Value.Eq(&i.input) {
			i.decide(net.ECChain{
				Base:      i.input.Base,
				BaseEpoch: i.input.BaseEpoch,
				Tail:      []net.TipSet{},
			})
		}
	case DECIDE:
		// Ignore
	}
}

func (i *instance) decide(value net.ECChain) {
	i.decision = value
	fmt.Printf("granite: %s decided %v\n", i.participantID, i.decision)
	i.broadcast(DECIDE, i.decision)
}

func (p *Participant) decided() bool {
	return !p.instance.decision.Eq(&net.ECChain{})
}

func (i *instance) broadcast(step string, msg net.ECChain) {
	i.ntwk.Broadcast(i.participantID, GraniteMessage{i.instanceID, step, msg})
}
