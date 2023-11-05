package granite

import (
	"fmt"
	"github.com/anorth/f3sim/net"
)

type Participant struct {
	id    string
	ntwk  net.NetworkSink
	mpool []GraniteMessage

	chain      net.ECChain
	instanceID int
	instance   *instance
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
	p.chain = chain
	if p.instance == nil {
		p.instanceID += 1
		p.instance = newInstance(p.ntwk, p.id, p.instanceID, p.chain)
		p.instance.start()
	}
}

// Receives a Granite message from some other participant.
func (p *Participant) ReceiveMessage(sender string, msg net.Message) {
	gmsg := msg.(GraniteMessage)
	if gmsg.Instance == p.instanceID && p.instance != nil {
		p.instance.receive(sender, gmsg)
		if p.decided() {
			// Decided, terminate instance.
			p.instance = nil
		}
	} else if gmsg.Instance > p.instanceID {
		// Queue messages for later instances
		p.mpool = append(p.mpool, gmsg)
	}
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
	// Placeholder algorithm: accept if any peer agrees, else decide nil.
	switch msg.Step {
	case SHARE:
		if msg.Value.Eq(&i.input) {
			i.decide(msg.Value)
		} else {
			i.decide(net.ECChain{})
		}
	case DECIDE:
		// no-op
	}
}

func (i *instance) decide(value net.ECChain) {
	i.decision = value
	fmt.Printf("Participant %s decided %v\n", i.participantID, i.decision)
	i.broadcast(DECIDE, i.decision)
}

func (p *Participant) decided() bool {
	return !p.instance.decision.Eq(&net.ECChain{})
}

func (i *instance) broadcast(step string, msg net.ECChain) {
	i.ntwk.Broadcast(i.participantID, GraniteMessage{i.instanceID, step, msg})
}
