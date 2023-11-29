package f3

import "github.com/anorth/f3sim/net"

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	id     net.ActorID
	config GraniteConfig
	ntwk   net.NetworkSink
	vrf    VRFer

	mpool []GMessage
	// Chain to use as input for the next Granite instance.
	nextChain net.ECChain
	// Instance identifier for the next Granite instance.
	nextInstance int
	// Current Granite instance.
	granite *instance
	// The output from the last decided Granite instance.
	finalised net.TipSet
	// The round number at which the last instance was decided.
	finalisedRound int
}

func NewParticipant(id net.ActorID, config GraniteConfig, ntwk net.NetworkSink, vrf VRFer) *Participant {
	return &Participant{id: id, config: config, ntwk: ntwk, vrf: vrf}
}

func (p *Participant) ID() net.ActorID {
	return p.id
}

func (p *Participant) CurrentRound() int {
	if p.granite == nil {
		return -1
	}
	return p.granite.round
}
func (p *Participant) Finalised() (net.TipSet, int) {
	return p.finalised, p.finalisedRound
}

// Receives a new canonical EC chain for the instance.
// This becomes the instance's preferred value to finalise.
func (p *Participant) ReceiveCanonicalChain(chain net.ECChain) {
	p.nextChain = chain
	if p.granite == nil {
		p.granite = newInstance(p.config, p.ntwk, p.vrf, p.id, p.nextInstance, chain)
		p.nextInstance += 1
		p.granite.Start()
	}
}

// Receives a Granite message from some other participant.
func (p *Participant) ReceiveMessage(_ net.ActorID, msg net.Message) {
	gmsg := msg.(GMessage)
	if p.granite != nil && gmsg.Instance == p.granite.instanceID {
		p.granite.Receive(gmsg)
		p.handleDecision()
	} else if gmsg.Instance >= p.nextInstance {
		// Queue messages for later instances
		p.mpool = append(p.mpool, gmsg)
	}
}

func (p *Participant) ReceiveAlarm(payload string) {
	// TODO include instance ID in alarm message, and filter here.
	if p.granite != nil {
		p.granite.receiveAlarm(payload)
		p.handleDecision()
	}
}

func (p *Participant) handleDecision() {
	if p.decided() {
		p.finalised = *p.granite.value.Head()
		p.finalisedRound = p.granite.round
		p.granite = nil
	}
}

func (p *Participant) decided() bool {
	return p.granite != nil && p.granite.phase == DECIDE
}
