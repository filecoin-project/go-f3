package f3

import "fmt"

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	id     ActorID
	config GraniteConfig
	host   Host
	vrf    VRFer

	mpool []*GMessage
	// Chain to use as input for the next Granite instance.
	nextChain ECChain
	// Instance identifier for the next Granite instance.
	nextInstance uint32
	// Current Granite instance.
	granite *instance
	// The output from the last terminated Granite instance.
	finalised TipSet
	// The round number at which the last instance was terminated.
	finalisedRound uint32
}

func NewParticipant(id ActorID, config GraniteConfig, host Host, vrf VRFer) *Participant {
	return &Participant{id: id, config: config, host: host, vrf: vrf}
}

func (p *Participant) ID() ActorID {
	return p.id
}

func (p *Participant) CurrentRound() uint32 {
	if p.granite == nil {
		return 0
	}
	return p.granite.round
}
func (p *Participant) Finalised() (TipSet, uint32) {
	return p.finalised, p.finalisedRound
}

// Receives a new canonical EC chain for the instance.
// This becomes the instance's preferred value to finalise.
func (p *Participant) ReceiveCanonicalChain(chain ECChain, power PowerTable, beacon []byte) error {
	p.nextChain = chain
	if p.granite == nil {
		var err error
		p.granite, err = newInstance(p.config, p.host, p.vrf, p.id, p.nextInstance, chain, power, beacon)
		if err != nil {
			return fmt.Errorf("failed creating new granite instance: %w", err)
		}
		p.nextInstance += 1
		return p.granite.Start()
	}
	return nil
}

// Receives a new EC chain, and notifies the current instance if it extends its current acceptable chain.
// This modifies the set of valid values for the current instance.
func (p *Participant) ReceiveECChain(chain ECChain) error {
	if p.granite != nil && chain.HasPrefix(p.granite.acceptable) {
		p.granite.receiveAcceptable(chain)
	}
	return nil
}

// Receives a Granite message from some other participant.
// The message is delivered to the Granite instance if it is for the current instance.
func (p *Participant) ReceiveMessage(msg *GMessage) error {
	if p.granite != nil && msg.Instance == p.granite.instanceID {
		if err := p.granite.Receive(msg); err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}
		p.handleDecision()
	} else if msg.Instance >= p.nextInstance {
		// Queue messages for later instances
		p.mpool = append(p.mpool, msg)
	}

	return nil
}

func (p *Participant) ReceiveAlarm(payload string) error {
	// TODO include instance ID in alarm message, and filter here.
	if p.granite != nil {
		if err := p.granite.ReceiveAlarm(payload); err != nil {
			return fmt.Errorf("failed receiving alarm: %w", err)
		}
		p.handleDecision()
	}
	return nil
}

func (p *Participant) handleDecision() {
	if p.terminated() {
		p.finalised = *p.granite.value.Head()
		p.finalisedRound = p.granite.round
		p.granite = nil
	}
}

func (p *Participant) terminated() bool {
	return p.granite != nil && p.granite.phase == TERMINATED
}

func (p *Participant) Describe() string {
	if p.granite == nil {
		return "nil"
	}
	return p.granite.Describe()
}
