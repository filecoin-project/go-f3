package gpbft

import (
	"fmt"
	"golang.org/x/xerrors"
)

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	id     ActorID
	config GraniteConfig
	host   Host
	vrf    VRFer

	// Chain to use as input for the next Granite instance.
	nextChain ECChain
	// Instance identifier for the next Granite instance.
	nextInstance uint64
	// Current Granite instance.
	granite *instance
	// The output from the last terminated Granite instance.
	finalised TipSet
	// The round number at which the last instance was terminated.
	finalisedRound uint64
}

func NewParticipant(id ActorID, config GraniteConfig, host Host, vrf VRFer) *Participant {
	return &Participant{id: id, config: config, host: host, vrf: vrf}
}

func (p *Participant) ID() ActorID {
	return p.id
}

func (p *Participant) CurrentRound() uint64 {
	if p.granite == nil {
		return 0
	}
	return p.granite.round
}
func (p *Participant) Finalised() (TipSet, uint64) {
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

// Receives a new EC chain, and notifies the current instance.
// This may modify the set of valid values for the current instance.
func (p *Participant) ReceiveECChain(chain ECChain) error {
	if p.granite != nil {
		p.granite.ReceiveAcceptable(chain)
	}
	return nil
}

// Validates a message received from another participant, if possible.
// An invalid message can never become valid, so may be dropped.
// A message can only be validated if it is for the currently-executing protocol instance.
// Returns whether the message could be validated, and an error if it was invalid.
func (p *Participant) ValidateMessage(msg *GMessage) (bool, error) {
	if p.granite != nil && msg.Vote.Instance == p.granite.instanceID {
		return true, p.granite.Validate(msg)
	}
	return false, nil
}

// Receives a Granite message from some other participant.
// The message is delivered to the Granite instance if it is for the current instance,
// else it is dropped.
// This method *does not check message validity*.
// The message must have been previously validated with ValidateMessage indicating success.
// Since messages for future instances cannot be validated, a valid message
// can only be for the current or some previous instance (hence dropping if not current).
// Returns whether the message was accepted for the instance, and an error if it could not be
// processed.
func (p *Participant) ReceiveMessage(msg *GMessage) (bool, error) {
	if p.granite != nil && msg.Vote.Instance == p.granite.instanceID {
		if err := p.granite.Receive(msg); err != nil {
			return true, fmt.Errorf("receiving message: %w", err)
		}
		p.handleDecision()
		return true, nil
	} else if msg.Vote.Instance >= p.nextInstance {
		// Queue messages for later instances
		return false, xerrors.Errorf("message for future instance cannot be valid")
	}
	// Message dropped.
	return false, nil
}

func (p *Participant) ReceiveAlarm() error {
	// TODO include instance ID in alarm message, and filter here.
	if p.granite != nil {
		if err := p.granite.ReceiveAlarm(); err != nil {
			return fmt.Errorf("failed receiving alarm: %w", err)
		}
		p.handleDecision()
	}
	return nil
}

func (p *Participant) handleDecision() {
	if p.terminated() {
		p.finalised = p.granite.value.Head()
		p.finalisedRound = p.granite.round
		p.granite = nil
	}
}

func (p *Participant) terminated() bool {
	return p.granite != nil && p.granite.phase == TERMINATED_PHASE
}

func (p *Participant) Describe() string {
	if p.granite == nil {
		return "nil"
	}
	return p.granite.Describe()
}
