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

	// Instance identifier for the next Granite instance.
	nextInstance uint64
	// Current Granite instance.
	granite *instance
	// The output from the last terminated Granite instance.
	finalised *Justification
	// The round number during which the last instance was terminated.
	// This is for informational purposes only. It does not necessarily correspond to the
	// protocol round for which a strong quorum of COMMIT messages was observed,
	// which may not be known to the participant.
	terminatedDuringRound uint64
}

type PanicError struct {
	Err error
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic recovered: %v", e.Err)
}

func (e *PanicError) Unwrap() error {
	return e.Err
}

func NewParticipant(id ActorID, config GraniteConfig, host Host) *Participant {
	return &Participant{id: id, config: config, host: host}
}

func (p *Participant) ID() ActorID {
	return p.id
}

// Fetches the preferred EC chain for the instance and begins the GPBFT protocol.
func (p *Participant) Start() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: err}
		}
	}()

	return p.beginInstance()
}

func (p *Participant) CurrentRound() uint64 {
	if p.granite == nil {
		return 0
	}
	return p.granite.round
}

// Receives a new EC chain, and notifies the current instance.
// This may modify the set of valid values for the current instance.
func (p *Participant) ReceiveECChain(chain ECChain) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: err}
		}
	}()

	if p.granite != nil {
		p.granite.ReceiveAcceptable(chain)
	}
	return err
}

// Validates a message received from another participant, if possible.
// An invalid message can never become valid, so may be dropped.
// A message can only be validated if it is for the currently-executing protocol instance.
// Returns whether the message could be validated, and an error if it was invalid.
func (p *Participant) ValidateMessage(msg *GMessage) (checked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: err}
		}
	}()

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
func (p *Participant) ReceiveMessage(msg *GMessage) (accepted bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: err}
		}
	}()

	if p.granite != nil && msg.Vote.Instance == p.granite.instanceID {
		if err := p.granite.Receive(msg); err != nil {
			return true, fmt.Errorf("receiving message: %w", err)
		}
		return true, p.handleDecision()
	} else if msg.Vote.Instance >= p.nextInstance {
		// Queue messages for later instances
		return false, xerrors.Errorf("message for future instance cannot be valid")
	}
	// Message dropped.
	return false, nil
}

func (p *Participant) ReceiveAlarm() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: err}
		}
	}()

	if p.granite == nil {
		// The alarm is for fetching the next chain and beginning a new instance.
		return p.beginInstance()
	} else {
		if err := p.granite.ReceiveAlarm(); err != nil {
			return fmt.Errorf("failed receiving alarm: %w", err)
		}
		return p.handleDecision()
	}
}

func (p *Participant) beginInstance() error {
	chain, power, beacon := p.host.GetCanonicalChain()
	var err error
	if p.granite, err = newInstance(p.config, p.host, p.id, p.nextInstance, chain, power, beacon); err != nil {
		return fmt.Errorf("failed creating new granite instance: %w", err)
	}
	p.nextInstance += 1
	if err := p.granite.Start(); err != nil {
		return fmt.Errorf("failed starting granite instance: %w", err)
	}
	return p.handleDecision()
}

func (p *Participant) handleDecision() error {
	if p.terminated() {
		p.finalised = p.granite.terminationValue
		p.terminatedDuringRound = p.granite.round
		p.granite = nil
		p.host.ReceiveDecision(p.finalised)

		// Set an alarm at which to fetch the next chain and begin a new instance.
		// At the moment, this is set to "immediately".
		// TODO: set delay based on tipset timestamp when they are provided by the host.
		// https://github.com/filecoin-project/go-f3/issues/113
		p.host.SetAlarm(p.host.Time())
		return nil
	}
	return nil
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
