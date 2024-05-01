package gpbft

import (
	"errors"
	"fmt"
)

// ErrECChainNotAcceptable signals that ECChain is not acceptable by gpbft instance, due to mismatching prefix.
var ErrECChainNotAcceptable = errors.New("ec chain is not acceptable")

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	*options
	id   ActorID
	host Host

	// Instance identifier for the next Granite instance.
	nextInstance uint64
	// Current Granite instance.
	granite *instance
	// Messages queued for future instances.
	// Unbounded queues are a denial-of-service risk.
	// See https://github.com/filecoin-project/go-f3/issues/12
	mqueue map[uint64][]*GMessage
	// The output from the last terminated Granite instance.
	finalised *Justification
	// The round number during which the last instance was terminated.
	// This is for informational purposes only. It does not necessarily correspond to the
	// protocol round for which a strong quorum of COMMIT messages was observed,
	// which may not be known to the participant.
	terminatedDuringRound uint64
}

type PanicError struct {
	Err any
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("participant panicked: %v", e.Err)
}

func NewParticipant(id ActorID, host Host, o ...Option) (*Participant, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &Participant{
		options:      opts,
		id:           id,
		host:         host,
		mqueue:       make(map[uint64][]*GMessage),
		nextInstance: opts.initialInstance,
	}, nil
}

func (p *Participant) ID() ActorID {
	return p.id
}

// Fetches the preferred EC chain for the instance and begins the GPBFT protocol.
func (p *Participant) Start() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: r}
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
			err = &PanicError{Err: r}
		}
	}()
	if chain.IsZero() {
		err = errors.New("cannot receive zero chain")
	} else if err = chain.Validate(); err == nil && p.granite != nil {
		if p.granite.ReceiveAcceptable(chain) {
			err = ErrECChainNotAcceptable
		}
	}
	return
}

// Validates a message received from another participant, if possible.
// An invalid message can never become valid, so may be dropped.
// A message can only be validated if it is for the currently-executing protocol instance.
// Returns whether the message could be validated, and an error if it was invalid.
func (p *Participant) ValidateMessage(msg *GMessage) (checked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: r}
		}
	}()

	if p.granite != nil && msg.Vote.Instance == p.granite.instanceID {
		return true, p.granite.Validate(msg)
	}
	return false, nil
}

// Receives a Granite message from some other participant.
// If the message is for the current instance, it is delivered immediately.
// If it is for a future instance, it is locally queued, to be delivered when that instance begins.
// If it is for a previous instance, it is dropped.
//
// This method checks message validity only if the validated parameter is false.
// Validity can only be checked when the messages instance begins.
// Returns whether the message was for the current instance, and an error if it was invalid or
// could not be processed.
func (p *Participant) ReceiveMessage(msg *GMessage, validated bool) (accepted bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: r}
		}
	}()

	if p.granite != nil && msg.Vote.Instance == p.granite.instanceID {
		// Validate message for the current instance if it wasn't already.
		if !validated {
			if err := p.granite.Validate(msg); err != nil {
				return true, err
			}
		}
		// Deliver message for the current instance.
		if err := p.granite.Receive(msg); err != nil {
			return true, fmt.Errorf("receiving message: %w", err)
		}
		return true, p.handleDecision()
	} else if msg.Vote.Instance >= p.nextInstance {
		// Locally queue message for a future instance.
		p.mqueue[msg.Vote.Instance] = append(p.mqueue[msg.Vote.Instance], msg)
	}
	// Drop message for a previous instance.
	return false, nil
}

func (p *Participant) ReceiveAlarm() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: r}
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
	if chain.IsZero() {
		return errors.New("canonical chain cannot be zero-valued")
	}
	if err := chain.Validate(); err != nil {
		return fmt.Errorf("invalid canonical chain: %w", err)
	}
	if err := power.Validate(); err != nil {
		return fmt.Errorf("invalid power table: %w", err)
	}
	var err error
	if p.granite, err = newInstance(p, p.nextInstance, chain, power, beacon); err != nil {
		return fmt.Errorf("failed creating new granite instance: %w", err)
	}
	p.nextInstance += 1
	if err := p.granite.Start(); err != nil {
		return fmt.Errorf("failed starting granite instance: %w", err)
	}
	// Deliver any queued messages for the new instance.
	for _, msg := range p.mqueue[p.granite.instanceID] {
		if p.terminated() {
			break
		}
		if p.tracer != nil {
			p.tracer.Log("Delivering queued P%d{%d} ← P%d: %v", p.id, p.granite.instanceID, msg.Sender, msg)
		}
		if err := p.granite.Receive(msg); err != nil {
			return fmt.Errorf("delivering queued message: %w", err)
		}
	}
	delete(p.mqueue, p.granite.instanceID)
	return p.handleDecision()
}

func (p *Participant) handleDecision() error {
	if p.terminated() {
		p.finalised = p.granite.terminationValue
		p.terminatedDuringRound = p.granite.round
		p.granite = nil
		nextStart := p.host.ReceiveDecision(p.finalised)

		// Set an alarm at which to fetch the next chain and begin a new instance.
		p.host.SetAlarm(nextStart)
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
