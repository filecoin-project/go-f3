package gpbft

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	*options
	host Host

	// Mutex protecting currentInstance and committees cache for concurrent validation.
	// Note that not every access need be protected:
	// - writes to currentInstance, and reads from it during validation,
	// - reads from or writes to committees (which is written during validation).
	mutex sync.Mutex
	// Instance identifier for the current (or, if none, next to start) GPBFT instance.
	currentInstance uint64
	// Cache of committees for the current or future instances.
	committees map[uint64]*committee

	// Current Granite instance.
	gpbft *instance
	// Messages queued for future instances.
	mqueue *messageQueue
	// The output from the last terminated Granite instance.
	finalised *Justification
	// The round number during which the last instance was terminated.
	// This is for informational purposes only. It does not necessarily correspond to the
	// protocol round for which a strong quorum of COMMIT messages was observed,
	// which may not be known to the participant.
	terminatedDuringRound uint64
}

type validatedMessage struct {
	msg *GMessage
}

func (v *validatedMessage) Message() *GMessage {
	return v.msg
}

var _ Receiver = (*Participant)(nil)

type PanicError struct {
	Err any
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("participant panicked: %v", e.Err)
}

func NewParticipant(host Host, o ...Option) (*Participant, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &Participant{
		options:         opts,
		host:            host,
		committees:      make(map[uint64]*committee),
		mqueue:          newMessageQueue(opts.maxLookaheadRounds),
		currentInstance: opts.initialInstance,
	}, nil
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
	if p.gpbft == nil {
		return 0
	}
	return p.gpbft.round
}

// Validates a message
func (p *Participant) ValidateMessage(msg *GMessage) (valid ValidatedMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: r}
		}
	}()

	comt, err := p.fetchCommittee(msg.Vote.Instance)
	if err != nil {
		return nil, err
	}

	// Validate the message.
	if err = ValidateMessage(comt.power, comt.beacon, p.host, msg); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidationInvalid, err)
	}
	return &validatedMessage{msg: msg}, nil
}

// Receives a validated Granite message from some other participant.
func (p *Participant) ReceiveMessage(vmsg ValidatedMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: r}
		}
	}()
	msg := vmsg.Message()

	// Drop messages for past instances.
	if msg.Vote.Instance < p.currentInstance {
		return fmt.Errorf("message %d, current instance %d: %w",
			msg.Vote.Instance, p.currentInstance, ErrValidationTooOld)
	}

	// If the message is for the current instance, deliver immediately.
	if p.gpbft != nil && msg.Vote.Instance == p.currentInstance {
		if err := p.gpbft.Receive(msg); err != nil {
			return fmt.Errorf("%w: %w", ErrReceivedInternalError, err)
		}
		p.handleDecision()
	} else {
		// Otherwise queue it for a future instance.
		p.mqueue.Add(msg)
	}
	return nil
}

func (p *Participant) ReceiveAlarm() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Err: r}
		}
	}()

	if p.gpbft == nil {
		// The alarm is for fetching the next chain and beginning a new instance.
		return p.beginInstance()
	}
	if err := p.gpbft.ReceiveAlarm(); err != nil {
		return fmt.Errorf("failed receiving alarm: %w", err)
	}
	p.handleDecision()
	return nil
}

// FinalityInfo is an auxiliary struct to pass relevant info from the finality certificate
// NOTE: It is not clear to me what information is available in the finality certificate
// and in what form. The information provided here may change, using it as a placeholder for now.
type FinalityInfo struct {
	Instance uint64
	Power    *PowerTable
	Beacon   []byte
}

func (p *Participant) ReceiveFinalityCertificate(f FinalityInfo) error {
	// Get ready to start the instance defined in the finality certificate for the next instance
	p.gpbft = nil
	prevInstance := p.currentInstance
	// Set the instance from the certificate as the current one.
	p.currentInstance = f.Instance
	// clean all messages queued for instances below the one in the finality certificate
	// and old committees
	for i := prevInstance + 1; i < p.currentInstance; i++ {
		delete(p.mqueue.messages, i)
		delete(p.committees, i)
	}

	// store the new committees provided by the finality certificate
	// overwriting whatever was there already (if anything).
	if err := f.Power.Validate(); err != nil {
		return fmt.Errorf("invalid power table: %w", err)
	}
	com := &committee{power: f.Power, beacon: f.Beacon}
	p.committees[f.Instance] = com

	// Set the alarm to begin a new instance immediately.
	// This will fetch the chain, drain existing messages for that instance,
	// and start the instance.
	p.host.SetAlarm(p.host.Time())

	return nil
}

func (p *Participant) beginInstance() error {
	data, chain, err := p.host.GetProposalForInstance(p.currentInstance)
	if err != nil {
		return fmt.Errorf("failed fetching chain for instance %d: %w", p.currentInstance, err)
	}
	// Limit length of the chain to be proposed.
	if chain.IsZero() {
		return errors.New("canonical chain cannot be zero-valued")
	}
	chain = chain.Prefix(CHAIN_MAX_LEN - 1)
	if err := chain.Validate(); err != nil {
		return fmt.Errorf("invalid canonical chain: %w", err)
	}

	comt, err := p.fetchCommittee(p.currentInstance)
	if err != nil {
		return err
	}
	if p.gpbft, err = newInstance(p, p.currentInstance, chain, data, *comt.power, comt.beacon); err != nil {
		return fmt.Errorf("failed creating new gpbft instance: %w", err)
	}
	if err := p.gpbft.Start(); err != nil {
		return fmt.Errorf("failed starting gpbft instance: %w", err)
	}
	// Deliver any queued messages for the new instance.
	for _, msg := range p.mqueue.Drain(p.gpbft.instanceID) {
		if p.terminated() {
			break
		}
		if p.tracer != nil {
			p.tracer.Log("Delivering queued {%d} ← P%d: %v", p.gpbft.instanceID, msg.Sender, msg)
		}
		if err := p.gpbft.Receive(msg); err != nil {
			if errors.Is(err, ErrValidationWrongBase) {
				// Silently ignore this late-binding validation error
			} else {
				return fmt.Errorf("delivering queued message: %w", err)
			}
		}
	}
	p.handleDecision()
	return nil
}

// Fetches the committee against which to validate messages for some instance.
func (p *Participant) fetchCommittee(instance uint64) (*committee, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Reject messages for past instances.
	if instance < p.currentInstance {
		return nil, fmt.Errorf("instance %d, current %d: %w",
			instance, p.currentInstance, ErrValidationTooOld)
	}

	comt, ok := p.committees[instance]
	if !ok {
		power, beacon, err := p.host.GetCommitteeForInstance(instance)
		if err != nil {
			return nil, fmt.Errorf("instance %d: %w: %w", instance, ErrValidationNoCommittee, err)
		}
		if err := power.Validate(); err != nil {
			return nil, fmt.Errorf("instance %d: %w: invalid power: %w", instance, ErrValidationNoCommittee, err)
		}
		comt = &committee{power: power, beacon: beacon}
		p.committees[instance] = comt
	}
	return comt, nil
}

func (p *Participant) handleDecision() {
	if p.terminated() {
		p.finishCurrentInstance()
		nextStart := p.host.ReceiveDecision(p.finalised)
		// Set an alarm at which to fetch the next chain and begin a new instance.
		p.host.SetAlarm(nextStart)
	}
}

func (p *Participant) finishCurrentInstance() {
	p.finalised = p.gpbft.terminationValue
	p.terminatedDuringRound = p.gpbft.round
	p.gpbft = nil

	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.committees, p.currentInstance)
	p.currentInstance++
}

func (p *Participant) terminated() bool {
	return p.gpbft != nil && p.gpbft.phase == TERMINATED_PHASE
}

func (p *Participant) Describe() string {
	if p.gpbft == nil {
		return "nil"
	}
	return p.gpbft.Describe()
}

// A power table and beacon value used as the committee inputs to an instance.
type committee struct {
	power  *PowerTable
	beacon []byte
}

// A collection of messages queued for delivery for a future instance.
// The queue drops equivocations and unjustified messages beyond some round number.
type messageQueue struct {
	maxRound uint64
	// Maps instance -> sender -> messages.
	// Note the relative order of messages is lost.
	messages map[uint64]map[ActorID][]*GMessage
}

func newMessageQueue(maxRound uint64) *messageQueue {
	return &messageQueue{
		maxRound: maxRound,
		messages: make(map[uint64]map[ActorID][]*GMessage),
	}
}

func (q *messageQueue) Add(msg *GMessage) {
	instanceQueue, ok := q.messages[msg.Vote.Instance]
	if !ok {
		// There's no check on instance number being within a reasonable range here.
		// It's assumed that spam messages for far future instances won't get this far.
		instanceQueue = make(map[ActorID][]*GMessage)
		q.messages[msg.Vote.Instance] = instanceQueue
	}
	// Drop unjustified messages beyond some round limit.
	if msg.Vote.Round > q.maxRound && isSpammable(msg) {
		return
	}
	// Drop equivocations and duplicates (messages with the same sender, round and step).
	for _, m := range instanceQueue[msg.Sender] {
		if m.Vote.Round == msg.Vote.Round && m.Vote.Step == msg.Vote.Step {
			return
		}
	}
	// Queue remaining good messages.
	instanceQueue[msg.Sender] = append(instanceQueue[msg.Sender], msg)
}

func (q *messageQueue) Drain(instance uint64) []*GMessage {
	var msgs []*GMessage
	for _, ms := range q.messages[instance] {
		msgs = append(msgs, ms...)
	}
	// Sort by round and then step so messages will be processed in a useful order.
	sort.SliceStable(msgs, func(i, j int) bool {
		if msgs[i].Vote.Round != msgs[j].Vote.Round {
			return msgs[i].Vote.Round < msgs[j].Vote.Round
		}
		return msgs[i].Vote.Step < msgs[j].Vote.Step
	})
	delete(q.messages, instance)
	return msgs
}
