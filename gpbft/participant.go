package gpbft

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	*options
	host Host

	// Mutex for detecting concurrent invocation of stateful API methods.
	// To be taken at the top of public API methods, and always taken before instanceMutex,
	// if both are to be taken.
	apiMutex sync.Mutex
	// Mutex protecting currentInstance and committees cache for concurrent validation.
	// Note that not every access need be protected:
	// - writes to currentInstance, and reads from it during validation,
	// - reads from or writes to committees (which is written during validation).
	instanceMutex sync.Mutex
	// Instance identifier for the current (or, if none, next to start) GPBFT instance.
	currentInstance uint64
	// Cache of committees for the current or future instances.
	committees map[uint64]*committee

	// Current Granite instance.
	gpbft *instance
	// Messages queued for future instances.
	mqueue *messageQueue
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

func newPanicError(panicCause any) *PanicError {
	return &PanicError{
		Err:        panicCause,
		stackTrace: string(debug.Stack()),
	}
}

type PanicError struct {
	Err        any
	stackTrace string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("participant panicked: %v\n%v", e.Err, e.stackTrace)
}

func NewParticipant(host Host, o ...Option) (*Participant, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &Participant{
		options:    opts,
		host:       host,
		committees: make(map[uint64]*committee),
		mqueue:     newMessageQueue(opts.maxLookaheadRounds),
	}, nil
}

func (p *Participant) StartInstanceAt(instance uint64, when time.Time) (err error) {
	if !p.apiMutex.TryLock() {
		panic("concurrent API method invocation")
	}
	defer p.apiMutex.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
		if err != nil {
			metrics.errorCounter.Add(context.TODO(), 1, metric.WithAttributes(metricAttributeFromError(err)))
		}
	}()

	// Finish current instance to clean old committees and old messages queued
	// and prepare to begin a new instance.
	_ = p.finishCurrentInstance()
	p.beginNextInstance(instance)

	// Set the alarm to begin a new instance at the specified time.
	p.host.SetAlarm(when)

	return err
}

func (p *Participant) CurrentRound() uint64 {
	if !p.apiMutex.TryLock() {
		panic("concurrent API method invocation")
	}
	defer p.apiMutex.Unlock()
	if p.gpbft == nil {
		return 0
	}
	return p.gpbft.round
}

func (p *Participant) CurrentInstance() uint64 {
	if !p.apiMutex.TryLock() {
		panic("concurrent API method invocation")
	}
	defer p.apiMutex.Unlock()
	p.instanceMutex.Lock()
	defer p.instanceMutex.Unlock()
	return p.currentInstance
}

// Validates a message
func (p *Participant) ValidateMessage(msg *GMessage) (valid ValidatedMessage, err error) {
	// This method is not protected by the API mutex, it is intended for concurrent use.
	// The instance mutex is taken when appropriate by inner methods.
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
		if err != nil {
			metrics.errorCounter.Add(context.TODO(), 1, metric.WithAttributes(metricAttributeFromError(err)))
		}
	}()

	comt, err := p.fetchCommittee(msg.Vote.Instance)
	if err != nil {
		return nil, err
	}

	// Validate the message.
	if err = ValidateMessage(comt.power, comt.beacon, p.host, msg); err != nil {
		return nil, fmt.Errorf("%v: %w", err, ErrValidationInvalid)
	}
	return &validatedMessage{msg: msg}, nil
}

// Receives a validated Granite message from some other participant.
func (p *Participant) ReceiveMessage(vmsg ValidatedMessage) (err error) {
	if !p.apiMutex.TryLock() {
		panic("concurrent API method invocation")
	}
	defer p.apiMutex.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
		if err != nil {
			metrics.errorCounter.Add(context.TODO(), 1, metric.WithAttributes(metricAttributeFromError(err)))
		}
	}()
	msg := vmsg.Message()

	// Drop messages for past instances.
	if msg.Vote.Instance < p.currentInstance {
		p.tracer.Log("dropping message from old instance %d while received in instance %d",
			msg.Vote.Instance, p.currentInstance)
		return nil
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
	if !p.apiMutex.TryLock() {
		panic("concurrent API method invocation")
	}
	defer p.apiMutex.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
		if err != nil {
			metrics.errorCounter.Add(context.TODO(), 1, metric.WithAttributes(metricAttributeFromError(err)))
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

func (p *Participant) beginInstance() error {
	data, chain, err := p.host.GetProposalForInstance(p.currentInstance)
	if err != nil {
		return fmt.Errorf("failed fetching chain for instance %d: %w", p.currentInstance, err)
	}
	// Limit length of the chain to be proposed.
	if chain.IsZero() {
		return errors.New("canonical chain cannot be zero-valued")
	}
	chain = chain.Prefix(ChainMaxLen - 1)
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
	queued := p.mqueue.Drain(p.gpbft.instanceID)
	if p.tracer != nil {
		for _, msg := range queued {
			p.tracer.Log("Delivering queued {%d} ← P%d: %v", p.gpbft.instanceID, msg.Sender, msg)
		}
	}
	if err := p.gpbft.ReceiveMany(queued); err != nil {
		return fmt.Errorf("delivering queued messages: %w", err)
	}
	p.handleDecision()
	return nil
}

// Fetches the committee against which to validate messages for some instance.
func (p *Participant) fetchCommittee(instance uint64) (*committee, error) {
	p.instanceMutex.Lock()
	defer p.instanceMutex.Unlock()

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
	if !p.terminated() {
		return
	}
	decision := p.finishCurrentInstance()
	nextStart, err := p.host.ReceiveDecision(decision)
	if err != nil {
		p.tracer.Log("failed to receive decision: %+v", err)
		p.host.SetAlarm(time.Time{})
	} else {
		p.beginNextInstance(p.currentInstance + 1)
		p.host.SetAlarm(nextStart)
	}
}

func (p *Participant) finishCurrentInstance() *Justification {
	var decision *Justification
	if p.gpbft != nil {
		decision = p.gpbft.terminationValue
		p.terminatedDuringRound = p.gpbft.round
	}
	p.gpbft = nil
	return decision
}

func (p *Participant) beginNextInstance(nextInstance uint64) {
	p.instanceMutex.Lock()
	defer p.instanceMutex.Unlock()
	// Clean all messages queued and old committees for instances below the next one.
	// Skip if there are none to avoid iterating from instance zero when starting up.
	if len(p.mqueue.messages) > 0 || len(p.committees) > 0 {
		for i := p.currentInstance; i < nextInstance; i++ {
			delete(p.mqueue.messages, i)
			delete(p.committees, i)
		}
	}
	p.currentInstance = nextInstance
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

// Removes and returns all messages for an instance.
// The returned messages are ordered by round and step.
func (q *messageQueue) Drain(instance uint64) []*GMessage {
	var msgs []*GMessage
	for _, ms := range q.messages[instance] {
		msgs = append(msgs, ms...)
	}
	sort.SliceStable(msgs, func(i, j int) bool {
		if msgs[i].Vote.Round != msgs[j].Vote.Round {
			return msgs[i].Vote.Round < msgs[j].Vote.Round
		}
		return msgs[i].Vote.Step < msgs[j].Vote.Step
	})
	delete(q.messages, instance)
	return msgs
}
