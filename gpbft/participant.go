package gpbft

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/internal/caching"
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel/metric"
)

var (
	log = logging.Logger("f3/gpbft")

	messageCacheNamespace       = []byte("message")
	justificationCacheNamespace = []byte("justification")
)

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	*options
	host Host

	// apiMutex prevents concurrent access to stateful API methods to ensure thread
	// safety. This mutex should be locked at the beginning of each public API method
	// that modifies state.
	apiMutex sync.Mutex
	// Cache of committees for the current or future instances.
	committeeProvider *cachedCommitteeProvider

	// Current Granite instance.
	gpbft *instance
	// Messages queued for future instances.
	mqueue *messageQueue
	// progression is the atomic reference to the current GPBFT instance being
	// progressed by this Participant, or the next instance to be started if no such
	// instance exists.
	progression *atomicProgression
	// messageCache maintains a cache of unique identifiers for valid messages or
	// justifications that this Participant has received since the previous instance.
	// It ensures that only relevant messages or justifications are retained by
	// automatically pruning entries from older instances as the participant
	// progresses to the next instance.
	//
	// See Participant.finishCurrentInstance, Participant.validator.
	messageCache *caching.GroupedSet
	validator    *cachingValidator
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
	ccp := newCachedCommitteeProvider(host)
	messageCache := caching.NewGroupedSet(opts.maxCachedInstances, opts.maxCachedMessagesPerInstance)
	progression := newAtomicProgression()
	return &Participant{
		options:           opts,
		host:              host,
		committeeProvider: ccp,
		mqueue:            newMessageQueue(opts.maxLookaheadRounds),
		messageCache:      messageCache,
		progression:       progression,
		validator:         newValidator(host, ccp, progression.Get, messageCache, opts.committeeLookback),
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

// Progress returns the latest progress of this Participant in terms of GPBFT
// instance ID, round and phase.
//
// This API is safe for concurrent use.
func (p *Participant) Progress() InstanceProgress {
	return p.progression.Get()
}

// ValidateMessage checks if the given message is valid. If invalid, an error is
// returned. ErrValidationInvalid indicates that the message will never be valid
// invalid and may be safely dropped.
func (p *Participant) ValidateMessage(ctx context.Context, msg *GMessage) (valid ValidatedMessage, err error) {
	// This method is not protected by the API mutex, it is intended for concurrent use.
	// The instance mutex is taken when appropriate by inner methods.
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
		if err != nil {
			metrics.errorCounter.Add(ctx, 1, metric.WithAttributes(metricAttributeFromError(err)))
		}
	}()
	return p.validator.ValidateMessage(ctx, msg)
}

// Receives a validated Granite message from some other participant.
func (p *Participant) ReceiveMessage(ctx context.Context, vmsg ValidatedMessage) (err error) {
	if !p.apiMutex.TryLock() {
		panic("concurrent API method invocation")
	}
	defer p.apiMutex.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
		if err != nil {
			metrics.errorCounter.Add(ctx, 1, metric.WithAttributes(metricAttributeFromError(err)))
		}
	}()
	msg := vmsg.Message()

	currentInstance := p.Progress().ID
	// Drop messages for past instances.
	if msg.Vote.Instance < currentInstance {
		p.trace("dropping message from old instance %d while received in instance %d",
			msg.Vote.Instance, currentInstance)
		return nil
	}

	// If the message is for the current instance, deliver immediately.
	if p.gpbft != nil && msg.Vote.Instance == currentInstance {
		if err := p.gpbft.Receive(msg); err != nil {
			return fmt.Errorf("%w: %w", ErrReceivedInternalError, err)
		}
		p.handleDecision(ctx)
	} else {
		// Otherwise queue it for a future instance.
		p.mqueue.Add(msg)
	}
	return nil
}

func (p *Participant) ReceiveAlarm(ctx context.Context) (err error) {
	if !p.apiMutex.TryLock() {
		panic("concurrent API method invocation")
	}
	defer p.apiMutex.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
		if err != nil {
			metrics.errorCounter.Add(ctx, 1, metric.WithAttributes(metricAttributeFromError(err)))
		}
	}()

	if p.gpbft == nil {
		// The alarm is for fetching the next chain and beginning a new instance.
		return p.beginInstance(ctx)
	}
	if err := p.gpbft.ReceiveAlarm(); err != nil {
		return fmt.Errorf("failed receiving alarm: %w", err)
	}
	p.handleDecision(ctx)
	return nil
}

func (p *Participant) beginInstance(ctx context.Context) error {
	currentInstance := p.Progress().ID
	data, chain, err := p.host.GetProposal(ctx, currentInstance)
	if err != nil {
		return fmt.Errorf("failed fetching chain for instance %d: %w", currentInstance, err)
	}
	// Limit the length of the chain to be proposed.
	if chain.IsZero() {
		return errors.New("canonical chain cannot be zero-valued")
	}
	chain = chain.Prefix(ChainMaxLen - 1)
	if err := chain.Validate(); err != nil {
		return fmt.Errorf("invalid canonical chain: %w", err)
	}

	comt, err := p.committeeProvider.GetCommittee(ctx, currentInstance)
	if err != nil {
		return err
	}
	if p.gpbft, err = newInstance(p, currentInstance, chain, data, comt.PowerTable, comt.AggregateVerifier, comt.Beacon); err != nil {
		return fmt.Errorf("failed creating new gpbft instance: %w", err)
	}
	if err := p.gpbft.Start(); err != nil {
		return fmt.Errorf("failed starting gpbft instance: %w", err)
	}
	// Deliver any queued messages for the new instance.
	queued := p.mqueue.Drain(p.gpbft.current.ID)
	if p.tracingEnabled() {
		for _, msg := range queued {
			p.trace("Delivering queued {%d} ← P%d: %v", p.gpbft.current.ID, msg.Sender, msg)
		}
	}
	if err := p.gpbft.ReceiveMany(queued); err != nil {
		return fmt.Errorf("delivering queued messages: %w", err)
	}
	p.handleDecision(ctx)
	return nil
}

func (p *Participant) handleDecision(ctx context.Context) {
	if !p.terminated() {
		return
	}
	decision := p.finishCurrentInstance()
	nextStart, err := p.host.ReceiveDecision(ctx, decision)
	if err != nil {
		p.trace("failed to receive decision: %+v", err)
		p.host.SetAlarm(time.Time{})
	} else {
		p.beginNextInstance(p.Progress().ID + 1)
		p.host.SetAlarm(nextStart)
	}
}

func (p *Participant) finishCurrentInstance() *Justification {
	var decision *Justification
	if p.gpbft != nil {
		decision = p.gpbft.terminationValue
	}
	p.gpbft = nil
	if currentInstance := p.Progress().ID; currentInstance > 1 {
		// Remove all cached messages that are older than the previous instance
		p.messageCache.RemoveGroupsLessThan(currentInstance - 1)
	}
	return decision
}

func (p *Participant) beginNextInstance(nextInstance uint64) {
	// Clean all messages queued and for instances below the next one.
	for inst := range p.mqueue.messages {
		if inst < nextInstance {
			delete(p.mqueue.messages, inst)
		}
	}
	// Clean committees from instances below the previous one. We keep the last committee so we
	// can continue to validate and propagate DECIDE messages.
	if nextInstance > 0 {
		p.committeeProvider.EvictCommitteesBefore(nextInstance - 1)
	}
	p.progression.NotifyProgress(InstanceProgress{
		Instant: Instant{ID: nextInstance, Round: 0, Phase: INITIAL_PHASE},
	})
}

func (p *Participant) terminated() bool {
	return p.gpbft != nil && p.gpbft.current.Phase == TERMINATED_PHASE
}

func (p *Participant) Describe() string {
	if p.gpbft == nil {
		return "nil"
	}
	return p.gpbft.Describe()
}

func (p *Participant) tracingEnabled() bool {
	return p.tracer != nil
}

func (p *Participant) trace(format string, args ...any) {
	if p.tracingEnabled() {
		p.tracer.Log(format, args...)
	}
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
	// Drop equivocations and duplicates (messages with the same sender, round and phase).
	for _, m := range instanceQueue[msg.Sender] {
		if m.Vote.Round == msg.Vote.Round && m.Vote.Phase == msg.Vote.Phase {
			return
		}
	}
	// Queue remaining good messages.
	instanceQueue[msg.Sender] = append(instanceQueue[msg.Sender], msg)
}

// Removes and returns all messages for an instance.
// The returned messages are ordered by round and phase.
func (q *messageQueue) Drain(instance uint64) []*GMessage {
	var msgs []*GMessage
	for _, ms := range q.messages[instance] {
		msgs = append(msgs, ms...)
	}
	sort.SliceStable(msgs, func(i, j int) bool {
		if msgs[i].Vote.Round != msgs[j].Vote.Round {
			return msgs[i].Vote.Round < msgs[j].Vote.Round
		}
		return msgs[i].Vote.Phase < msgs[j].Vote.Phase
	})
	delete(q.messages, instance)
	return msgs
}
