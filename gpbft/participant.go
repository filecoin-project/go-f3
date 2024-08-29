package gpbft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
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

	// validationCache is a bounded cache of messages that have already been
	// validated by the participant, grouped by instance.
	validationCache *caching.GroupedSet
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
		options:         opts,
		host:            host,
		committees:      make(map[uint64]*committee),
		mqueue:          newMessageQueue(opts.maxLookaheadRounds),
		validationCache: caching.NewGroupedSet(opts.maxCachedInstances, opts.maxCachedMessagesPerInstance),
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

// ValidateMessage checks if the given message is valid. If invalid, an error is
// returned. ErrValidationInvalid indicates that the message will never be valid
// invalid and may be safely dropped.
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

	// TODO: Refactor validation into its own struct such that it encapsulates
	//       caching, metrics etc.
	//       See: https://github.com/filecoin-project/go-f3/issues/561

	var buf bytes.Buffer
	var cacheMessage bool
	if err := msg.MarshalCBOR(&buf); err != nil {
		log.Errorw("failed to marshal message for caching", "err", err)
	} else if alreadyValidated, err := p.validationCache.Contains(msg.Vote.Instance, messageCacheNamespace, buf.Bytes()); err != nil {
		log.Errorw("failed to check already validated messages", "err", err)
	} else if alreadyValidated {
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheHit, attrCacheKindMessage))
		return &validatedMessage{msg: msg}, nil
	} else {
		cacheMessage = true
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheMiss, attrCacheKindMessage))
	}

	// Check sender is eligible.
	senderPower, senderPubKey := comt.power.Get(msg.Sender)
	if senderPower == 0 {
		return nil, fmt.Errorf("sender %d with zero power or not in power table: %w", msg.Sender, ErrValidationInvalid)
	}

	// Check that message value is a valid chain.
	if err := msg.Vote.Value.Validate(); err != nil {
		return nil, fmt.Errorf("invalid message vote value chain: %w: %w", err, ErrValidationInvalid)
	}

	// Check phase-specific constraints.
	switch msg.Vote.Step {
	case QUALITY_PHASE:
		if msg.Vote.Round != 0 {
			return nil, fmt.Errorf("unexpected round %d for quality phase: %w", msg.Vote.Round, ErrValidationInvalid)
		}
		if msg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for quality phase: %w", ErrValidationInvalid)
		}
	case CONVERGE_PHASE:
		if msg.Vote.Round == 0 {
			return nil, fmt.Errorf("unexpected round 0 for converge phase: %w", ErrValidationInvalid)
		}
		if msg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for converge phase: %w", ErrValidationInvalid)
		}
		if !VerifyTicket(p.host.NetworkName(), comt.beacon, msg.Vote.Instance, msg.Vote.Round, senderPubKey, p.host, msg.Ticket) {
			return nil, fmt.Errorf("failed to verify ticket from %v: %w", msg.Sender, ErrValidationInvalid)
		}
	case DECIDE_PHASE:
		if msg.Vote.Round != 0 {
			return nil, fmt.Errorf("unexpected non-zero round %d for decide phase: %w", msg.Vote.Round, ErrValidationInvalid)
		}
		if msg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for decide phase: %w", ErrValidationInvalid)
		}
	case PREPARE_PHASE, COMMIT_PHASE:
		// No additional checks for PREPARE and COMMIT.
	default:
		return nil, fmt.Errorf("invalid vote step: %d: %w", msg.Vote.Step, ErrValidationInvalid)
	}

	// Check vote signature.
	sigPayload := p.host.MarshalPayloadForSigning(p.host.NetworkName(), &msg.Vote)
	if err := p.host.Verify(senderPubKey, sigPayload, msg.Signature); err != nil {
		return nil, fmt.Errorf("invalid signature on %v, %v: %w", msg, err, ErrValidationInvalid)
	}

	// Check justification
	needsJustification := !(msg.Vote.Step == QUALITY_PHASE ||
		(msg.Vote.Step == PREPARE_PHASE && msg.Vote.Round == 0) ||
		(msg.Vote.Step == COMMIT_PHASE && msg.Vote.Value.IsZero()))

	if needsJustification {
		if err := p.validateJustification(msg, comt); err != nil {
			return nil, fmt.Errorf("%v: %w", err, ErrValidationInvalid)
		}
	} else if msg.Justification != nil {
		return nil, fmt.Errorf("message %v has unexpected justification: %w", msg, ErrValidationInvalid)
	}

	if cacheMessage {
		if _, err := p.validationCache.Add(msg.Vote.Instance, messageCacheNamespace, buf.Bytes()); err != nil {
			log.Warnw("failed to cache to already validated message", "err", err)
		}
	}
	return &validatedMessage{msg: msg}, nil
}

func (p *Participant) validateJustification(msg *GMessage, comt *committee) error {

	if msg.Justification == nil {
		return fmt.Errorf("message for phase %v round %v has no justification", msg.Vote.Step, msg.Vote.Round)
	}

	// Only cache the justification if:
	// * marshalling it was successful
	// * it is not already present in the cache.
	var cacheJustification bool
	var buf bytes.Buffer
	if err := msg.Justification.MarshalCBOR(&buf); err != nil {
		log.Errorw("failed to marshal justification for caching", "err", err)
	} else if alreadyValidated, err := p.validationCache.Contains(msg.Vote.Instance, justificationCacheNamespace, buf.Bytes()); err != nil {
		log.Warnw("failed to check if justification is already cached", "err", err)
	} else if alreadyValidated {
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheHit, attrCacheKindJustification))
		return nil
	} else {
		cacheJustification = true
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheMiss, attrCacheKindJustification))
	}

	// Check that the justification is for the same instance.
	if msg.Vote.Instance != msg.Justification.Vote.Instance {
		return fmt.Errorf("message with instanceID %v has evidence from instanceID: %v", msg.Vote.Instance, msg.Justification.Vote.Instance)
	}
	if !msg.Vote.SupplementalData.Eq(&msg.Justification.Vote.SupplementalData) {
		return fmt.Errorf("message and justification have inconsistent supplemental data: %v != %v", msg.Vote.SupplementalData, msg.Justification.Vote.SupplementalData)
	}
	// Check that justification vote value is a valid chain.
	if err := msg.Justification.Vote.Value.Validate(); err != nil {
		return fmt.Errorf("invalid justification vote value chain: %w", err)
	}

	// Check every remaining field of the justification, according to the phase requirements.
	// This map goes from the message phase to the expected justification phase(s),
	// to the required vote values for justification by that phase.
	// Anything else is disallowed.
	expectations := map[Phase]map[Phase]struct {
		Round uint64
		Value ECChain
	}{
		// CONVERGE is justified by a strong quorum of COMMIT for bottom,
		// or a strong quorum of PREPARE for the same value, from the previous round.
		CONVERGE_PHASE: {
			COMMIT_PHASE:  {msg.Vote.Round - 1, ECChain{}},
			PREPARE_PHASE: {msg.Vote.Round - 1, msg.Vote.Value},
		},
		// PREPARE is justified by the same rules as CONVERGE (in rounds > 0).
		PREPARE_PHASE: {
			COMMIT_PHASE:  {msg.Vote.Round - 1, ECChain{}},
			PREPARE_PHASE: {msg.Vote.Round - 1, msg.Vote.Value},
		},
		// COMMIT is justified by strong quorum of PREPARE from the same round with the same value.
		COMMIT_PHASE: {
			PREPARE_PHASE: {msg.Vote.Round, msg.Vote.Value},
		},
		// DECIDE is justified by strong quorum of COMMIT with the same value.
		// The DECIDE message doesn't specify a round.
		DECIDE_PHASE: {
			COMMIT_PHASE: {math.MaxUint64, msg.Vote.Value},
		},
	}

	if expectedPhases, ok := expectations[msg.Vote.Step]; ok {
		if expected, ok := expectedPhases[msg.Justification.Vote.Step]; ok {
			if msg.Justification.Vote.Round != expected.Round && expected.Round != math.MaxUint64 {
				return fmt.Errorf("message %v has justification from wrong round %d", msg, msg.Justification.Vote.Round)
			}
			if !msg.Justification.Vote.Value.Eq(expected.Value) {
				return fmt.Errorf("message %v has justification for a different value: %v", msg, msg.Justification.Vote.Value)
			}
		} else {
			return fmt.Errorf("message %v has justification with unexpected phase: %v", msg, msg.Justification.Vote.Step)
		}
	} else {
		return fmt.Errorf("message %v has unexpected phase for justification", msg)
	}

	// Check justification power and signature.
	var justificationPower int64
	signers := make([]int, 0)
	if err := msg.Justification.Signers.ForEach(func(bit uint64) error {
		if int(bit) >= len(comt.power.Entries) {
			return fmt.Errorf("invalid signer index: %d", bit)
		}
		power := comt.power.ScaledPower[bit]
		if power == 0 {
			return fmt.Errorf("signer with ID %d has no power", comt.power.Entries[bit].ID)
		}
		justificationPower += power
		signers = append(signers, int(bit))
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over signers: %w", err)
	}

	if !IsStrongQuorum(justificationPower, comt.power.ScaledTotal) {
		return fmt.Errorf("message %v has justification with insufficient power: %v", msg, justificationPower)
	}

	payload := p.host.MarshalPayloadForSigning(p.host.NetworkName(), &msg.Justification.Vote)
	if err := comt.aggregateVerifier.VerifyAggregate(signers, payload, msg.Justification.Signature); err != nil {
		return fmt.Errorf("verification of the aggregate failed: %+v: %w", msg.Justification, err)
	}

	if cacheJustification {
		if _, err := p.validationCache.Add(msg.Vote.Instance, justificationCacheNamespace, buf.Bytes()); err != nil {
			log.Warnw("failed to cache to already validated justification", "err", err)
		}
	}
	return nil
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
	if p.gpbft, err = newInstance(p, p.currentInstance, chain, data, *comt.power, comt.aggregateVerifier, comt.beacon); err != nil {
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

		// TODO: filter out participants with no effective power after rounding?
		// TODO: this is slow and under a lock, but we only want to do it once per
		// instance... ideally we'd have a per-instance lock/once, but that probably isn't
		// worth it.
		agg, err := p.host.Aggregate(power.Entries.PublicKeys())
		if err != nil {
			return nil, fmt.Errorf("failed to pre-compute aggregate mask for instance %d: %w: %w", instance, ErrValidationNoCommittee, err)
		}
		comt = &committee{power: power, beacon: beacon, aggregateVerifier: agg}
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
		p.validationCache.RemoveGroup(p.gpbft.instanceID)
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
	power             *PowerTable
	beacon            []byte
	aggregateVerifier Aggregate
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
