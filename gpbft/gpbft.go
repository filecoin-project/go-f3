package gpbft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/filecoin-project/go-bitfield"
	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Phase uint8

const (
	INITIAL_PHASE Phase = iota
	QUALITY_PHASE
	CONVERGE_PHASE
	PREPARE_PHASE
	COMMIT_PHASE
	DECIDE_PHASE
	TERMINATED_PHASE
)

func (p Phase) String() string {
	switch p {
	case INITIAL_PHASE:
		return "INITIAL"
	case QUALITY_PHASE:
		return "QUALITY"
	case CONVERGE_PHASE:
		return "CONVERGE"
	case PREPARE_PHASE:
		return "PREPARE"
	case COMMIT_PHASE:
		return "COMMIT"
	case DECIDE_PHASE:
		return "DECIDE"
	case TERMINATED_PHASE:
		return "TERMINATED"
	default:
		return "UNKNOWN"
	}
}

const DomainSeparationTag = "GPBFT"

// A message in the Granite protocol.
// The same message structure is used for all rounds and phases.
// Note that the message is self-attesting so no separate envelope or signature is needed.
// - The signature field fixes the included sender ID via the implied public key;
// - The signature payload includes all fields a sender can freely choose;
// - The ticket field is a signature of the same public key, so also self-attesting.
type GMessage struct {
	// ID of the sender/signer of this message (a miner actor ID).
	Sender ActorID
	// Vote is the payload that is signed by the signature
	Vote Payload
	// Signature by the sender's public key over Instance || Round || Phase || Value.
	Signature []byte `cborgen:"maxlen=96"`
	// VRF ticket for CONVERGE messages (otherwise empty byte array).
	Ticket Ticket `cborgen:"maxlen=96"`
	// Justification for this message (some messages must be justified by a strong quorum of messages from some previous phase).
	Justification *Justification
}

type Justification struct {
	// Vote is the payload that is signed by the signature
	Vote Payload
	// Indexes in the base power table of the signers (bitset)
	Signers bitfield.BitField
	// BLS aggregate signature of signers
	Signature []byte `cborgen:"maxlen=96"`
}

type SupplementalData struct {
	// Merkle-tree of instance-specific commitments. Currently empty but this will eventually
	// include things like snark-friendly power-table commitments.
	Commitments [32]byte `cborgen:"maxlen=32"`
	// The DagCBOR-blake2b256 CID of the power table used to validate the next instance, taking
	// lookback into account.
	PowerTable cid.Cid // []PowerEntry
}

func (d *SupplementalData) Eq(other *SupplementalData) bool {
	return d.Commitments == other.Commitments && d.PowerTable == other.PowerTable
}

// Custom JSON marshalling for SupplementalData to achieve a commitment field
// that is a base64-encoded string.

type supplementalDataSub SupplementalData
type supplementalDataJson struct {
	Commitments []byte
	*supplementalDataSub
}

func (sd SupplementalData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&supplementalDataJson{
		Commitments:         sd.Commitments[:],
		supplementalDataSub: (*supplementalDataSub)(&sd),
	})
}

func (sd *SupplementalData) UnmarshalJSON(b []byte) error {
	aux := &supplementalDataJson{supplementalDataSub: (*supplementalDataSub)(sd)}
	var err error
	if err = json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if len(aux.Commitments) != 32 {
		return errors.New("commitments must be 32 bytes")
	}
	copy(sd.Commitments[:], aux.Commitments)
	return nil
}

// Fields of the message that make up the signature payload.
type Payload struct {
	// GossiPBFT instance (epoch) number.
	Instance uint64
	// GossiPBFT round number.
	Round uint64
	// GossiPBFT phase name.
	Phase Phase
	// The common data.
	SupplementalData SupplementalData
	// The value agreed-upon in a single instance.
	Value *ECChain
}

func (p *Payload) Eq(other *Payload) bool {
	if p == other {
		return true
	}
	if other == nil {
		return false
	}
	return p.Instance == other.Instance &&
		p.Round == other.Round &&
		p.Phase == other.Phase &&
		p.SupplementalData.Eq(&other.SupplementalData) &&
		p.Value.Eq(other.Value)
}

func (p *Payload) MarshalForSigning(nn NetworkName) []byte {
	var buf bytes.Buffer
	buf.WriteString(DomainSeparationTag)
	buf.WriteString(":")
	buf.WriteString(string(nn))
	buf.WriteString(":")

	_ = binary.Write(&buf, binary.BigEndian, p.Phase)
	_ = binary.Write(&buf, binary.BigEndian, p.Round)
	_ = binary.Write(&buf, binary.BigEndian, p.Instance)
	_, _ = buf.Write(p.SupplementalData.Commitments[:])
	key := p.Value.Key()
	_, _ = buf.Write(key[:])
	_, _ = buf.Write(p.SupplementalData.PowerTable.Bytes())
	return buf.Bytes()
}

func (m GMessage) String() string {
	return fmt.Sprintf("%s{%d}(%d %s)", m.Vote.Phase, m.Vote.Instance, m.Vote.Round, m.Vote.Value)
}

// A single Granite consensus instance.
type instance struct {
	participant *Participant
	// The EC chain input to this instance.
	input *ECChain
	// The power table for the base chain, used for power in this instance.
	powerTable *PowerTable
	// The aggregate signature verifier/aggregator.
	aggregateVerifier Aggregate
	// The beacon value from the base chain, used for tickets in this instance.
	beacon []byte
	// current stores information about the current GPBFT instant in terms of
	// instance ID, round and phase.
	current InstanceProgress
	// Time at which the current phase can or must end.
	// For QUALITY, PREPARE, and COMMIT, this is the latest time (the phase can end sooner).
	// For CONVERGE, this is the exact time (the timeout solely defines the phase end).
	phaseTimeout time.Time
	// rebroadcastTimeout is the time at which the current phase should attempt to
	// rebroadcast messages in order to further its progress.
	//
	// See tryRebroadcast.
	rebroadcastTimeout time.Time
	// rebroadcastAttempts counts the number of times messages at a round have been
	// rebroadcasted in order to determine the backoff duration until next rebroadcast.
	//
	// See tryRebroadcast.
	rebroadcastAttempts int
	// Supplemental data that all participants must agree on ahead of time. Messages that
	// propose supplemental data that differs with our supplemental data will be discarded.
	supplementalData *SupplementalData
	// This instance's proposal for the current round. Never bottom.
	// This is set after the QUALITY phase, and changes only at the end of a full round.
	proposal *ECChain
	// The value to be transmitted at the next phase, which may be bottom.
	// This value may change away from the proposal between phases.
	value *ECChain
	// candidates contain a set of values that are acceptable candidates to this
	// instance. This includes the base chain, all prefixes of proposal that found a
	// strong quorum of support in the QUALITY phase or late arriving quality
	// messages, including any chains that could possibly have been decided by
	// another participant.
	candidates map[ECChainKey]struct{}
	// The final termination value of the instance, for communication to the participant.
	// This field is an alternative to plumbing an optional decision value out through
	// all the method calls, or holding a callback handle to receive it here.
	terminationValue *Justification
	// Quality phase state (only for round 0)
	quality *quorumState
	// State for each round of phases.
	// State from prior rounds must be maintained to provide justification for values in subsequent rounds.
	rounds map[uint64]*roundState
	// Decision state. Collects DECIDE messages until a decision can be made,
	// independently of protocol phases/rounds.
	decision *quorumState
	// tracer traces logic logs for debugging and simulation purposes.
	tracer Tracer
}

func newInstance(
	participant *Participant,
	instanceID uint64,
	input *ECChain,
	data *SupplementalData,
	powerTable *PowerTable,
	aggregateVerifier Aggregate,
	beacon []byte) (*instance, error) {
	if input.IsZero() {
		return nil, fmt.Errorf("input is empty")
	}

	metrics.phaseCounter.Add(context.TODO(), 1, metric.WithAttributes(attrInitialPhase))
	metrics.currentInstance.Record(context.TODO(), int64(instanceID))
	metrics.currentPhase.Record(context.TODO(), int64(INITIAL_PHASE))

	return &instance{
		participant:       participant,
		input:             input,
		powerTable:        powerTable,
		aggregateVerifier: aggregateVerifier,
		beacon:            beacon,
		current: InstanceProgress{
			Instant: Instant{
				ID:    instanceID,
				Round: 0,
				Phase: INITIAL_PHASE,
			},
			Input: input,
		},
		supplementalData: data,
		proposal:         input,
		value:            &ECChain{},
		candidates: map[ECChainKey]struct{}{
			input.BaseChain().Key(): {},
		},
		quality: newQuorumState(powerTable, attrQualityPhase),
		rounds: map[uint64]*roundState{
			0: newRoundState(0, powerTable),
		},
		decision: newQuorumState(powerTable, attrDecidePhase),
		tracer:   participant.tracer,
	}, nil
}

type roundState struct {
	converged *convergeState
	prepared  *quorumState
	committed *quorumState
}

func newRoundState(roundNumber uint64, powerTable *PowerTable) *roundState {
	roundAttr := attrKeyRound.Int(int(roundNumber))
	return &roundState{
		converged: newConvergeState(roundAttr),
		prepared:  newQuorumState(powerTable, attrPreparePhase, roundAttr),
		committed: newQuorumState(powerTable, attrCommitPhase, roundAttr),
	}
}

func (i *instance) Start() error {
	return i.beginQuality()
}

// Receives and processes a message.
// Returns an error indicating either message invalidation or a programming error.
func (i *instance) Receive(msg *GMessage) error {
	if i.terminated() {
		return ErrReceivedAfterTermination
	}
	stateChanged, err := i.receiveOne(msg)
	if err != nil {
		return err
	}
	if stateChanged {
		// Further process the message's round only if it may have had an effect.
		// This avoids loading state for dropped messages (including spam).
		i.postReceive(msg.Vote.Round)
	}
	return nil
}

// Receives and processes a batch of queued messages.
// Messages should be ordered by round for most effective processing.
func (i *instance) ReceiveMany(msgs []*GMessage) error {
	if i.terminated() {
		return ErrReceivedAfterTermination
	}

	// Received each message and remember which rounds were received.
	roundsReceived := map[uint64]struct{}{}
	for _, msg := range msgs {
		stateChanged, err := i.receiveOne(msg)
		if err != nil {
			if errors.As(err, &ValidationError{}) {
				// Drop late-binding validation errors.
				i.log("dropping invalid message: %s", err)
			} else {
				return err
			}
		}
		if stateChanged {
			roundsReceived[msg.Vote.Round] = struct{}{}
		}
	}
	// Build unique, ordered list of rounds received.
	rounds := make([]uint64, 0, len(roundsReceived))
	for r := range roundsReceived {
		rounds = append(rounds, r)
	}
	sort.Slice(rounds, func(i, j int) bool { return rounds[i] < rounds[j] })
	i.postReceive(rounds...)
	return nil
}

func (i *instance) ReceiveAlarm() error {
	if err := i.tryCurrentPhase(); err != nil {
		return fmt.Errorf("failed completing protocol phase: %w", err)
	}
	return nil
}

func (i *instance) Describe() string {
	return fmt.Sprintf("{%d}, round %d, phase %s", i.current.ID, i.current.Round, i.current.Phase)
}

// Processes a single message.
// Returns true if the message might have caused a change in state.
func (i *instance) receiveOne(msg *GMessage) (bool, error) {
	// Check the message is for this instance, to guard against programming error.
	if msg.Vote.Instance != i.current.ID {
		return false, fmt.Errorf("%w: message for instance %d, expected %d",
			ErrReceivedWrongInstance, msg.Vote.Instance, i.current.ID)
	}
	// Perform validation that could not be done until the instance started.
	// Check supplemental data matches this instance's expectation.
	if !msg.Vote.SupplementalData.Eq(i.supplementalData) {
		return false, fmt.Errorf("%w: message supplement %s, expected %s",
			ErrValidationWrongSupplement, msg.Vote.SupplementalData, i.supplementalData)
	}
	// Check proposal has the expected base chain.
	if !(msg.Vote.Value.IsZero() || msg.Vote.Value.HasBase(i.input.Base())) {
		return false, fmt.Errorf("%w: message base %s, expected %s",
			ErrValidationWrongBase, msg.Vote.Value, i.input.Base())
	}

	if i.current.Phase == TERMINATED_PHASE {
		return false, nil // No-op
	}
	// Ignore CONVERGE and PREPARE messages for prior rounds.
	forPriorRound := msg.Vote.Round < i.current.Round
	if (forPriorRound && msg.Vote.Phase == CONVERGE_PHASE) ||
		(forPriorRound && msg.Vote.Phase == PREPARE_PHASE) {
		return false, nil
	}

	// Drop message that:
	//  * belong to future rounds, beyond the configured max lookahead threshold, and
	//  * carry no justification, i.e. are spammable.
	beyondMaxLookaheadRounds := msg.Vote.Round > i.current.Round+i.participant.maxLookaheadRounds
	if beyondMaxLookaheadRounds && isSpammable(msg) {
		return false, nil
	}

	// Load the round state and process further only valid, non-spammable messages.
	// Equivocations are handled by the quorum state.
	msgRound := i.getRound(msg.Vote.Round)
	switch msg.Vote.Phase {
	case QUALITY_PHASE:
		// Receive each prefix of the proposal independently, which is accepted at any
		// round/phase.
		i.quality.ReceiveEachPrefix(msg.Sender, msg.Vote.Value)
		// If the instance has surpassed QUALITY phase, update the candidates based
		// on possible quorum of input prefixes.
		if i.current.Phase != QUALITY_PHASE {
			return true, i.updateCandidatesFromQuality()
		}
	case CONVERGE_PHASE:
		if err := msgRound.converged.Receive(msg.Sender, i.powerTable, msg.Vote.Value, msg.Ticket, msg.Justification); err != nil {
			return false, fmt.Errorf("failed processing CONVERGE message: %w", err)
		}
	case PREPARE_PHASE:
		msgRound.prepared.Receive(msg.Sender, msg.Vote.Value, msg.Signature)
	case COMMIT_PHASE:
		msgRound.committed.Receive(msg.Sender, msg.Vote.Value, msg.Signature)
		// The only justifications that need to be stored for future propagation are for COMMITs
		// to non-bottom values.
		// This evidence can be brought forward to justify a CONVERGE message in the next round.
		if !msg.Vote.Value.IsZero() {
			msgRound.committed.ReceiveJustification(msg.Vote.Value, msg.Justification)
		}
		// Every COMMIT phase stays open to new messages even after the protocol moves on
		// to a new round. Late-arriving COMMITs can still (must) cause a local decision,
		// *in that round*. Try to complete the COMMIT phase for the round specified by
		// the message.
		if i.current.Phase != DECIDE_PHASE {
			return true, i.tryCommit(msg.Vote.Round)
		}
	case DECIDE_PHASE:
		i.decision.Receive(msg.Sender, msg.Vote.Value, msg.Signature)
		if i.current.Phase != DECIDE_PHASE {
			i.skipToDecide(msg.Vote.Value, msg.Justification)
		}
	default:
		return false, fmt.Errorf("unexpected message phase %s", msg.Vote.Phase)
	}

	// Try to complete the current phase in the current round.
	return true, i.tryCurrentPhase()
}

func (i *instance) postReceive(roundsReceived ...uint64) {
	// Check whether the instance should skip ahead to future round, in descending order.
	slices.Reverse(roundsReceived)
	for _, r := range roundsReceived {
		round := i.getRound(r)
		if chain, justification, skip := i.shouldSkipToRound(r, round); skip {
			i.skipToRound(r, chain, justification)
			return
		}
	}
}

// shouldSkipToRound determines whether to skip to round, and justification
// either for a value to sway to, or of COMMIT bottom to justify our own
// proposal. Otherwise, it returns nil chain, nil justification and false.
//
// See: skipToRound.
func (i *instance) shouldSkipToRound(round uint64, state *roundState) (*ECChain, *Justification, bool) {
	// Check if the given round is ahead of current round and this instance is not in
	// DECIDE phase.
	if round <= i.current.Round || i.current.Phase == DECIDE_PHASE {
		return nil, nil, false
	}
	if !state.prepared.ReceivedFromWeakQuorum() {
		return nil, nil, false
	}
	proposal := state.converged.FindBestTicketProposal(nil)
	if !proposal.IsValid() {
		// FindMaxTicketProposal returns a zero-valued ConvergeValue if no such ticket is
		// found. Hence the check for nil. Otherwise, if found such ConvergeValue must
		// have a non-nil justification.
		return nil, nil, false
	}
	return proposal.Chain, proposal.Justification, true
}

// Attempts to complete the current phase and round.
func (i *instance) tryCurrentPhase() error {
	i.log("try phase %s", i.current.Phase)
	switch i.current.Phase {
	case QUALITY_PHASE:
		return i.tryQuality()
	case CONVERGE_PHASE:
		return i.tryConverge()
	case PREPARE_PHASE:
		return i.tryPrepare()
	case COMMIT_PHASE:
		return i.tryCommit(i.current.Round)
	case DECIDE_PHASE:
		return i.tryDecide()
	case TERMINATED_PHASE:
		return nil // No-op
	default:
		return fmt.Errorf("unexpected phase %s", i.current.Phase)
	}
}

func (i *instance) reportPhaseMetrics() {
	attr := metric.WithAttributes(attrPhase[i.current.Phase])

	metrics.phaseCounter.Add(context.TODO(), 1, attr)
	metrics.currentPhase.Record(context.TODO(), int64(i.current.Phase))
	metrics.proposalLength.Record(context.TODO(), int64(i.proposal.Len()-1), attr)
}

// Sends this node's QUALITY message and begins the QUALITY phase.
func (i *instance) beginQuality() error {
	if i.current.Phase != INITIAL_PHASE {
		return fmt.Errorf("cannot transition from %s to %s", i.current.Phase, QUALITY_PHASE)
	}
	// Broadcast input value and wait to receive from others.
	i.current.Phase = QUALITY_PHASE
	i.participant.progression.NotifyProgress(i.current)
	i.phaseTimeout = i.alarmAfterSynchronyWithMulti(i.participant.qualityDeltaMulti)
	i.resetRebroadcastParams()
	i.broadcast(i.current.Round, QUALITY_PHASE, i.proposal, false, nil)
	i.reportPhaseMetrics()
	return nil
}

// Attempts to end the QUALITY phase and begin PREPARE based on current state.
func (i *instance) tryQuality() error {
	if i.current.Phase != QUALITY_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.current.Phase, QUALITY_PHASE)
	}

	// Wait either for a strong quorum that agree on our proposal, or for the timeout
	// to expire.
	foundQuorum := i.quality.HasStrongQuorumFor(i.proposal.Key())
	timeoutExpired := atOrAfter(i.participant.host.Time(), i.phaseTimeout)

	if foundQuorum || timeoutExpired {
		// If strong quorum of input is found the proposal will remain unchanged.
		// Otherwise, change the proposal to the longest prefix of input with strong
		// quorum.
		i.proposal = i.quality.FindStrongQuorumValueForLongestPrefixOf(i.input)
		// Add prefixes with quorum to candidates.
		i.addCandidatePrefixes(i.proposal)
		i.value = i.proposal
		i.log("adopting proposal/value %s", i.proposal)
		i.beginPrepare(nil)
	}
	return nil
}

// updateCandidatesFromQuality updates candidates as a result of late-arriving
// QUALITY messages based on the longest input prefix with strong quorum.
func (i *instance) updateCandidatesFromQuality() error {
	// Find the longest input prefix that has reached strong quorum as a result of
	// late-arriving QUALITY messages and update candidates with each of its
	// prefixes.
	longestPrefix := i.quality.FindStrongQuorumValueForLongestPrefixOf(i.input)
	if i.addCandidatePrefixes(longestPrefix) {
		i.log("expanded candidates for proposal %s from QUALITY quorum of %s", i.proposal, longestPrefix)
	}
	return nil
}

// beginConverge initiates CONVERGE_PHASE justified by the given justification.
func (i *instance) beginConverge(justification *Justification) {
	if justification.Vote.Round != i.current.Round-1 {
		// For safety assert that the justification given belongs to the right round.
		panic("justification for which to begin converge does not belong to expected round")
	}

	i.current.Phase = CONVERGE_PHASE
	i.participant.progression.NotifyProgress(i.current)
	i.phaseTimeout = i.alarmAfterSynchrony()
	i.resetRebroadcastParams()

	// Notify the round's convergeState that the self participant has begun the
	// CONVERGE phase. Because, we cannot guarantee that the CONVERGE message
	// broadcasts are delivered to self synchronously.
	i.getRound(i.current.Round).converged.SetSelfValue(i.proposal, justification)

	i.broadcast(i.current.Round, CONVERGE_PHASE, i.proposal, true, justification)
	i.reportPhaseMetrics()
}

// Attempts to end the CONVERGE phase and begin PREPARE based on current state.
func (i *instance) tryConverge() error {
	if i.current.Phase != CONVERGE_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.current.Phase, CONVERGE_PHASE)
	}
	// The CONVERGE phase timeout doesn't wait to hear from >⅔ of power.
	timeoutExpired := atOrAfter(i.participant.host.Time(), i.phaseTimeout)
	if !timeoutExpired {
		return nil
	}
	commitRoundState := i.getRound(i.current.Round - 1).committed

	isValidConvergeValue := func(cv ConvergeValue) bool {
		// If it is in candidate set
		if i.isCandidate(cv.Chain) {
			return true
		}
		// If it is not a candidate but it could possibly have been decided by another participant
		// in the last round, consider it a candidate.
		if cv.Justification.Vote.Phase != PREPARE_PHASE {
			return false
		}
		possibleDecision := commitRoundState.CouldReachStrongQuorumFor(cv.Chain.Key(), true)
		return possibleDecision
	}

	winner := i.getRound(i.current.Round).converged.FindBestTicketProposal(isValidConvergeValue)
	if !winner.IsValid() {
		return fmt.Errorf("no values at CONVERGE")
	}

	if !i.isCandidate(winner.Chain) {
		// if winner.Chain is not in candidate set then it means we got swayed
		i.log("⚠️ swaying from %s to %s by CONVERGE", i.proposal, winner.Chain)
		i.addCandidate(winner.Chain)
	} else {
		i.log("adopting proposal %s after converge (old proposal %s)", winner.Chain, i.proposal)
	}

	i.proposal = winner.Chain
	i.value = winner.Chain
	i.beginPrepare(winner.Justification)
	return nil
}

// Sends this node's PREPARE message and begins the PREPARE phase.
func (i *instance) beginPrepare(justification *Justification) {
	// Broadcast preparation of value and wait for everyone to respond.
	i.current.Phase = PREPARE_PHASE
	i.participant.progression.NotifyProgress(i.current)
	i.phaseTimeout = i.alarmAfterSynchrony()
	i.resetRebroadcastParams()

	i.broadcast(i.current.Round, PREPARE_PHASE, i.value, false, justification)
	i.reportPhaseMetrics()
}

// Attempts to end the PREPARE phase and begin COMMIT based on current state.
func (i *instance) tryPrepare() error {
	if i.current.Phase != PREPARE_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.current.Phase, PREPARE_PHASE)
	}

	prepared := i.getRound(i.current.Round).prepared
	proposalKey := i.proposal.Key()
	foundQuorum := prepared.HasStrongQuorumFor(proposalKey)
	timedOut := atOrAfter(i.participant.host.Time(), i.phaseTimeout)
	quorumNotPossible := !prepared.CouldReachStrongQuorumFor(proposalKey, false)
	phaseComplete := timedOut && prepared.ReceivedFromStrongQuorum()

	if foundQuorum {
		i.value = i.proposal
	} else if quorumNotPossible || phaseComplete {
		i.value = &ECChain{}
	}

	if foundQuorum || quorumNotPossible || phaseComplete {
		i.beginCommit()
	} else if timedOut {
		i.tryRebroadcast()
	}
	return nil
}

func (i *instance) beginCommit() {
	i.current.Phase = COMMIT_PHASE
	i.participant.progression.NotifyProgress(i.current)
	i.phaseTimeout = i.alarmAfterSynchrony()
	i.resetRebroadcastParams()

	// The PREPARE phase exited either with i.value == i.proposal having a strong quorum agreement,
	// or with i.value == bottom otherwise.
	// No justification is required for committing bottom.
	var justification *Justification
	if !i.value.IsZero() {
		if quorum, ok := i.getRound(i.current.Round).prepared.FindStrongQuorumFor(i.value.Key()); ok {
			// Found a strong quorum of PREPARE, build the justification for it.
			justification = i.buildJustification(quorum, i.current.Round, PREPARE_PHASE, i.value)
		} else {
			panic("beginCommit with no strong quorum for non-bottom value")
		}
	}

	i.broadcast(i.current.Round, COMMIT_PHASE, i.value, false, justification)
	i.reportPhaseMetrics()
}

func (i *instance) tryCommit(round uint64) error {
	// Unlike all other phases, the COMMIT phase stays open to new messages even
	// after an initial quorum is reached, and the algorithm moves on to the next
	// round. A subsequent COMMIT message can cause the node to decide, so there is
	// no check on the current phase.
	committed := i.getRound(round).committed
	quorumValue, foundStrongQuorum := committed.FindStrongQuorumValue()
	timedOut := atOrAfter(i.participant.host.Time(), i.phaseTimeout)
	phaseComplete := timedOut && committed.ReceivedFromStrongQuorum()

	switch {
	case foundStrongQuorum && !quorumValue.IsZero():
		// There is a strong quorum for a non-zero value; accept it. A participant may be
		// forced to decide a value that's not its preferred chain. The participant isn't
		// influencing that decision against their interest, just accepting it.
		i.value = quorumValue
		i.beginDecide(round)
	case i.current.Round != round, i.current.Phase != COMMIT_PHASE:
		// We are at a phase other than COMMIT or round does not match the current one;
		// nothing else to do.
	case foundStrongQuorum:
		// There is a strong quorum for bottom, carry forward the existing proposal.
		i.beginNextRound()
	case phaseComplete:
		// There is no strong quorum for bottom, which implies there must be a COMMIT for
		// some other value. There can only be one such value since it must be justified
		// by a strong quorum of PREPAREs. Some other participant could possibly have
		// observed a strong quorum for that value, since they might observe votes from ⅓
		// of honest power plus a ⅓ equivocating adversary. Sway to consider that value
		// as a candidate, even if it wasn't the local proposal.
		for _, v := range committed.ListAllValues() {
			if !v.IsZero() {
				if !i.isCandidate(v) {
					i.log("⚠️ swaying from %s to %s by COMMIT", i.input, v)
					i.addCandidate(v)
				}
				if !v.Eq(i.proposal) {
					i.proposal = v
					i.log("adopting proposal %s after commit", i.proposal)
				}
				break
			}
		}
		i.beginNextRound()
	case timedOut:
		// The phase has timed out. Attempt to re-broadcast messages.
		i.tryRebroadcast()
	}
	return nil
}

func (i *instance) beginDecide(round uint64) {
	i.current.Phase = DECIDE_PHASE
	i.participant.progression.NotifyProgress(i.current)
	i.resetRebroadcastParams()
	var justification *Justification
	// Value cannot be empty here.
	if quorum, ok := i.getRound(round).committed.FindStrongQuorumFor(i.value.Key()); ok {
		// Build justification for strong quorum of COMMITs for the value.
		justification = i.buildJustification(quorum, round, COMMIT_PHASE, i.value)
	} else {
		panic("beginDecide with no strong quorum for value")
	}

	// DECIDE messages always specify round = 0.
	// Extreme out-of-order message delivery could result in different nodes deciding
	// in different rounds (but for the same value).
	// Since each node sends only one DECIDE message, they must share the same vote
	// in order to be aggregated.
	i.broadcast(0, DECIDE_PHASE, i.value, false, justification)
	i.reportPhaseMetrics()
}

// Skips immediately to the DECIDE phase and sends a DECIDE message
// without waiting for a strong quorum of COMMITs in any round.
// The provided justification must justify the value being decided.
func (i *instance) skipToDecide(value *ECChain, justification *Justification) {
	i.current.Phase = DECIDE_PHASE
	i.participant.progression.NotifyProgress(i.current)
	i.proposal = value
	i.value = i.proposal
	i.resetRebroadcastParams()
	i.broadcast(0, DECIDE_PHASE, i.value, false, justification)

	metrics.skipCounter.Add(context.TODO(), 1, metric.WithAttributes(attrSkipToDecide))
	i.reportPhaseMetrics()
}

func (i *instance) tryDecide() error {
	quorumValue, ok := i.decision.FindStrongQuorumValue()
	if ok {
		if quorum, ok := i.decision.FindStrongQuorumFor(quorumValue.Key()); ok {
			decision := i.buildJustification(quorum, 0, DECIDE_PHASE, quorumValue)
			i.terminate(decision)
		} else {
			panic("tryDecide with no strong quorum for value")
		}
	} else {
		i.tryRebroadcast()
	}
	return nil
}

func (i *instance) getRound(r uint64) *roundState {
	round, ok := i.rounds[r]
	if !ok {
		round = newRoundState(r, i.powerTable)
		i.rounds[r] = round
	}
	return round
}

var bottomECChain = &ECChain{}

func (i *instance) beginNextRound() {
	i.log("moving to round %d with %s", i.current.Round+1, i.proposal.String())
	i.current.Round += 1
	metrics.currentRound.Record(context.TODO(), int64(i.current.Round))

	prevRound := i.getRound(i.current.Round - 1)
	// Proposal was updated at the end of COMMIT phase to be some value for which
	// this node received a COMMIT message (bearing justification), if there were any.
	// If there were none, there must have been a strong quorum for bottom instead.
	var justification *Justification
	if quorum, ok := prevRound.committed.FindStrongQuorumFor(bottomECChain.Key()); ok {
		// Build justification for strong quorum of COMMITs for bottom in the previous round.
		justification = i.buildJustification(quorum, i.current.Round-1, COMMIT_PHASE, nil)
	} else {
		// Extract the justification received from some participant (possibly this node itself).
		justification, ok = prevRound.committed.receivedJustification[i.proposal.Key()]
		if !ok {
			panic("beginConverge called but no justification for proposal")
		}
	}

	i.beginConverge(justification)
}

// skipToRound jumps ahead to the given round by initiating CONVERGE with the given justification.
//
// See shouldSkipToRound.
func (i *instance) skipToRound(round uint64, chain *ECChain, justification *Justification) {
	i.log("skipping from round %d to round %d with %s", i.current.Round, round, i.proposal.String())
	i.current.Round = round
	metrics.currentRound.Record(context.TODO(), int64(i.current.Round))
	metrics.skipCounter.Add(context.TODO(), 1, metric.WithAttributes(attrSkipToRound))

	if justification.Vote.Phase == PREPARE_PHASE {
		i.log("⚠️ swaying from %s to %s by skip to round %d", i.proposal, chain, i.current.Round)
		i.addCandidate(chain)
		i.proposal = chain
	}
	i.beginConverge(justification)
}

// Returns whether a chain is acceptable as a proposal for this instance to vote for.
// This is "EC Compatible" in the pseudocode.
func (i *instance) isCandidate(c *ECChain) bool {
	_, exists := i.candidates[c.Key()]
	return exists
}

func (i *instance) addCandidatePrefixes(c *ECChain) bool {
	var addedAny bool
	for l := c.Len() - 1; l > 0 && !addedAny; l-- {
		addedAny = i.addCandidate(c.Prefix(l))
	}
	return addedAny
}

func (i *instance) addCandidate(c *ECChain) bool {
	key := c.Key()
	if _, exists := i.candidates[key]; !exists {
		i.candidates[key] = struct{}{}
		return true
	}
	return false
}

func (i *instance) terminate(decision *Justification) {
	i.log("✅ terminated %s during round %d", i.value, i.current.Round)
	i.current.Phase = TERMINATED_PHASE
	i.participant.progression.NotifyProgress(i.current)
	i.value = decision.Vote.Value
	i.terminationValue = decision
	i.resetRebroadcastParams()

	metrics.roundHistogram.Record(context.TODO(), int64(i.current.Round))
	i.reportPhaseMetrics()
}

func (i *instance) terminated() bool {
	return i.current.Phase == TERMINATED_PHASE
}

func (i *instance) broadcast(round uint64, phase Phase, value *ECChain, createTicket bool, justification *Justification) {
	p := Payload{
		Instance:         i.current.ID,
		Round:            round,
		Phase:            phase,
		SupplementalData: *i.supplementalData,
		Value:            value,
	}

	mb := &MessageBuilder{
		NetworkName:      i.participant.host.NetworkName(),
		PowerTable:       i.powerTable,
		SigningMarshaler: i.participant.host,
		Payload:          p,
		Justification:    justification,
	}
	if createTicket {
		mb.BeaconForTicket = i.beacon
	}

	metrics.broadcastCounter.Add(context.TODO(), 1, metric.WithAttributes(attrPhase[p.Phase]))
	if err := i.participant.host.RequestBroadcast(mb); err != nil {
		i.log("failed to request broadcast: %v", err)
	}
}

// tryRebroadcast checks whether re-broadcast timeout has elapsed, and if so
// rebroadcasts messages from current and previous rounds. If not, it sets an
// alarm for re-broadcast relative to the number of attempts.
func (i *instance) tryRebroadcast() {
	switch {
	case i.rebroadcastAttempts == 0 && i.rebroadcastTimeout.IsZero():
		// It is the first time that rebroadcast has become necessary; set initial
		// rebroadcast timeout relative to the phase timeout, and schedule a rebroadcast.
		//
		// Determine the offset for the first rebroadcast alarm depending on current
		// instance phase and schedule the first alarm:
		//  * If in DECIDE phase, use current time as offset. Because, DECIDE phase does
		//    not have any phase timeout and may be too far in the past.
		//  * Otherwise, use the phase timeout.
		var rebroadcastTimeoutOffset time.Time
		if i.current.Phase == DECIDE_PHASE {
			rebroadcastTimeoutOffset = i.participant.host.Time()
		} else {
			rebroadcastTimeoutOffset = i.phaseTimeout
		}
		i.rebroadcastTimeout = rebroadcastTimeoutOffset.Add(i.participant.rebroadcastAfter(0))
		i.participant.host.SetAlarm(i.rebroadcastTimeout)
		i.log("scheduled initial rebroadcast at %v", i.rebroadcastTimeout)
	case i.rebroadcastTimeoutElapsed():
		// Rebroadcast now that the corresponding timeout has elapsed, and schedule the
		// successive rebroadcast.
		i.rebroadcast()
		i.rebroadcastAttempts++

		// Use current host time as the offset for the next alarm to assure that rate of
		// broadcasted messages grows relative to the actual time at which an alarm is
		// triggered , not the absolute alarm time. This would avoid a "runaway
		// rebroadcast" scenario where rebroadcast timeout consistently remains behind
		// current time due to the discrepancy between set alarm time and the actual time
		// at which the alarm is triggered.
		i.rebroadcastTimeout = i.participant.host.Time().Add(i.participant.rebroadcastAfter(i.rebroadcastAttempts))
		i.participant.host.SetAlarm(i.rebroadcastTimeout)
		i.log("scheduled next rebroadcast at %v", i.rebroadcastTimeout)
	default:
		// Rebroadcast timeout is set but has not elapsed yet; nothing to do.
	}
}

func (i *instance) resetRebroadcastParams() {
	i.rebroadcastAttempts = 0
	i.rebroadcastTimeout = time.Time{}
}

func (i *instance) rebroadcastTimeoutElapsed() bool {
	now := i.participant.host.Time()
	return atOrAfter(now, i.rebroadcastTimeout)
}

func (i *instance) rebroadcast() {
	// Rebroadcast quality and all messages from the current and previous rounds, unless the
	// instance has progressed to DECIDE phase. In which case, only DECIDE message is
	// rebroadcasted.
	//
	// Note that the implementation here rebroadcasts more messages than FIP-0086
	// strictly requires. Because, the cost of rebroadcasting additional messages is
	// small compared to the reduction in need for rebroadcast.
	switch i.current.Phase {
	case QUALITY_PHASE, CONVERGE_PHASE, PREPARE_PHASE, COMMIT_PHASE:
		// Rebroadcast request for missing messages are silently ignored. Hence the
		// simpler bulk rebroadcast if we are not in DECIDE phase.
		i.rebroadcastQuietly(0, QUALITY_PHASE)

		i.rebroadcastQuietly(i.current.Round, COMMIT_PHASE)
		i.rebroadcastQuietly(i.current.Round, PREPARE_PHASE)
		i.rebroadcastQuietly(i.current.Round, CONVERGE_PHASE)
		if i.current.Round > 0 {
			i.rebroadcastQuietly(i.current.Round-1, COMMIT_PHASE)
			i.rebroadcastQuietly(i.current.Round-1, PREPARE_PHASE)
			i.rebroadcastQuietly(i.current.Round-1, CONVERGE_PHASE)
		}
	case DECIDE_PHASE:
		i.rebroadcastQuietly(0, DECIDE_PHASE)
	default:
		log.Errorw("rebroadcast attempted for unexpected phase", "round", i.current.Round, "phase", i.current.Phase)
	}
}

func (i *instance) rebroadcastQuietly(round uint64, phase Phase) {
	instant := Instant{i.current.ID, round, phase}
	if err := i.participant.host.RequestRebroadcast(instant); err != nil {
		// Silently log the error and proceed. This is consistent with the behaviour of
		// instance for regular broadcasts.
		i.log("failed to request rebroadcast %s at round %d: %v", phase, round, err)
	} else {
		i.log("rebroadcasting %s at round %d", phase, round)
		metrics.reBroadcastCounter.Add(context.TODO(), 1)
	}
}

// Sets an alarm to be delivered after a synchrony delay.
// The delay duration increases with each round.
// Returns the absolute time at which the alarm will fire.
func (i *instance) alarmAfterSynchrony() time.Time {
	return i.alarmAfterSynchronyWithMulti(1)
}

// Sets an alarm to be delivered after a synchrony delay including a multiplier on the duration.
// The delay duration increases with each round.
// Returns the absolute time at which the alarm will fire.
func (i *instance) alarmAfterSynchronyWithMulti(multi float64) time.Time {
	delta := time.Duration(float64(i.participant.delta) * multi *
		math.Pow(i.participant.deltaBackOffExponent, float64(i.current.Round)))
	timeout := i.participant.host.Time().Add(2 * delta)
	i.participant.host.SetAlarm(timeout)
	return timeout
}

// Builds a justification for a value from a quorum result.
func (i *instance) buildJustification(quorum QuorumResult, round uint64, phase Phase, value *ECChain) *Justification {
	aggSignature, err := quorum.Aggregate(i.aggregateVerifier)
	if err != nil {
		panic(fmt.Errorf("aggregating for phase %v: %v", phase, err))
	}
	return &Justification{
		Vote: Payload{
			Instance:         i.current.ID,
			Round:            round,
			Phase:            phase,
			Value:            value,
			SupplementalData: *i.supplementalData,
		},
		Signers:   quorum.SignersBitfield(),
		Signature: aggSignature,
	}
}

func (i *instance) log(format string, args ...any) {
	if i.tracer != nil {
		msg := fmt.Sprintf(format, args...)
		i.tracer.Log("{%d}: %s (round %d, phase %s, proposal %s, value %s)", i.current.ID, msg,
			i.current.Round, i.current.Phase, i.proposal, i.value)
	}
}

///// Incremental quorum-calculation helper /////

// Accumulates values from a collection of senders and incrementally calculates
// which values have reached a strong quorum of support.
// Supports receiving multiple values from a sender at once, and hence multiple strong quorum values.
// Subsequent messages from a single sender are dropped.
type quorumState struct {
	// Set of senders from which a message has been received.
	senders map[ActorID]struct{}
	// Total power of all distinct senders from which some chain has been received so far.
	sendersTotalPower int64
	// The power supporting each chain so far.
	chainSupport map[ECChainKey]chainSupport
	// Table of senders' power.
	powerTable *PowerTable
	// Stores justifications received for some value.
	receivedJustification map[ECChainKey]*Justification
	// attributes for metrics
	attributes []attribute.KeyValue
}

// A chain value and the total power supporting it
type chainSupport struct {
	chain           *ECChain
	power           int64
	signatures      map[ActorID][]byte
	hasStrongQuorum bool
}

// Creates a new, empty quorum state.
func newQuorumState(powerTable *PowerTable, attributes ...attribute.KeyValue) *quorumState {
	return &quorumState{
		senders:               map[ActorID]struct{}{},
		chainSupport:          map[ECChainKey]chainSupport{},
		powerTable:            powerTable,
		receivedJustification: map[ECChainKey]*Justification{},
		attributes:            attributes,
	}
}

// Receives a chain from a sender.
// Ignores any subsequent value from a sender from which a value has already been received.
func (q *quorumState) Receive(sender ActorID, value *ECChain, signature []byte) {
	senderPower, ok := q.receiveSender(sender)
	if !ok {
		return
	}
	q.receiveInner(sender, value, senderPower, signature)
}

// Receives each prefix of a chain as a distinct value from a sender.
// Note that this method does not store signatures, so it is not possible later to
// create an aggregate for these prefixes.
// This is intended for use in the QUALITY phase.
// Ignores any subsequent values from a sender from which a value has already been received.
func (q *quorumState) ReceiveEachPrefix(sender ActorID, values *ECChain) {
	senderPower, ok := q.receiveSender(sender)
	if !ok {
		return
	}
	for j := range values.Suffix() {
		prefix := values.Prefix(j + 1)
		q.receiveInner(sender, prefix, senderPower, nil)
	}
}

// Adds sender's power to total the first time a value is received from them.
// Returns the sender's power, and whether this was the first invocation for this sender.
func (q *quorumState) receiveSender(sender ActorID) (int64, bool) {
	if _, found := q.senders[sender]; found {
		return 0, false
	}
	q.senders[sender] = struct{}{}
	senderPower, _ := q.powerTable.Get(sender)
	q.sendersTotalPower += senderPower
	if len(q.attributes) != 0 {
		metrics.quorumParticipation.Record(context.Background(),
			float64(q.sendersTotalPower)/float64(q.powerTable.ScaledTotal),
			metric.WithAttributes(q.attributes...))
	}
	return senderPower, true
}

// Receives a chain from a sender.
func (q *quorumState) receiveInner(sender ActorID, value *ECChain, power int64, signature []byte) {
	key := value.Key()
	candidate, ok := q.chainSupport[key]
	if !ok {
		candidate = chainSupport{
			chain:           value,
			signatures:      map[ActorID][]byte{},
			hasStrongQuorum: false,
		}
	}

	candidate.power += power
	if candidate.signatures[sender] != nil {
		panic("duplicate message should have been dropped")
	}
	candidate.signatures[sender] = signature
	candidate.hasStrongQuorum = IsStrongQuorum(candidate.power, q.powerTable.ScaledTotal)
	q.chainSupport[key] = candidate
}

// Receives and stores justification for a value from another participant.
func (q *quorumState) ReceiveJustification(value *ECChain, justification *Justification) {
	if justification == nil {
		panic("nil justification")
	}
	// Keep only the first one received.
	key := value.Key()
	if _, ok := q.receivedJustification[key]; !ok {
		q.receivedJustification[key] = justification
	}
}

// Lists all values that have been senders from any sender.
// The order of returned values is not defined.
func (q *quorumState) ListAllValues() []*ECChain {
	var chains []*ECChain
	for _, cp := range q.chainSupport {
		chains = append(chains, cp.chain)
	}
	return chains
}

// Checks whether at least one message has been senders from a strong quorum of senders.
func (q *quorumState) ReceivedFromStrongQuorum() bool {
	return IsStrongQuorum(q.sendersTotalPower, q.powerTable.ScaledTotal)
}

// ReceivedFromWeakQuorum checks whether at least one message has been received
// from a weak quorum of senders.
func (q *quorumState) ReceivedFromWeakQuorum() bool {
	return hasWeakQuorum(q.sendersTotalPower, q.powerTable.ScaledTotal)
}

// Checks whether a chain has reached a strong quorum.
func (q *quorumState) HasStrongQuorumFor(key ECChainKey) bool {
	supportForChain, ok := q.chainSupport[key]
	return ok && supportForChain.hasStrongQuorum
}

// CouldReachStrongQuorumFor checks whether the given chain can possibly reach
// strong quorum given the locally received messages.
// If withAdversary is true, an additional ⅓ of total power is added to the possible support,
// representing an equivocating adversary. This is appropriate for testing whether
// any other participant could have observed a strong quorum in the presence of such adversary.
func (q *quorumState) CouldReachStrongQuorumFor(key ECChainKey, withAdversary bool) bool {
	var supportingPower int64
	if supportForChain, found := q.chainSupport[key]; found {
		supportingPower = supportForChain.power
	}
	// A strong quorum is only feasible when the total support for the given chain,
	// combined with the aggregate power of not yet voted participants, exceeds ⅔ of
	// total power.
	unvotedPower := q.powerTable.ScaledTotal - q.sendersTotalPower
	adversaryPower := int64(0)
	if withAdversary {
		// Account for the fact that the adversary may have double-voted here.
		adversaryPower = q.powerTable.ScaledTotal / 3
	}
	// We're double-counting adversary power, so we need to cap the power at the total available
	// power.
	possibleSupport := min(supportingPower+unvotedPower+adversaryPower, q.powerTable.ScaledTotal)
	return IsStrongQuorum(possibleSupport, q.powerTable.ScaledTotal)
}

type QuorumResult struct {
	// Signers is an array of indexes into the powertable, sorted in increasing order
	Signers    []int
	Signatures [][]byte
}

func (q QuorumResult) Aggregate(v Aggregate) ([]byte, error) {
	return v.Aggregate(q.Signers, q.Signatures)
}

func (q QuorumResult) SignersBitfield() bitfield.BitField {
	signers := make([]uint64, 0, len(q.Signers))
	for _, s := range q.Signers {
		signers = append(signers, uint64(s))
	}
	ri, _ := rlepluslazy.RunsFromSlice(signers)
	bf, _ := bitfield.NewFromIter(ri)
	return bf
}

// Checks whether a chain has reached a strong quorum.
// If so returns a set of signers and signatures for the value that form a strong quorum.
func (q *quorumState) FindStrongQuorumFor(key ECChainKey) (QuorumResult, bool) {
	chainSupport, ok := q.chainSupport[key]
	if !ok || !chainSupport.hasStrongQuorum {
		return QuorumResult{}, false
	}

	// Build an array of indices of signers in the power table.
	signers := make([]int, 0, len(chainSupport.signatures))
	for id := range chainSupport.signatures {
		entryIndex, found := q.powerTable.Lookup[id]
		if !found {
			panic(fmt.Sprintf("signer not found in power table: %d", id))
		}
		signers = append(signers, entryIndex)
	}
	// Sort power table indices.
	// If the power table entries are ordered by decreasing power,
	// then the first strong quorum found will be the smallest.
	sort.Ints(signers)

	// Accumulate signers and signatures until they reach a strong quorum.
	signatures := make([][]byte, 0, len(chainSupport.signatures))
	var justificationPower int64
	for i, idx := range signers {
		if idx >= len(q.powerTable.Entries) {
			panic(fmt.Sprintf("invalid signer index: %d for %d entries", idx, len(q.powerTable.Entries)))
		}
		power := q.powerTable.ScaledPower[idx]
		entry := q.powerTable.Entries[idx]
		justificationPower += power
		signatures = append(signatures, chainSupport.signatures[entry.ID])
		if IsStrongQuorum(justificationPower, q.powerTable.ScaledTotal) {
			return QuorumResult{
				Signers:    signers[:i+1],
				Signatures: signatures,
			}, true
		}
	}

	// There is likely a bug. Because, chainSupport.hasStrongQuorum must have been
	// true for the code to reach this point. Hence, the fatal error.
	panic("strong quorum exists but could not be found")
}

// FindStrongQuorumValueForLongestPrefixOf finds the longest prefix of preferred
// chain which has strong quorum, or the base of preferred if no such prefix
// exists.
func (q *quorumState) FindStrongQuorumValueForLongestPrefixOf(preferred *ECChain) *ECChain {
	if q.HasStrongQuorumFor(preferred.Key()) {
		return preferred
	}
	for i := preferred.Len() - 1; i >= 0; i-- {
		longestPrefix := preferred.Prefix(i)
		if q.HasStrongQuorumFor(longestPrefix.Key()) {
			return longestPrefix
		}
	}
	return preferred.BaseChain()
}

// Returns the chain with a strong quorum of support, if there is one.
// This is appropriate for use in PREPARE/COMMIT/DECIDE phases, where each participant
// casts a single vote.
// Panics if there are multiple chains with strong quorum
// (signalling a violation of assumptions about the adversary).
func (q *quorumState) FindStrongQuorumValue() (quorumValue *ECChain, foundQuorum bool) {
	for key, cp := range q.chainSupport {
		if cp.hasStrongQuorum {
			if foundQuorum {
				panic("multiple chains with strong quorum")
			}
			foundQuorum = true
			quorumValue = q.chainSupport[key].chain
		}
	}
	return
}

//// CONVERGE phase helper /////

type convergeState struct {
	// Participants from which a message has been received.
	senders map[ActorID]struct{}
	// Chains indexed by key.
	values map[ECChainKey]ConvergeValue

	// sendersTotalPower is only used for metrics reporting
	sendersTotalPower int64
	attributes        []attribute.KeyValue
}

// ConvergeValue is valid when the Chain is non-zero and Justification is non-nil
type ConvergeValue struct {
	Chain         *ECChain
	Justification *Justification
	Rank          float64
}

// IsOtherBetter returns true if the argument is better than self
func (cv *ConvergeValue) IsOtherBetter(other ConvergeValue) bool {
	return !cv.IsValid() || other.Rank < cv.Rank
}

func (cv *ConvergeValue) IsValid() bool {
	return !cv.Chain.IsZero() && cv.Justification != nil
}

func newConvergeState(attributes ...attribute.KeyValue) *convergeState {
	return &convergeState{
		senders:    map[ActorID]struct{}{},
		values:     map[ECChainKey]ConvergeValue{},
		attributes: append([]attribute.KeyValue{attrConvergePhase}, attributes...),
	}
}

// SetSelfValue sets the participant's locally-proposed converge value. This
// means the participant need not to rely on messages broadcast to be received by
// itself.
func (c *convergeState) SetSelfValue(value *ECChain, justification *Justification) {
	// any converge for the given value is better than self-reported
	// as self-reported has no ticket
	key := value.Key()
	if _, ok := c.values[key]; !ok {
		c.values[key] = ConvergeValue{
			Chain:         value,
			Justification: justification,
			Rank:          math.Inf(1), // +Inf because any real ConvergeValue is better than self-value
		}
	}
}

// Receives a new CONVERGE value from a sender.
// Ignores any subsequent value from a sender from which a value has already been received.
func (c *convergeState) Receive(sender ActorID, table *PowerTable, value *ECChain, ticket Ticket, justification *Justification) error {
	if value.IsZero() {
		return fmt.Errorf("bottom cannot be justified for CONVERGE")
	}
	if justification == nil {
		return fmt.Errorf("converge message cannot carry nil-justification")
	}

	if _, ok := c.senders[sender]; ok {
		return nil
	}
	c.senders[sender] = struct{}{}
	senderPower, _ := table.Get(sender)
	c.sendersTotalPower += senderPower

	metrics.quorumParticipation.Record(context.Background(),
		float64(c.sendersTotalPower)/float64(table.ScaledTotal),
		metric.WithAttributes(c.attributes...))

	key := value.Key()
	// Keep only the first justification and best ticket.
	if v, found := c.values[key]; !found {
		c.values[key] = ConvergeValue{
			Chain:         value,
			Justification: justification,
			Rank:          ComputeTicketRank(ticket, senderPower),
		}
	} else {
		// The best ticket is the one that ranks first, i.e. smallest rank value.
		rank := ComputeTicketRank(ticket, senderPower)
		if rank < v.Rank {
			v.Rank = rank
			c.values[key] = v
		}
	}
	return nil
}

// FindBestTicketProposal finds the value with the best ticket, weighted by
// sender power. The filter is applied to select considered converge values.
// nil value filter is equivalent to consider all.
// Returns an invalid (zero-value) ConvergeValue if no converge value is found.
func (c *convergeState) FindBestTicketProposal(filter func(ConvergeValue) bool) ConvergeValue {
	// Non-determinism in case of matching tickets from an equivocation is ok.
	// If the same ticket is used for two different values then either we get a decision on one of them
	// only or we go to a new round. Eventually there is a round where the max ticket is held by a
	// correct participant, who will not double vote.
	var bestValue ConvergeValue

	for _, value := range c.values {
		if bestValue.IsOtherBetter(value) && (filter == nil || filter(value)) {
			bestValue = value
		}
	}

	return bestValue
}

// Finds some proposal which matches a specific value.
// This searches values received in messages first, falling back to the participant's self value
// only if necessary.
func (c *convergeState) FindProposalFor(chain *ECChain) ConvergeValue {
	for _, value := range c.values {
		if value.Chain.Eq(chain) {
			return value
		}
	}

	// Default converge value is not valid
	return ConvergeValue{}
}

///// General helpers /////

// The only messages that are spammable are COMMIT for bottom. QUALITY and
// PREPARE messages may also not carry justification, but they are not
// spammable. Because:
//   - QUALITY is only valid for round zero.
//   - PREPARE must carry justification for non-zero rounds.
//
// Therefore, we are only left with COMMIT for bottom messages as potentially
// spammable for rounds beyond zero.
//
// To drop such messages, the implementation below defensively uses a stronger
// condition of "nil justification with round larger than zero" to determine
// whether a message is "spammable".
func isSpammable(msg *GMessage) bool {
	return msg.Justification == nil && msg.Vote.Round > 0
}

func divCeil(a, b int64) int64 {
	quo := a / b
	rem := a % b
	if rem != 0 {
		quo += 1
	}
	return quo
}

// Check whether a portion of storage power is a strong quorum of the total
func IsStrongQuorum(part int64, whole int64) bool {
	// uint32 because 2 * whole exceeds int64
	return part >= divCeil(2*whole, 3)
}

// Check whether a portion of storage power is a weak quorum of the total
func hasWeakQuorum(part, whole int64) bool {
	// Must be strictly greater than 1/3. Otherwise, there could be a strong quorum.
	return part > divCeil(whole, 3)
}

// Tests whether lhs is equal to or greater than rhs.
func atOrAfter(lhs time.Time, rhs time.Time) bool {
	return lhs.After(rhs) || lhs.Equal(rhs)
}
