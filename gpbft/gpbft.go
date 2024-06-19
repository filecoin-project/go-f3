package gpbft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"time"

	"github.com/filecoin-project/go-bitfield"
	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	"github.com/filecoin-project/go-f3/merkle"
	"golang.org/x/xerrors"
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

const DOMAIN_SEPARATION_TAG = "GPBFT"

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
	// Signature by the sender's public key over Instance || Round || Step || Value.
	Signature []byte
	// VRF ticket for CONVERGE messages (otherwise empty byte array).
	Ticket Ticket
	// Justification for this message (some messages must be justified by a strong quorum of messages from some previous step).
	Justification *Justification
}

type Justification struct {
	// Vote is the payload that is signed by the signature
	Vote Payload
	// Indexes in the base power table of the signers (bitset)
	Signers bitfield.BitField
	// BLS aggregate signature of signers
	Signature []byte
}

type SupplementalData struct {
	// Merkle-tree of instance-specific commitments. Currently empty but this will eventually
	// include things like snark-friendly power-table commitments.
	Commitments [32]byte
	// The DagCBOR-blake2b256 CID of the power table used to validate the next instance, taking
	// lookback into account.
	PowerTable CID `cborgen:"maxlen=38"` // []PowerEntry
}

func (d *SupplementalData) Eq(other *SupplementalData) bool {
	return d.Commitments == other.Commitments &&
		bytes.Equal(d.PowerTable, other.PowerTable)
}

// Fields of the message that make up the signature payload.
type Payload struct {
	// GossiPBFT instance (epoch) number.
	Instance uint64
	// GossiPBFT round number.
	Round uint64
	// GossiPBFT step name.
	Step Phase
	// The common data.
	SupplementalData SupplementalData
	// The value agreed-upon in a single instance.
	Value ECChain
}

func (p *Payload) Eq(other *Payload) bool {
	return p.Instance == other.Instance &&
		p.Round == other.Round &&
		p.Step == other.Step &&
		p.SupplementalData.Eq(&other.SupplementalData) &&
		p.Value.Eq(other.Value)
}

func (p *Payload) MarshalForSigning(nn NetworkName) []byte {
	values := make([][]byte, len(p.Value))
	for i := range p.Value {
		values[i] = p.Value[i].MarshalForSigning()
	}
	root := merkle.Tree(values)

	var buf bytes.Buffer
	buf.WriteString(DOMAIN_SEPARATION_TAG)
	buf.WriteString(":")
	buf.WriteString(string(nn))
	buf.WriteString(":")

	_ = binary.Write(&buf, binary.BigEndian, p.Step)
	_ = binary.Write(&buf, binary.BigEndian, p.Round)
	_ = binary.Write(&buf, binary.BigEndian, p.Instance)
	_, _ = buf.Write(p.SupplementalData.Commitments[:])
	_, _ = buf.Write(root[:])
	_, _ = buf.Write(p.SupplementalData.PowerTable)
	return buf.Bytes()
}

func (m GMessage) String() string {
	return fmt.Sprintf("%s{%d}(%d %s)", m.Vote.Step, m.Vote.Instance, m.Vote.Round, &m.Vote.Value)
}

// A single Granite consensus instance.
type instance struct {
	participant *Participant
	instanceID  uint64
	// The EC chain input to this instance.
	input ECChain
	// The power table for the base chain, used for power in this instance.
	powerTable PowerTable
	// The beacon value from the base chain, used for tickets in this instance.
	beacon []byte
	// Current round number.
	round uint64
	// Current phase in the round.
	phase Phase
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
	// broadcasted selectively stores messages that have been broadcast in case
	// rebroadcast becomes necessary.
	//
	// See: broadcast, tryRebroadcast.
	broadcasted *broadcastState
	// Supplemental data that all participants must agree on ahead of time. Messages that
	// propose supplemental data that differs with our supplemental data will be discarded.
	supplementalData *SupplementalData
	// This instance's proposal for the current round. Never bottom.
	// This is set after the QUALITY phase, and changes only at the end of a full round.
	proposal ECChain
	// The value to be transmitted at the next phase, which may be bottom.
	// This value may change away from the proposal between phases.
	value ECChain
	// The set of values that are acceptable candidates to this instance.
	// This includes the base chain, all prefixes of proposal that found a strong quorum
	// of support in the QUALITY phase, and any chains that could possibly have been
	// decided by another participant.
	candidates []ECChain
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
	input ECChain,
	data *SupplementalData,
	powerTable PowerTable,
	beacon []byte) (*instance, error) {
	if input.IsZero() {
		return nil, fmt.Errorf("input is empty")
	}
	return &instance{
		participant:      participant,
		instanceID:       instanceID,
		input:            input,
		powerTable:       powerTable,
		beacon:           beacon,
		round:            0,
		phase:            INITIAL_PHASE,
		supplementalData: data,
		proposal:         input,
		broadcasted:      newBroadcastState(),
		value:            ECChain{},
		candidates:       []ECChain{input.BaseChain()},
		quality:          newQuorumState(powerTable),
		rounds: map[uint64]*roundState{
			0: newRoundState(powerTable),
		},
		decision: newQuorumState(powerTable),
		tracer:   participant.tracer,
	}, nil
}

type roundState struct {
	converged *convergeState
	prepared  *quorumState
	committed *quorumState
}

func newRoundState(powerTable PowerTable) *roundState {
	return &roundState{
		converged: newConvergeState(),
		prepared:  newQuorumState(powerTable),
		committed: newQuorumState(powerTable),
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
	return fmt.Sprintf("{%d}, round %d, phase %s", i.instanceID, i.round, i.phase)
}

// Processes a single message.
// Returns true if the message might have caused a change in state.
func (i *instance) receiveOne(msg *GMessage) (bool, error) {
	// Check the message is for this instance, to guard against programming error.
	if msg.Vote.Instance != i.instanceID {
		return false, fmt.Errorf("%w: message for instance %d, expected %d",
			ErrReceivedWrongInstance, msg.Vote.Instance, i.instanceID)
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
			ErrValidationWrongBase, &msg.Vote.Value, i.input.Base())
	}

	if i.phase == TERMINATED_PHASE {
		return false, nil // No-op
	}
	// Ignore QUALITY messages after exiting the QUALITY phase.
	// Ignore CONVERGE and PREPARE messages for prior rounds.
	forPriorRound := msg.Vote.Round < i.round
	if (msg.Vote.Step == QUALITY_PHASE && i.phase != QUALITY_PHASE) ||
		(forPriorRound && msg.Vote.Step == CONVERGE_PHASE) ||
		(forPriorRound && msg.Vote.Step == PREPARE_PHASE) {
		return false, nil
	}

	// Drop message that:
	//  * belong to future rounds, beyond the configured max lookahead threshold, and
	//  * carry no justification, i.e. are spammable.
	beyondMaxLookaheadRounds := msg.Vote.Round > i.round+i.participant.maxLookaheadRounds
	if beyondMaxLookaheadRounds && isSpammable(msg) {
		return false, nil
	}

	// Load the round state and process further only valid, non-spammable messages.
	// Equivocations are handled by the quorum state.
	msgRound := i.getRound(msg.Vote.Round)
	switch msg.Vote.Step {
	case QUALITY_PHASE:
		// Receive each prefix of the proposal independently.
		i.quality.ReceiveEachPrefix(msg.Sender, msg.Vote.Value)
	case CONVERGE_PHASE:
		if err := msgRound.converged.Receive(msg.Sender, msg.Vote.Value, msg.Ticket, msg.Justification); err != nil {
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
	case DECIDE_PHASE:
		i.decision.Receive(msg.Sender, msg.Vote.Value, msg.Signature)
		if i.phase != DECIDE_PHASE {
			i.skipToDecide(msg.Vote.Value, msg.Justification)
		}
	default:
		return false, fmt.Errorf("unexpected message step %s", msg.Vote.Step)
	}

	// Every COMMIT phase stays open to new messages even after the protocol moves on to
	// a new round. Late-arriving COMMITS can still (must) cause a local decision, *in that round*.
	// Try to complete the COMMIT phase for the round specified by the message.
	if msg.Vote.Step == COMMIT_PHASE && i.phase != DECIDE_PHASE {
		return true, i.tryCommit(msg.Vote.Round)
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
func (i *instance) shouldSkipToRound(round uint64, state *roundState) (ECChain, *Justification, bool) {
	// Check if the given round is ahead of current round and this instance is not in
	// DECIDE phase.
	if round <= i.round || i.phase == DECIDE_PHASE {
		return nil, nil, false
	}
	if !state.prepared.ReceivedFromWeakQuorum() {
		return nil, nil, false
	}
	proposal := state.converged.FindMaxTicketProposal(i.powerTable)
	if proposal.Justification == nil {
		// FindMaxTicketProposal returns a zero-valued ConvergeValue if no such ticket is
		// found. Hence the check for nil. Otherwise, if found such ConvergeValue must
		// have a non-nil justification.
		return nil, nil, false
	}
	return proposal.Chain, proposal.Justification, true
}

// Attempts to complete the current phase and round.
func (i *instance) tryCurrentPhase() error {
	i.log("try step %s", i.phase)
	switch i.phase {
	case QUALITY_PHASE:
		return i.tryQuality()
	case CONVERGE_PHASE:
		return i.tryConverge()
	case PREPARE_PHASE:
		return i.tryPrepare()
	case COMMIT_PHASE:
		return i.tryCommit(i.round)
	case DECIDE_PHASE:
		return i.tryDecide()
	case TERMINATED_PHASE:
		return nil // No-op
	default:
		return fmt.Errorf("unexpected phase %s", i.phase)
	}
}

// Checks message validity, including justification and signatures.
// An invalid message can never become valid, so may be dropped.
// This is a pure function and does not modify its arguments.
// It must be safe for concurrent use.
func ValidateMessage(powerTable *PowerTable, beacon []byte, host Host, msg *GMessage) error {
	// Check sender is eligible.
	senderPower, _, senderPubKey := powerTable.Get(msg.Sender)
	if senderPower == 0 {
		return xerrors.Errorf("sender %d with zero power or not in power table", msg.Sender)
	}

	// Check that message value is a valid chain.
	if err := msg.Vote.Value.Validate(); err != nil {
		return xerrors.Errorf("invalid message vote value chain: %w", err)
	}

	// Check phase-specific constraints.
	switch msg.Vote.Step {
	case QUALITY_PHASE:
		if msg.Vote.Round != 0 {
			return xerrors.Errorf("unexpected round %d for quality phase", msg.Vote.Round)
		}
		if msg.Vote.Value.IsZero() {
			return xerrors.Errorf("unexpected zero value for quality phase")
		}
	case CONVERGE_PHASE:
		if msg.Vote.Round == 0 {
			return xerrors.Errorf("unexpected round 0 for converge phase")
		}
		if msg.Vote.Value.IsZero() {
			return xerrors.Errorf("unexpected zero value for converge phase")
		}
		if !VerifyTicket(host.NetworkName(), beacon, msg.Vote.Instance, msg.Vote.Round, senderPubKey, host, msg.Ticket) {
			return xerrors.Errorf("failed to verify ticket from %v", msg.Sender)
		}
	case DECIDE_PHASE:
		if msg.Vote.Round != 0 {
			return xerrors.Errorf("unexpected non-zero round %d for decide phase", msg.Vote.Round)
		}
		if msg.Vote.Value.IsZero() {
			return xerrors.Errorf("unexpected zero value for decide phase")
		}
	case PREPARE_PHASE, COMMIT_PHASE:
		// No additional checks for PREPARE and COMMIT.
	default:
		return xerrors.Errorf("invalid vote step: %d", msg.Vote.Step)
	}

	// Check vote signature.
	sigPayload := host.MarshalPayloadForSigning(host.NetworkName(), &msg.Vote)
	if err := host.Verify(senderPubKey, sigPayload, msg.Signature); err != nil {
		return xerrors.Errorf("invalid signature on %v, %v", msg, err)
	}

	// Check justification
	needsJustification := !(msg.Vote.Step == QUALITY_PHASE ||
		(msg.Vote.Step == PREPARE_PHASE && msg.Vote.Round == 0) ||
		(msg.Vote.Step == COMMIT_PHASE && msg.Vote.Value.IsZero()))
	if needsJustification {
		if msg.Justification == nil {
			return fmt.Errorf("message for phase %v round %v has no justification", msg.Vote.Step, msg.Vote.Round)
		}
		// Check that the justification is for the same instance.
		if msg.Vote.Instance != msg.Justification.Vote.Instance {
			return fmt.Errorf("message with instanceID %v has evidence from instanceID: %v", msg.Vote.Instance, msg.Justification.Vote.Instance)
		}
		// Check that justification vote value is a valid chain.
		if err := msg.Justification.Vote.Value.Validate(); err != nil {
			return xerrors.Errorf("invalid justification vote value chain: %w", err)
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
		var justificationPower uint16
		signers := make([]PubKey, 0)
		if err := msg.Justification.Signers.ForEach(func(bit uint64) error {
			if int(bit) >= len(powerTable.Entries) {
				return fmt.Errorf("invalid signer index: %d", bit)
			}
			power := powerTable.ScaledPower[bit]
			if power == 0 {
				return xerrors.Errorf("signer with ID %d has no power", powerTable.Entries[bit].ID)
			}
			justificationPower += power
			signers = append(signers, powerTable.Entries[bit].PubKey)
			return nil
		}); err != nil {
			return fmt.Errorf("failed to iterate over signers: %w", err)
		}

		if !IsStrongQuorum(justificationPower, powerTable.ScaledTotal) {
			return fmt.Errorf("message %v has justification with insufficient power: %v", msg, justificationPower)
		}

		payload := host.MarshalPayloadForSigning(host.NetworkName(), &msg.Justification.Vote)
		if err := host.VerifyAggregate(payload, msg.Justification.Signature, signers); err != nil {
			return xerrors.Errorf("verification of the aggregate failed: %+v: %w", msg.Justification, err)
		}
	} else if msg.Justification != nil {
		return fmt.Errorf("message %v has unexpected justification", msg)
	}

	return nil
}

// Sends this node's QUALITY message and begins the QUALITY phase.
func (i *instance) beginQuality() error {
	if i.phase != INITIAL_PHASE {
		return fmt.Errorf("cannot transition from %s to %s", i.phase, QUALITY_PHASE)
	}
	// Broadcast input value and wait up to Δ to receive from others.
	i.phase = QUALITY_PHASE
	i.phaseTimeout = i.alarmAfterSynchrony()
	i.broadcast(i.round, QUALITY_PHASE, i.input, false, nil)
	return nil
}

// Attempts to end the QUALITY phase and begin PREPARE based on current state.
func (i *instance) tryQuality() error {
	if i.phase != QUALITY_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, QUALITY_PHASE)
	}
	// Wait either for a strong quorum that agree on our proposal,
	// or for the timeout to expire.
	foundQuorum := i.quality.HasStrongQuorumFor(i.proposal.Key())
	timeoutExpired := atOrAfter(i.participant.host.Time(), i.phaseTimeout)

	if foundQuorum {
		// Keep current proposal.
	} else if timeoutExpired {
		strongQuora := i.quality.ListStrongQuorumValues()
		i.proposal = findFirstPrefixOf(i.proposal, strongQuora)
	}

	if foundQuorum || timeoutExpired {
		// Add prefixes with quorum to candidates (skipping base chain, which is already there).
		for l := range i.proposal {
			if l > 0 {
				i.candidates = append(i.candidates, i.proposal.Prefix(l))
			}
		}
		i.value = i.proposal
		i.log("adopting proposal/value %s", &i.proposal)
		i.beginPrepare(nil)
	}

	return nil
}

// beginConverge initiates CONVERGE_PHASE justified by the given justification.
func (i *instance) beginConverge(justification *Justification) {
	if justification.Vote.Round != i.round-1 {
		// For safety assert that the justification given belongs to the right round.
		panic("justification for which to begin converge does not belong to expected round")
	}

	i.phase = CONVERGE_PHASE
	i.phaseTimeout = i.alarmAfterSynchrony()

	// Notify the round's convergeState that the self participant has begun the
	// CONVERGE phase. Because, we cannot guarantee that the CONVERGE message
	// broadcasts are delivered to self synchronously.
	i.getRound(i.round).converged.SetSelfValue(i.proposal, justification)

	i.broadcast(i.round, CONVERGE_PHASE, i.proposal, true, justification)
}

// Attempts to end the CONVERGE phase and begin PREPARE based on current state.
func (i *instance) tryConverge() error {
	if i.phase != CONVERGE_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, CONVERGE_PHASE)
	}
	// The CONVERGE phase timeout doesn't wait to hear from >⅔ of power.
	timeoutExpired := atOrAfter(i.participant.host.Time(), i.phaseTimeout)
	if !timeoutExpired {
		return nil
	}

	possibleDecisionLastRound := !i.getRound(i.round - 1).committed.HasStrongQuorumFor("")
	winner := i.getRound(i.round).converged.FindMaxTicketProposal(i.powerTable)
	if winner.Chain.IsZero() {
		return fmt.Errorf("no values at CONVERGE")
	}
	justification := winner.Justification
	// If the winner is not a candidate but it could possibly have been decided by another participant
	// in the last round, consider it a candidate.
	if !i.isCandidate(winner.Chain) && winner.Justification.Vote.Step == PREPARE_PHASE && possibleDecisionLastRound {
		i.log("⚠️ swaying from %s to %s by CONVERGE", &i.proposal, &winner.Chain)
		i.candidates = append(i.candidates, winner.Chain)
	}
	if i.isCandidate(winner.Chain) {
		i.proposal = winner.Chain
		i.log("adopting proposal %s after converge", &winner.Chain)
	} else {
		// Else preserve own proposal.
		fallback, ok := i.getRound(i.round).converged.FindProposalFor(i.proposal)
		if !ok {
			panic("own proposal not found at CONVERGE")
		}
		justification = fallback.Justification
	}
	// NOTE: FIP-0086 says to loop to next lowest ticket, rather than fall back to own proposal.
	// But using own proposal is valid (the spec can't assume any others have been received),
	// considering others is an optimisation.

	i.value = i.proposal
	i.beginPrepare(justification)
	return nil
}

// Sends this node's PREPARE message and begins the PREPARE phase.
func (i *instance) beginPrepare(justification *Justification) {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE_PHASE
	i.phaseTimeout = i.alarmAfterSynchrony()

	i.broadcast(i.round, PREPARE_PHASE, i.value, false, justification)
}

// Attempts to end the PREPARE phase and begin COMMIT based on current state.
func (i *instance) tryPrepare() error {
	if i.phase != PREPARE_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, PREPARE_PHASE)
	}

	prepared := i.getRound(i.round).prepared
	proposalKey := i.proposal.Key()
	foundQuorum := prepared.HasStrongQuorumFor(proposalKey)
	timedOut := atOrAfter(i.participant.host.Time(), i.phaseTimeout)
	quorumNotPossible := !prepared.couldReachStrongQuorumFor(proposalKey)
	phaseComplete := timedOut && prepared.ReceivedFromStrongQuorum()

	if foundQuorum {
		i.value = i.proposal
	} else if quorumNotPossible || phaseComplete {
		i.value = ECChain{}
	}

	if foundQuorum || quorumNotPossible || phaseComplete {
		i.beginCommit()
	} else if timedOut {
		if err := i.tryRebroadcast(); err != nil {
			return fmt.Errorf("failed to rebroadcast at %s step: %w", i.phase, err)
		}
	}

	return nil
}

func (i *instance) beginCommit() {
	i.phase = COMMIT_PHASE
	i.phaseTimeout = i.alarmAfterSynchrony()

	// The PREPARE phase exited either with i.value == i.proposal having a strong quorum agreement,
	// or with i.value == bottom otherwise.
	// No justification is required for committing bottom.
	var justification *Justification
	if !i.value.IsZero() {
		if quorum, ok := i.getRound(i.round).prepared.FindStrongQuorumFor(i.value.Key()); ok {
			// Found a strong quorum of PREPARE, build the justification for it.
			justification = i.buildJustification(quorum, i.round, PREPARE_PHASE, i.value)

			// Update the round's committed quorumState with the self participant
			// justification for non-zero value. This is to relax the need for guarantee that
			// the justification from COMMIT message broadcasts are delivered to self
			// synchronously.
			i.getRound(i.round).committed.ReceiveJustification(i.value, justification)
		} else {
			panic("beginCommit with no strong quorum for non-bottom value")
		}
	}

	i.broadcast(i.round, COMMIT_PHASE, i.value, false, justification)
}

func (i *instance) tryCommit(round uint64) error {
	// Unlike all other phases, the COMMIT phase stays open to new messages even after an initial quorum is reached,
	// and the algorithm moves on to the next round.
	// A subsequent COMMIT message can cause the node to decide, so there is no check on the current phase.
	committed := i.getRound(round).committed
	quorumValue, foundStrongQuorum := committed.FindStrongQuorumValue()
	timedOut := atOrAfter(i.participant.host.Time(), i.phaseTimeout)
	phaseComplete := timedOut && committed.ReceivedFromStrongQuorum()

	if foundStrongQuorum && !quorumValue.IsZero() {
		// A participant may be forced to decide a value that's not its preferred chain.
		// The participant isn't influencing that decision against their interest, just accepting it.
		i.value = quorumValue
		i.beginDecide(round)
	} else if i.round == round && i.phase == COMMIT_PHASE && (foundStrongQuorum || phaseComplete) {
		if foundStrongQuorum {
			// If there is a strong quorum for bottom, carry forward the existing proposal.
		} else {
			// If there is no strong quorum for bottom, there must be a COMMIT for some other value.
			// There can only be one such value since it must be justified by a strong quorum of PREPAREs.
			// Some other participant could possibly have observed a strong quorum for that value,
			// since they might observe votes from ⅓ of honest power plus a ⅓ equivocating adversary.
			// Sway to consider that value as a candidate, even if it wasn't the local proposal.
			for _, v := range committed.ListAllValues() {
				if !v.IsZero() {
					if !i.isCandidate(v) {
						i.log("⚠️ swaying from %s to %s by COMMIT", &i.input, &v)
						i.candidates = append(i.candidates, v)
					}
					if !v.Eq(i.proposal) {
						i.proposal = v
						i.log("adopting proposal %s after commit", &i.proposal)
					}
					break
				}
			}
		}
		i.beginNextRound()
	} else if timedOut {
		if err := i.tryRebroadcast(); err != nil {
			return fmt.Errorf("failed to rebroadcast at %s step: %w", i.phase, err)
		}
	}

	return nil
}

func (i *instance) beginDecide(round uint64) {
	i.phase = DECIDE_PHASE
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
}

// Skips immediately to the DECIDE phase and sends a DECIDE message
// without waiting for a strong quorum of COMMITs in any round.
// The provided justification must justify the value being decided.
func (i *instance) skipToDecide(value ECChain, justification *Justification) {
	i.phase = DECIDE_PHASE
	i.proposal = value
	i.value = i.proposal
	i.broadcast(0, DECIDE_PHASE, i.value, false, justification)
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
		if err := i.tryRebroadcast(); err != nil {
			return fmt.Errorf("failed to rebroadcast at %s step: %w", i.phase, err)
		}
	}

	return nil
}

func (i *instance) getRound(r uint64) *roundState {
	round, ok := i.rounds[r]
	if !ok {
		round = newRoundState(i.powerTable)
		i.rounds[r] = round
	}
	return round
}

func (i *instance) beginNextRound() {
	i.round += 1
	i.log("moving to round %d with %s", i.round, i.proposal.String())

	prevRound := i.getRound(i.round - 1)
	// Proposal was updated at the end of COMMIT phase to be some value for which
	// this node received a COMMIT message (bearing justification), if there were any.
	// If there were none, there must have been a strong quorum for bottom instead.
	var justification *Justification
	if quorum, ok := prevRound.committed.FindStrongQuorumFor(""); ok {
		// Build justification for strong quorum of COMMITs for bottom in the previous round.
		justification = i.buildJustification(quorum, i.round-1, COMMIT_PHASE, ECChain{})
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
func (i *instance) skipToRound(round uint64, chain ECChain, justification *Justification) {
	i.log("skipping from round %d to round %d with %s", i.round, round, i.proposal.String())
	i.round = round

	if justification.Vote.Step == PREPARE_PHASE {
		i.log("⚠️ swaying from %s to %s by skip to round %d", &i.proposal, chain, i.round)
		i.candidates = append(i.candidates, chain)
		i.proposal = chain
	}
	i.beginConverge(justification)
}

// Returns whether a chain is acceptable as a proposal for this instance to vote for.
// This is "EC Compatible" in the pseudocode.
func (i *instance) isCandidate(c ECChain) bool {
	for _, candidate := range i.candidates {
		if c.Eq(candidate) {
			return true
		}
	}
	return false
}

func (i *instance) terminate(decision *Justification) {
	i.log("✅ terminated %s during round %d", &i.value, i.round)
	i.phase = TERMINATED_PHASE
	i.value = decision.Vote.Value
	i.terminationValue = decision
}

func (i *instance) terminated() bool {
	return i.phase == TERMINATED_PHASE
}

func (i *instance) broadcast(round uint64, step Phase, value ECChain, createTicket bool, justification *Justification) {
	p := Payload{
		Instance:         i.instanceID,
		Round:            round,
		Step:             step,
		SupplementalData: *i.supplementalData,
		Value:            value,
	}
	mb := NewMessageBuilder(&i.powerTable)
	mb.SetPayload(p)
	mb.SetJustification(justification)
	if createTicket {
		mb.SetBeaconForTicket(i.beacon)
	}

	_ = i.participant.host.RequestBroadcast(mb)
	i.broadcasted.record(mb)
}

// tryRebroadcast checks whether the conditions for rebroadcasting messages are
// met, and if so rebroadcasts messages from current and previous rounds.
// Rebroadcast is needed when both of the following conditions are met:
//   - There has been no progress since latest broadcast.
//   - Rebroadcast backoff duration has elapsed.
//
// If there has been progress since latest broadcast this function resets the
// number of rebroadcast attempts and returns immediately.
func (i *instance) tryRebroadcast() error {
	if i.progressedSinceLatestBroadcast() && i.rebroadcastAttempts != 0 {
		// Rebroadcast is not necessary, since progress has been made. Reset rebroadcast
		// parameters.
		i.rebroadcastAttempts = 0
		i.rebroadcastTimeout = time.Time{}
		i.broadcasted.lastBroadcastRound = 0
		i.broadcasted.lastBroadcastPhase = INITIAL_PHASE
	}

	if i.rebroadcastAttempts == 0 && i.rebroadcastTimeout.IsZero() {
		// It is the first time that rebroadcast has become necessary; set initial
		// rebroadcast timeout relative to the phase timeout, and schedule a rebroadcast.
		//
		// Determine the offset for the first rebroadcast alarm depending on current
		// instance phase and schedule the first alarm:
		//  * If in DECIDE phase, use current time as offset. Because, DECIDE phase does
		//    not have any phase timeout and may be too far in the past.
		//  * Otherwise, use the phase timeout.
		var rebroadcastTimeoutOffset time.Time
		if i.phase == DECIDE_PHASE {
			rebroadcastTimeoutOffset = i.participant.host.Time()
		} else {
			rebroadcastTimeoutOffset = i.phaseTimeout
		}
		i.rebroadcastTimeout = rebroadcastTimeoutOffset.Add(i.participant.rebroadcastAfter(0))
		i.participant.host.SetAlarm(i.rebroadcastTimeout)
		return nil
	}

	if i.rebroadcastTimeoutElapsed() {
		// Rebroadcast now that the corresponding timeout has elapsed, and schedule the
		// successive rebroadcast.
		err := i.rebroadcast()
		i.rebroadcastAttempts++

		// Use current host time as the offset for the next alarm to assure that rate of
		// broadcasted messages grows relative to the actual time at which an alarm is
		// triggered , not the absolute alarm time. This would avoid a "runaway
		// rebroadcast" scenario where rebroadcast timeout consistently remains behind
		// current time due to the discrepancy between set alarm time and the actual time
		// at which the alarm is triggered.
		i.rebroadcastTimeout = i.participant.host.Time().Add(i.participant.rebroadcastAfter(i.rebroadcastAttempts))
		i.participant.host.SetAlarm(i.rebroadcastTimeout)
		return err
	}

	// Rebroadcast timout is set but has not elapsed yet; nothing to do.
	return nil
}

func (i *instance) rebroadcastTimeoutElapsed() bool {
	now := i.participant.host.Time()
	return atOrAfter(now, i.rebroadcastTimeout)
}

func (i *instance) progressedSinceLatestBroadcast() bool {
	if i.broadcasted.lastBroadcastPhase == DECIDE_PHASE {
		// Progress could not have been made if the latest broadcasted phase is DECIDE.
		// Otherwise this instance would have been terminated.
		return false
	}
	return i.phase != i.broadcasted.lastBroadcastPhase || i.round != i.broadcasted.lastBroadcastRound
}

func (i *instance) rebroadcast() error {

	// Rebroadcast all messages from the current and previous rounds, unless the
	// instance has progressed to DECIDE phase. In which case, only DECIDE message is
	// rebroadcasted.
	//
	// Note that the implementation here rebroadcasts more messages than FIP-0086
	// strictly requires. Because, the cost of rebroadcasting additional messages is
	// small compared to the reduction in need for rebroadcast.
	for _, mbs := range i.broadcasted.messagesByRound {
		for _, mb := range mbs {
			if err := i.participant.host.RequestBroadcast(mb); err != nil {
				return err
			}
		}
	}
	return nil
}

// Sets an alarm to be delivered after a synchrony delay.
// The delay duration increases with each round.
// Returns the absolute time at which the alarm will fire.
func (i *instance) alarmAfterSynchrony() time.Time {
	delta := time.Duration(float64(i.participant.delta) *
		math.Pow(i.participant.deltaBackOffExponent, float64(i.round)))
	timeout := i.participant.host.Time().Add(2 * delta)
	i.participant.host.SetAlarm(timeout)
	return timeout
}

// Builds a justification for a value from a quorum result.
func (i *instance) buildJustification(quorum QuorumResult, round uint64, phase Phase, value ECChain) *Justification {
	aggSignature, err := quorum.Aggregate(i.participant.host)
	if err != nil {
		panic(xerrors.Errorf("aggregating for phase %v: %v", phase, err))
	}
	return &Justification{
		Vote: Payload{
			Instance:         i.instanceID,
			Round:            round,
			Step:             phase,
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
		i.tracer.Log("{%d}: %s (round %d, step %s, proposal %s, value %s)", i.instanceID, msg,
			i.round, i.phase, &i.proposal, &i.value)
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
	sendersTotalPower uint16
	// The power supporting each chain so far.
	chainSupport map[ChainKey]chainSupport
	// Table of senders' power.
	powerTable PowerTable
	// Stores justifications received for some value.
	receivedJustification map[ChainKey]*Justification
}

// A chain value and the total power supporting it
type chainSupport struct {
	chain           ECChain
	power           uint16
	signatures      map[ActorID][]byte
	hasStrongQuorum bool
}

// Creates a new, empty quorum state.
func newQuorumState(powerTable PowerTable) *quorumState {
	return &quorumState{
		senders:               map[ActorID]struct{}{},
		chainSupport:          map[ChainKey]chainSupport{},
		powerTable:            powerTable,
		receivedJustification: map[ChainKey]*Justification{},
	}
}

// Receives a chain from a sender.
// Ignores any subsequent value from a sender from which a value has already been received.
func (q *quorumState) Receive(sender ActorID, value ECChain, signature []byte) {
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
func (q *quorumState) ReceiveEachPrefix(sender ActorID, values ECChain) {
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
func (q *quorumState) receiveSender(sender ActorID) (uint16, bool) {
	if _, found := q.senders[sender]; found {
		return 0, false
	}
	q.senders[sender] = struct{}{}
	senderPower, _, _ := q.powerTable.Get(sender)
	q.sendersTotalPower += senderPower
	return senderPower, true
}

// Receives a chain from a sender.
func (q *quorumState) receiveInner(sender ActorID, value ECChain, power uint16, signature []byte) {
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
func (q *quorumState) ReceiveJustification(value ECChain, justification *Justification) {
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
func (q *quorumState) ListAllValues() []ECChain {
	var chains []ECChain
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
func (q *quorumState) HasStrongQuorumFor(key ChainKey) bool {
	supportForChain, ok := q.chainSupport[key]
	return ok && supportForChain.hasStrongQuorum
}

// couldReachStrongQuorumFor checks whether the given chain can possibly reach
// strong quorum.
func (q *quorumState) couldReachStrongQuorumFor(key ChainKey) bool {
	var supportingPower uint16
	if supportForChain, found := q.chainSupport[key]; found {
		supportingPower = supportForChain.power
	}
	// A strong quorum is only feasible when the total support for the given chain,
	// combined with the aggregate power of not yet voted participants, exceeds ⅔ of
	// total power.
	unvotedPower := q.powerTable.ScaledTotal - q.sendersTotalPower
	possibleSupport := supportingPower + unvotedPower
	return IsStrongQuorum(possibleSupport, q.powerTable.ScaledTotal)
}

type QuorumResult struct {
	// Signers is an array of indexes into the powertable, sorted in increasing order
	Signers    []int
	PubKeys    []PubKey
	Signatures [][]byte
}

func (q QuorumResult) Aggregate(v Verifier) ([]byte, error) {
	return v.Aggregate(q.PubKeys, q.Signatures)
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
func (q *quorumState) FindStrongQuorumFor(key ChainKey) (QuorumResult, bool) {
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
	pubkeys := make([]PubKey, 0, len(signatures))
	var justificationPower uint16
	for i, idx := range signers {
		if idx >= len(q.powerTable.Entries) {
			panic(fmt.Sprintf("invalid signer index: %d for %d entries", idx, len(q.powerTable.Entries)))
		}
		power := q.powerTable.ScaledPower[idx]
		entry := q.powerTable.Entries[idx]
		justificationPower += power
		signatures = append(signatures, chainSupport.signatures[entry.ID])
		pubkeys = append(pubkeys, entry.PubKey)
		if IsStrongQuorum(justificationPower, q.powerTable.ScaledTotal) {
			return QuorumResult{
				Signers:    signers[:i+1],
				PubKeys:    pubkeys,
				Signatures: signatures,
			}, true
		}
	}

	// There is likely a bug. Because, chainSupport.hasStrongQuorum must have been
	// true for the code to reach this point. Hence, the fatal error.
	panic("strong quorum exists but could not be found")
}

// Returns a list of the chains which have reached an agreeing strong quorum.
// Chains are returned in descending length order.
// This is appropriate for use in the QUALITY phase, where each participant
// votes for every prefix of their preferred chain.
// Panics if there are multiple chains of the same length with strong quorum
// (signalling a violation of assumptions about the adversary).
func (q *quorumState) ListStrongQuorumValues() []ECChain {
	var withQuorum []ECChain
	for key, cp := range q.chainSupport {
		if cp.hasStrongQuorum {
			withQuorum = append(withQuorum, q.chainSupport[key].chain)
		}
	}
	sort.Slice(withQuorum, func(i, j int) bool {
		return len(withQuorum[i]) > len(withQuorum[j])
	})
	prevLength := 0
	for _, v := range withQuorum {
		if len(v) == prevLength {
			panic(fmt.Sprintf("multiple chains of length %d with strong quorum", prevLength))
		}
		prevLength = len(v)
	}
	return withQuorum
}

// Returns the chain with a strong quorum of support, if there is one.
// This is appropriate for use in PREPARE/COMMIT/DECIDE phases, where each participant
// casts a single vote.
// Panics if there are multiple chains with strong quorum
// (signalling a violation of assumptions about the adversary).
func (q *quorumState) FindStrongQuorumValue() (quorumValue ECChain, foundQuorum bool) {
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
	// Stores this participant's value so the participant can use it even if it doesn't receive its own
	// CONVERGE message (which carries the ticket) in a timely fashion.
	self *ConvergeValue
	// Participants from which a message has been received.
	senders map[ActorID]struct{}
	// Chains indexed by key.
	values map[ChainKey]ConvergeValue
	// Tickets provided by proposers of each chain.
	tickets map[ChainKey][]ConvergeTicket
}

type ConvergeValue struct {
	Chain         ECChain
	Justification *Justification
}

type ConvergeTicket struct {
	Sender ActorID
	Ticket Ticket
}

func newConvergeState() *convergeState {
	return &convergeState{
		senders: map[ActorID]struct{}{},
		values:  map[ChainKey]ConvergeValue{},
		tickets: map[ChainKey][]ConvergeTicket{},
	}
}

// SetSelfValue sets the participant's locally-proposed converge value.
// This means the participant need not rely on messages broadcast to be received by itself.
// See HasSelfValue.
func (c *convergeState) SetSelfValue(value ECChain, justification *Justification) {
	c.self = &ConvergeValue{
		Chain:         value,
		Justification: justification,
	}
}

// HasSelfValue checks whether the participant recorded a converge value.
// See SetSelfValue.
func (c *convergeState) HasSelfValue() bool {
	return c.self != nil
}

// Receives a new CONVERGE value from a sender.
// Ignores any subsequent value from a sender from which a value has already been received.
func (c *convergeState) Receive(sender ActorID, value ECChain, ticket Ticket, justification *Justification) error {
	if value.IsZero() {
		return fmt.Errorf("bottom cannot be justified for CONVERGE")
	}
	if _, ok := c.senders[sender]; ok {
		return nil
	}
	c.senders[sender] = struct{}{}
	key := value.Key()

	// Keep only the first justification and ticket received for a value.
	if _, found := c.values[key]; !found {
		c.values[key] = ConvergeValue{Chain: value, Justification: justification}
		c.tickets[key] = append(c.tickets[key], ConvergeTicket{Sender: sender, Ticket: ticket})
	}
	return nil
}

// Returns the value with the highest ticket, weighted by sender power.
// Returns the self value (which may be zero) if and only if no other value is found.
// Note that introduces a brief possibility where the nodes own value may be ignored
// while waiting to learn its ticket after receiving a message from some other.
func (c *convergeState) FindMaxTicketProposal(table PowerTable) ConvergeValue {
	// Non-determinism in case of matching tickets from an equivocation is ok.
	// If the same ticket is used for two different values then either we get a decision on one of them
	// only or we go to a new round. Eventually there is a round where the max ticket is held by a
	// correct participant, who will not double vote.
	var maxTicket *big.Int
	var maxValue ConvergeValue

	for key, value := range c.values {
		for _, ticket := range c.tickets[key] {
			_, senderPower, _ := table.Get(ticket.Sender)
			ticketAsInt := new(big.Int).SetBytes(ticket.Ticket)
			weightedTicket := new(big.Int).Mul(ticketAsInt, senderPower)
			if maxTicket == nil || weightedTicket.Cmp(maxTicket) > 0 {
				maxTicket = weightedTicket
				maxValue = value
			}
		}
	}

	if maxTicket == nil && c.HasSelfValue() {
		return *c.self
	}
	return maxValue
}

// Finds some proposal which matches a specific value.
// This searches values received in messages first, falling back to the participant's self value
// only if necessary.
func (c *convergeState) FindProposalFor(chain ECChain) (ConvergeValue, bool) {
	for _, value := range c.values {
		if value.Chain.Eq(chain) {
			return value, true
		}
	}

	if c.HasSelfValue() && c.self.Chain.Eq(chain) {
		return *c.self, true
	}
	return ConvergeValue{}, false
}

type broadcastState struct {
	lastBroadcastPhase Phase
	lastBroadcastRound uint64
	messagesByRound    map[uint64][]*MessageBuilder
}

func newBroadcastState() *broadcastState {
	return &broadcastState{
		messagesByRound: make(map[uint64][]*MessageBuilder),
	}
}

// record stores messages that are required should rebroadcast becomes necessary.
// The messages stored depend on the current progress of instance. If the
// instance progresses to DECIDE then only the decide message will be recorded.
// Otherwise, all messages except QUALITY from the current and previous rounds
// (i.e. the latest two rounds) are recorded.
//
// Note, the messages recorded are more than what FIL-00896 strictly requires at
// the benefit of less reliance on rebroadcast and reduction in the possibility
// of participants getting stuck.
func (bs *broadcastState) record(mb *MessageBuilder) {
	bs.lastBroadcastPhase = mb.payload.Step
	// Payload round will always be zero at DECIDE step. This is OK because
	// progressedSinceLatestBroadcast handles this complexity by always returning
	// false if lastBroadcastPhase is DECIDE. Therefore, the actual round at which
	// DECIDE step is reached is irrelevant.
	bs.lastBroadcastRound = mb.payload.Round
	switch mb.payload.Step {
	case DECIDE_PHASE:
		// Clear all previous messages, as only DECIDE message need to be rebroadcasted.
		// Note that DECIDE message is not associated to any round, and is always
		// broadcasted using round zero.
		bs.messagesByRound = make(map[uint64][]*MessageBuilder)
		bs.messagesByRound[0] = []*MessageBuilder{mb}
	case COMMIT_PHASE, PREPARE_PHASE, CONVERGE_PHASE:
		bs.messagesByRound[mb.payload.Round] = append(bs.messagesByRound[mb.payload.Round], mb)
		// Remove all messages that are older than the latest two rounds.
		for round := int(mb.payload.Round) - 2; round >= 0; round-- {
			redundantRound := uint64(round)
			delete(bs.messagesByRound, redundantRound)
		}
	default:
		// No need to rebroadcast QUALITY messages.
	}
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

// Returns the first candidate value that is a prefix of the preferred value, or the base of preferred.
func findFirstPrefixOf(preferred ECChain, candidates []ECChain) ECChain {
	for _, v := range candidates {
		if preferred.HasPrefix(v) {
			return v
		}
	}

	// No candidates are a prefix of preferred.
	return preferred.BaseChain()
}

func divCeil(a, b uint32) uint32 {
	quo := a / b
	rem := a % b
	if rem != 0 {
		quo += 1
	}
	return quo
}

// Check whether a portion of storage power is a strong quorum of the total
func IsStrongQuorum(part uint16, whole uint16) bool {
	// uint32 because 2 * whole exceeds uint16
	return uint32(part) >= divCeil(2*uint32(whole), 3)
}

// Check whether a portion of storage power is a weak quorum of the total
func hasWeakQuorum(part, whole uint16) bool {
	// Must be strictly greater than 1/3. Otherwise, there could be a strong quorum.
	return uint32(part) > divCeil(uint32(whole), 3)
}

// Tests whether lhs is equal to or greater than rhs.
func atOrAfter(lhs time.Time, rhs time.Time) bool {
	return lhs.After(rhs) || lhs.Equal(rhs)
}
