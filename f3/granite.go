package f3

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/filecoin-project/go-bitfield"
	"math"
	"sort"
)

type GraniteConfig struct {
	// Initial delay for partial synchrony.
	Delta float64
	// Change to delta in each round after the first.
	DeltaRate float64
	// Absolut extra value to add to delta after the first few unsuccessful rounds.
	DeltaExtra float64
	// Time to next clock tick (according to local clock)
	// Used in timeouts in order to synchronize participants
	// (by not carrying significant delays from previous/starting rounds)
	ClockTickDelta float64
}

type VRFer interface {
	VRFTicketSource
	VRFTicketVerifier
}

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
	Justification Justification
}

type Justification struct {
	// Vote is the payload that is signed by the signature
	Vote Payload
	// Indexes in the base power table of the signers (bitset)
	Signers bitfield.BitField
	// BLS aggregate signature of signers
	Signature []byte
}

// Fields of the message that make up the signature payload.
type Payload struct {
	// GossiPBFT instance (epoch) number.
	Instance uint64
	// GossiPBFT round number.
	Round uint64
	// GossiPBFT step name.
	Step Phase
	// Chain of tipsets proposed/voted for finalisation.
	// Always non-empty; the first entry is the base tipset finalised in the previous instance.
	Value ECChain
}

func (p Payload) MarshalForSigning(nn NetworkName) []byte {
	var buf bytes.Buffer
	buf.WriteString(DOMAIN_SEPARATION_TAG)
	buf.WriteString(nn.SignatureSeparationTag())
	_ = binary.Write(&buf, binary.BigEndian, p.Instance)
	_ = binary.Write(&buf, binary.BigEndian, p.Round)
	_ = binary.Write(&buf, binary.BigEndian, []byte(p.Step.String()))
	for _, t := range p.Value {
		_ = binary.Write(&buf, binary.BigEndian, t.Epoch)
		buf.Write(t.CID.Bytes())
		_ = binary.Write(&buf, binary.BigEndian, t.Weight)
	}
	return buf.Bytes()
}

func (m GMessage) String() string {
	// FIXME This needs value receiver to work, for reasons I cannot figure out.
	return fmt.Sprintf("%s{%d}(%d %s)", m.Vote.Step, m.Vote.Instance, m.Vote.Round, &m.Vote.Value)
}

// A single Granite consensus instance.
type instance struct {
	config        GraniteConfig
	host          Host
	vrf           VRFer
	participantID ActorID
	instanceID    uint64
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
	phaseTimeout float64
	// This instance's proposal for the current round.
	// This is set after the QUALITY phase, and changes only at the end of a full round.
	proposal ECChain
	// The value to be transmitted at the next phase.
	// This value may change away from the proposal between phases.
	value ECChain
	// Queue of messages to be synchronously processed before returning from top-level call.
	inbox []*GMessage
	// Quality phase state (only for round 0)
	quality *quorumState
	// State for each round of phases.
	// State from prior rounds must be maintained to provide justification for values in subsequent rounds.
	rounds map[uint64]*roundState
	// Acceptable chain
	acceptable ECChain
	// Decision state. Collects DECIDE messages until a decision can be made, independently of protocol phases/rounds.
	decision *quorumState
	// The latest multiplier applied to the timeout
	deltaRate float64
}

func newInstance(
	config GraniteConfig,
	host Host,
	vrf VRFer,
	participantID ActorID,
	instanceID uint64,
	input ECChain,
	powerTable PowerTable,
	beacon []byte) (*instance, error) {
	if input.IsZero() {
		return nil, fmt.Errorf("input is empty")
	}
	return &instance{
		config:        config,
		host:          host,
		vrf:           vrf,
		participantID: participantID,
		instanceID:    instanceID,
		input:         input,
		powerTable:    powerTable,
		beacon:        beacon,
		round:         0,
		phase:         INITIAL_PHASE,
		proposal:      input,
		value:         ECChain{},
		quality:       newQuorumState(powerTable),
		rounds: map[uint64]*roundState{
			0: newRoundState(powerTable),
		},
		acceptable: input,
		decision:   newQuorumState(powerTable),
		deltaRate:  config.DeltaRate,
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
	if err := i.beginQuality(); err != nil {
		return err
	}
	return i.drainInbox()
}

// Receives a new acceptable chain and updates its current acceptable chain.
func (i *instance) receiveAcceptable(chain ECChain) {
	i.acceptable = chain
}

func (i *instance) Receive(msg *GMessage) error {
	if i.terminated() {
		return fmt.Errorf("senders message after decision")
	}
	if len(i.inbox) > 0 {
		return fmt.Errorf("senders message while already processing inbox")
	}

	// Enqueue the message for synchronous processing.
	i.enqueueInbox(msg)
	return i.drainInbox()
}

func (i *instance) ReceiveAlarm(_ string) error {
	if err := i.tryCompletePhase(); err != nil {
		return fmt.Errorf("failed completing protocol phase: %w", err)
	}

	// A phase may have been successfully completed.
	// Re-process any queued messages for the next phase.
	return i.drainInbox()
}

func (i *instance) Describe() string {
	return fmt.Sprintf("P%d{%d}, round %d, phase %s", i.participantID, i.instanceID, i.round, i.phase)
}

func (i *instance) enqueueInbox(msg *GMessage) {
	i.inbox = append(i.inbox, msg)
}

func (i *instance) drainInbox() error {
	for len(i.inbox) > 0 {
		// Process one message.
		// Note the message being processed is left in the inbox until after processing,
		// as a signal that this loop is currently draining the inbox.
		if err := i.receiveOne(i.inbox[0]); err != nil {
			return fmt.Errorf("failed receiving message: %w", err)
		}
		i.inbox = i.inbox[1:]
	}

	return nil
}

// Processes a single message.
func (i *instance) receiveOne(msg *GMessage) error {
	if i.phase == TERMINATED_PHASE {
		return nil // No-op
	}
	round := i.roundState(msg.Vote.Round)

	// Drop any messages that can never be valid.
	if !i.isValid(msg) {
		i.log("dropping invalid %s", msg)
		return nil
	}

	if err := i.isJustified(msg); err != nil {
		i.log("dropping unjustified %s from sender %v, error: %s", msg, msg.Sender, err)
		return nil
	}

	switch msg.Vote.Step {
	case QUALITY_PHASE:
		// Receive each prefix of the proposal independently.
		for j := range msg.Vote.Value.Suffix() {
			prefix := msg.Vote.Value.Prefix(j + 1)
			i.quality.Receive(msg.Sender, prefix, msg.Signature, msg.Justification)
		}
	case CONVERGE_PHASE:
		if err := round.converged.Receive(msg.Vote.Value, msg.Ticket); err != nil {
			return fmt.Errorf("failed processing CONVERGE message: %w", err)
		}
	case PREPARE_PHASE:
		round.prepared.Receive(msg.Sender, msg.Vote.Value, msg.Signature, msg.Justification)
	case COMMIT_PHASE:
		round.committed.Receive(msg.Sender, msg.Vote.Value, msg.Signature, msg.Justification)
	case DECIDE_PHASE:
		i.decision.Receive(msg.Sender, msg.Vote.Value, msg.Signature, msg.Justification)
	default:
		i.log("unexpected message %v", msg)
	}

	// Try to complete the current phase.
	// Every COMMIT phase stays open to new messages even after the protocol moves on to
	// a new round. Late-arriving COMMITS can still (must) cause a local decision, *in that round*.
	if msg.Vote.Step == COMMIT_PHASE && i.phase != DECIDE_PHASE {
		return i.tryCommit(msg.Vote.Round)
	}
	return i.tryCompletePhase()
}

// Attempts to complete the current phase and round.
func (i *instance) tryCompletePhase() error {
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

// Checks whether a message is valid.
// An invalid message can never become valid, so may be dropped.
func (i *instance) isValid(msg *GMessage) bool {
	if !i.powerTable.Has(msg.Sender) {
		i.log("sender with zero power or not in power table")
		return false
	}

	_, pubKey := i.powerTable.Get(msg.Sender)

	if !(msg.Vote.Value.IsZero() || msg.Vote.Value.HasBase(i.input.Base())) {
		i.log("unexpected base %s", &msg.Vote.Value)
		return false
	}
	if msg.Vote.Step == QUALITY_PHASE {
		return msg.Vote.Round == 0 && !msg.Vote.Value.IsZero()
	} else if msg.Vote.Step == CONVERGE_PHASE {
		if msg.Vote.Round == 0 ||
			msg.Vote.Value.IsZero() ||
			!i.vrf.VerifyTicket(i.beacon, i.instanceID, msg.Vote.Round, pubKey, msg.Ticket) {
			return false
		}
	} else if msg.Vote.Step == DECIDE_PHASE {
		// DECIDE needs no justification
		return !msg.Vote.Value.IsZero()
	}

	sigPayload := msg.Vote.MarshalForSigning(TODONetworkName)
	if !i.host.Verify(pubKey, sigPayload, msg.Signature) {
		i.log("invalid signature on %v", msg)
		return false
	}

	return true
}

func (i *instance) VerifyJustification(justification Justification) error {
	power := NewStoragePower(0)
	signers := make([]PubKey, 0)
	if err := justification.Signers.ForEach(func(bit uint64) error {
		if int(bit) >= len(i.powerTable.Entries) {
			return fmt.Errorf("invalid signer index: %d", bit)
		}
		power.Add(power, i.powerTable.Entries[bit].Power)
		signers = append(signers, i.powerTable.Entries[bit].PubKey)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over signers: %w", err)
	}

	payload := justification.Vote.MarshalForSigning(TODONetworkName)

	if !i.host.VerifyAggregate(payload, justification.Signature, signers) {
		return fmt.Errorf("verification of the aggregate failed: %v", justification)
	}

	return nil
}

func (i *instance) isJustified(msg *GMessage) error {
	switch msg.Vote.Step {
	case QUALITY_PHASE, PREPARE_PHASE:
		return nil

	case CONVERGE_PHASE:
		//CONVERGE is justified by a strong quorum of COMMIT for bottom from the previous round.
		// or a strong quorum of PREPARE for the same value from the previous round.

		prevRound := msg.Vote.Round - 1
		if msg.Justification.Vote.Round != prevRound {
			return fmt.Errorf("CONVERGE %v has evidence from wrong round %d", msg.Vote.Round, msg.Justification.Vote.Round)
		}

		if msg.Justification.Vote.Step == PREPARE_PHASE {
			if msg.Vote.Value.HeadCIDOrZero() != msg.Justification.Vote.Value.HeadCIDOrZero() {
				return fmt.Errorf("CONVERGE for value %v has PREPARE evidence for a different value: %v", msg.Vote.Value, msg.Justification.Vote.Value)
			}
			if msg.Vote.Value.IsZero() {
				return fmt.Errorf("CONVERGE with PREPARE evidence for zero value: %v", msg.Justification.Vote.Value)
			}
		} else if msg.Justification.Vote.Step == COMMIT_PHASE {
			if !msg.Justification.Vote.Value.IsZero() {
				return fmt.Errorf("CONVERGE with COMMIT evidence for non-zero value: %v", msg.Justification.Vote.Value)
			}
		} else {
			return fmt.Errorf("CONVERGE with evidence from wrong step %v", msg.Justification.Vote.Step)
		}

	case COMMIT_PHASE:
		// COMMIT is justified by strong quorum of PREPARE from the same round with the same value.
		// COMMIT for bottom is always justified.

		if msg.Vote.Value.IsZero() {
			//TODO make sure justification is default zero?
			return nil
		}

		if msg.Vote.Round != msg.Justification.Vote.Round {
			return fmt.Errorf("COMMIT %v has evidence from wrong round %d", msg.Vote.Round, msg.Justification.Vote.Round)
		}

		if msg.Justification.Vote.Step != PREPARE_PHASE {
			return fmt.Errorf("COMMIT %v has evidence from wrong step %v", msg.Vote.Round, msg.Justification.Vote.Step)
		}

		if msg.Vote.Value.Head().CID != msg.Justification.Vote.Value.HeadCIDOrZero() {
			return fmt.Errorf("COMMIT %v has evidence for a different value: %v", msg.Vote.Value, msg.Justification.Vote.Value)
		}

	case DECIDE_PHASE:
		// Implement actual justification of DECIDES
		// Example: return fmt.Errorf("DECIDE phase not implemented")
		if msg.Justification.Vote.Step != COMMIT_PHASE {
			return fmt.Errorf("dropping DECIDE %v with evidence from wrong step %v", msg.Vote.Round, msg.Justification.Vote.Step)
		}
		if msg.Vote.Value.IsZero() || msg.Justification.Vote.Value.IsZero() {
			return fmt.Errorf("dropping DECIDE %v with evidence for a zero value: %v", msg.Vote.Value, msg.Justification.Vote.Value)
		}
		if msg.Vote.Value.Head().CID != msg.Justification.Vote.Value.Head().CID {
			return fmt.Errorf("dropping DECIDE %v with evidence for a different value: %v", msg.Vote.Value, msg.Justification.Vote.Value)
		}
		return nil

	default:
		return fmt.Errorf("unknown message step: %v", msg.Vote.Step)
	}

	if msg.Vote.Instance != msg.Justification.Vote.Instance {
		return fmt.Errorf("message with instanceID %v has evidence from wrong instanceID: %v", msg.Vote.Instance, msg.Justification.Vote.Instance)
	}

	return i.VerifyJustification(msg.Justification)

}

// Sends this node's QUALITY message and begins the QUALITY phase.
func (i *instance) beginQuality() error {
	if i.phase != INITIAL_PHASE {
		return fmt.Errorf("cannot transition from %s to %s", i.phase, QUALITY_PHASE)
	}
	// Broadcast input value and wait up to Δ to receive from others.
	i.phase = QUALITY_PHASE
	i.phaseTimeout = i.alarmAfterSynchrony(QUALITY_PHASE.String())
	i.broadcast(i.round, QUALITY_PHASE, i.input, nil, Justification{})
	return nil
}

// Attempts to end the QUALITY phase and begin PREPARE based on current state.
func (i *instance) tryQuality() error {
	if i.phase != QUALITY_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, QUALITY_PHASE)
	}
	// Wait either for a strong quorum that agree on our proposal,
	// or for the timeout to expire.
	foundQuorum := i.quality.HasStrongQuorumAgreement(i.proposal.Head().CID)
	timeoutExpired := i.host.Time() >= i.phaseTimeout

	if foundQuorum {
		// Keep current proposal.
	} else if timeoutExpired {
		strongQuora := i.quality.ListStrongQuorumAgreedValues()
		i.proposal = findFirstPrefixOf(strongQuora, i.proposal)
	}

	if foundQuorum || timeoutExpired {
		i.value = i.proposal
		i.log("adopting proposal/value %s", &i.proposal)
		i.beginPrepare()
	}

	return nil
}

func (i *instance) beginConverge() {
	i.phase = CONVERGE_PHASE
	ticket := i.vrf.MakeTicket(i.beacon, i.instanceID, i.round, i.participantID)
	i.phaseTimeout = i.alarmAfterSynchrony(CONVERGE_PHASE.String())
	prevRoundState := i.roundState(i.round - 1)

	var justification Justification
	if signers, signatures, ok := prevRoundState.committed.FindStrongQuorumAgreement(ZeroTipSetID()); ok {
		value := ECChain{}
		aggSignature := i.host.Aggregate(signatures, nil)
		justificationPayload := Payload{
			Instance: i.instanceID,
			Round:    i.round - 1,
			Step:     COMMIT_PHASE,
			Value:    value,
		}
		justification = Justification{
			Vote:      justificationPayload,
			Signers:   signers,
			Signature: aggSignature,
		}
	} else if signers, signatures, ok = prevRoundState.prepared.FindStrongQuorumAgreement(i.proposal.Head().CID); ok {
		value := i.proposal
		aggSignature := i.host.Aggregate(signatures, nil)
		justificationPayload := Payload{
			Instance: i.instanceID,
			Round:    i.round - 1,
			Step:     PREPARE_PHASE,
			Value:    value,
		}
		justification = Justification{
			Vote:      justificationPayload,
			Signers:   signers,
			Signature: aggSignature,
		}
	} else if justification, ok = prevRoundState.committed.justifiedMessages[i.proposal.Head().CID]; ok {
		//justificationPayload already assigned in the if statement
	} else {
		panic("beginConverge called but no evidence found")
	}

	i.broadcast(i.round, CONVERGE_PHASE, i.proposal, ticket, justification)
}

// Attempts to end the CONVERGE phase and begin PREPARE based on current state.
func (i *instance) tryConverge() error {
	if i.phase != CONVERGE_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, CONVERGE_PHASE)
	}
	timeoutExpired := i.host.Time() >= i.phaseTimeout
	if !timeoutExpired {
		return nil
	}

	i.value = i.roundState(i.round).converged.findMinTicketProposal()
	if i.value.IsZero() {
		return fmt.Errorf("no values at CONVERGE")
	}
	if i.isAcceptable(i.value) {
		// Sway to proposal if the value is acceptable.
		if !i.proposal.Eq(i.value) {
			i.proposal = i.value
			i.log("adopting proposal %s after converge", &i.proposal)
		}
	} else {
		// Vote for not deciding in this round
		i.value = ECChain{}
	}
	i.beginPrepare()

	return nil
}

// Sends this node's PREPARE message and begins the PREPARE phase.
func (i *instance) beginPrepare() {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE_PHASE
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE_PHASE.String())
	i.broadcast(i.round, PREPARE_PHASE, i.value, nil, Justification{})
}

// Attempts to end the PREPARE phase and begin COMMIT based on current state.
func (i *instance) tryPrepare() error {
	if i.phase != PREPARE_PHASE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, PREPARE_PHASE)
	}

	prepared := i.roundState(i.round).prepared
	// Optimisation: we could advance phase once a strong quorum on our proposal is not possible.
	foundQuorum := prepared.HasStrongQuorumAgreement(i.proposal.Head().CID)
	timeoutExpired := i.host.Time() >= i.phaseTimeout

	if foundQuorum {
		i.value = i.proposal
	} else if timeoutExpired {
		i.value = ECChain{}
	}

	if foundQuorum || timeoutExpired {
		i.beginCommit()
	}

	return nil
}

func (i *instance) beginCommit() {
	i.phase = COMMIT_PHASE
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE_PHASE.String())

	var justification Justification
	if signers, signatures, ok := i.roundState(i.round).prepared.FindStrongQuorumAgreement(i.value.HeadCIDOrZero()); ok {
		// Strong quorum found, aggregate the evidence for it.
		aggSignature := i.host.Aggregate(signatures, nil)
		justification = Justification{
			Vote: Payload{
				Instance: i.instanceID,
				Round:    i.round,
				Step:     PREPARE_PHASE,
				Value:    i.value,
			},
			Signers:   signers,
			Signature: aggSignature,
		}
	} else {
		// No strong quorum of PREPARE found, which is ok iff the value to commit is zero.
		if !i.value.IsZero() {
			panic("beginCommit with no strong quorum for non-bottom value")
		}
	}

	i.broadcast(i.round, COMMIT_PHASE, i.value, nil, justification)
}

func (i *instance) tryCommit(round uint64) error {
	// Unlike all other phases, the COMMIT phase stays open to new messages even after an initial quorum is reached,
	// and the algorithm moves on to the next round.
	// A subsequent COMMIT message can cause the node to decide, so there is no check on the current phase.
	committed := i.roundState(round).committed
	foundQuorum := committed.ListStrongQuorumAgreedValues()
	timeoutExpired := i.host.Time() >= i.phaseTimeout

	if len(foundQuorum) > 0 && !foundQuorum[0].IsZero() {
		// A participant may be forced to decide a value that's not its preferred chain.
		// The participant isn't influencing that decision against their interest, just accepting it.
		i.value = foundQuorum[0]
		i.beginDecide(round)
	} else if i.round == round && i.phase == COMMIT_PHASE &&
		timeoutExpired && committed.ReceivedFromStrongQuorum() {
		// Adopt any non-empty value committed by another participant (there can only be one).
		// This node has observed the strong quorum of PREPARE messages that justify it,
		// and mean that some other nodes may decide that value (if they observe more COMMITs).
		for _, v := range committed.ListAllValues() {
			if !v.IsZero() {
				if !i.isAcceptable(v) {
					i.log("⚠️ swaying from %s to %s by COMMIT", &i.input, &v)
				}
				if !v.Eq(i.proposal) {
					i.proposal = v
					i.log("adopting proposal %s after commit", &i.proposal)
				}
				break
			}
		}
		i.beginNextRound()
	}

	return nil
}

func (i *instance) beginDecide(round uint64) {
	i.phase = DECIDE_PHASE
	roundState := i.roundState(round)

	var justification Justification
	// Value cannot be empty here.
	if signers, signatures, ok := roundState.committed.FindStrongQuorumAgreement(i.value.Head().CID); ok {
		aggSignature := i.host.Aggregate(signatures, nil)
		justificationPayload := Payload{
			Instance: i.instanceID,
			Round:    round,
			Step:     COMMIT_PHASE,
			Value:    i.value,
		}
		justification = Justification{
			Vote:      justificationPayload,
			Signers:   signers,
			Signature: aggSignature,
		}

	} else {
		panic("beginDecide with no strong quorum for value")
	}

	i.broadcast(0, DECIDE_PHASE, i.value, nil, justification)
}

func (i *instance) tryDecide() error {
	foundQuorum := i.decision.ListStrongQuorumAgreedValues()
	if len(foundQuorum) > 0 {
		i.terminate(foundQuorum[0], i.round)
	}

	return nil
}

func (i *instance) roundState(r uint64) *roundState {
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
	i.beginConverge()
}

// Returns whether a chain is acceptable as a proposal for this instance to vote for.
// This is "EC Compatible" in the pseudocode.
func (i *instance) isAcceptable(c ECChain) bool {
	return i.acceptable.HasPrefix(c)
}

func (i *instance) terminate(value ECChain, round uint64) {
	i.log("✅ terminated %s in round %d", &i.value, round)
	i.phase = TERMINATED_PHASE
	// Round is a parameter since a late COMMIT message can result in a decision for a round prior to the current one.
	i.round = round
	i.value = value
}

func (i *instance) terminated() bool {
	return i.phase == TERMINATED_PHASE
}

func (i *instance) broadcast(round uint64, step Phase, value ECChain, ticket Ticket, justification Justification) *GMessage {
	p := Payload{
		Instance: i.instanceID,
		Round:    round,
		Step:     step,
		Value:    value,
	}
	sp := p.MarshalForSigning(TODONetworkName)

	sig := i.host.Sign(i.participantID, sp)
	gmsg := &GMessage{
		Sender:        i.participantID,
		Vote:          p,
		Signature:     sig,
		Ticket:        ticket,
		Justification: justification,
	}
	i.host.Broadcast(gmsg)
	i.enqueueInbox(gmsg)
	return gmsg
}

// Sets an alarm to be delivered after a synchrony delay.
// The delay duration increases with each round.
// Returns the absolute time at which the alarm will fire.
func (i *instance) alarmAfterSynchrony(payload string) float64 {
	timeout := i.host.Time() + i.config.Delta

	if i.round >= 2 {
		timeout += i.config.DeltaExtra
	}

	if i.round >= 5 {
		timeout *= i.deltaRate
		i.deltaRate *= i.config.DeltaRate
	}

	if i.round > 0 && i.round%5 == 0 && payload == CONVERGE_PHASE.String() {
		// Wait for clock tick assuming synchronized clocks
		timeToNextClockTick := i.config.ClockTickDelta - math.Mod(timeout, i.config.ClockTickDelta)
		timeout += timeToNextClockTick
	}

	i.host.SetAlarm(i.participantID, payload, timeout)
	return timeout
}

func (i *instance) log(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	i.host.Log("P%d{%d}: %s (round %d, step %s, proposal %s, value %s)", i.participantID, i.instanceID, msg,
		i.round, i.phase, &i.proposal, &i.value)
}

///// Incremental quorum-calculation helper /////

// Accumulates values from a collection of senders and incrementally calculates
// which values have reached a strong quorum of support.
// Supports receiving multiple values from each sender, and hence multiple strong quorum values.
type quorumState struct {
	// CID of each chain, used to track the first time a message from a sender is received.
	senders map[ActorID]struct{}
	// The power supporting each chain so far.
	chainSupport map[TipSetID]chainSupport
	// Total power of all distinct senders from which some chain has been senders so far.
	sendersTotalPower *StoragePower
	// Table of senders' power.
	powerTable PowerTable
	// justifiedMessages stores the senders evidences for each message, indexed by the message's head CID.
	justifiedMessages map[TipSetID]Justification
}

// A chain value and the total power supporting it
type chainSupport struct {
	chain           ECChain
	power           *StoragePower
	signers         map[ActorID][]byte
	hasStrongQuorum bool
	hasWeakQuorum   bool
}

// Creates a new, empty quorum state.
func newQuorumState(powerTable PowerTable) *quorumState {
	return &quorumState{
		senders:           map[ActorID]struct{}{},
		chainSupport:      map[TipSetID]chainSupport{},
		sendersTotalPower: NewStoragePower(0),
		powerTable:        powerTable,
		justifiedMessages: map[TipSetID]Justification{},
	}
}

// Receives a new chain from a sender.
func (q *quorumState) Receive(sender ActorID, value ECChain, signature []byte, justification Justification) {
	senderPower, _ := q.powerTable.Get(sender)

	// Add sender's power to total the first time a value is senders from them.
	if _, ok := q.senders[sender]; !ok {
		q.senders[sender] = struct{}{}
		q.sendersTotalPower.Add(q.sendersTotalPower, senderPower)
	}

	head := value.HeadCIDOrZero()
	candidate, ok := q.chainSupport[head]
	if ok {
		// Don't double-count the same chain head for a single participant.
		if _, ok := candidate.signers[sender]; ok {
			return
		}
	} else {
		candidate = chainSupport{
			chain:           value,
			power:           NewStoragePower(0),
			signers:         map[ActorID][]byte{},
			hasStrongQuorum: false,
			hasWeakQuorum:   false,
		}
		q.chainSupport[value.HeadCIDOrZero()] = candidate
	}

	sigCopy := make([]byte, len(signature))
	copy(sigCopy, signature)
	candidate.signers[sender] = sigCopy

	// Add sender's power to the chain's total power.
	candidate.power.Add(candidate.power, senderPower)

	candidate.hasStrongQuorum = hasStrongQuorum(candidate.power, q.powerTable.Total)
	candidate.hasWeakQuorum = hasWeakQuorum(candidate.power, q.powerTable.Total)

	if !value.IsZero() && justification.Vote.Step == PREPARE_PHASE { //only committed roundStates need to store justifications
		q.justifiedMessages[value.Head().CID] = justification
	}
	q.chainSupport[head] = candidate
}

// Checks whether a value has been senders before.
func (q *quorumState) HasReceived(value ECChain) bool {
	_, ok := q.chainSupport[value.HeadCIDOrZero()]
	return ok
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

// Checks whether at most one distinct value has been senders.
func (q *quorumState) HasAgreement() bool {
	return len(q.chainSupport) <= 1
}

// Checks whether at least one message has been senders from a strong quorum of senders.
func (q *quorumState) ReceivedFromStrongQuorum() bool {
	return hasStrongQuorum(q.sendersTotalPower, q.powerTable.Total)
}

// Checks whether a chain (head) has reached a strong quorum.
func (q *quorumState) HasStrongQuorumAgreement(cid TipSetID) bool {
	cp, ok := q.chainSupport[cid]
	return ok && cp.hasStrongQuorum
}

// Checks whether a chain (head) has reached a strong quorum.
// If so returns a set of signers and signatures for the value that form a strong quorum.
func (q *quorumState) FindStrongQuorumAgreement(value TipSetID) (bitfield.BitField, [][]byte, bool) {
	chainSupport, ok := q.chainSupport[value]
	if !ok || !chainSupport.hasStrongQuorum {
		return bitfield.New(), nil, false
	}

	// Build an array of indices of signers in the power table.
	signers := make([]int, 0, len(chainSupport.signers))
	for key := range chainSupport.signers {
		signers = append(signers, q.powerTable.Lookup[key])
	}
	// Sort power table indices.
	// If the power table entries are ordered by decreasing power,
	// then the first strong quorum found will be the smallest.
	sort.Ints(signers)

	// Accumulate signers and signatures until they reach a strong quorum.
	strongQuorumSigners := bitfield.New()
	signatures := make([][]byte, 0, len(chainSupport.signers))
	justificationPower := NewStoragePower(0)
	for _, idx := range signers {
		if idx >= len(q.powerTable.Entries) {
			panic(fmt.Sprintf("invalid signer index: %d for %d entries", idx, len(q.powerTable.Entries)))
		}
		justificationPower.Add(justificationPower, q.powerTable.Entries[idx].Power)
		strongQuorumSigners.Set(uint64(idx))
		signatures = append(signatures, chainSupport.signers[q.powerTable.Entries[idx].ID])
		if hasStrongQuorum(justificationPower, q.powerTable.Total) {
			return strongQuorumSigners, signatures, true
		}
	}
	return bitfield.New(), nil, false
}

// Checks whether a chain (head) has reached weak quorum.
func (q *quorumState) HasWeakQuorumAgreement(cid TipSetID) bool {
	cp, ok := q.chainSupport[cid]
	return ok && cp.hasWeakQuorum
}

// Returns a list of the chains which have reached an agreeing strong quorum.
// The order of returned values is not defined.
func (q *quorumState) ListStrongQuorumAgreedValues() []ECChain {
	var withQuorum []ECChain
	for cid, cp := range q.chainSupport {
		if cp.hasStrongQuorum {
			withQuorum = append(withQuorum, q.chainSupport[cid].chain)
		}
	}
	sortByWeight(withQuorum)
	return withQuorum
}

//// CONVERGE phase helper /////

type convergeState struct {
	// Chains indexed by head CID
	values map[TipSetID]ECChain
	// Tickets provided by proposers of each chain.
	tickets map[TipSetID][]Ticket
}

func newConvergeState() *convergeState {
	return &convergeState{
		values:  map[TipSetID]ECChain{},
		tickets: map[TipSetID][]Ticket{},
	}
}

// Receives a new CONVERGE value from a sender.
func (c *convergeState) Receive(value ECChain, ticket Ticket) error {
	if value.IsZero() {
		return fmt.Errorf("bottom cannot be justified for CONVERGE")
	}
	key := value.Head().CID
	c.values[key] = value
	c.tickets[key] = append(c.tickets[key], ticket)

	return nil
}

func (c *convergeState) findMinTicketProposal() ECChain {
	var minTicket Ticket
	var minValue ECChain
	for cid, value := range c.values {
		for _, ticket := range c.tickets[cid] {
			if minTicket == nil || ticket.Compare(minTicket) < 0 {
				minTicket = ticket
				minValue = value
			}
		}
	}
	return minValue
}

///// General helpers /////

// Returns the first candidate value that is a prefix of the preferred value, or the base of preferred.
func findFirstPrefixOf(candidates []ECChain, preferred ECChain) ECChain {
	for _, v := range candidates {
		if preferred.HasPrefix(v) {
			return v
		}
	}

	// No candidates are a prefix of preferred.
	return preferred.BaseChain()
}

// Sorts chains by weight of their head, descending
func sortByWeight(chains []ECChain) {
	sort.Slice(chains, func(i, j int) bool {
		if chains[i].IsZero() {
			return false
		} else if chains[j].IsZero() {
			return true
		}
		hi := chains[i].Head()
		hj := chains[j].Head()
		return hi.Compare(hj) > 0
	})
}

// Check whether a portion of storage power is a strong quorum of the total
func hasStrongQuorum(part, total *StoragePower) bool {
	two := NewStoragePower(2)
	three := NewStoragePower(3)

	strongThreshold := new(StoragePower).Mul(total, two)
	strongThreshold.Div(strongThreshold, three)
	return part.Cmp(strongThreshold) > 0
}

// Check whether a portion of storage power is a weak quorum of the total
func hasWeakQuorum(part, total *StoragePower) bool {
	three := NewStoragePower(3)

	weakThreshold := new(StoragePower).Div(total, three)
	return part.Cmp(weakThreshold) > 0
}
