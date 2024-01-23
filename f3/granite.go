package f3

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/filecoin-project/go-bitfield"
	"sort"
)

type GraniteConfig struct {
	// Initial delay for partial synchrony.
	Delta float64
	// Change to delta in each round after the first.
	DeltaRate float64
}

type VRFer interface {
	VRFTicketSource
	VRFTicketVerifier
}

const QUALITY = "QUALITY"
const CONVERGE = "CONVERGE"
const PREPARE = "PREPARE"
const COMMIT = "COMMIT"
const DECIDE = "DECIDE"
const TERMINATED = "TERMINATED"

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
	// GossiPBFT instance (epoch) number.
	Instance uint32
	// GossiPBFT round number.
	Round uint32
	// GossiPBFT step name.
	Step string
	// Chain of tipsets proposed/voted for finalisation.
	// Always non-empty; the first entry is the base tipset finalised in the previous instance.
	Value ECChain
	// VRF ticket for CONVERGE messages (otherwise empty byte array).
	Ticket Ticket
	// Signature by the sender's public key over Instance || Round || Step || Value.
	Signature []byte

	Justification Justification
}

// Aggregated list of GossiPBFT messages with the same instance, round and value. Used as evidence for justification of messages
type Justification struct {
	Instance uint32

	Round uint32

	Step string

	Value ECChain

	// Indexes in the base power table of the signers (bitset)
	Signers bitfield.BitField
	// BLS aggregate signature of signers
	Signature []byte
}

func (m GMessage) String() string {
	// FIXME This needs value receiver to work, for reasons I cannot figure out.
	return fmt.Sprintf("%s{%d}(%d %s)", m.Step, m.Instance, m.Round, &m.Value)
}

// Computes the payload for a GMessage signature.
func SignaturePayload(instance uint32, round uint32, step string, value ECChain) []byte {
	var buf bytes.Buffer
	buf.WriteString(DOMAIN_SEPARATION_TAG)
	_ = binary.Write(&buf, binary.BigEndian, instance)
	_ = binary.Write(&buf, binary.BigEndian, round)
	buf.WriteString(step)
	for _, t := range value {
		_ = binary.Write(&buf, binary.BigEndian, t.Epoch)
		buf.Write(t.CID.Bytes())
		_ = binary.Write(&buf, binary.BigEndian, t.Weight)
	}
	return buf.Bytes()
}

// A single Granite consensus instance.
type instance struct {
	config        GraniteConfig
	host          Host
	vrf           VRFer
	participantID ActorID
	instanceID    uint32
	// The EC chain input to this instance.
	input ECChain
	// The power table for the base chain, used for power in this instance.
	powerTable PowerTable
	// The beacon value from the base chain, used for tickets in this instance.
	beacon []byte
	// Current round number.
	round uint32
	// Current phase in the round.
	phase string
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
	rounds map[uint32]*roundState
	// Acceptable chain
	acceptable ECChain
	// Decision state. Collects DECIDE messages until a decision can be made, independently of protocol phases/rounds.
	decision *quorumState
}

func newInstance(
	config GraniteConfig,
	host Host,
	vrf VRFer,
	participantID ActorID,
	instanceID uint32,
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
		phase:         "",
		proposal:      input,
		value:         ECChain{},
		quality:       newQuorumState(powerTable),
		rounds: map[uint32]*roundState{
			0: newRoundState(powerTable),
		},
		acceptable: input,
		decision:   newQuorumState(powerTable),
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
	i.beginQuality()
	return i.drainInbox()
}

// Receives a new acceptable chain and updates its current acceptable chain.
func (i *instance) receiveAcceptable(chain ECChain) {
	i.acceptable = chain
}

func (i *instance) Receive(msg *GMessage) error {
	if i.terminated() {
		return fmt.Errorf("received message after decision")
	}
	if len(i.inbox) > 0 {
		return fmt.Errorf("received message while already processing inbox")
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
	if i.phase == TERMINATED {
		return nil // No-op
	}
	round := i.roundState(msg.Round)

	// Drop any messages that can never be valid.
	if !i.isValid(msg) {
		i.log("dropping invalid %s", msg)
		return nil
	}

	if !i.isJustified(msg) {
		// No implicit justification:
		// if message not justified explicitly, then it will not be justified
		i.log("dropping unjustified %s from sender %v", msg, msg.Sender)
		return nil
	}

	switch msg.Step {
	case QUALITY:
		// Receive each prefix of the proposal independently.
		for j := range msg.Value.Suffix() {
			prefix := msg.Value.Prefix(j + 1)
			i.quality.Receive(msg.Sender, prefix, msg.Signature, msg.Justification)
		}
	case CONVERGE:
		if err := round.converged.Receive(msg.Value, msg.Ticket); err != nil {
			return fmt.Errorf("failed processing CONVERGE message: %w", err)
		}
	case PREPARE:
		round.prepared.Receive(msg.Sender, msg.Value, msg.Signature, msg.Justification)
	case COMMIT:
		round.committed.Receive(msg.Sender, msg.Value, msg.Signature, msg.Justification)
	case DECIDE:
		i.decision.Receive(msg.Sender, msg.Value, msg.Signature, msg.Justification)
	default:
		i.log("unexpected message %v", msg)
	}

	// Try to complete the current phase.
	// Every COMMIT phase stays open to new messages even after the protocol moves on to
	// a new round. Late-arriving COMMITS can still (must) cause a local decision, *in that round*.

	if msg.Step == COMMIT && i.phase != DECIDE {
		return i.tryCommit(msg.Round)
	}
	return i.tryCompletePhase()
}

// Attempts to complete the current phase and round.
func (i *instance) tryCompletePhase() error {
	i.log("try step %s", i.phase)
	switch i.phase {
	case QUALITY:
		return i.tryQuality()
	case CONVERGE:
		return i.tryConverge()
	case PREPARE:
		return i.tryPrepare()
	case COMMIT:
		return i.tryCommit(i.round)
	case DECIDE:
		return i.tryDecide()
	case TERMINATED:
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

	if !(msg.Value.IsZero() || msg.Value.HasBase(i.input.Base())) {
		i.log("unexpected base %s", &msg.Value)
		return false
	}
	if msg.Step == QUALITY {
		return msg.Round == 0 && !msg.Value.IsZero()
	} else if msg.Step == CONVERGE {

		if msg.Round == 0 ||
			msg.Value.IsZero() ||
			!i.vrf.VerifyTicket(i.beacon, i.instanceID, msg.Round, pubKey, msg.Ticket) {
			return false
		}
	} else if msg.Step == DECIDE {
		// DECIDE needs no justification
		return !msg.Value.IsZero()
	}

	sigPayload := SignaturePayload(msg.Instance, msg.Round, msg.Step, msg.Value)
	if !i.host.Verify(pubKey, sigPayload, msg.Signature) {
		i.log("invalid signature on %v", msg)
		return false
	}

	return true
}

// Checks whether a message is justified.
func (i *instance) isJustified(msg *GMessage) bool {
	if msg.Step == QUALITY || msg.Step == PREPARE {
		//TODO check that the justification is Zero? necessary?
		return true
	}
	if msg.Step == CONVERGE {
		//CONVERGE is justified by a strong quorum of COMMIT for bottom from the previous round.
		// or a strong quorum of PREPARE for the same value from the previous round.
		prevRound := msg.Round - 1
		if msg.Justification.Round != prevRound {
			i.log("dropping CONVERGE %v with evidence from wrong round %d", msg.Round, msg.Justification.Round)
			return false
		}

		if msg.Justification.Step == PREPARE {
			if msg.Value.HeadCIDOrZero() != msg.Justification.Value.HeadCIDOrZero() {
				i.log("dropping CONVERGE for value %v with PREPARE evidence for a different value: %v", msg.Value, msg.Justification.Value)
				return false
			}
		} else if msg.Justification.Step == COMMIT {
			if msg.Justification.Value.HeadCIDOrZero() != ZeroTipSetID() {
				i.log("dropping CONVERGE with COMMIT evidence for non-zero value: %v", msg.Justification.Value)
				return false
			}
		} else {
			i.log("dropping CONVERGE with evidence from wrong step %v\n", msg.Justification.Step)
			return false
		}

	} else if msg.Step == COMMIT {
		// COMMIT is justified by strong quorum of PREPARE from the same round with the same value.
		// COMMIT for bottom is always justified.
		if msg.Value.IsZero() {
			return true
		}

		if msg.Round != msg.Justification.Round {
			i.log("dropping COMMIT %v with evidence from wrong round %d", msg.Round, msg.Justification.Round)
			return false
		}

		if msg.Justification.Step != PREPARE {
			i.log("dropping COMMIT %v with evidence from wrong step %v", msg.Round, msg.Justification.Step)
			return false
		}

		if msg.Value.Head().CID != msg.Justification.Value.HeadCIDOrZero() {
			i.log("dropping COMMIT %v with evidence for a different value: %v", msg.Value, msg.Justification.Value)
			return false
		}
	} else if msg.Step == DECIDE {
		if msg.Justification.Step != COMMIT {
			i.log("dropping DECIDE %v with evidence from wrong step %v", msg.Round, msg.Justification.Step)
			return false
		}
		if msg.Value.IsZero() || msg.Justification.Value.IsZero() {
			i.log("dropping DECIDE %v with evidence for a zero value: %v", msg.Value, msg.Justification.Value)
			return false
		}
		if msg.Value.Head().CID != msg.Justification.Value.Head().CID {
			i.log("dropping DECIDE %v with evidence for a different value: %v", msg.Value, msg.Justification.Value)
			return false
		}
	}

	if msg.Instance != msg.Justification.Instance {
		i.log("dropping message with instanceID %v with evidence from wrong instanceID: %v", msg.Instance, msg.Justification.Instance)
		return false
	}

	signers := make([]PubKey, 0)
	setBits, _ := msg.Justification.Signers.All(uint64(len(i.powerTable.Entries)))
	justificationPower := NewStoragePower(0)
	var bitIndex int
	var bit uint64
	for bitIndex, bit = range setBits {
		if int(bit) >= len(i.powerTable.Entries) {
			break //TODO handle error
		}
		signers = append(signers, i.powerTable.Entries[bit].PubKey)
		justificationPower.Add(justificationPower, i.powerTable.Entries[bit].Power)
		if hasStrongQuorum(justificationPower, i.powerTable.Total) {
			break // no need to keep calculating
		}
	}

	if !hasStrongQuorum(justificationPower, i.powerTable.Total) {
		i.log("dropping message as no evidence from a strong quorum: %v", msg.Justification.Signers)
		return false
	}

	// need to retrieve the remaining pubkeys just to verify the aggregate
	//TODO we could enforce here a tight strong quorum and no extra signatures
	// to prevent wasting time in verifying too many aggregated signatures
	// but should be careful with oligopolies (check out issue #49)
	// A deterministic but drand-sourced permutation could prevent oligopolies
	// if a random permutation affects performance we could do a weighted random permutation
	// To favour with more power.
	for bitIndex++; bitIndex < len(setBits); bitIndex++ {
		bit = setBits[bitIndex]
		if int(bit) >= len(i.powerTable.Entries) {
			break //TODO handle error
		}
		signers = append(signers, i.powerTable.Entries[bit].PubKey)
	}

	payload := SignaturePayload(msg.Justification.Instance, msg.Justification.Round, msg.Justification.Step, msg.Justification.Value)
	if !i.host.VerifyAggregate(payload, msg.Justification.Signature, signers) {
		i.log("dropping Message %v with invalid evidence signature: %v", msg, msg.Justification)
		return false
	}

	return true
}

// Sends this node's QUALITY message and begins the QUALITY phase.
func (i *instance) beginQuality() {
	// Broadcast input value and wait up to Δ to receive from others.
	i.phase = QUALITY
	i.phaseTimeout = i.alarmAfterSynchrony(QUALITY)
	i.broadcast(i.round, QUALITY, i.input, nil, Justification{})
}

// Attempts to end the QUALITY phase and begin PREPARE based on current state.
func (i *instance) tryQuality() error {
	if i.phase != QUALITY {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, QUALITY)
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
	i.phase = CONVERGE
	ticket := i.vrf.MakeTicket(i.beacon, i.instanceID, i.round, i.participantID)
	i.phaseTimeout = i.alarmAfterSynchrony(CONVERGE)
	prevRoundState := i.roundState(i.round - 1)
	var justification Justification
	var ok bool
	if prevRoundState.committed.HasStrongQuorumAgreement(ZeroTipSetID()) {
		value := ECChain{}
		signers := prevRoundState.committed.getStrongQuorumSigners(value)
		signatures := prevRoundState.committed.getSignatures(value, signers)
		aggSignature := make([]byte, 0)
		for _, sig := range signatures {
			aggSignature = i.host.Aggregate([][]byte{sig}, aggSignature)
		}
		justification = Justification{
			Instance:  i.instanceID,
			Round:     i.round - 1,
			Step:      COMMIT,
			Value:     value,
			Signers:   signers,
			Signature: aggSignature,
		}
	} else if prevRoundState.prepared.HasStrongQuorumAgreement(i.proposal.Head().CID) {
		value := i.proposal
		signers := prevRoundState.prepared.getStrongQuorumSigners(value)
		signatures := prevRoundState.prepared.getSignatures(value, signers)
		aggSignature := make([]byte, 0)
		for _, sig := range signatures {
			aggSignature = i.host.Aggregate([][]byte{sig}, aggSignature)
		}

		justification = Justification{
			Instance:  i.instanceID,
			Round:     i.round - 1,
			Step:      PREPARE,
			Value:     value,
			Signers:   signers,
			Signature: aggSignature,
		}
	} else if justification, ok = prevRoundState.committed.justifiedMessages[i.proposal.Head().CID]; ok {
		//justification already assigned in the if statement
	} else {
		panic("beginConverge called but no evidence found")
	}
	i.broadcast(i.round, CONVERGE, i.proposal, ticket, justification)
}

// Attempts to end the CONVERGE phase and begin PREPARE based on current state.
func (i *instance) tryConverge() error {
	if i.phase != CONVERGE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, CONVERGE)
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
	i.phase = PREPARE
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE)
	i.broadcast(i.round, PREPARE, i.value, nil, Justification{})
}

// Attempts to end the PREPARE phase and begin COMMIT based on current state.
func (i *instance) tryPrepare() error {
	if i.phase != PREPARE {
		return fmt.Errorf("unexpected phase %s, expected %s", i.phase, PREPARE)
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
	i.phase = COMMIT
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE)
	signers := i.roundState(i.round).prepared.getSigners(i.value)
	signatures := i.roundState(i.round).prepared.getSignatures(i.value, signers)
	aggSignature := make([]byte, 0)
	for _, sig := range signatures {
		aggSignature = i.host.Aggregate([][]byte{sig}, aggSignature)
	}
	justification := Justification{
		Instance:  i.instanceID,
		Round:     i.round,
		Step:      PREPARE,
		Value:     i.value,
		Signers:   signers,
		Signature: aggSignature,
	}
	i.broadcast(i.round, COMMIT, i.value, nil, justification)
}

func (i *instance) tryCommit(round uint32) error {
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
	} else if i.round == round && i.phase == COMMIT && timeoutExpired && committed.ReceivedFromStrongQuorum() {
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

func (i *instance) beginDecide(round uint32) {
	i.phase = DECIDE
	roundState := i.roundState(round)
	if !roundState.committed.HasStrongQuorumAgreement(i.value.Head().CID) {
		panic("beginDecide called but no evidence found")
	}
	signers := roundState.committed.getSigners(i.value)
	signatures := roundState.committed.getSignatures(i.value, signers)
	aggSignature := make([]byte, 0)
	for _, sig := range signatures {
		aggSignature = i.host.Aggregate([][]byte{sig}, aggSignature)
	}
	justification := Justification{
		Instance:  i.instanceID,
		Round:     round,
		Step:      COMMIT,
		Value:     i.value,
		Signers:   signers,
		Signature: aggSignature,
	}
	i.broadcast(0, DECIDE, i.value, nil, justification)
}

func (i *instance) tryDecide() error {
	foundQuorum := i.decision.ListStrongQuorumAgreedValues()
	if len(foundQuorum) > 0 {
		i.terminate(foundQuorum[0], i.round)
	}

	return nil
}

func (i *instance) roundState(r uint32) *roundState {
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

func (i *instance) terminate(value ECChain, round uint32) {
	i.log("✅ terminated %s in round %d", &i.value, round)
	i.phase = TERMINATED
	// Round is a parameter since a late COMMIT message can result in a decision for a round prior to the current one.
	i.round = round
	i.value = value
}

func (i *instance) terminated() bool {
	return i.phase == TERMINATED
}

func (i *instance) broadcast(round uint32, step string, value ECChain, ticket Ticket, justification Justification) *GMessage {
	payload := SignaturePayload(i.instanceID, round, step, value)
	signature := i.host.Sign(i.participantID, payload)
	gmsg := &GMessage{i.participantID, i.instanceID, round, step, value, ticket, signature, justification}
	i.host.Broadcast(gmsg)
	i.enqueueInbox(gmsg)
	return gmsg
}

// Sets an alarm to be delivered after a synchrony delay.
// The delay duration increases with each round.
// Returns the absolute time at which the alarm will fire.
func (i *instance) alarmAfterSynchrony(payload string) float64 {
	timeout := i.host.Time() + i.config.Delta + (float64(i.round) * i.config.DeltaRate)
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
	// CID of each chain received, by sender. Allows detecting and ignoring duplicates.
	received map[ActorID]senderSent
	// The power supporting each chain so far.
	chainSupport map[TipSetID]chainSupport
	// Total power of all distinct senders from which some chain has been received so far.
	sendersTotalPower *StoragePower
	// Table of senders' power.
	powerTable PowerTable
	// justifiedMessages stores the received evidences for each message, indexed by the message's head CID.
	justifiedMessages map[TipSetID]Justification
}

// The set of chain heads from one sender and associated signature, and that sender's power.
type senderSent struct {
	heads map[TipSetID][]byte
	power *StoragePower
}

// A chain value and the total power supporting it
type chainSupport struct {
	chain           ECChain
	power           *StoragePower
	signers         map[ActorID]struct{}
	hasStrongQuorum bool
	hasWeakQuorum   bool
}

// Creates a new, empty quorum state.
func newQuorumState(powerTable PowerTable) *quorumState {
	return &quorumState{
		received:          map[ActorID]senderSent{},
		chainSupport:      map[TipSetID]chainSupport{},
		sendersTotalPower: NewStoragePower(0),
		powerTable:        powerTable,
		justifiedMessages: map[TipSetID]Justification{},
	}
}

// Receives a new chain from a sender.
func (q *quorumState) Receive(sender ActorID, value ECChain, signature []byte, justification Justification) {
	head := value.HeadCIDOrZero()
	fromSender, ok := q.received[sender]
	senderPower, _ := q.powerTable.Get(sender)
	sigCopy := make([]byte, len(signature))
	copy(sigCopy, signature)
	if ok {
		// Don't double-count the same chain head for a single participant.
		if _, ok := fromSender.heads[head]; ok {
			return
		}
		fromSender.heads[head] = sigCopy
	} else {
		// Add sender's power to total the first time a value is received from them.
		q.sendersTotalPower.Add(q.sendersTotalPower, senderPower)
		fromSender = senderSent{
			heads: map[TipSetID][]byte{
				head: sigCopy,
			},
			power: senderPower,
		}
	}
	q.received[sender] = fromSender

	candidate := chainSupport{
		chain:           value,
		power:           senderPower,
		signers:         make(map[ActorID]struct{}),
		hasStrongQuorum: false,
		hasWeakQuorum:   false,
	}
	if found, ok := q.chainSupport[head]; ok {
		candidate.power.Add(candidate.power, found.power)
		candidate.signers = found.signers
	}
	candidate.signers[sender] = struct{}{}

	candidate.hasStrongQuorum = hasStrongQuorum(candidate.power, q.powerTable.Total)
	candidate.hasWeakQuorum = hasWeakQuorum(candidate.power, q.powerTable.Total)

	if !value.IsZero() && justification.Step == "PREPARE" { //only committed roundStates need to store justifications
		q.justifiedMessages[value.Head().CID] = justification
	}
	q.chainSupport[head] = candidate
}

// Checks whether a value has been received before.
func (q *quorumState) HasReceived(value ECChain) bool {
	_, ok := q.chainSupport[value.HeadCIDOrZero()]
	return ok
}

// getSigners retrieves the signers of the given ECChain.
func (q *quorumState) getSigners(value ECChain) bitfield.BitField {
	head := value.HeadCIDOrZero()
	chainSupport, ok := q.chainSupport[head]
	signers := bitfield.New()
	if !ok {
		return signers
	}

	// Copy each element from the original map
	for key := range chainSupport.signers {
		signers.Set(uint64(q.powerTable.Lookup[key]))
	}

	return signers
}

// getStrongQuorumSigners retrieves just a strong quorum of signers of the given ECChain.
// At the moment, this is the signers with the most power until reaching a strong quorum.
func (q *quorumState) getStrongQuorumSigners(value ECChain) bitfield.BitField {
	signers := q.getSigners(value)
	strongQuorumSigners := bitfield.New()
	justificationPower := NewStoragePower(0)
	setBits, _ := signers.All(uint64(len(q.powerTable.Entries)))
	for _, bit := range setBits {
		justificationPower.Add(justificationPower, q.powerTable.Entries[bit].Power)
		strongQuorumSigners.Set(bit)
		if hasStrongQuorum(justificationPower, q.powerTable.Total) {
			break // no need to keep calculating
		}
	}
	if !hasStrongQuorum(justificationPower, q.powerTable.Total) {
		// if we didn't find a strong quorum, return an empty bitfield
		return bitfield.New()
	}
	return strongQuorumSigners
}

// getSignatures returns the corresponding signatures for a given bitset of signers
func (q *quorumState) getSignatures(value ECChain, signers bitfield.BitField) [][]byte {
	head := value.HeadCIDOrZero()
	signatures := make([][]byte, 0)
	if err := signers.ForEach(func(i uint64) error {
		if signature, ok := q.received[q.powerTable.Entries[i].ID].heads[head]; ok {
			if len(signature) == 0 {
				panic("signature is 0")
			}
			signatures = append(signatures, signature)
		} else {
			panic("Signature not found")
		}
		return nil
	}); err != nil {
		panic("Error while iterating over signers")
		//TODO handle error
	}
	return signatures
}

// Lists all values that have been received from any sender.
// The order of returned values is not defined.
func (q *quorumState) ListAllValues() []ECChain {
	var chains []ECChain
	for _, cp := range q.chainSupport {
		chains = append(chains, cp.chain)
	}
	return chains
}

// Checks whether at most one distinct value has been received.
func (q *quorumState) HasAgreement() bool {
	return len(q.chainSupport) <= 1
}

// Checks whether at least one message has been received from a strong quorum of senders.
func (q *quorumState) ReceivedFromStrongQuorum() bool {
	return hasStrongQuorum(q.sendersTotalPower, q.powerTable.Total)
}

// Checks whether a chain (head) has reached a strong quorum.
func (q *quorumState) HasStrongQuorumAgreement(cid TipSetID) bool {
	cp, ok := q.chainSupport[cid]
	return ok && cp.hasStrongQuorum
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
