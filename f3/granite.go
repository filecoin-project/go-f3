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

const DOMAIN_SEPARATION_TAG = "GPBFT"

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
	// Evidence is the aggregated list of GossiPBFT msgs that justify this message
	Evidence AggEvidence
}

// Aggregated list of GossiPBFT messages with the same instance, round and value. Used as evidence for justification of messages
type AggEvidence struct {
	Instance uint32

	Round uint32

	Step string

	Value ECChain

	// Indexes in the base power table of the signers (bitset)
	Signers bitfield.BitField
	// BLS aggregate signature of signers
	Signature []byte
}

func ZeroAggEvidence() AggEvidence {
	return AggEvidence{}
}

func (a AggEvidence) isZero() bool {
	signersCount, err := a.Signers.Count()
	if err != nil {
		panic(err)
	}
	return a.Step == "" && a.Value.IsZero() && a.Instance == 0 && a.Round == 0 && signersCount == 0 && len(a.Signature) == 0
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
	// Messages received earlier but not yet justified.
	pending *pendingQueue
	// Quality phase state (only for round 0)
	quality quorumState
	// State for each round of phases.
	// State from prior rounds must be maintained to provide justification for values in subsequent rounds.
	rounds map[uint32]*roundState
	// Acceptable chain
	acceptable ECChain
	//quorumStateType defines the message justification method, either EXPLICIT for messages attaching a BLS-aggregate of other messages that justify it
	// or IMPLICIT for messages being justified by previously received messages
	quorumStateType quorumStateType
	//senderIndex
	senderIndex *SenderIndex
}

func newInstance(
	config GraniteConfig,
	host Host,
	vrf VRFer,
	participantID ActorID,
	instanceID uint32,
	input ECChain,
	powerTable PowerTable,
	beacon []byte,
	quorumStateType quorumStateType) *instance {
	if input.IsZero() {
		panic("input is empty")
	}
	var senderIndex *SenderIndex
	if quorumStateType == EXPLICIT {
		senderIndex = NewSenderIndex(powerTable)
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
		pending:       newPendingQueue(),
		quality:       newQuorumState(quorumStateType, powerTable, host, instanceID, 0, QUALITY, senderIndex),
		rounds: map[uint32]*roundState{
			0: newRoundState(powerTable, quorumStateType, host, instanceID, 0, senderIndex),
		},
		acceptable:      input,
		quorumStateType: quorumStateType,
		senderIndex:     senderIndex,
	}
}

type roundState struct {
	converged *convergeState
	prepared  quorumState
	committed quorumState
}

func newRoundState(powerTable PowerTable, quorumStateType quorumStateType, aggregator Aggregator, instanceID uint32, round uint32, senderIndex *SenderIndex) *roundState {
	return &roundState{
		converged: newConvergeState(),
		prepared:  newQuorumState(quorumStateType, powerTable, aggregator, instanceID, round, PREPARE, senderIndex),
		committed: newQuorumState(quorumStateType, powerTable, aggregator, instanceID, round, COMMIT, senderIndex),
	}
}

func (i *instance) Start() {
	i.beginQuality()
	i.drainInbox()
}

// Receives a new acceptable chain and updates its current acceptable chain.
func (i *instance) receiveAcceptable(chain ECChain) {
	i.acceptable = chain
}

func (i *instance) Receive(msg *GMessage) {
	if i.decided() {
		panic("received message after decision")
	}
	if len(i.inbox) > 0 {
		panic("received message while already processing inbox")
	}

	// Enqueue the message for synchronous processing.
	i.enqueueInbox(msg)
	i.drainInbox()
}

func (i *instance) ReceiveAlarm(_ string) {
	i.tryCompletePhase()

	// A phase may have been successfully completed.
	// Re-process any queued messages for the next phase.
	i.tryPendingMessages()
	i.drainInbox()
}

func (i *instance) Describe() string {
	return fmt.Sprintf("P%d{%d}, round %d, phase %s", i.participantID, i.instanceID, i.round, i.phase)
}

func (i *instance) enqueueInbox(msg *GMessage) {
	i.inbox = append(i.inbox, msg)
}

func (i *instance) drainInbox() {
	for len(i.inbox) > 0 {
		// Process one message.
		// Note the message being processed is left in the inbox until after processing,
		// as a signal that this loop is currently draining the inbox.
		i.receiveOne(i.inbox[0])
		i.inbox = i.inbox[1:]

		// Retry pending messages that might now be valid for the now-current phase.
		// It's important that this be done after every message, else the phase might be
		// advanced twice in a row, and pending messages for the skipped phase would be stranded.
		i.tryPendingMessages()
	}
}

// Pops any now-valid messages for the current round/phase and enqueue for receiving them again.
func (i *instance) tryPendingMessages() {
	replay := i.pending.PopWhere(i.round, i.phase, i.isJustified)
	if len(replay) > 0 {
		i.log("replay: %s", replay)
	}
	i.inbox = append(i.inbox, replay...)
}

// Processes a single message.
func (i *instance) receiveOne(msg *GMessage) {
	// Drop any messages that can never be valid.
	if !i.isValid(msg) {
		i.log("dropping invalid %s", msg)
		return
	}

	// Hold (if using IMPLICIT justification) as pending any message with a value not yet justified by the prior phase.
	if !i.isJustified(msg) {
		if i.quorumStateType == IMPLICIT {
			i.log("enqueue %s", msg)
			i.pending.Add(msg)
		} else if i.quorumStateType == EXPLICIT {
			// Ignore message
			return
		}
	}

	round := i.roundState(msg.Round)
	switch msg.Step {
	case QUALITY:
		// Receive each prefix of the proposal independently.
		for j := range msg.Value.Suffix() {
			prefix := msg.Value.Prefix(j + 1)
			i.quality.Receive(msg.Sender, prefix, msg.Signature)
		}
	case CONVERGE:
		round.converged.Receive(msg.Value, msg.Ticket)
	case PREPARE:
		round.prepared.Receive(msg.Sender, msg.Value, msg.Signature)
	case COMMIT:
		round.committed.Receive(msg.Sender, msg.Value, msg.Signature)
	default:
		i.log("unexpected message %v", msg)
	}

	// Try to complete the current phase.
	// Every COMMIT phase stays open to new messages even after the protocol moves on to
	// a new round. Late-arriving COMMITS can still (must) cause a local decision, *in that round*.
	if msg.Step == COMMIT {
		i.tryCommit(msg.Round)
	} else {
		i.tryCompletePhase()
	}
}

// Attempts to complete the current phase and round.
func (i *instance) tryCompletePhase() {
	i.log("try step %s", i.phase)
	switch i.phase {
	case QUALITY:
		i.tryQuality()
	case CONVERGE:
		i.tryConverge()
	case PREPARE:
		i.tryPrepare()
	case COMMIT:
		i.tryCommit(i.round)
	case DECIDE:
		// No-op
	default:
		panic(fmt.Sprintf("unexpected phase %s", i.phase))
	}
}

// Checks whether a message is valid.
// An invalid message can never become valid, so may be dropped.
func (i *instance) isValid(msg *GMessage) bool {
	if !(msg.Value.IsZero() || msg.Value.HasBase(i.input.Base())) {
		i.log("unexpected base %s", &msg.Value)
		return false
	}
	if msg.Step == CONVERGE {
		if !i.vrf.VerifyTicket(i.beacon, i.instanceID, msg.Round, msg.Sender, msg.Ticket) {
			return false
		}
	}
	return true
}

// Checks whether a message is justified by prior messages.
// An unjustified message may later be justified by subsequent messages.
func (i *instance) isJustified(msg *GMessage) bool {
	if msg.Step == QUALITY {
		// QUALITY needs no justification by prior messages.
		return msg.Round == 0 && !msg.Value.IsZero()
	} else if msg.Step == CONVERGE {
		// CONVERGE is justified by a previous round strong quorum of PREPARE for the same value,
		// or strong quorum of COMMIT for bottom.
		// Bottom is not allowed as a value.
		if msg.Round == 0 || msg.Value.IsZero() {
			return false
		}
		prevRound := i.roundState(msg.Round - 1)
		if msg.Evidence.Step == PREPARE {
			return prevRound.prepared.IsJustified(msg, msg.Value)
		} else if msg.Evidence.Step == COMMIT {
			return prevRound.committed.IsJustified(msg, ECChain{})
		} else {
			return false
		}
	} else if msg.Step == PREPARE {
		// PREPARE needs no justification by prior messages.
		return true // i.quality.AllowsValue(msg.Value)
	} else if msg.Step == COMMIT {
		// COMMIT is justified by strong quorum of PREPARE from the same round with the same value.
		// COMMIT for bottom is always justified.
		round := i.roundState(msg.Round)

		return msg.Value.IsZero() || round.prepared.IsJustified(msg, msg.Value)
	}
	return false
}

// Sends this node's QUALITY message and begins the QUALITY phase.
func (i *instance) beginQuality() {
	// Broadcast input value and wait up to Δ to receive from others.
	i.phase = QUALITY
	i.phaseTimeout = i.alarmAfterSynchrony(QUALITY)
	i.broadcast(QUALITY, i.input, nil, ZeroAggEvidence())
}

// Attempts to end the QUALITY phase and begin PREPARE based on current state.
// No-op if the current phase is not QUALITY.
func (i *instance) tryQuality() {
	if i.phase != QUALITY {
		panic(fmt.Sprintf("unexpected phase %s", i.phase))
	}
	// Wait either for a strong quorum that agree on our proposal,
	// or for the timeout to expire.
	foundQuorum := i.quality.HasQuorumAgreement(i.proposal.Head().CID)
	timeoutExpired := i.host.Time() >= i.phaseTimeout

	if foundQuorum {
		// Keep current proposal.
	} else if timeoutExpired {
		strongQuora := i.quality.ListQuorumAgreedValues()
		i.proposal = findFirstPrefixOf(strongQuora, i.proposal)
	}

	if foundQuorum || timeoutExpired {
		i.value = i.proposal
		i.log("adopting proposal/value %s", &i.proposal)
		i.beginPrepare()
	}
}

func (i *instance) beginConverge() {
	i.phase = CONVERGE
	ticket := i.vrf.MakeTicket(i.beacon, i.instanceID, i.round, i.participantID)
	i.phaseTimeout = i.alarmAfterSynchrony(CONVERGE)
	aggEvidence, isJustified := i.roundState(i.round - 1).committed.Justify(ECChain{})
	if !isJustified {
		aggEvidence, isJustified = i.roundState(i.round - 1).prepared.Justify(i.proposal)
	}
	if !isJustified && i.quorumStateType == EXPLICIT {
		preparedStateExplicit, ok := i.roundState(i.round - 1).prepared.(*quorumStateExplicit)
		if ok {
			if !i.proposal.IsZero() {
				aggEvidence, ok = preparedStateExplicit.justifiedMessages[i.proposal.HeadCIDOrZero()]

				if ok {
					isJustified = true
				}
			}
		} else {
			committedStateExplicit, ok := i.roundState(i.round - 1).committed.(*quorumStateExplicit)
			if ok {
				if !i.proposal.IsZero() {
					aggEvidence, ok = committedStateExplicit.justifiedMessages[ZeroTipSetID()]
					if ok {
						isJustified = true
					}
				}
			}
		}
	}
	if !isJustified {
		// error, there should be a justification for CONVERGE, otherwise we would not be here now
		panic(fmt.Sprintf("no justification for CONVERGE %v", i.proposal))
	}
	i.broadcast(CONVERGE, i.proposal, ticket, aggEvidence)
}

// Attempts to end the CONVERGE phase and begin PREPARE based on current state.
// No-op if the current phase is not CONVERGE.
func (i *instance) tryConverge() {
	if i.phase != CONVERGE {
		panic(fmt.Sprintf("unexpected phase %s", i.phase))
	}
	timeoutExpired := i.host.Time() >= i.phaseTimeout
	if !timeoutExpired {
		return
	}

	i.value = i.roundState(i.round).converged.findMinTicketProposal()
	if i.value.IsZero() {
		panic("no values at CONVERGE")
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
}

// Sends this node's PREPARE message and begins the PREPARE phase.
func (i *instance) beginPrepare() {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE)
	i.broadcast(PREPARE, i.value, nil, AggEvidence{})
}

// Attempts to end the PREPARE phase and begin COMMIT based on current state.
// No-op if the current phase is not PREPARE.
func (i *instance) tryPrepare() {
	if i.phase != PREPARE {
		panic(fmt.Sprintf("unexpected phase %s", i.phase))
	}

	prepared := i.roundState(i.round).prepared
	// Optimisation: we could advance phase once quorum on our proposal is not possible.
	foundQuorum := prepared.HasQuorumAgreement(i.proposal.Head().CID)
	timeoutExpired := i.host.Time() >= i.phaseTimeout

	if foundQuorum {
		i.value = i.proposal
	} else if timeoutExpired {
		i.value = ECChain{}
	}

	if foundQuorum || timeoutExpired {
		i.beginCommit()
	}
}

func (i *instance) beginCommit() {
	i.phase = COMMIT
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE)
	var (
		isJustified bool
		aggEvidence = ZeroAggEvidence()
	)

	if !i.value.IsZero() { // if it is zero then justification is not really needed
		aggEvidence, isJustified = i.roundState(i.round).prepared.Justify(i.value)
		if !isJustified {
			// error, there should be a justification for a non-bottom COMMIT, otherwise we would not be here now
			panic(fmt.Sprintf("no justification for COMMIT %v", i.value))
		}
	}

	i.broadcast(COMMIT, i.value, nil, aggEvidence)
}

func (i *instance) tryCommit(round uint32) {
	// Unlike all other phases, the COMMIT phase stays open to new messages even after an initial quorum is reached,
	// and the algorithm moves on to the next round.
	// A subsequent COMMIT message can cause the node to decide, so there is no check on the current phase.
	committed := i.roundState(round).committed
	foundQuorum := committed.ListQuorumAgreedValues()
	timeoutExpired := i.host.Time() >= i.phaseTimeout

	if len(foundQuorum) > 0 && !foundQuorum[0].IsZero() {
		// A participant may be forced to decide a value that's not its preferred chain.
		// The participant isn't influencing that decision against their interest, just accepting it.
		i.decide(foundQuorum[0], round)
	} else if i.round == round && i.phase == COMMIT && timeoutExpired && committed.ReceivedFromQuorum() {
		// Adopt any non-empty value committed by another participant (there can only be one).
		// This node has observed the strong quorum of PREPARE messages that justify it,
		// and mean that some other nodes may decide that value (if they observe more COMMITs).

		// 	committedExplicit := committed.(*quorumStateExplicit)

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
}

func (i *instance) roundState(r uint32) *roundState {
	round, ok := i.rounds[r]
	if !ok {
		round = newRoundState(i.powerTable, i.quorumStateType, i.host, i.instanceID, r, i.senderIndex)
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

func (i *instance) decide(value ECChain, round uint32) {
	i.log("✅ decided %s in round %d", &i.value, round)
	i.phase = DECIDE
	// Round is a parameter since a late COMMIT message can result in a decision for a round prior to the current one.
	i.round = round
	i.value = value
}

func (i *instance) decided() bool {
	return i.phase == DECIDE
}

func (i *instance) broadcast(step string, value ECChain, ticket Ticket, evidence AggEvidence) *GMessage {
	payload := SignaturePayload(i.instanceID, i.round, step, value)
	signature := i.host.Sign(i.participantID, payload)
	gmsg := &GMessage{i.participantID, i.instanceID, i.round, step, value, ticket, signature, evidence}
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

///// Message validation/justification helpers /////

// Holds a collection messages received but not yet justified.
type pendingQueue struct {
	// Map by round and phase to list of messages.
	rounds map[uint32]map[string][]*GMessage
}

func newPendingQueue() *pendingQueue {
	return &pendingQueue{
		rounds: map[uint32]map[string][]*GMessage{},
	}
}

// Queues a message for future re-validation.
func (v *pendingQueue) Add(msg *GMessage) {
	rv := v.getRound(msg.Round)
	rv[msg.Step] = append(rv[msg.Step], msg)
}

// Dequeues all messages from some round and phase matching a predicate.
func (v *pendingQueue) PopWhere(round uint32, phase string, pred func(msg *GMessage) bool) []*GMessage {
	var found []*GMessage
	queue := v.getRound(round)[phase]
	// Remove all entries matching predicate, in-place.
	n := 0
	for _, msg := range queue {
		if pred(msg) {
			found = append(found, msg)
		} else {
			queue[n] = msg
			n += 1
		}
	}
	v.rounds[round][phase] = queue[:n]
	return found
}

func (v *pendingQueue) getRound(round uint32) map[string][]*GMessage {
	var rv map[string][]*GMessage
	rv, ok := v.rounds[round]
	if !ok {
		rv = map[string][]*GMessage{
			CONVERGE: nil,
			PREPARE:  nil,
			COMMIT:   nil,
		}
		v.rounds[round] = rv
	}
	return rv
}

// TODO Add comments
type quorumState interface {
	Receive(sender ActorID, value ECChain, sig []byte)

	IsJustified(msg *GMessage, justification ECChain) bool
	Justify(justification ECChain) (AggEvidence, bool)

	HasQuorumAgreement(cid TipSetID) bool
	ListQuorumAgreedValues() []ECChain
	ReceivedFromQuorum() bool
	ListAllValues() []ECChain
}

// /// Incremental quorum-calculation helper /////
// Accumulates values from a collection of senders and incrementally calculates
// which values have reached a strong quorum of support.
// Supports receiving multiple values from each sender, and hence multiple strong quorum values.
type quorumStateImplicit struct {
	// CID of each chain received, by sender. Allows detecting and ignoring duplicates.
	received map[ActorID]senderSent
	// The power supporting each chain so far.
	chainPower map[TipSetID]chainPower
	// Total power of all distinct senders from which some chain has been received so far.
	sendersTotalPower uint
	// Table of senders' power.
	powerTable PowerTable
}

// The set of chain heads from one sender, and that sender's power.
type senderSent struct {
	heads []TipSetID
	power uint
}

// A chain value and the total power supporting it.
type chainPower struct {
	chain     ECChain
	power     uint
	hasQuorum bool
}

type quorumStateType int

const (
	IMPLICIT quorumStateType = iota
	EXPLICIT
)

// Creates a new, empty quorum state.
func newQuorumState(quorumStateType quorumStateType, powerTable PowerTable, aggregator Aggregator, instanceID uint32, round uint32, step string, senderIndex *SenderIndex) quorumState {
	switch quorumStateType {
	case IMPLICIT:
		return newQuorumStateImplicit(powerTable)
	case EXPLICIT:
		return newQuorumStateExplicit(powerTable, aggregator, instanceID, round, step, senderIndex) //TODO This is suboptimal (as the senderIndex needs to be recalculated every time unnecessarily. Fix)
	default:
		panic(fmt.Sprintf("quorumStateType not considered: %v", quorumStateType))
	}
}

func newQuorumStateImplicit(powerTable PowerTable) *quorumStateImplicit {
	return &quorumStateImplicit{
		received:          map[ActorID]senderSent{},
		chainPower:        map[TipSetID]chainPower{},
		sendersTotalPower: 0,
		powerTable:        powerTable,
	}
}

// Receives a new chain from a sender.
func (q *quorumStateImplicit) Receive(sender ActorID, value ECChain, _ []byte) {
	head := value.HeadCIDOrZero()
	fromSender, ok := q.received[sender]
	if ok {
		// Don't double-count the same chain head for a single participant.
		for _, old := range fromSender.heads {
			if head == old {
				return
			}
		}
		fromSender.heads = append(fromSender.heads, head)
	} else {
		// Add sender's power to total the first time a value is received from them.
		senderPower := q.powerTable.Entries[sender]
		q.sendersTotalPower += senderPower
		fromSender = senderSent{[]TipSetID{head}, senderPower}
	}
	q.received[sender] = fromSender

	candidate := chainPower{
		chain:     value,
		power:     q.powerTable.Entries[sender],
		hasQuorum: false,
	}
	if found, ok := q.chainPower[head]; ok {
		candidate.power += found.power
	}
	threshold := q.powerTable.Total * 2 / 3
	if candidate.power > threshold {
		candidate.hasQuorum = true
	}
	q.chainPower[head] = candidate
}

// Checks whether a value has been received before.
func (q *quorumStateImplicit) HasReceived(value ECChain) bool {
	_, ok := q.chainPower[value.HeadCIDOrZero()]
	return ok
}

// Lists all values that have been received from any sender.
// The order of returned values is not defined.
func (q *quorumStateImplicit) ListAllValues() []ECChain {
	var chains []ECChain
	for _, cp := range q.chainPower {
		chains = append(chains, cp.chain)
	}
	return chains
}

// Checks whether at most one distinct value has been received.
func (q *quorumStateImplicit) HasAgreement() bool {
	return len(q.chainPower) <= 1
}

// Checks whether at least one message has been received from a strong quorum of senders.
func (q *quorumStateImplicit) ReceivedFromQuorum() bool {
	return q.sendersTotalPower > q.powerTable.Total*2/3
}

// Checks whether a chain (head) has reached quorum.
func (q *quorumStateImplicit) HasQuorumAgreement(cid TipSetID) bool {
	cp, ok := q.chainPower[cid]
	return ok && cp.hasQuorum
}

// Returns a list of the chains which have reached an agreeing quorum.
// The order of returned values is not defined.
func (q *quorumStateImplicit) ListQuorumAgreedValues() []ECChain {
	var withQuorum []ECChain
	for cid, cp := range q.chainPower {
		if cp.hasQuorum {
			withQuorum = append(withQuorum, q.chainPower[cid].chain)
		}
	}
	sortByWeight(withQuorum)
	return withQuorum
}

func (q *quorumStateImplicit) IsJustified(_ *GMessage, value ECChain) bool {
	return q.HasQuorumAgreement(value.HeadCIDOrZero())
}

func (q *quorumStateImplicit) Justify(_ ECChain) (AggEvidence, bool) {
	return AggEvidence{}, true // IMPLICIT justification
}

type chainSupport struct {
	chainPower   chainPower
	aggSignature []byte
	signers      *bitfield.BitField
}

type quorumStateExplicit struct {
	// Aggregator for BLS signatures.
	aggregator Aggregator
	// Whether a message has been received from each sender.
	received map[ActorID]struct{}
	// The power supporting each chain so far.
	chainSupport map[TipSetID]chainSupport
	// Total power of all distinct senders from which some chain has been received so far.
	sendersTotalPower uint
	// justifiedMessages stores the received evidences for each message, indexed by the message's head CID.
	justifiedMessages map[TipSetID]AggEvidence
	// Table of senders' power.
	powerTable PowerTable
	instanceID uint32
	round      uint32
	step       string

	senderIndex *SenderIndex
}

func newQuorumStateExplicit(powerTable PowerTable, aggregator Aggregator, instanceID uint32, round uint32, step string, senderIndex *SenderIndex) *quorumStateExplicit {
	return &quorumStateExplicit{
		aggregator:        aggregator,
		received:          map[ActorID]struct{}{},
		chainSupport:      map[TipSetID]chainSupport{},
		sendersTotalPower: 0,
		justifiedMessages: map[TipSetID]AggEvidence{},
		powerTable:        powerTable,
		instanceID:        instanceID,
		round:             round,
		step:              step,
		senderIndex:       senderIndex,
	}
}

// Receives a new chain from a sender.
func (q *quorumStateExplicit) Receive(sender ActorID, value ECChain, signature []byte) {
	head := value.HeadCIDOrZero()
	if _, ok := q.received[sender]; !ok {
		// Add sender's power to total the first time a value is received from them.
		senderPower := q.powerTable.Entries[sender]
		q.sendersTotalPower += senderPower
		q.received[sender] = struct{}{}
	}

	candidate := chainSupport{
		chainPower: chainPower{
			chain:     value,
			power:     0,
			hasQuorum: false,
		},
		aggSignature: []byte{},
		signers: func() *bitfield.BitField {
			bf := bitfield.New()
			return &bf
		}(),
	}

	found, ok := q.chainSupport[head]
	if !ok {

		found = candidate
	} else {

		// Don't double-count the same chain head for a single participant.
		isSet, err := found.signers.IsSet(q.senderIndex.Actor2Index[sender])
		if err != nil {
			panic(err)
		}
		if isSet {
			return
		}
	}

	candidate.chainPower.power = found.chainPower.power + q.powerTable.Entries[sender]

	candidate.aggSignature, candidate.signers = q.aggregator.Aggregate(signature, sender, found.aggSignature, found.signers, q.senderIndex.Actor2Index)

	threshold := q.powerTable.Total * 2 / 3
	if candidate.chainPower.power > threshold {
		candidate.chainPower.hasQuorum = true
	}
	q.chainSupport[head] = candidate
}

// IsJustified checks whether a message is justified by the aggregated signature contained in its Evidence field.
func (q *quorumStateExplicit) IsJustified(msg *GMessage, value ECChain) bool {

	if msg.Evidence.isZero() {
		return false
	}

	if msg.Evidence.Step != q.step || msg.Evidence.Instance != q.instanceID || msg.Evidence.Round != q.round {
		return false // this quorumState should not be verifying this msg
	}

	// Verify aggregated signature
	if !q.aggregator.VerifyAggregate(SignaturePayload(q.instanceID, q.round, q.step, value), msg.Evidence.Signature, &msg.Evidence.Signers, q.senderIndex.Actor2Index) {
		return false
	}

	// Verify strong quorum
	totalPower := uint(0)

	for i, j, count := uint64(0), 0, func() uint64 {
		c, err := msg.Evidence.Signers.Count() // Error ignored
		if err != nil {
			panic(err)
		}
		return c
	}(); i < count || j < len(q.senderIndex.index2Actor); {
		isSet, err := msg.Evidence.Signers.IsSet(uint64(j))

		if err != nil {
			panic(err)
		}

		if isSet {
			totalPower += q.powerTable.Entries[q.senderIndex.index2Actor[j]]
			if totalPower > q.powerTable.Total*2/3 {

				q.justifiedMessages[value.HeadCIDOrZero()] = msg.Evidence

				return true
			}
			i++
		}

		j++
	}

	return false
}

// Justify returns the aggregated signature that justifies the given value, if it exists.
func (q *quorumStateExplicit) Justify(justification ECChain) (AggEvidence, bool) {
	head := justification.HeadCIDOrZero()

	if !q.chainSupport[head].chainPower.hasQuorum {
		return AggEvidence{}, false
	}

	signers, err := q.chainSupport[head].signers.Copy()
	if err != nil {
		panic(err)
	}
	//copy aggSignature
	aggSignature := make([]byte, len(q.chainSupport[head].aggSignature))
	copy(aggSignature, q.chainSupport[head].aggSignature)
	aggEvidence := AggEvidence{
		Step:      q.step,
		Value:     justification,
		Instance:  q.instanceID,
		Round:     q.round,
		Signers:   signers,
		Signature: aggSignature,
	}
	return aggEvidence, true
}

// Lists all values that have been received from any sender.
// The order of returned values is not defined.
func (q *quorumStateExplicit) ListAllValues() []ECChain {
	var chains []ECChain
	for _, cp := range q.chainSupport {
		chains = append(chains, cp.chainPower.chain)
	}
	return chains
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

// Checks whether a chain (head) has reached quorum.
func (q *quorumStateExplicit) HasQuorumAgreement(cid TipSetID) bool {
	cp, ok := q.chainSupport[cid]
	return ok && cp.chainPower.hasQuorum
}

// Returns a list of the chains which have reached an agreeing quorum.
// The order of returned values is not defined.
func (q *quorumStateExplicit) ListQuorumAgreedValues() []ECChain {
	var withQuorum []ECChain
	for cid, cp := range q.chainSupport {
		if cp.chainPower.hasQuorum {
			withQuorum = append(withQuorum, q.chainSupport[cid].chainPower.chain)
		}
	}
	sortByWeight(withQuorum)
	return withQuorum
}

// Checks whether at least one message has been received from a strong quorum of senders.
func (q *quorumStateExplicit) ReceivedFromQuorum() bool {
	return q.sendersTotalPower > q.powerTable.Total*2/3
}

// Receives a new CONVERGE value from a sender.
func (c *convergeState) Receive(value ECChain, ticket Ticket) {
	if value.IsZero() {
		panic("bottom cannot be justified for CONVERGE")
	}
	key := value.Head().CID
	c.values[key] = value
	c.tickets[key] = append(c.tickets[key], ticket)
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

// SenderIndex maps ActorID to a unique index in the range [0, len(powerTable.Entries)).
// This is used to index into a BitSet. index2Actor is the reverse mapping.
type SenderIndex struct {
	Actor2Index map[ActorID]uint64
	index2Actor []ActorID
}

func NewSenderIndex(table PowerTable) *SenderIndex {
	senderIndex := &SenderIndex{
		Actor2Index: make(map[ActorID]uint64, len(table.Entries)),
		index2Actor: make([]ActorID, len(table.Entries)),
	}
	keys := make([]ActorID, 0, len(table.Entries))

	// Extract keys from the map
	for senderID := range table.Entries {
		keys = append(keys, senderID)
	}

	// Sort the keys by descending power
	sort.Slice(keys, func(i, j int) bool {
		return table.Entries[keys[i]] > table.Entries[keys[j]] || (table.Entries[keys[i]] == table.Entries[keys[j]] && keys[i] < keys[j])
	})
	senderIndex.index2Actor = keys

	// Iterate over the sorted keys
	var i uint64 = 0
	for _, senderID := range keys {
		senderIndex.Actor2Index[senderID] = i
		i++
	}

	return senderIndex
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
