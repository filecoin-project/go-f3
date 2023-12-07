package f3

import (
	"fmt"
	"github.com/filecoin-project/go-f3/net"
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

type GMessage struct {
	Instance int
	Round    int
	Sender   net.ActorID
	Step     string
	Ticket   Ticket
	Value    net.ECChain
}

func (m GMessage) String() string {
	// FIXME This needs value receiver to work, for reasons I cannot figure out.
	return fmt.Sprintf("%s{%d}(%d %s)", m.Step, m.Instance, m.Round, &m.Value)
}

// A single Granite consensus instance.
type instance struct {
	config        GraniteConfig
	ntwk          net.NetworkSink
	vrf           VRFer
	participantID net.ActorID
	instanceID    int
	// The EC chain input to this instance.
	input net.ECChain
	// The power table for the base chain, used for power in this instance.
	powerTable net.PowerTable
	// The beacon value from the base chain, used for tickets in this instance.
	beacon []byte
	// Current round number.
	round int
	// Current phase in the round.
	phase string
	// Time at which the current phase can or must end.
	// For QUALITY, PREPARE, and COMMIT, this is the latest time (the phase can end sooner).
	// For CONVERGE, this is the exact time (the timeout solely defines the phase end).
	phaseTimeout float64
	// This instance's proposal for the current round.
	// This is set after the QUALITY phase, and changes only at the end of a full round.
	proposal net.ECChain
	// The value to be transmitted at the next phase.
	// This value may change away from the proposal between phases.
	value net.ECChain
	// Queue of messages to be synchronously processed before returning from top-level call.
	inbox []GMessage
	// Messages received earlier but not yet justified.
	pending *pendingQueue
	// Quality phase state (only for round 0)
	quality *quorumState
	// State for each round of phases.
	// State from prior rounds must be maintained to provide justification for values in subsequent rounds.
	rounds map[int]*roundState
}

func newInstance(
	config GraniteConfig,
	ntwk net.NetworkSink,
	vrf VRFer,
	participantID net.ActorID,
	instanceID int,
	input net.ECChain,
	powerTable net.PowerTable,
	beacon []byte) *instance {
	if input.IsZero() {
		panic("input is empty")
	}
	return &instance{
		config:        config,
		ntwk:          ntwk,
		vrf:           vrf,
		participantID: participantID,
		instanceID:    instanceID,
		input:         input,
		powerTable:    powerTable,
		beacon:        beacon,
		round:         0,
		phase:         "",
		proposal:      input,
		value:         net.ECChain{},
		pending:       newPendingQueue(),
		quality:       newQuorumState(powerTable),
		rounds: map[int]*roundState{
			0: newRoundState(powerTable),
		},
	}
}

type roundState struct {
	converged *convergeState
	prepared  *quorumState
	committed *quorumState
}

func newRoundState(powerTable net.PowerTable) *roundState {
	return &roundState{
		converged: newConvergeState(),
		prepared:  newQuorumState(powerTable),
		committed: newQuorumState(powerTable),
	}
}

func (i *instance) Start() {
	i.beginQuality()
	i.drainInbox()
}

func (i *instance) Receive(msg GMessage) {
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

func (i *instance) enqueueInbox(msg GMessage) {
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
func (i *instance) receiveOne(msg GMessage) {
	// Drop any messages that can never be valid.
	if !i.isValid(&msg) {
		i.log("dropping invalid %s", msg)
		return
	}

	// Hold as pending any message with a value not yet justified by the prior phase.
	if !i.isJustified(&msg) {
		i.log("enqueue %s", msg)
		i.pending.Add(msg)
		return
	}

	round := i.roundState(msg.Round)
	switch msg.Step {
	case QUALITY:
		// Receive each prefix of the proposal independently.
		for j := range msg.Value.Suffix() {
			prefix := msg.Value.Prefix(j + 1)
			i.quality.Receive(msg.Sender, prefix)
		}
	case CONVERGE:
		round.converged.Receive(msg.Value, msg.Ticket)
	case PREPARE:
		round.prepared.Receive(msg.Sender, msg.Value)
	case COMMIT:
		round.committed.Receive(msg.Sender, msg.Value)
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
		return prevRound.prepared.HasQuorumAgreement(msg.Value.Head().CID) ||
			prevRound.committed.HasQuorumAgreement("")
	} else if msg.Step == PREPARE {
		// PREPARE needs no justification by prior messages.
		return true // i.quality.AllowsValue(msg.Value)
	} else if msg.Step == COMMIT {
		// COMMIT is justified by strong quorum of PREPARE from the same round with the same value.
		// COMMIT for bottom is always justified.
		round := i.roundState(msg.Round)
		return msg.Value.IsZero() || round.prepared.HasQuorumAgreement(msg.Value.HeadCIDOrZero())
	}
	return false
}

// Sends this node's QUALITY message and begins the QUALITY phase.
func (i *instance) beginQuality() {
	// Broadcast input value and wait up to Δ to receive from others.
	i.phase = QUALITY
	i.phaseTimeout = i.alarmAfterSynchrony(QUALITY)
	i.broadcast(QUALITY, i.input, nil)
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
	timeoutExpired := i.ntwk.Time() >= i.phaseTimeout

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
	i.broadcast(CONVERGE, i.proposal, ticket)
}

// Attempts to end the CONVERGE phase and begin PREPARE based on current state.
// No-op if the current phase is not CONVERGE.
func (i *instance) tryConverge() {
	if i.phase != CONVERGE {
		panic(fmt.Sprintf("unexpected phase %s", i.phase))
	}
	timeoutExpired := i.ntwk.Time() >= i.phaseTimeout
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
		i.value = net.ECChain{}
	}
	i.beginPrepare()
}

// Sends this node's PREPARE message and begins the PREPARE phase.
func (i *instance) beginPrepare() {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE)
	i.broadcast(PREPARE, i.value, nil)
}

// Attempts to end the PREPARE phase and begin COMMIT based on current state.
// No-op if the current phase is not PREPARE.
func (i *instance) tryPrepare() {
	if i.phase != PREPARE {
		panic(fmt.Sprintf("unexpected phase %s", i.phase))
	}

	prepared := i.roundState(i.round).prepared
	// TODO: (optimisation) Advance phase once quorum on our proposal is not possible.
	foundQuorum := prepared.HasQuorumAgreement(i.proposal.Head().CID)
	timeoutExpired := i.ntwk.Time() >= i.phaseTimeout

	if foundQuorum {
		i.value = i.proposal
	} else if timeoutExpired {
		i.value = net.ECChain{}
	}

	if foundQuorum || timeoutExpired {
		i.beginCommit()
	}
}

func (i *instance) beginCommit() {
	i.phase = COMMIT
	i.phaseTimeout = i.alarmAfterSynchrony(PREPARE)
	i.broadcast(COMMIT, i.value, nil)
}

func (i *instance) tryCommit(round int) {
	// Unlike all other phases, the COMMIT phase stays open to new messages even after an initial quorum is reached,
	// and the algorithm moves on to the next round.
	// A subsequent COMMIT message can cause the node to decide, so there is no check on the current phase.
	committed := i.roundState(round).committed
	foundQuorum := committed.ListQuorumAgreedValues()
	timeoutExpired := i.ntwk.Time() >= i.phaseTimeout

	if len(foundQuorum) > 0 && !foundQuorum[0].IsZero() {
		// A participant may be forced to decide a value that's not its preferred chain.
		// The participant isn't influencing that decision against their interest, just accepting it.
		i.decide(foundQuorum[0], round)
	} else if i.round == round && i.phase == COMMIT && timeoutExpired && committed.ReceivedFromQuorum() {
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
}

func (i *instance) roundState(r int) *roundState {
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
func (i *instance) isAcceptable(c net.ECChain) bool {
	// TODO: expand to include subsequently notified chains.
	return i.input.HasPrefix(c)
}

func (i *instance) decide(value net.ECChain, round int) {
	i.log("✅ decided %s in round %d", &i.value, round)
	i.phase = DECIDE
	// Round is a parameter since a late COMMIT message can result in a decision for a round prior to the current one.
	i.round = round
	i.value = value
}

func (i *instance) decided() bool {
	return i.phase == DECIDE
}

func (i *instance) broadcast(step string, value net.ECChain, ticket Ticket) GMessage {
	gmsg := GMessage{i.instanceID, i.round, i.participantID, step, ticket, value}
	i.ntwk.Broadcast(i.participantID, gmsg)
	i.enqueueInbox(gmsg)
	return gmsg
}

// Sets an alarm to be delivered after a synchrony delay.
// The delay duration increases with each round.
// Returns the absolute time at which the alarm will fire.
func (i *instance) alarmAfterSynchrony(payload string) float64 {
	timeout := i.ntwk.Time() + i.config.Delta + (float64(i.round) * i.config.DeltaRate)
	i.ntwk.SetAlarm(i.participantID, payload, timeout)
	return timeout
}

func (i *instance) log(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	i.ntwk.Log("P%d{%d}: %s (round %d, step %s, proposal %s, value %s)", i.participantID, i.instanceID, msg,
		i.round, i.phase, &i.proposal, &i.value)
}

///// Message validation/justification helpers /////

// Holds a collection messages received but not yet justified.
type pendingQueue struct {
	// Map by round and phase to list of messages.
	rounds map[int]map[string][]GMessage
}

func newPendingQueue() *pendingQueue {
	return &pendingQueue{
		rounds: map[int]map[string][]GMessage{},
	}
}

// Queues a message for future re-validation.
func (v *pendingQueue) Add(msg GMessage) {
	rv := v.getRound(msg.Round)
	rv[msg.Step] = append(rv[msg.Step], msg)
}

// Dequeues all messages from some round and phase matching a predicate.
func (v *pendingQueue) PopWhere(round int, phase string, pred func(msg *GMessage) bool) []GMessage {
	var found []GMessage
	queue := v.getRound(round)[phase]
	// Remove all entries matching predicate, in-place.
	n := 0
	for _, msg := range queue {
		if pred(&msg) {
			found = append(found, msg)
		} else {
			queue[n] = msg
			n += 1
		}
	}
	v.rounds[round][phase] = queue[:n]
	return found
}

func (v *pendingQueue) getRound(round int) map[string][]GMessage {
	var rv map[string][]GMessage
	rv, ok := v.rounds[round]
	if !ok {
		rv = map[string][]GMessage{
			CONVERGE: nil,
			PREPARE:  nil,
			COMMIT:   nil,
		}
		v.rounds[round] = rv
	}
	return rv
}

///// Incremental quorum-calculation helper /////

// Accumulates values from a collection of senders and incrementally calculates
// which values have reached a strong quorum of support.
// Supports receiving multiple values from each sender, and hence multiple strong quorum values.
type quorumState struct {
	// CID of each chain received, by sender. Allows detecting and ignoring duplicates.
	received map[net.ActorID]senderSent
	// The power supporting each chain so far.
	chainPower map[net.CID]chainPower
	// Set of chains that have reached the threshold power so far.
	// TODO: this could be folded into the chainPower map.
	withQuorumAgreement map[net.CID]struct{}
	// Total power of all distinct senders from which some chain has been received so far.
	sendersTotalPower uint
	// Table of senders' power.
	powerTable net.PowerTable
}

// The set of chain heads from one sender, and that sender's power.
type senderSent struct {
	heads []net.CID
	power uint
}

// A chain value and the total power supporting it.
type chainPower struct {
	chain net.ECChain
	power uint
}

// Creates a new, empty quorum state.
func newQuorumState(powerTable net.PowerTable) *quorumState {
	return &quorumState{
		received:            map[net.ActorID]senderSent{},
		chainPower:          map[net.CID]chainPower{},
		withQuorumAgreement: map[net.CID]struct{}{},
		sendersTotalPower:   0,
		powerTable:          powerTable,
	}
}

// Receives a new chain from a sender.
func (q *quorumState) Receive(sender net.ActorID, value net.ECChain) {
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
		fromSender = senderSent{[]net.CID{head}, senderPower}
	}
	q.received[sender] = fromSender

	candidate := chainPower{
		chain: value,
		power: q.powerTable.Entries[sender],
	}
	if found, ok := q.chainPower[head]; ok {
		candidate.power += found.power
	}
	q.chainPower[head] = candidate

	threshold := q.powerTable.Total * 2 / 3
	if candidate.power > threshold {
		q.withQuorumAgreement[head] = struct{}{}
	}
}

// Checks whether a value has been received before.
func (q *quorumState) HasReceived(value net.ECChain) bool {
	_, ok := q.chainPower[value.HeadCIDOrZero()]
	return ok
}

// Lists all values that have been received from any sender.
// The order of returned values is not defined.
func (q *quorumState) ListAllValues() []net.ECChain {
	var chains []net.ECChain
	for _, cp := range q.chainPower {
		chains = append(chains, cp.chain)
	}
	return chains
}

// Checks whether at most one distinct value has been received.
func (q *quorumState) HasAgreement() bool {
	return len(q.chainPower) <= 1
}

// Checks whether at least one message has been received from a strong quorum of senders.
func (q *quorumState) ReceivedFromQuorum() bool {
	return q.sendersTotalPower > q.powerTable.Total*2/3
}

// Checks whether a chain (head) has reached quorum.
func (q *quorumState) HasQuorumAgreement(cid net.CID) bool {
	_, ok := q.withQuorumAgreement[cid]
	return ok
}

// Returns a list of the chains which have reached an agreeing quorum.
// The order of returned values is not defined.
func (q *quorumState) ListQuorumAgreedValues() []net.ECChain {
	var quorum []net.ECChain
	for cid := range q.withQuorumAgreement {
		quorum = append(quorum, q.chainPower[cid].chain)
	}
	sortByWeight(quorum)
	return quorum
}

//// CONVERGE phase helper /////

type convergeState struct {
	// Chains indexed by head CID
	values map[net.CID]net.ECChain
	// Tickets provided by proposers of each chain.
	tickets map[net.CID][]Ticket
}

func newConvergeState() *convergeState {
	return &convergeState{
		values:  map[net.CID]net.ECChain{},
		tickets: map[net.CID][]Ticket{},
	}
}

// Receives a new CONVERGE value from a sender.
func (c *convergeState) Receive(value net.ECChain, ticket Ticket) {
	if value.IsZero() {
		panic("bottom cannot be justified for CONVERGE")
	}
	key := value.Head().CID
	c.values[key] = value
	c.tickets[key] = append(c.tickets[key], ticket)
}

func (c *convergeState) findMinTicketProposal() net.ECChain {
	var minTicket Ticket
	var minValue net.ECChain
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
func findFirstPrefixOf(candidates []net.ECChain, preferred net.ECChain) net.ECChain {
	for _, v := range candidates {
		if preferred.HasPrefix(v) {
			return v
		}
	}

	// No candidates are a prefix of preferred.
	return preferred.BaseChain()
}

// Sorts chains by weight of their head, descending
func sortByWeight(chains []net.ECChain) {
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
