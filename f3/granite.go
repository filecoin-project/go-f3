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
	return fmt.Sprintf("%s(%d/%d %s)", m.Step, m.Instance, m.Round, &m.Value)
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
	// Earliest at which the PREPARE phase can end.
	prepareTimeout float64
	// This instance's proposal for the current round.
	// This is set after the QUALITY phase, and changes only at the end of a full round.
	proposal net.ECChain
	// The value to be transmitted at the next phase.
	// This value may change away from the proposal between phases.
	value net.ECChain
	// Queue of messages to be synchronously processed before returning from top-level receive call.
	inbox []GMessage
	// Message validation predicates and queues, by round and phase.
	validation *validationQueue
	// Quality phase state (only for round 0)
	quality *qualityState
	// State for each round of phases.
	// State from prior rounds must be maintained to provide justification for values in subsequent rounds.
	rounds map[int]*roundState
}

type roundState struct {
	converged *convergeState
	prepared  *prepareState
	committed *commitState
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
		validation:    newValidationQueue(input.Base().CID),
		quality:       newQualityState(input.Base().CID, powerTable),
		rounds: map[int]*roundState{
			0: newRoundState(powerTable),
		},
	}
}

func newRoundState(powerTable net.PowerTable) *roundState {
	return &roundState{
		converged: newConvergeState(),
		prepared:  newPrepareState(powerTable),
		committed: newCommitState(powerTable),
	}
}

func (i *instance) Start() {
	i.beginQuality()
}

func (i *instance) Receive(msg GMessage) {
	if i.decided() {
		panic("received message after decision")
	}
	isTopLevel := len(i.inbox) == 0
	// Enqueue the message for synchronous processing.
	i.inbox = append(i.inbox, msg)
	// If the inbox was empty, drain it now.
	// If it wasn't, that means this message was received recursively while already draining the inbox.
	if isTopLevel {
		i.drainInbox()
	}
}

func (i *instance) drainInbox() {
	for len(i.inbox) > 0 {
		// Process one message.
		// Note the message being processed is left in the inbox until after processing,
		// as a signal that this loop is currently draining the inbox.
		nextRound, nextPhase := i.receiveOne(i.inbox[0])
		i.inbox = i.inbox[1:]

		// Pop all now-valid messages for the subsequent round/phase and enqueue for receiving them again.
		if nextPhase != "" {
			replay := i.validation.PopJustified(nextRound, nextPhase)
			i.inbox = append(i.inbox, replay...)
		}
	}
}

// Processes a single message.
// Returns the round and name of a phase for which the message may have justified other pending messages, or empty.
func (i *instance) receiveOne(msg GMessage) (int, string) {
	round, ok := i.rounds[msg.Round]
	if !ok {
		round = newRoundState(i.powerTable)
		i.rounds[msg.Round] = round
	}

	if !(msg.Value.IsZero() || msg.Value.HasBase(i.input.Base())) {
		panic(fmt.Sprintf("unexpected base %s", &msg.Value))
	}
	// A message must have both
	// - value allowed by the QUALITY phase, and
	// - justification from tne previous phase
	if !((msg.Step == QUALITY || i.quality.AllowsValue(msg.Value)) && i.validation.IsJustified(&msg)) {
		i.validation.Enqueue(msg)
		return 0, ""
	}

	var newAllowedValues []net.ECChain
	var nextRound int
	var nextPhase string
	if msg.Step == QUALITY && msg.Round == 0 {
		// Just collect all the messages until the alarm triggers the end of QUALITY phase.
		// Messages continue being collected after QUALITY timeout passes, in case they justify later messages.
		newAllowedValues = i.quality.Receive(msg.Sender, msg.Value)
		nextRound = msg.Round
		// FIXME: quality is a gate for all phases, so all phases need to be reprocessed.
		nextPhase = PREPARE
	} else if msg.Step == CONVERGE && msg.Round > 0 &&
		i.vrf.VerifyTicket(i.beacon, i.instanceID, msg.Round, msg.Sender, msg.Ticket) {
		// Collect messages until the alarm triggers the end of CONVERGE phase.
		newAllowedValues = round.converged.Receive(msg.Value, msg.Ticket)
		nextRound = msg.Round
		nextPhase = PREPARE
	} else if msg.Step == PREPARE {
		newAllowedValues = round.prepared.Receive(msg.Sender, msg.Value)
		i.tryPrepare(msg.Round) // FIXME should try the next step only after adding justifications from this step.
		nextRound = msg.Round
		nextPhase = COMMIT
	} else if msg.Step == COMMIT {
		newAllowedValues = round.committed.Receive(msg.Sender, msg.Value)
		i.tryCommit(msg.Round) // FIXME should try the next step only after adding justifications from this step.
		nextRound = msg.Round + 1
		nextPhase = CONVERGE
	} else {
		panic(fmt.Sprintf("unexpected message %v", msg))
	}

	// Propagate any new values that have been allowed by the message's phase into the subsequent phase.
	for _, v := range newAllowedValues {
		if v.IsZero() && nextPhase == CONVERGE {
			// Bottom as an allowed value is a sentinel for justifying any CONVERGE.
			i.validation.JustifyAll(nextRound, nextPhase)
		} else {
			i.validation.AddJustified(nextRound, nextPhase, v.HeadCIDOrZero())
		}
	}
	if len(newAllowedValues) > 0 {
		return nextRound, nextPhase
	}
	return 0, ""
}

func (i *instance) receiveAlarm(payload string) {
	if payload == QUALITY {
		i.endQuality()
	} else if payload == CONVERGE {
		i.endConverge(i.round)
	} else if payload == PREPARE {
		i.tryPrepare(i.round)
	}
}

func (i *instance) beginQuality() {
	// Broadcast input value and wait 2Δ to receive from others.
	i.phase = QUALITY
	msg := i.broadcast(QUALITY, i.input, nil)
	i.Receive(msg)
	i.alarmAfterSynchrony(QUALITY)
}

func (i *instance) endQuality() {
	allowed := i.quality.ListQuorumAgreedValues()
	i.proposal = findFirstPrefixOf(allowed, i.proposal)
	i.value = i.proposal
	i.log("adopting proposal and value %s", &i.proposal)
	i.beginPrepare()
}

func (i *instance) beginConverge() {
	i.phase = CONVERGE
	ticket := i.vrf.MakeTicket(i.beacon, i.instanceID, i.round, i.participantID)
	msg := i.broadcast(CONVERGE, i.proposal, ticket)
	i.Receive(msg)
	i.alarmAfterSynchrony(CONVERGE)
}

func (i *instance) endConverge(round int) {
	i.value = i.rounds[round].converged.findMinTicketProposal()
	if i.isAcceptable(i.value) {
		if !i.value.Eq(i.proposal) {
			i.proposal = i.value
			i.log("adopting proposal %s after converge", &i.proposal)
		}
	} else {
		i.log("⚠️ voting from %s to %s by min ticket", &i.input, &i.value)
	}
	i.beginPrepare()
}

func (i *instance) beginPrepare() {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE
	i.prepareTimeout = i.alarmAfterSynchrony(PREPARE)
	msg := i.broadcast(PREPARE, i.value, nil)
	i.Receive(msg)
}

func (i *instance) tryPrepare(round int) {
	if i.round != round || i.phase != PREPARE {
		return
	}
	// Wait at least 2Δ.
	if i.ntwk.Time() < i.prepareTimeout {
		return
	}

	prepared := i.rounds[round].prepared
	if prepared.ReceivedFromQuorum() {
		foundQuorum := prepared.ListQuorumAgreedHeadValues()
		var v net.ECChain
		if len(foundQuorum) > 0 {
			v = foundQuorum[0]
		}

		// INCENTIVE-COMPATIBLE: Only commit a value equal to this node's proposal.
		if v.Eq(i.proposal) {
			i.value = i.proposal
		} else {
			// Update our proposal for next round to accept a prefix of it, if that gained quorum, else baseChain.
			// Note: these two lines were present in an earlier version of the algorithm, but have been removed.
			// The attempt to improve progress by adopting a prefix of our proposal more likely to gain quorum.
			// They can cause problems with a proposal incompatible with a value preventing message validation.
			//prefixes := prepared.ListQuorumAgreedPrefixValues()
			//i.proposal = findFirstPrefixOf(prefixes, i.proposal)
			//i.log("adopting proposal %s after prepare", &i.proposal)

			// Commit bottom in this round anyway.
			i.value = net.ECChain{}
		}

		i.beginCommit()
	}
}

func (i *instance) beginCommit() {
	i.phase = COMMIT
	msg := i.broadcast(COMMIT, i.value, nil)
	i.Receive(msg)
}

func (i *instance) tryCommit(round int) {
	// Unlike all previous phases, the COMMIT phase stays open to new messages even after an initial quorum is reached,
	// and the algorithm moves on to the next round.
	// A subsequent COMMIT message can cause the node to decide, if it forms a quorum in agreement.
	//if i.phase != COMMIT {
	//	return
	//}
	committed := i.rounds[round].committed
	if committed.ReceivedFromQuorum() {
		foundQuorum := committed.ListQuorumAgreedValues()
		if len(foundQuorum) > 0 && !foundQuorum[0].IsZero() {
			// A participant may be forced to decide a value that's not its preferred chain.
			// The participant isn't influencing that decision against their interest, just accepting it.
			i.decide(foundQuorum[0], round)

		} else if i.phase == COMMIT {
			// Adopt any non-empty value committed by another participant.
			// (There can only be one, since they needed to see a strong quorum of PREPARE to commit it).
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
	i.ntwk.Log("P%d/%d: %v", i.participantID, i.instanceID, msg)
}

///// Message validation/justification helpers /////

// Holds the set of justified values for each phase in each round,
// and a set of messages received but not yet justified.
type validationQueue struct {
	base   net.CID
	rounds map[int]map[string]*phaseValidationQueue
}

// A predicate validating messages for a single phase, and a queue of messages not yet validated.
type phaseValidationQueue struct {
	// Whether all chains are justified for this phase.
	justifiesAll bool
	// Chain heads allowed by the prior phase.
	justified map[net.CID]struct{}
	// Messages received but not yet allowed by prior phase.
	pending []GMessage
}

func newValidationQueue(base net.CID) *validationQueue {
	return &validationQueue{
		base:   base,
		rounds: map[int]map[string]*phaseValidationQueue{},
	}
}

func newPhaseValidationState() *phaseValidationQueue {
	return &phaseValidationQueue{
		justifiesAll: false,
		justified:    map[net.CID]struct{}{},
		pending:      []GMessage{},
	}
}

func (v *validationQueue) IsJustified(msg *GMessage) bool {
	// No justification needed for QUALITY messages beyond the right base chain.
	if msg.Round == 0 && msg.Step == QUALITY {
		return true
	}
	// First round prepare are always justified (if allowed by quality).
	if msg.Round == 0 && msg.Step == PREPARE {
		return true
	}
	// PREPARE for base chain or bottom is always justified.
	if msg.Step == PREPARE && (msg.Value.IsZero() || msg.Value.Base().CID == v.base) {
		return true
	}
	if rv, ok := v.rounds[msg.Round]; ok {
		if pv, ok := rv[msg.Step]; ok {
			_, ok := pv.justified[msg.Value.HeadCIDOrZero()]
			ok = ok || pv.justifiesAll
			return ok
		}
	}
	return false
}

// Marks a new values as being allowed by the previous phase.
func (v *validationQueue) AddJustified(round int, phase string, value net.CID) {
	pv := v.getPhase(round, phase)
	pv.justified[value] = struct{}{}
}

func (v *validationQueue) JustifyAll(round int, phase string) {
	pv := v.getPhase(round, phase)
	pv.justifiesAll = true
}

// Queues a message for future re-validation.
func (v *validationQueue) Enqueue(msg GMessage) {
	pv := v.getPhase(msg.Round, msg.Step)
	pv.pending = append(pv.pending, msg)
}

func (v *validationQueue) PopJustified(round int, phase string) []GMessage {
	if phase == QUALITY {
		return nil
	}
	pv := v.getPhase(round, phase)
	var justified []GMessage
	// Remove all justified entries, in-place.
	n := 0
	for _, msg := range pv.pending {
		if v.IsJustified(&msg) {
			justified = append(justified, msg)
		} else {
			pv.pending[n] = msg
			n += 1
		}
	}
	pv.pending = pv.pending[:n]
	return justified
}

func (v *validationQueue) getPhase(round int, phase string) *phaseValidationQueue {
	var rv map[string]*phaseValidationQueue
	rv, ok := v.rounds[round]
	if !ok {
		rv = map[string]*phaseValidationQueue{
			CONVERGE: newPhaseValidationState(),
			PREPARE:  newPhaseValidationState(),
			COMMIT:   newPhaseValidationState(),
		}
		v.rounds[round] = rv
	}

	pv := rv[phase]
	return pv
}

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
// Returns whether the chain produced a new quorum in agreement on that chain (can be true multiple times).
func (q *quorumState) Receive(sender net.ActorID, value net.ECChain) bool {
	threshold := q.powerTable.Total * 2 / 3

	head := value.HeadCIDOrZero()
	ss, ok := q.received[sender]
	if ok {
		// Don't double-count the same chain head for a single participant.
		for _, recvd := range ss.heads {
			if head == recvd {
				// Ignore duplicate.
				return false
			}
		}
		ss.heads = append(ss.heads, head)
	} else {
		// Add sender's power to total the first time a value is received from them.
		senderPower := q.powerTable.Entries[sender]
		q.sendersTotalPower += senderPower
		ss = senderSent{[]net.CID{head}, senderPower}
	}
	q.received[sender] = ss

	candidate := chainPower{
		chain: value,
		power: q.powerTable.Entries[sender],
	}
	if found, ok := q.chainPower[head]; ok {
		candidate.power += found.power
	}
	q.chainPower[head] = candidate

	if candidate.power > threshold {
		if _, ok := q.withQuorumAgreement[head]; !ok {
			q.withQuorumAgreement[head] = struct{}{}
			return true
		}
	}
	return false
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
	return quorum
}

///// QUALITY phase helpers /////

// State for the quality phase.
// Incrementally calculates the set of prefixes allowed (having sufficient power) by messages received so far.
type qualityState struct {
	// Base tipset CID
	base net.CID
	// Quorum state for each prefix of each chain received.
	prefixes *quorumState
}

// Creates a new, empty quality state.
func newQualityState(base net.CID, powerTable net.PowerTable) *qualityState {
	return &qualityState{
		base:     base,
		prefixes: newQuorumState(powerTable),
	}
}

// Receives a new QUALITY value from a sender.
// Returns a collection of chains (including prefixes) that reached quorum as a result of the new value,
// and did not have quorum before.
func (q *qualityState) Receive(sender net.ActorID, value net.ECChain) []net.ECChain {
	var newlyAllowed []net.ECChain
	// Add each non-empty prefix of the chain to the quorum state.
	for j := range value.Suffix() {
		prefix := value.Prefix(j + 1)
		foundQuorumValue := q.prefixes.Receive(sender, prefix)
		if foundQuorumValue {
			newlyAllowed = append(newlyAllowed, prefix)
		}
	}
	return newlyAllowed
}

// Checks whether a chain head is allowed by the received QUALITY messages.
func (q *qualityState) AllowsValue(t net.ECChain) bool {
	return t.IsZero() || t.Head().CID == q.base || q.prefixes.HasQuorumAgreement(t.Head().CID)
}

// Returns a list of chains allowed by the received QUALITY values, in descending weight order.
func (q *qualityState) ListQuorumAgreedValues() []net.ECChain {
	quora := q.prefixes.ListQuorumAgreedValues()
	sortByWeight(quora)
	return quora
}

///// PREPARE phase helpers /////

type prepareState struct {
	// Quorum state for the head of each allowed full chain.
	// This provides the value (if any) to be committed in the current round.
	chains *quorumState
	// Quorum state for each prefix of each allowed chain.
	// This is used when a strong quorum is not reached on the heads to find a prefix that is supported,
	// to take to the next round.
	prefixes *quorumState
}

func newPrepareState(power net.PowerTable) *prepareState {
	return &prepareState{
		chains:   newQuorumState(power),
		prefixes: newQuorumState(power),
	}
}

// Receives a new PREPARE value from a sender.
// Returns values that are newly justified for the next phase:
// - a chain that reached strong quorum as a result of the new value,
// - bottom if the collection created a disagreeing quorum,
func (p *prepareState) Receive(sender net.ActorID, value net.ECChain) []net.ECChain {
	wasBelowQuorum := !p.chains.ReceivedFromQuorum()
	wasInAgreement := p.chains.HasAgreement()

	foundQuorumValue := p.chains.Receive(sender, value)
	p.prefixes.Receive(sender, value)

	// COMMIT is justified by a strong quorum that agree on the value.
	var newlyAllowed []net.ECChain
	if foundQuorumValue {
		newlyAllowed = append(newlyAllowed, value)
	}

	// COMMIT ⏊ is justified by a disagreeing quorum.
	isAboveQuorum := p.chains.ReceivedFromQuorum()
	hasDisagreement := !p.chains.HasAgreement()
	foundDisagreement := (wasBelowQuorum || wasInAgreement) && isAboveQuorum && hasDisagreement
	if foundDisagreement {
		newlyAllowed = append(newlyAllowed, net.ECChain{})
	}
	return newlyAllowed
}

// Checks whether at least one message has been received from a strong quorum of senders.
func (p *prepareState) ReceivedFromQuorum() bool {
	return p.chains.ReceivedFromQuorum()
}

// Lists heads of proposals that gained strong quorum, ordered descending by weight.
// There may be more than one if some sender equivocated.
func (p *prepareState) ListQuorumAgreedHeadValues() []net.ECChain {
	quora := p.chains.ListQuorumAgreedValues()
	sortByWeight(quora)
	return quora
}

// List prefixes of proposals that gained strong quorum, ordered descending by weight.
func (p *prepareState) ListQuorumAgreedPrefixValues() []net.ECChain {
	quora := p.prefixes.ListQuorumAgreedValues()
	sortByWeight(quora)
	return quora
}

///// COMMIT phase helpers /////

type commitState struct {
	// Quorum state for the head of each allowed full chain.
	// This provides the value (if any) to be committed in the current round.
	chains *quorumState
}

func newCommitState(power net.PowerTable) *commitState {
	return &commitState{
		chains: newQuorumState(power),
	}
}

// Receives a new COMMIT value from a sender.
// Returns values that are newly justified for the next phase:
// - any value contained in commits
// - anything, if commits agreed on ⏊
func (c *commitState) Receive(sender net.ActorID, value net.ECChain) []net.ECChain {
	seenValue := c.chains.HasReceived(value)
	hadQuorum := c.chains.ReceivedFromQuorum()
	foundQuorumValue := c.chains.Receive(sender, value)
	nowHasQuorum := c.chains.ReceivedFromQuorum()

	var newlyAllowed []net.ECChain
	if !hadQuorum && nowHasQuorum {
		// On first reaching a strong quorum, all non-bottom values received justify a subsequent CONVERGE.
		for _, v := range c.chains.ListAllValues() {
			if !v.IsZero() {
				newlyAllowed = append(newlyAllowed, v)
			}
		}
	} else if hadQuorum && !seenValue {
		// With quorum already reached, any new values now justify a subsequent CONVERGE.
		newlyAllowed = append(newlyAllowed, value)
	}

	// Any CONVERGE is justified by a strong quorum that agree on ⏊.
	if foundQuorumValue && value.IsZero() {
		// The bottom value is interpreted as a sentinel for justifying anything.
		newlyAllowed = append(newlyAllowed, value)
	}

	// TODO: the spec is currently ignoring prefixes of PREPARE as necessary justification for a subsequent CONVERGE.
	//   If the spec updates, this needs more.
	return newlyAllowed
}

// Checks whether at least one message has been received from a strong quorum of senders.
func (c *commitState) ReceivedFromQuorum() bool {
	return c.chains.ReceivedFromQuorum()
}

// Lists heads of proposals that gained strong quorum, ordered descending by weight.
// There may be more than one if some sender equivocated.
func (c *commitState) ListQuorumAgreedValues() []net.ECChain {
	quora := c.chains.ListQuorumAgreedValues()
	sortByWeight(quora)
	return quora
}

func (c *commitState) ListAllValues() []net.ECChain {
	return c.chains.ListAllValues()
}

//// CONVERGE phase helpers /////

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
// Returns values that are newly justified for the next phase: any value the first time it is received.
func (c *convergeState) Receive(value net.ECChain, ticket Ticket) []net.ECChain {
	if value.IsZero() {
		panic("bottom cannot be justified for CONVERGE")
	}
	key := value.Head().CID
	_, found := c.values[key]
	c.values[key] = value
	c.tickets[key] = append(c.tickets[key], ticket)
	if !found {
		return []net.ECChain{value}
	}
	return nil
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
// This takes ownership of the candidates slice and mutates it in-place.
func findFirstPrefixOf(candidates []net.ECChain, preferred net.ECChain) net.ECChain {
	// Filter the candidates (in place) to those that are a prefix of the preferred value.
	n := 0
	for _, v := range candidates {
		if preferred.HasPrefix(v) {
			candidates[n] = v
			n += 1
		}
	}
	candidates = candidates[:n]

	// Return the first candidate, else base.
	if len(candidates) > 0 {
		return candidates[0]
	} else {
		return preferred.BaseChain()
	}
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
