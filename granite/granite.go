package granite

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/anorth/f3sim/net"
	"sort"
)

// TODO
// - Implement message validation logic (or prove it unnecessary)
// - Implement detection of equivocations
// - Attempt to reduce lots of heavy ECChain copying by using pointers or head CIDs instead.

type Participant struct {
	id    string
	ntwk  net.NetworkSink
	delta float64 // Message propagation time parameter
	mpool []GMessage

	// Chain to use as input for the next Granite instance.
	nextChain net.ECChain
	// Instance identifier for the next Granite instance.
	nextInstance int
	// Current Granite instance.
	granite *instance
	// The output from the last decided Granite instance.
	finalised net.TipSet
	// The round number at which the last instance was decided.
	finalisedRound int
}

func NewParticipant(id string, ntwk net.NetworkSink, delta float64) *Participant {
	return &Participant{id: id, ntwk: ntwk, delta: delta}
}

func (p *Participant) ID() string {
	return p.id
}

func (p *Participant) CurrentRound() int {
	if p.granite == nil {
		return -1
	}
	return p.granite.round
}
func (p *Participant) Finalised() (net.TipSet, int) {
	return p.finalised, p.finalisedRound
}

// Receives a new canonical EC chain for the instance.
// This becomes the instance's preferred value to finalise.
func (p *Participant) ReceiveCanonicalChain(chain net.ECChain) {
	p.nextChain = chain
	if p.granite == nil {
		p.granite = newInstance(p.ntwk, p.id, p.nextInstance, p.delta, p.nextChain)
		p.granite.Start()
		p.nextInstance += 1
	}
}

// Receives a Granite message from some other participant.
func (p *Participant) ReceiveMessage(sender string, msg net.Message) {
	gmsg := msg.(GMessage)
	if p.granite != nil && gmsg.Instance == p.granite.instanceID {
		p.granite.Receive(gmsg)
		p.handleDecision()
	} else if gmsg.Instance >= p.nextInstance {
		// Queue messages for later instances
		p.mpool = append(p.mpool, gmsg)
	}
}

func (p *Participant) ReceiveAlarm(payload string) {
	// TODO include instance ID in alarm message, and filter here.
	if p.granite != nil {
		p.granite.receiveAlarm(payload)
		p.handleDecision()
	}
}

func (p *Participant) handleDecision() {
	if p.decided() {
		p.finalised = *p.granite.value.Head()
		p.finalisedRound = p.granite.round
		p.granite = nil
	}
}

func (p *Participant) decided() bool {
	return p.granite != nil && p.granite.phase == DECIDE
}

const QUALITY = "QUALITY"
const CONVERGE = "CONVERGE"
const PREPARE = "PREPARE"
const COMMIT = "COMMIT"
const DECIDE = "DECIDE"

type GMessage struct {
	Instance int
	Round    int
	Sender   string
	Step     string
	Value    net.ECChain
	Bottom   bool
}

func (m GMessage) String() string {
	// FIXME This needs value receiver to work, for reasons I cannot figure out.
	return fmt.Sprintf("%s(%d/%d %s)", m.Step, m.Instance, m.Round, &m.Value)
}

// A single Granite consensus instance.
type instance struct {
	ntwk          net.NetworkSink
	participantID string
	instanceID    int
	delta         float64
	// The EC chain input to this instance.
	input net.ECChain
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

func newInstance(ntwk net.NetworkSink, participantID string, instanceID int, delta float64, input net.ECChain) *instance {
	return &instance{
		ntwk:          ntwk,
		participantID: participantID,
		instanceID:    instanceID,
		delta:         delta,
		input:         input,
		round:         0,
		phase:         "",
		proposal:      input,
		value:         net.ECChain{},
		validation:    newValidationQueue(input.Base.CID),
		quality:       newQualityState(input.Base.CID, input.Base.PowerTable),
		rounds: map[int]*roundState{
			0: newRoundState(0, input.Base.PowerTable),
		},
	}
}

func newRoundState(round int, powerTable net.PowerTable) *roundState {
	return &roundState{
		converged: newConvergeState(round),
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
		round = newRoundState(msg.Round, i.input.Base.PowerTable)
		i.rounds[msg.Round] = round
	}

	// A message must have both
	// - value allowed by the QUALITY phase, and
	// - justification from tne previous phase
	if !((msg.Step == QUALITY || i.quality.AllowsValue(msg.Value.Head().CID)) && i.validation.IsJustified(&msg)) {
		i.validation.Enqueue(msg)
		return 0, ""
	}

	var newAllowedValues []net.ECChain
	var nextRound int
	var nextPhase string
	if msg.Step == QUALITY && msg.Round == 0 && msg.Value.Base.Eq(&i.input.Base) {
		// Just collect all the messages until the alarm triggers the end of QUALITY phase.
		// Messages continue being collected after QUALITY timeout passes, in case they justify later messages.
		newAllowedValues = i.quality.Receive(msg.Sender, msg.Value)
		nextRound = msg.Round
		// FIXME: quality is a gate for all phases, so all phases need to be reprocessed.
		nextPhase = PREPARE
	} else if msg.Step == CONVERGE && msg.Round > 0 && msg.Value.Base.Eq(&i.input.Base) {
		// TODO: check ticket validity
		// Collect messages until the alarm triggers the end of CONVERGE phase.
		newAllowedValues = round.converged.Receive(msg.Sender, msg.Value)
		nextRound = msg.Round
		nextPhase = PREPARE
	} else if msg.Step == PREPARE && msg.Value.Base.Eq(&i.input.Base) {
		newAllowedValues = round.prepared.Receive(msg.Sender, msg.Value)
		i.tryPrepare(msg.Round) // FIXME should try the next step only after adding justifications from this step.
		nextRound = msg.Round
		nextPhase = COMMIT
	} else if msg.Step == COMMIT && (msg.Bottom || msg.Value.Base.Eq(&i.input.Base)) {
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
			i.validation.AddJustified(nextRound, nextPhase, v.Head().CID)
		}
	}
	if len(newAllowedValues) > 0 {
		return nextRound, nextPhase
	}
	return 0, ""
}

func (i *instance) receiveAlarm(payload string) {
	if payload == QUALITY {
		i.endQuality(i.round)
	} else if payload == CONVERGE {
		i.endConverge(i.round)
	} else if payload == PREPARE {
		i.tryPrepare(i.round)
	}
}

func (i *instance) beginQuality() {
	// Broadcast input value and wait 2Δ to receive from others.
	i.phase = QUALITY
	msg := i.broadcast(QUALITY, i.input)
	i.Receive(msg)
	i.alarmAfter(QUALITY, 2*i.delta)
}

func (i *instance) endQuality(round int) {
	allowed := i.quality.ListQuorumAgreedValues()
	i.proposal = findFirstPrefixOf(allowed, i.proposal)
	i.value = i.proposal
	i.log("adopting proposal and value %s", &i.proposal)
	i.beginPrepare()
}

func (i *instance) beginConverge() {
	i.phase = CONVERGE
	msg := i.broadcast(CONVERGE, i.proposal)
	i.Receive(msg)
	i.alarmAfter(CONVERGE, 2*i.delta)
}

func (i *instance) endConverge(round int) {
	i.value = i.rounds[round].converged.findMinTicketProposal()
	if i.isAcceptable(&i.value) {
		if !i.value.Eq(&i.proposal) {
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
	i.prepareTimeout = i.ntwk.Time() + 2*i.delta
	msg := i.broadcast(PREPARE, i.value)
	i.Receive(msg)
	i.alarmAfter(PREPARE, 2*i.delta)
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
		if v.Eq(&i.proposal) {
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
	msg := i.broadcast(COMMIT, i.value)
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
					if !i.isAcceptable(&v) {
						i.log("⚠️ swaying from %s to %s by COMMIT", &i.input, &v)
					}
					if !v.Eq(&i.proposal) {
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
func (i *instance) isAcceptable(c *net.ECChain) bool {
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

func (i *instance) broadcast(step string, msg net.ECChain) GMessage {
	bottom := msg.IsZero()
	gmsg := GMessage{i.instanceID, i.round, i.participantID, step, msg, bottom}
	i.ntwk.Broadcast(i.participantID, gmsg)
	return gmsg
}

func (i *instance) alarmAfter(payload string, delay float64) {
	i.ntwk.SetAlarm(i.participantID, payload, i.ntwk.Time()+delay)
}

func (i *instance) log(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	i.ntwk.Log("%s/%d: %v", i.participantID, i.instanceID, msg)
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
	// PREPARE for base chain is always justified.
	if msg.Step == PREPARE && msg.Value.Base.CID == v.base {
		return true
	}
	if rv, ok := v.rounds[msg.Round]; ok {
		if pv, ok := rv[msg.Step]; ok {
			_, ok := pv.justified[msg.Value.Head().CID]
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

func (v *validationQueue) PopJustified(round int, phase string) []GMessage {
	if phase == QUALITY {
		return nil
	}
	pv := v.getPhase(round, phase)
	var justified []GMessage
	// Remove all justified entries, in-place.
	n := 0
	for _, msg := range pv.pending {
		if _, ok := pv.justified[msg.Value.Head().CID]; ok {
			justified = append(justified, msg)
		} else {
			pv.pending[n] = msg
			n += 1
		}
	}
	pv.pending = pv.pending[:n]
	return justified
}

// Accumulates values from a collection of senders and incrementally calculates
// which values have reached a strong quorum of support.
// Supports receiving multiple values from each sender, and hence multiple strong quorum values.
type quorumState struct {
	// CID of each chain received, by sender. Allows detecting and ignoring duplicates.
	received map[string]senderSent
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
		received:            map[string]senderSent{},
		chainPower:          map[net.CID]chainPower{},
		withQuorumAgreement: map[net.CID]struct{}{},
		sendersTotalPower:   0,
		powerTable:          powerTable,
	}
}

// Receives a new chain from a sender.
// Returns whether the chain produced a new quorum in agreement on that chain (can be true multiple times).
func (q *quorumState) Receive(sender string, value net.ECChain) bool {
	threshold := q.powerTable.Total * 2 / 3

	head := value.Head().CID
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
func (q *quorumState) HasReceived(value *net.ECChain) bool {
	_, ok := q.chainPower[value.Head().CID]
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
func (q *qualityState) Receive(sender string, value net.ECChain) []net.ECChain {
	var newlyAllowed []net.ECChain
	// Add each non-empty prefix of the chain to the quorum state.
	for j := range value.Suffix {
		prefix := *value.Prefix(j + 1)
		foundQuorumValue := q.prefixes.Receive(sender, prefix)
		if foundQuorumValue {
			newlyAllowed = append(newlyAllowed, prefix)
		}
	}
	return newlyAllowed
}

// Checks whether a chain head is allowed by the received QUALITY messages.
func (q *qualityState) AllowsValue(cid net.CID) bool {
	return cid == "" || cid == q.base || q.prefixes.HasQuorumAgreement(cid)
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
func (p *prepareState) Receive(sender string, value net.ECChain) []net.ECChain {
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
func (c *commitState) Receive(sender string, value net.ECChain) []net.ECChain {
	seenValue := c.chains.HasReceived(&value)
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
	values  map[net.CID]net.ECChain
	senders map[net.CID][]string
	round   int
}

func newConvergeState(round int) *convergeState {
	return &convergeState{
		values:  map[net.CID]net.ECChain{},
		senders: map[net.CID][]string{},
		// TODO: remove round here when tickets are propagated with messages
		round: round,
	}
}

// Receives a new CONVERGE value from a sender.
// Returns values that are newly justified for the next phase: any value the first time it is received.
func (c *convergeState) Receive(sender string, value net.ECChain) []net.ECChain {
	_, found := c.values[value.Head().CID]
	c.values[value.Head().CID] = value
	c.senders[value.Head().CID] = append(c.senders[value.Head().CID], sender)
	if !found {
		return []net.ECChain{value}
	}
	return nil
}

func (c *convergeState) findMinTicketProposal() net.ECChain {
	var minTicket []byte
	var minValue net.ECChain
	// Emulate a ticket draw by hashing the sender and round number.
	for cid, value := range c.values {
		for _, sender := range c.senders[cid] {
			input := fmt.Sprintf("%s%d", sender, c.round)
			digest := sha256.Sum224([]byte(input))
			if minTicket == nil || bytes.Compare(digest[:], minTicket[:]) < 0 {
				minTicket = digest[:]
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
		if preferred.HasPrefix(&v) {
			candidates[n] = v
			n += 1
		}
	}
	candidates = candidates[:n]

	// Return the first candidate, else base.
	if len(candidates) > 0 {
		return candidates[0]
	} else {
		return *net.NewChain(preferred.Base)
	}
}

// Sorts chains by weight of their head, descending
func sortByWeight(chains []net.ECChain) {
	sort.Slice(chains, func(i, j int) bool {
		hi := chains[i].Head()
		hj := chains[j].Head()
		return hi.Compare(hj) > 0
	})
}
