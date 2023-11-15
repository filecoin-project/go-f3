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
		p.granite.start()
		p.nextInstance += 1
	}
}

// Receives a Granite message from some other participant.
func (p *Participant) ReceiveMessage(sender string, msg net.Message) {
	gmsg := msg.(GMessage)
	if p.granite != nil && gmsg.Instance == p.granite.instanceID {
		p.granite.receive(sender, gmsg)
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
	// Valid QUALITY values, by sender.
	quality map[string]net.ECChain
	// Valid CONVERGE messages received, by round, by sender.
	converged map[int]map[string]net.ECChain
	// Valid PREPARE values, by round, by sender.
	prepared map[int]map[string]net.ECChain
	// Valid COMMIT values, by round, by sender.
	committed map[int]map[string]net.ECChain
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
		proposal:      *input.BaseChain(),
		value:         net.ECChain{},
		quality:       map[string]net.ECChain{},
		converged:     map[int]map[string]net.ECChain{},
		prepared:      map[int]map[string]net.ECChain{},
		committed:     map[int]map[string]net.ECChain{},
	}
}

func (i *instance) start() {
	i.beginQuality()
}

func (i *instance) receive(sender string, msg GMessage) {
	if i.decided() {
		panic("received message after decision")
	}
	round := msg.Round
	if msg.Step == QUALITY && msg.Value.Base.Eq(&i.input.Base) {
		// Just collect all the messages until the alarm triggers the end of QUALITY phase.
		// Note the message will be collected but ignored if QUALITY timeout has already passed.
		i.quality[sender] = msg.Value
	} else if msg.Step == CONVERGE && msg.Value.Base.Eq(&i.input.Base) {
		// Collect messages until the alarm triggers the end of CONVERGE phase.
		setRoundValue(i.converged, round, sender, msg.Value)
	} else if msg.Step == PREPARE && msg.Value.Base.Eq(&i.input.Base) {
		setRoundValue(i.prepared, round, sender, msg.Value)
		i.tryPrepare()
	} else if msg.Step == COMMIT && (msg.Bottom || msg.Value.Base.Eq(&i.input.Base)) {
		setRoundValue(i.committed, round, sender, msg.Value)
		i.tryCommit(round)
	} else {
		panic(fmt.Sprintf("unexpected message %v", msg))
	}
}

func (i *instance) receiveAlarm(payload string) {
	if payload == QUALITY {
		i.endQuality()
	} else if payload == CONVERGE {
		i.endConverge()
	} else if payload == PREPARE {
		i.tryPrepare()
	}
}

func (i *instance) beginQuality() {
	// Broadcast input value and wait 2Δ to receive from others.
	i.phase = QUALITY
	msg := i.broadcast(QUALITY, i.input)
	i.receive(i.participantID, msg)
	i.alarmAfter(QUALITY, 2*i.delta)
}

func (i *instance) endQuality() {
	i.proposal = findBestQualityProposal(i.quality, i.participantID, i.input.Base)
	i.value = i.proposal
	i.beginPrepare()
}

func (i *instance) beginConverge() {
	i.phase = CONVERGE
	msg := i.broadcast(CONVERGE, i.proposal)
	i.receive(i.participantID, msg)
	i.alarmAfter(CONVERGE, 2*i.delta)
}

func (i *instance) endConverge() {
	// XXX: This can lead to a node proposing a chain that's not a prefix of its input chain.
	i.value = findMinTicketProposal(i.converged[i.round], i.round)
	if !i.input.HasPrefix(&i.value) {
		i.log("⚠️ swayed from %s to %s by min ticket", &i.input, &i.value)
	}
	// TODO update proposal too if ECKnowsAbout(value)
	i.beginPrepare()
}

func (i *instance) beginPrepare() {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE
	i.prepareTimeout = i.ntwk.Time() + 2*i.delta
	msg := i.broadcast(PREPARE, i.value)
	i.receive(i.participantID, msg)
	i.alarmAfter(PREPARE, 2*i.delta)
}

func (i *instance) tryPrepare() {
	if i.phase != PREPARE {
		return
	}
	// Wait at least 2Δ.
	if i.ntwk.Time() < i.prepareTimeout {
		return
	}
	if done, v := findQuorum(i.prepared[i.round], i.input.Base.PowerTable); done {
		// INCENTIVE-COMPATIBLE: Only commit a value equal to this node's proposal.
		if v.Eq(&i.proposal) {
			i.value = v
		} else {
			// Update our proposal for next round to accept a prefix of it, if that gained quorum, else baseChain.
			findBestQualityProposal(i.prepared[i.round], i.participantID, i.input.Base)
			// Commit bottom in this round anyway.
			i.value = net.ECChain{}
		}
		i.beginCommit()
	}
}

func (i *instance) beginCommit() {
	i.phase = COMMIT
	msg := i.broadcast(COMMIT, i.value)
	i.receive(i.participantID, msg)
}

func (i *instance) tryCommit(round int) {
	// Unlike all previous phases, the COMMIT phase stays open to new messages even after an initial quorum is reached.
	// A subsequent COMMIT message can cause the node to decide, if it forms a quorum in agreement.
	// It can't change the value propagated to the next round, though.
	//if i.phase != COMMIT {
	//	return
	//}
	if done, v := findQuorum(i.committed[round], i.input.Base.PowerTable); done {
		if !v.IsZero() {
			// A participant may be forced to decide a value that's not its preferred chain.
			// The participant isn't influencing that decision against their interest, just accepting it.
			i.decide(v, round)
		} else if i.phase == COMMIT {
			// Adopt any non-empty value committed by another participant.
			// (There can only be one, since they needed a strong quorum to commit it).
			// XXX: only with message validation justifying it!
			for sender, v := range i.committed[i.round] {
				if !v.IsZero() {
					if !i.input.HasPrefix(&v) {
						i.log("⚠️ swaying from %s to %s by COMMIT from %s", &i.input, &v, sender)
					}
					i.proposal = v
					break
				}
			}
			i.beginNextRound()
		}
	}
}

func (i *instance) beginNextRound() {
	i.round += 1
	i.log("x moving to round %d with %s", i.round, i.proposal.String())
	i.beginConverge()
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

///// Helpers

func setRoundValue(collection map[int]map[string]net.ECChain, round int, sender string, value net.ECChain) {
	if _, exists := collection[round]; !exists {
		collection[round] = make(map[string]net.ECChain)
	}
	collection[round][sender] = value
}

// Finds the highest weight chain prefix with at least half of power in favour.
// The first proposal is assumed to be the node's own proposal, and any result
// must be compatible with it.
func findBestQualityProposal(proposals map[string]net.ECChain, me string, base net.TipSet) net.ECChain {
	// Calculate the set of prefixes with sufficient weight, and then find the best one.
	type candidate struct {
		chain net.ECChain
		power uint
	}
	// Candidate chains indexed by final tipset CID.
	candidates := map[net.CID]candidate{}
	// Establish the incentive-compatible set of values, which are the prefixes of our proposal.
	mine := proposals[me]
	for j := range mine.Suffix {
		prefix := mine.Prefix(j + 1)
		candidates[prefix.Head().CID] = candidate{*prefix, base.PowerTable.Entries[me]}
	}

	// Add up power in support of each non-empty prefix of each message received.
	// Chains that are not a prefix of our input are ignored.
	for sender, value := range proposals {
		if sender == me {
			continue
		}
		for j := range value.Suffix {
			prefix := value.Prefix(j + 1)
			if found, ok := candidates[prefix.Head().CID]; ok {
				candidates[prefix.Head().CID] = candidate{
					chain: found.chain,
					power: found.power + base.PowerTable.Entries[sender],
				}
			} else {
				// INCENTIVE-COMPATIBLE: Not a prefix of our proposal (and no longer prefix can be either).
				break
			}
		}
	}

	// Filter received tipsets to those with strictly enough power in favour.
	threshold := 2 * base.PowerTable.Total / 3
	allowed := []candidate{}
	for _, c := range candidates {
		if c.power > threshold {
			allowed = append(allowed, c)
		}
	}

	// Sort allowed candidates by tipset weight, descending.
	sort.Slice(allowed, func(i, j int) bool {
		hi := allowed[i].chain.Head()
		hj := allowed[j].chain.Head()
		return hi.Compare(hj) > 0
	})

	if len(allowed) > 0 {
		return allowed[0].chain
	} else {
		return *net.NewChain(base)
	}
}

// Finds the lowest ticket value from a set of proposals.
func findMinTicketProposal(proposals map[string]net.ECChain, round int) net.ECChain {
	var minTicket []byte
	var minValue net.ECChain
	// Emulate a ticket draw by hashing the sender and round number.
	for sender, value := range proposals {
		input := fmt.Sprintf("%s%d", sender, round)
		digest := sha256.Sum224([]byte(input))
		if minTicket == nil || bytes.Compare(digest[:], minTicket[:]) < 0 {
			minTicket = digest[:]
			minValue = value
		}
	}
	return minValue
}

// Returns whether the proposals constitute a quorum of voting power, and if so, either the quorum chain or empty (bottom).
func findQuorum(proposals map[string]net.ECChain, power net.PowerTable) (bool, net.ECChain) {
	threshold := power.Total * 2 / 3
	votingPower := uint(0)
	tipsetPower := map[net.CID]uint{}
	tipsetChain := map[net.CID]net.ECChain{}

	for sender, proposal := range proposals {
		tipsetPower[proposal.Head().CID] += power.Entries[sender]
		tipsetChain[proposal.Head().CID] = proposal
		votingPower += power.Entries[sender]
	}
	// If any proposal has more than 2/3 of power, return it.
	for cid, power := range tipsetPower {
		if power > threshold {
			chain := tipsetChain[cid]
			return true, chain
		}
	}
	// If the proposals total more than 2/3 of power, return bottom.
	if votingPower > threshold {
		return true, net.ECChain{}
	}
	return false, net.ECChain{}
}
