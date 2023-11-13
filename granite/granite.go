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

	nextChain    net.ECChain
	nextInstance int
	granite      *instance
	finalised    net.TipSet
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

func (p *Participant) Finalised() net.TipSet {
	return p.finalised
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
	} else if gmsg.Instance >= p.nextInstance {
		// Queue messages for later instances
		p.mpool = append(p.mpool, gmsg)
	}
	if p.decided() {
		p.finalised = *p.granite.value.Head()
		p.granite = nil
	}
}

func (p *Participant) ReceiveAlarm() {
	if p.granite == nil || p.decided() {
		panic("unexpected alarm")
	}
	p.granite.receiveAlarm()
	if p.decided() {
		p.finalised = *p.granite.value.Head()
		p.granite = nil
	}
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
	// This instance's proposal for the current round.
	// This is set after the QUALITY phase, and changes only at the end of a full round.
	proposal net.ECChain
	// The value to be transmitted at the next phase.
	// This value may change away from the proposal between phases.
	value net.ECChain
	// Valid QUALITY messages received by this instance.
	quality []GMessage
	// Valid CONVERGE messages received, by round
	converged map[int][]GMessage
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
		proposal:      input,
		value:         net.ECChain{},
		quality:       []GMessage{},
		converged:     map[int][]GMessage{},
		prepared:      map[int]map[string]net.ECChain{},
		committed:     map[int]map[string]net.ECChain{},
	}
}

func (i *instance) start() {
	i.beginQuality()
}

func (i *instance) receive(sender string, msg GMessage) {
	round := msg.Round
	if msg.Step == QUALITY && msg.Value.Base.Eq(&i.input.Base) {
		// Just collect all the messages until the alarm triggers the end of QUALITY phase.
		// Note the message will be collected but ignored if QUALITY timeout has already passed.
		i.quality = append(i.quality, msg)
	} else if msg.Step == CONVERGE && msg.Value.Base.Eq(&i.input.Base) {
		// Collect messages until the alarm triggers the end of CONVERGE phase.
		i.converged[round] = append(i.converged[round], msg)
	} else if msg.Step == PREPARE && msg.Value.Base.Eq(&i.input.Base) {
		if _, exists := i.prepared[round]; !exists {
			i.prepared[round] = make(map[string]net.ECChain)
		}
		i.prepared[round][sender] = msg.Value
		i.tryPrepare()
	} else if msg.Step == COMMIT && (msg.Bottom || msg.Value.Base.Eq(&i.input.Base)) {
		if _, exists := i.committed[round]; !exists {
			i.committed[round] = make(map[string]net.ECChain)
		}
		i.committed[round][sender] = msg.Value
		i.tryCommit()
	} else {
		panic(fmt.Sprintf("unexpected message %v", msg))
	}
}

func (i *instance) receiveAlarm() {
	if i.round == 0 {
		i.endQuality()
	} else {
		i.endConverge()
	}
}

func (i *instance) beginQuality() {
	// Broadcast proposal value and wait 2Δ to receive from others.
	i.phase = QUALITY
	msg := i.broadcast(QUALITY, i.proposal)
	i.receive(i.participantID, msg)
	i.alarmAfter(2 * i.delta)
}

func (i *instance) endQuality() {
	i.proposal = findBestQualityProposal(i.quality, i.input.Base)
	i.value = i.proposal
	i.beginPrepare()
}

func (i *instance) beginConverge() {
	i.phase = CONVERGE
	msg := i.broadcast(CONVERGE, i.proposal)
	i.receive(i.participantID, msg)
	i.alarmAfter(2 * i.delta)
}

func (i *instance) endConverge() {
	// XXX: This can lead to a node proposing a chain that's not a prefix of its input chain.
	// (If the QUALITY threshold is only 1/2 with honest nodes).
	// Is it safe if QUALITY threshold is >2/3?
	i.value = findMinTicketProposal(i.converged[i.round], i.round)
	i.beginPrepare()
}

func (i *instance) beginPrepare() {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE
	msg := i.broadcast(PREPARE, i.value)
	i.receive(i.participantID, msg)
}

func (i *instance) tryPrepare() {
	if i.phase != PREPARE {
		return
	}
	if done, v := findQuorum(i.participantID, i.value, i.prepared[i.round], i.input.Base.PowerTable); done {
		// INCENTIVE-COMPATIBLE: Only commit a prepared value if it's equal to this node's proposal.
		if !v.Eq(&i.proposal) {
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

func (i *instance) tryCommit() {
	if i.phase != COMMIT {
		return
	}
	if done, v := findQuorum(i.participantID, i.value, i.committed[i.round], i.input.Base.PowerTable); done {
		if !v.IsZero() {
			// A participant may be forced to decide a value that's not its preferred chain.
			// The participant isn't influencing that decision against their interest, just accepting it.
			i.decide(v)
		} else {
			// Adopt any non-empty value committed by another participant.
			// (There can only be one, since they needed a strong quorum to commit it).
			// XXX: only with message validation justifying it!
			for _, v := range i.committed[i.round] {
				if !v.IsZero() {
					if !v.Eq(&i.proposal) {
						i.log("⚠️ swaying from %s to %s", &i.proposal, &v)
					}
					i.proposal = v
				}
			}
			i.beginNextRound()
		}
	}
}

func (i *instance) beginNextRound() {
	i.round += 1
	i.value = i.proposal
	i.log("x moving to round %d with %s", i.round, i.value.String())
	i.beginConverge()
}

func (i *instance) decide(value net.ECChain) {
	i.log("✓ decided %s", &i.value)
	i.phase = DECIDE
	i.value = value
}

func (p *Participant) decided() bool {
	return p.granite != nil && p.granite.phase == DECIDE
}

func (i *instance) broadcast(step string, msg net.ECChain) GMessage {
	bottom := msg.IsZero()
	gmsg := GMessage{i.instanceID, i.round, i.participantID, step, msg, bottom}
	i.ntwk.Broadcast(i.participantID, gmsg)
	return gmsg
}

func (i *instance) alarmAfter(delay float64) {
	i.ntwk.SetAlarm(i.participantID, i.ntwk.Time()+delay)
}

func (i *instance) log(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	i.ntwk.Log("%s/%d: %v", i.participantID, i.instanceID, msg)
}

///// Helpers

// Finds the highest weight chain prefix with at least half of power in favour.
// The first proposal is assumed to be the node's own proposal, and any result
// must be compatible with it.
func findBestQualityProposal(proposals []GMessage, base net.TipSet) net.ECChain {
	// Calculate the set of prefixes with sufficient weight, and then find the best one.
	type candidate struct {
		chain net.ECChain
		power uint
	}
	// Candidate chains indexed by final tipset CID.
	candidates := map[net.CID]candidate{}
	incentiveCompatible := map[net.CID]struct{}{}
	// Add power to candidates from messages received.
	for i, msg := range proposals {
		for j := range msg.Value.Suffix {
			prefix := msg.Value.Prefix(j + 1)
			if found, ok := candidates[prefix.Head().CID]; ok {
				candidates[prefix.Head().CID] = candidate{
					chain: found.chain,
					power: found.power + base.PowerTable.Entries[msg.Sender],
				}
			} else {
				candidates[prefix.Head().CID] = candidate{*prefix, base.PowerTable.Entries[msg.Sender]}
				// The first message is assumed to be ours, so specifies the incentive-compatible set.
				if i == 0 {
					incentiveCompatible[prefix.Head().CID] = struct{}{}
				}
			}
		}
	}

	// Filter received tipsets to those with strictly more than half of power in favour.
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

	var proposal net.ECChain
	if len(allowed) > 0 {
		// INCENTIVE-COMPATIBLE: Accept only tipsets that are on this node's input chain.
		// Note: this binary accept/reject of the heaviest is stricter than necessary.
		// We could instead filter the proposals earlier on and take the heaviest tipset
		// that _is_ on our input chain.
		if _, ok := incentiveCompatible[allowed[0].chain.Head().CID]; ok {
			proposal = allowed[0].chain
		}
	}
	if proposal.IsZero() {
		proposal = *net.NewChain(base)
	}
	return proposal
}

// Finds the lowest ticket value from a set of proposals.
func findMinTicketProposal(proposals []GMessage, round int) net.ECChain {
	var minTicket []byte
	var minValue net.ECChain
	// Emulate a ticket draw by hashing the sender and round number.
	for _, v := range proposals {
		input := fmt.Sprintf("%s%d", v.Sender, round)
		digest := sha256.Sum224([]byte(input))
		if minTicket == nil || bytes.Compare(digest[:], minTicket[:]) < 0 {
			minTicket = digest[:]
			minValue = v.Value
		}
	}
	return minValue
}

// Returns whether the proposals constitute a quorum of voting power, and if so, either the quorum chain or empty (bottom).
func findQuorum(me string, mine net.ECChain, proposals map[string]net.ECChain, power net.PowerTable) (bool, net.ECChain) {
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
