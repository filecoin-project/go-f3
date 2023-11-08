package granite

import (
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
	mpool []GraniteMessage

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
	gmsg := msg.(GraniteMessage)
	if p.granite != nil && gmsg.Instance == p.granite.instanceID {
		p.granite.receive(sender, gmsg)
	} else if gmsg.Instance >= p.nextInstance {
		// Queue messages for later instances
		p.mpool = append(p.mpool, gmsg)
	}
	if p.decided() {
		p.finalised = p.granite.current.Head()
		p.granite = nil
	}
}

func (p *Participant) ReceiveAlarm() {
	if p.granite == nil || p.decided() {
		panic("unexpected alarm")
	}
	p.granite.receiveAlarm()
	if p.decided() {
		p.finalised = p.granite.current.Head()
		p.granite = nil
	}
}

const QUALITY = "QUALITY"
const PREPARE = "PREPARE"
const COMMIT = "COMMIT"
const DECIDE = "DECIDE"

type GraniteMessage struct {
	Instance int
	Sender   string
	Step     string
	Value    net.ECChain
}

func (m GraniteMessage) String() string {
	// FIXME This needs value receiver to work, for reasons I cannot figure out.
	return fmt.Sprintf("%s(%d %s)", m.Step, m.Instance, &m.Value)
}

// A single Granite consensus instance.
type instance struct {
	ntwk          net.NetworkSink
	participantID string
	instanceID    int
	delta         float64
	// The EC chain input to this instance.
	input net.ECChain
	// Current phase of the protocol.
	phase string
	// This instance's preferred value to finalise, as updated by the protocol.
	current net.ECChain
	// Valid QUALITY messages received by this instance.
	quality []GraniteMessage
	// Valid PREPARE values, by sender.
	prepared map[string]net.ECChain
	// Valid COMMIT values, by sender.
	committed map[string]net.ECChain
}

func newInstance(ntwk net.NetworkSink, participantID string, instanceID int, delta float64, input net.ECChain) *instance {
	return &instance{ntwk: ntwk, participantID: participantID, instanceID: instanceID, delta: delta, input: input,
		quality: []GraniteMessage{}, prepared: map[string]net.ECChain{}, committed: map[string]net.ECChain{}}
}

func (i *instance) start() {
	i.beginQuality()
}

func (i *instance) receive(sender string, msg GraniteMessage) {
	if msg.Step == QUALITY && msg.Value.Base.Eq(&i.input.Base) {
		// Just collect all the messages until the alarm triggers the end of QUALITY phase.
		// Note the message will be collected but ignored if QUALITY timeout has already passed.
		i.quality = append(i.quality, msg)
	} else if msg.Step == PREPARE && msg.Value.Base.Eq(&i.input.Base) {
		i.prepared[sender] = msg.Value
		i.tryPrepare()
	} else if msg.Step == COMMIT && msg.Value.Base.Eq(&i.input.Base) {
		i.committed[sender] = msg.Value
		i.tryCommit()
	}
}

func (i *instance) receiveAlarm() {
	i.endQuality()
}

func (i *instance) beginQuality() {
	// Broadcast input value and wait 2Δ to receive from others.
	i.phase = QUALITY
	i.broadcast(QUALITY, i.input)
	i.alarmAfter(2 * i.delta)
}

func (i *instance) endQuality() {
	// QUALITY phase ends.
	// Calculate the set of allowed proposals, and then find the best one.
	type candidate struct {
		chain net.ECChain
		power uint64
	}
	base := i.input.Base
	// Candidate chains indexed by final tipset CID.
	candidates := map[net.CID]candidate{
		base.CID: {i.input.BaseChain(), base.PowerTable.Entries[i.participantID]},
	}
	// Add non-empty prefixes of own input chain as a candidates.
	for j := range i.input.Suffix {
		prefix := i.input.Prefix(j + 1)
		candidates[prefix.Head().CID] = candidate{prefix, base.PowerTable.Entries[i.participantID]}
	}
	// Add power to candidates from messages received.
	for _, msg := range i.quality {
		for j := range msg.Value.Suffix {
			prefix := i.input.Prefix(j + 1)
			if found, ok := candidates[prefix.Head().CID]; ok {
				candidates[prefix.Head().CID] = candidate{
					chain: found.chain,
					power: found.power + base.PowerTable.Entries[msg.Sender],
				}
			} else {
				// XXX: If the tipset isn't in our input chain, we can't verify its weight or power table.
				// This boils down to just trusting the other nodes to have computed it correctly.
				candidates[prefix.Head().CID] = candidate{prefix, base.PowerTable.Entries[msg.Sender]}
			}
		}
	}

	// Filter received tipsets to those with more than half of power in favour.
	threshold := base.PowerTable.Total / 2
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
		return hi.Compare(&hj) > 0
	})

	// XXX: This can cause a participant to vote for a chain that is not its heaviest,
	// or that it can't even validate, which is irrational.
	// TODO: detect and log this.
	if len(allowed) > 0 {
		i.current = allowed[0].chain
	} else {
		i.current = i.input.BaseChain()
	}
	i.beginPrepare()
}

func (i *instance) beginPrepare() {
	// Broadcast preparation of value and wait for everyone to respond.
	i.phase = PREPARE
	i.broadcast(PREPARE, i.current)
	// Check whether we've already received enough PREPARE messages to proceed.
	i.tryPrepare()
}

func (i *instance) tryPrepare() {
	if i.phase != PREPARE {
		return
	}
	if done, v := findQuorum(i.participantID, i.current, i.prepared); done {
		// XXX: This can cause a participant to vote for a chain that is not its heaviest,
		// or that it can't even see.
		// TODO: detect and log this.
		i.current = v
		i.beginCommit()
	}
}

func (i *instance) beginCommit() {
	i.phase = COMMIT
	i.broadcast(COMMIT, i.current)
	// Check whether we've already received enough COMMIT messages to decide.
	i.tryCommit()
}

func (i *instance) tryCommit() {
	if i.phase != COMMIT {
		return
	}
	if done, v := findQuorum(i.participantID, i.current, i.committed); done {
		// A participant may be forced to decide a value that's not its preferred chain.
		// The participant isn't influencing that decision against their interest, just accepting it.
		i.decide(v)
	}
}

func (i *instance) decide(value net.ECChain) {
	i.phase = DECIDE
	i.current = value
	i.log("✓ decided %s", &i.current)
	//i.broadcast(DECIDE, net.ECChain{Base: value.Head()})
}

func (p *Participant) decided() bool {
	return p.granite != nil && p.granite.phase == DECIDE
}

func (i *instance) broadcast(step string, msg net.ECChain) {
	i.ntwk.Broadcast(i.participantID, GraniteMessage{i.instanceID, i.participantID, step, msg})
}

func (i *instance) alarmAfter(delay float64) {
	i.ntwk.SetAlarm(i.participantID, i.ntwk.Time()+delay)
}

func (i *instance) log(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	i.ntwk.Log("%s/%d: %v", i.participantID, i.instanceID, msg)
}

// Returns whether the proposals constitute a quorum of voting power, and if so, either the quorum chain or the base.
func findQuorum(me string, preferred net.ECChain, proposals map[string]net.ECChain) (bool, net.ECChain) {
	pt := preferred.Base.PowerTable
	threshold := pt.Total * 2 / 3
	// Initialise mapping of tipset->power with preferred proposal.
	votingPower := pt.Entries[me]
	powers := map[net.CID]uint64{
		preferred.Head().CID: pt.Entries[me],
	}
	// Mapping of chain tips to chains.
	chains := map[net.CID]net.ECChain{
		preferred.Head().CID: preferred,
	}

	for sender, proposal := range proposals {
		powers[proposal.Head().CID] += pt.Entries[sender]
		chains[proposal.Head().CID] = proposal
		votingPower += pt.Entries[sender]
	}
	// If any proposal has more than 2/3 of power, return it.
	for cid, power := range powers {
		if power > threshold {
			chain := chains[cid]
			return true, chain
		}
	}
	// If the proposals total more than 2/3 of power, return the base.
	if votingPower > threshold {
		return true, preferred.BaseChain()
	}
	return false, net.ECChain{}
}
