package granite

import (
	"fmt"
	"github.com/anorth/f3sim/net"
	"sort"
)

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
		p.finalised = p.granite.decision
		p.granite = nil
	}
}

func (p *Participant) ReceiveAlarm() {
	if p.granite == nil || p.decided() {
		panic("unexpected alarm")
	}
	p.granite.receiveAlarm()
	if p.decided() {
		p.finalised = p.granite.decision
		p.granite = nil
	}
}

const QUALITY = "QUALITY"
const DECIDE = "DECIDE"

type GraniteMessage struct {
	Instance int
	Sender   string
	Step     string
	Value    net.ECChain
}

// A single Granite consensus instance.
type instance struct {
	ntwk          net.NetworkSink
	participantID string
	instanceID    int
	delta         float64
	// This instance's preferred value to finalise.
	input net.ECChain
	// Messages received by this instance.
	received []GraniteMessage
	phase    string
	// The value this instance has decided on (zero until a decision).
	decision net.TipSet
}

func newInstance(ntwk net.NetworkSink, participantID string, instanceID int, delta float64, input net.ECChain) *instance {
	return &instance{ntwk: ntwk, participantID: participantID, instanceID: instanceID, delta: delta, input: input}
}

func (i *instance) start() {
	// Start QUALITY phase.
	i.phase = QUALITY
	// Broadcast input value and wait 2Δ to receive from others.
	i.broadcast(QUALITY, i.input)
	i.alarmAfter(2 * i.delta)

}

func (i *instance) receive(sender string, msg GraniteMessage) {
	// Just collect all the messages until the alarm triggers the end of QUALITY phase.
	i.received = append(i.received, msg)
}

func (i *instance) receiveAlarm() {
	i.endQualityPhase()
}

func (i *instance) endQualityPhase() {
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
	for _, msg := range i.received {
		if msg.Step == QUALITY && msg.Value.Base.Eq(&base) {
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
		} else {
			i.log("discarded %v", msg)
		}
	}
	i.received = nil

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

	// TODO: move on to subsequent phases.
	// XXX: This can cause a participant to vote for a chain that is not its heaviest,
	// or that it can't even validate, which is irrational.
	if len(allowed) > 0 {
		i.decide(allowed[0].chain)
	} else {
		i.decide(i.input.BaseChain())
	}
	i.phase = DECIDE
}

func (i *instance) decide(value net.ECChain) {
	i.decision = value.Head()
	i.log("decided %v", i.decision)
	//i.broadcast(DECIDE, net.ECChain{Base: value.Head()})
}

func (p *Participant) decided() bool {
	return p.granite != nil && !p.granite.decision.Eq(&net.TipSet{})
}

func (i *instance) broadcast(step string, msg net.ECChain) {
	i.ntwk.Broadcast(i.participantID, GraniteMessage{i.instanceID, i.participantID, step, msg})
}

func (i *instance) alarmAfter(delay float64) {
	i.ntwk.SetAlarm(i.participantID, i.ntwk.Time()+delay)
}

func (i *instance) log(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("granite [%s/%d]: %v\n", i.participantID, i.instanceID, msg)
}
