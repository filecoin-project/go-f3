package f3

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	id     ActorID
	config GraniteConfig
	host   Host
	vrf    VRFer

	mpool []*GMessage
	// Chain to use as input for the next Granite instance.
	nextChain ECChain
	// Instance identifier for the next Granite instance.
	nextInstance uint32
	// Current Granite instance.
	granite *instance
	// The output from the last decided Granite instance.
	finalised TipSet
	// The round number at which the last instance was decided.
	finalisedRound uint32
}

func NewParticipant(id ActorID, config GraniteConfig, host Host, vrf VRFer) *Participant {
	return &Participant{id: id, config: config, host: host, vrf: vrf}
}

func (p *Participant) ID() ActorID {
	return p.id
}

func (p *Participant) CurrentRound() uint32 {
	if p.granite == nil {
		return 0
	}
	return p.granite.round
}
func (p *Participant) Finalised() (TipSet, uint32) {
	return p.finalised, p.finalisedRound
}

// Receives a new canonical EC chain for the instance.
// This becomes the instance's preferred value to finalise.
func (p *Participant) ReceiveCanonicalChain(chain ECChain, power PowerTable, beacon []byte) {
	p.nextChain = chain
	if p.granite == nil {
		p.granite = newInstance(p.config, p.host, p.vrf, p.id, p.nextInstance, chain, power, beacon)
		p.nextInstance += 1
		p.granite.Start()
	}
}

// Receives a new EC chain, and notifies the current instance if it extends its current acceptable chain.
// This modifies the set of valid values for the current instance.
func (p *Participant) ReceiveECChain(chain ECChain) {
	if p.granite != nil && chain.HasPrefix(p.granite.acceptable) {
		p.granite.receiveAcceptable(chain)
	}
}

// Receives a Granite message from some other participant.
func (p *Participant) ReceiveMessage(msg *GMessage) {
	sigPayload := SignaturePayload(msg.Instance, msg.Round, msg.Step, msg.Value)
	if !p.host.Verify(msg.Sender, sigPayload, msg.Signature) {
		p.host.Log("P%d: invalid signature on %v", p.id, msg)
		return
	}
	if p.granite != nil && msg.Instance == p.granite.instanceID {
		p.granite.Receive(msg)
		p.handleDecision()
	} else if msg.Instance >= p.nextInstance {
		// Queue messages for later instances
		p.mpool = append(p.mpool, msg)
	}
}

func (p *Participant) ReceiveAlarm(payload string) {
	// TODO include instance ID in alarm message, and filter here.
	if p.granite != nil {
		p.granite.ReceiveAlarm(payload)
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

func (p *Participant) Describe() string {
	if p.granite == nil {
		return "nil"
	}
	return p.granite.Describe()
}
