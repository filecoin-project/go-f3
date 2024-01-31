package f3

import (
	"fmt"
)

// An F3 participant runs repeated instances of Granite to finalise longer chains.
type Participant struct {
	id     ActorID
	config GraniteConfig
	host   Host
	vrf    VRFer

	mpool map[uint64][]*GMessage
	// Chain to use as input for the next Granite instance.
	nextInstanceParams *InstanceParams
	// Instance identifier for the next Granite instance.
	nextInstance uint64
	// Current Granite instance.
	granite *instance
	// The output from the last terminated Granite instance.
	finalised TipSet
	// The round number at which the last instance was terminated.
	finalisedRound uint64
}

type InstanceParams struct {
	chain  ECChain
	power  PowerTable
	beacon []byte
}

func NewParticipant(id ActorID, config GraniteConfig, host Host, vrf VRFer, startingBase TipSet) *Participant {
	return &Participant{id: id, config: config, host: host, vrf: vrf, finalised: startingBase}
}

func (p *Participant) ID() ActorID {
	return p.id
}

func (p *Participant) CurrentRound() uint64 {
	if p.granite == nil {
		return 0
	}
	return p.granite.round
}
func (p *Participant) Finalised() (TipSet, uint64) {
	return p.finalised, p.finalisedRound
}

// Receives a new canonical EC chain for the instance.
// This becomes the instance's preferred value to finalise.
// TODO there should also be a pull-base mechanism, where the participant requests the chain from EC after providing it with
// the newly finalised tipset. Otherwise, the participant needs to store multiple chains and reason about the heaviest of them
// all when a newly finalized tipset changes the weight of each chain according to the fork choice rule (and also reason
// for combinations/fractions of chains to make the heaviest s.t. the base is the latest finalized tipset and the head the heaviest tipset).
func (p *Participant) ReceiveCanonicalChain(chain ECChain, power PowerTable, beacon []byte) error {
	if !chain.HasBase(&p.finalised) {
		return fmt.Errorf("chain does not extend finalised chain: %v %v", chain, p.finalised)
	}

	//TODO this can override a less heavy chain that ends up being an extension of the finalized chain
	// by the running p.granite instance. Store instead all chains that have the current finalised tipset as base
	//
	p.nextInstanceParams = &InstanceParams{
		chain:  chain,
		power:  power,
		beacon: beacon,
	}
	return p.tryNewInstance()
}

func (p *Participant) tryNewInstance() error {
	//TODO according to FIP we should wait also for the drand epoch value for the epoch immediately
	// following the latest finalized tipset is received before starting the next instance.
	// Add that condition to the "if" here and functionality for Lotus to push epoch values to the f3 participant
	// the beacon value stored in nextInstanceParams is then probably not required anymore, but a way to buffer epoch values
	// while a decision is taking place and then delete to keep the only useful one will be needed.
	if p.granite == nil && p.nextInstanceParams != nil {
		var err error
		p.granite, err = newInstance(p.config, p.host, p.vrf, p.id, p.nextInstance, p.nextInstanceParams.chain, p.nextInstanceParams.power, p.nextInstanceParams.beacon)
		if err != nil {
			return fmt.Errorf("failed creating new granite instance: %w", err)
		}
		p.granite.enqueueInbox(p.mpool[p.nextInstance])
		delete(p.mpool, p.nextInstance)
		p.nextInstance += 1
		return p.granite.Start()
	}
	return nil
}

// Receives a new EC chain, and notifies the current instance if it extends its current acceptable chain.
// This modifies the set of valid values for the current instance.
func (p *Participant) ReceiveECChain(chain ECChain) error {
	if p.granite != nil && chain.HasPrefix(p.granite.acceptable) {
		p.granite.receiveAcceptable(chain)
	}
	return nil
}

// Receives a Granite message from some other participant.
// The message is delivered to the Granite instance if it is for the current instance.
func (p *Participant) ReceiveMessage(msg *GMessage) error {
	if p.granite != nil && msg.Vote.Instance == p.granite.instanceID {
		if err := p.granite.Receive(msg); err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}
		p.handleDecision()
	} else if msg.Vote.Instance >= p.nextInstance {
		// Queue messages for later instances
		//TODO make quick verification to avoid spamming? (i.e. signature of the message)
		p.mpool[msg.Vote.Instance] = append(p.mpool[msg.Vote.Instance], msg)
	}

	return nil
}

func (p *Participant) ReceiveAlarm(msg *AlarmMsg) error {
	if p.granite != nil && p.granite.instanceID == msg.InstanceID {
		if err := p.granite.ReceiveAlarm(msg.Payload.(string)); err != nil {
			return fmt.Errorf("failed receiving alarm: %w", err)
		}
		return p.handleDecision()
	}
	return nil
}

func (p *Participant) handleDecision() error {
	if p.terminated() {
		value := p.granite.value
		p.finalised = *value.Head()
		p.finalisedRound = p.granite.round
		p.granite = nil
		// reset nextInstanceParams if newly finalized head makes it impossible for the chain to be decided
		if p.nextInstanceParams != nil || !p.nextInstanceParams.chain.HasPrefix(p.granite.value) || p.nextInstanceParams.chain.Eq(p.granite.value) {
			p.nextInstanceParams = nil
		}
		return p.tryNewInstance()
	}
	return nil
}

func (p *Participant) terminated() bool {
	return p.granite != nil && p.granite.phase == TERMINATED_PHASE
}

func (p *Participant) Describe() string {
	if p.granite == nil {
		return "nil"
	}
	return p.granite.Describe()
}

type AlarmMsg struct {
	InstanceID uint64
	Payload    interface{}
}
