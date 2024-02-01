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
	// Params to use as input for the next Granite instance.
	nextInstanceParams *InstanceParams
	// Instance identifier for the next Granite instance.
	nextInstance uint64
	// Current Granite instance.
	granite *instance
	// The output from the last terminated Granite instance.
	finalised TipSet
	// The round number at which the last instance was terminated.
	finalisedRound uint64
	// list of beacon values pushed by Lotus, sorted by ascending epoch
	beacons BeaconStore
}

type InstanceParams struct {
	chain ECChain
	power PowerTable
}

type BeaconStore struct {
	epoch2beacon map[int64][]byte
	beacon2epoch map[string]int64
}

func (b *BeaconStore) store(value []byte, epoch int64) {
	b.epoch2beacon[epoch] = value
	b.beacon2epoch[string(value)] = epoch
}

// cleanStore deletes all beacon values in the range
func (b *BeaconStore) cleanStore(from, until int64) {
	for i := from; i <= until; i++ {
		if beacon, ok := b.epoch2beacon[i]; ok {
			delete(b.beacon2epoch, string(beacon))
			delete(b.epoch2beacon, i)
		}
	}
}

func NewParticipant(id ActorID, config GraniteConfig, host Host, vrf VRFer, startingBase TipSet) *Participant {
	return &Participant{id: id, config: config, host: host, vrf: vrf, finalised: startingBase, beacons: BeaconStore{
		epoch2beacon: make(map[int64][]byte),
		beacon2epoch: make(map[string]int64)},
		mpool: make(map[uint64][]*GMessage),
	}
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
		chain: chain,
		power: power,
	}
	//bootstrapping with an artificial finalised tipset
	p.finalised = TipSet{
		Epoch:  chain.Base().Epoch,
		CID:    chain.Base().CID,
		Weight: chain.Base().Weight,
	}
	p.beacons.store(beacon, chain.Base().Epoch+1)
	//Notify of this canonical chain
	if err := p.ReceiveECChain(chain); err != nil {
		return err
	}
	return p.tryNewInstance()
}

func (p *Participant) tryNewInstance() error {
	//TODO according to FIP we should wait also for the drand epoch value for the epoch immediately
	// following the latest finalized tipset is received before starting the next instance.
	// Add that condition to the "if" here and functionality for Lotus to push epoch values to the f3 participant
	// the beacon value stored in nextInstanceParams is then probably not required anymore, but a way to buffer epoch values
	// while a decision is taking place and then delete to keep the only useful one will be needed.
	// Also: we might have the new base but not the new power table, so we need to pull it from Lotus as well or Lotus needs to push it to us.
	if p.granite == nil && p.nextInstanceParams != nil && p.beacons.epoch2beacon[p.finalised.Epoch+1] != nil {
		var err error
		p.granite, err = newInstance(p.config, p.host, p.vrf, p.id, p.nextInstance, p.nextInstanceParams.chain, p.nextInstanceParams.power, p.beacons.epoch2beacon[p.finalised.Epoch+1])
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
// TODO Why should this call be any different than receive canonical chain?
// Any new chain received by lotus is by definition heavier, and the check for it being a prefix is done here
// So we might as well have Lotus new chains only call receiveCanonicalChain and then that call
// call this instead if p.granite != nil (see what receiveCanonicalChain does now)
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
		prevFinalised := p.finalised
		p.beacons.cleanStore(p.finalised.Epoch, value.Head().Epoch)
		p.finalised = *value.Head()
		p.finalisedRound = p.granite.round
		p.granite = nil
		// If we decided on baseChain then try to run again if ready (no changes to nextInstanceParams as a result of decision)
		if !prevFinalised.Eq(&p.finalised) && p.nextInstanceParams != nil {
			// reset nextInstanceParams to nil if newly finalized head makes it impossible for the next chain to be decided
			if !p.nextInstanceParams.chain.HasPrefix(value) || value.HasPrefix(p.nextInstanceParams.chain) {
				p.nextInstanceParams = nil
			} else if p.nextInstanceParams.chain.HasPrefix(value) {
				// update nextInstanceParams to have as base the latest finalised head
				// the power table remains the same as the head remains the same
				// Start from the first Tipset and iterate removing tipsets until reaching the latest decision's head
				for !p.nextInstanceParams.chain.Base().Eq(value.Head()) {
					p.nextInstanceParams.chain = p.nextInstanceParams.chain.Suffix()
					//TODO in this case we do not have the power table to start the next instance, need to pull it from Lotus.
					// p.nextInstanceParams.power [...]
				}
			}
		}
	}
	//TODO Push new decision to Lotus, and expect Lotus to push to us the power table (and chain, and beacon if available)
	// or pull the power table from Lotus.
	return nil
}

// Receives a beacon value from Lotus.
func (p *Participant) receiveBeacon(value []byte, epoch int64) error {
	p.beacons.store(value, epoch)
	return p.tryNewInstance()
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
