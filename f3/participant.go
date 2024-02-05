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
	// Next chain to use as input for the next Granite instance.
	nextChain ECChain
	// Next power table to sue as input for the next Granite instance
	nextPower PowerTable
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
// The participant also receives all the beacon values for the epochs between the last finalized tipset and the new head.
func (p *Participant) ReceiveCanonicalChain(chain ECChain, beacons [][]byte) error {
	if !chain.HasBase(&p.finalised) {
		return fmt.Errorf("chain does not extend finalised chain: %v %v", chain, p.finalised)
	}

	p.nextChain = chain

	for i, beacon := range beacons {
		p.beacons.store(beacon, chain[i].Epoch)
	}

	//Notify of this canonical chain
	if err := p.receiveECChain(chain); err != nil {
		return err
	}

	return p.tryNewInstance()
}

// Receives a new EC chain, and notifies the current instance if it extends its current acceptable chain.
// This modifies the set of valid values for the current instance.
// Any new chain received by lotus is by definition heavier, and the check for it being a prefix is done here
// This call is internal, unlike ReceiveCanonicalChain.
func (p *Participant) receiveECChain(chain ECChain) error {
	if p.granite != nil && chain.HasPrefix(p.granite.acceptable) {
		p.granite.receiveAcceptable(chain)
	}
	return nil
}

// Receives the requested power table from Lotus.
func (p *Participant) ReceivePowerTable(power PowerTable, headTipsetID TipSetID) error {
	//check that the power table is the one we requested
	if headTipsetID != p.finalised.CID {
		//TODO panic? request again?
		return nil
	}
	p.nextPower = power
	return p.tryNewInstance()
}

// Tries to start a new instance of Granite if the next chain, the next power table, and the corresponding beacon are all available.
func (p *Participant) tryNewInstance() error {
	if p.granite == nil && p.nextChain != nil && p.beacons.epoch2beacon[p.finalised.Epoch+1] != nil && !p.nextPower.IsZero() {
		var err error
		p.granite, err = newInstance(p.config, p.host, p.vrf, p.id, p.nextInstance, p.nextChain, p.nextPower, p.beacons.epoch2beacon[p.finalised.Epoch+1])
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
		p.finalised = *value.Head() //This is at least the previous head, never empty
		p.finalisedRound = p.granite.round
		p.granite = nil
		// If we decided on baseChain then run again if ready (no changes to beacon or power table as a result of decision)
		if prevFinalised.Eq(&p.finalised) {
			//TODO probably introduce artificial delay (drand beacon clock tick perhaps)
			// or 3 seconds or something
			return p.tryNewInstance()
		}

		// reset nextChain to nil if newly finalized head makes it impossible for nextChain to be decided
		// In this case we are guaranteed to receive a new chain by Lotus (as the latest Lotus head will change due to the
		// update to the fork choice rule derived from the newly finalised head)
		if !p.nextChain.HasPrefix(value) {
			p.nextChain = ECChain{}
		} else if p.nextChain.HasPrefix(value) {
			// update the chain to have the latest finalised head as base
			// the power table remains the same as the head remains the same
			// Start from the first tipset and iterate removing tipsets until reaching the latest decision's head
			for !p.nextChain.Base().Eq(value.Head()) {
				p.nextChain = p.nextChain.Suffix()
			}
		}
	}

	p.nextPower = PowerTable{} // clean power table to prevent starting the new instance until new power table is received

	if err := p.SendNewFinalisedChain(); err != nil {
		return fmt.Errorf("failed sending new finalised chain: %w", err)
	}

	return nil
}

// TODO Push new decision to Lotus, and expect Lotus to push to us the new table (if any) and chain (always)
func (p *Participant) SendNewFinalisedChain() error {
	//TODO send to lotus p.finalised.CID. Or should we send the entire chain? (in that case take as parameter)
	return nil
}

func (p *Participant) RequestPowerTable() {
	//TODO request power table from Lotus
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
