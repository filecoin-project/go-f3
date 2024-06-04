package adversary

import (
	"errors"
	"sort"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*WithholdCommit)(nil)

// This adversary send its COMMIT message to only a single victim, withholding it from others.
// Against a naive algorithm, when set up with 30% of power, and a victim set with 40%,
// it can cause one victim to decide, while others revert to the base.
type WithholdCommit struct {
	id   gpbft.ActorID
	host Host
	// The first victim is the target, others are those who need to confirm.
	victims     []gpbft.ActorID
	victimValue gpbft.ECChain
}

// A participant that never sends anything.
func NewWitholdCommit(id gpbft.ActorID, host Host) *WithholdCommit {
	return &WithholdCommit{
		id:   id,
		host: host,
	}
}

func NewWitholdCommitGenerator(power *gpbft.StoragePower, victims []gpbft.ActorID, victimValue gpbft.ECChain) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		wc := NewWitholdCommit(id, host)
		wc.SetVictim(victims, victimValue)
		return &Adversary{
			Receiver: wc,
			Power:    power,
		}
	}
}

func (w *WithholdCommit) SetVictim(victims []gpbft.ActorID, victimValue gpbft.ECChain) {
	w.victims = victims
	w.victimValue = victimValue
}

func (w *WithholdCommit) ID() gpbft.ActorID {
	return w.id
}

func (w *WithholdCommit) Start() error {

	if len(w.victims) == 0 {
		return errors.New("victims must be set")
	}

	powertable, _, _ := w.host.GetCommitteeForInstance(0)
	broadcast := w.broadcastHelper(w.id, powertable)
	// All victims need to see QUALITY and PREPARE in order to send their COMMIT,
	// but only the one victim will see our COMMIT.
	broadcast(gpbft.Payload{
		Instance: 0,
		Round:    0,
		Step:     gpbft.QUALITY_PHASE,
		Value:    w.victimValue,
	}, nil)
	preparePayload := gpbft.Payload{
		Instance: 0,
		Round:    0,
		Step:     gpbft.PREPARE_PHASE,
		Value:    w.victimValue,
	}
	broadcast(preparePayload, nil)

	commitPayload := gpbft.Payload{
		Instance: 0,
		Round:    0,
		Step:     gpbft.COMMIT_PHASE,
		Value:    w.victimValue,
	}

	justification := gpbft.Justification{
		Vote:      preparePayload,
		Signers:   bitfield.New(),
		Signature: nil,
	}
	// NOTE: this is a super-unrealistic adversary that can forge messages from other participants!
	// This power is used to simplify the logic here so it doesn't have to execute the protocol
	// properly to accumulate the evidence for its COMMIT message.
	signers := make([]int, 0)
	for _, actorID := range w.victims {
		signers = append(signers, powertable.Lookup[actorID])
	}
	signers = append(signers, powertable.Lookup[w.id])
	sort.Ints(signers)

	signatures := make([][]byte, 0)
	pubKeys := make([]gpbft.PubKey, 0)
	prepareMarshalled := w.host.MarshalPayloadForSigning(w.host.NetworkName(), &preparePayload)
	for _, signerIndex := range signers {
		entry := powertable.Entries[signerIndex]
		signatures = append(signatures, w.sign(entry.PubKey, prepareMarshalled))
		pubKeys = append(pubKeys, entry.PubKey)
		justification.Signers.Set(uint64(signerIndex))
	}
	var err error
	justification.Signature, err = w.host.Aggregate(pubKeys, signatures)
	if err != nil {
		panic(err)
	}

	broadcast(commitPayload, &justification)
	return nil
}

func (*WithholdCommit) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}

func (*WithholdCommit) ReceiveMessage(_ gpbft.ValidatedMessage) error {
	return nil
}

func (*WithholdCommit) SkipToInstance(uint64) {}

func (*WithholdCommit) ReceiveAlarm() error {
	return nil
}

func (w *WithholdCommit) AllowMessage(_ gpbft.ActorID, to gpbft.ActorID, msg gpbft.GMessage) bool {
	toMainVictim := to == w.victims[0]
	toAnyVictim := false
	for _, v := range w.victims {
		if to == v {
			toAnyVictim = true
		}
	}
	if msg.Vote.Step == gpbft.QUALITY_PHASE {
		// Don't allow victims to see dissenting QUALITY.
		if toAnyVictim && !msg.Vote.Value.Eq(w.victimValue) {
			return false
		}
	} else if msg.Vote.Step == gpbft.PREPARE_PHASE {
		// Don't allow victims to see dissenting PREPARE.
		if toAnyVictim && !msg.Vote.Value.Eq(w.victimValue) {
			return false
		}
	} else if msg.Vote.Step == gpbft.COMMIT_PHASE {
		// Allow only the main victim to see our COMMIT.
		if !toMainVictim && msg.Sender == w.id {
			return false
		}
		// Don't allow the main victim to see any dissenting COMMIts.
		if toMainVictim && !msg.Vote.Value.Eq(w.victimValue) {
			return false
		}
	}
	return true
}

func (w *WithholdCommit) sign(pubkey gpbft.PubKey, msg []byte) []byte {
	sig, err := w.host.Sign(pubkey, msg)
	if err != nil {
		panic(err)
	}
	return sig
}

func (w *WithholdCommit) broadcastHelper(sender gpbft.ActorID, powertable *gpbft.PowerTable) func(gpbft.Payload, *gpbft.Justification) {
	return func(payload gpbft.Payload, justification *gpbft.Justification) {
		pS := w.host.MarshalPayloadForSigning(w.host.NetworkName(), &payload)
		_, pubkey := powertable.Get(sender)
		sig, err := w.host.Sign(pubkey, pS)
		if err != nil {
			panic(err)
		}

		w.host.BroadcastSynchronous(&gpbft.GMessage{
			Sender:        sender,
			Vote:          payload,
			Signature:     sig,
			Justification: justification,
		})
	}
}
