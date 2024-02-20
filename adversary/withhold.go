package adversary

import (
	"sort"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
)

// This adversary send its COMMIT message to only a single victim, withholding it from others.
// Against a naive algorithm, when set up with 30% of power, and a victim set with 40%,
// it can cause one victim to decide, while others revert to the base.
type WithholdCommit struct {
	id         gpbft.ActorID
	host       sim.AdversaryHost
	powertable *gpbft.PowerTable
	// The first victim is the target, others are those who need to confirm.
	victims     []gpbft.ActorID
	victimValue gpbft.ECChain
}

// A participant that never sends anything.
func NewWitholdCommit(id gpbft.ActorID, host sim.AdversaryHost, powertable *gpbft.PowerTable) *WithholdCommit {
	return &WithholdCommit{
		id:         id,
		host:       host,
		powertable: powertable,
	}
}

func (w *WithholdCommit) SetVictim(victims []gpbft.ActorID, victimValue gpbft.ECChain) {
	w.victims = victims
	w.victimValue = victimValue
}

func (w *WithholdCommit) ID() gpbft.ActorID {
	return w.id
}

func (w *WithholdCommit) ReceiveCanonicalChain(_ gpbft.ECChain, _ gpbft.PowerTable, _ []byte) error {
	return nil
}

func (w *WithholdCommit) ReceiveECChain(_ gpbft.ECChain) error {
	return nil
}

func (w *WithholdCommit) ReceiveMessage(_ *gpbft.GMessage) error {
	return nil
}

func (w *WithholdCommit) ReceiveAlarm() error {
	return nil
}

func (w *WithholdCommit) Begin() {
	broadcast := w.broadcastHelper(w.id)
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
		signers = append(signers, w.powertable.Lookup[actorID])
	}
	signers = append(signers, w.powertable.Lookup[w.id])
	sort.Ints(signers)

	signatures := make([][]byte, 0)
	pubKeys := make([]gpbft.PubKey, 0)
	prepareMarshalled := preparePayload.MarshalForSigning(w.host.NetworkName())
	for _, signerIndex := range signers {
		entry := w.powertable.Entries[signerIndex]
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
}

func (w *WithholdCommit) AllowMessage(_ gpbft.ActorID, to gpbft.ActorID, msg gpbft.Message) bool {
	gmsg, ok := msg.(gpbft.GMessage)
	if ok {
		toMainVictim := to == w.victims[0]
		toAnyVictim := false
		for _, v := range w.victims {
			if to == v {
				toAnyVictim = true
			}
		}
		if gmsg.Vote.Step == gpbft.QUALITY_PHASE {
			// Don't allow victims to see dissenting QUALITY.
			if toAnyVictim && !gmsg.Vote.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Vote.Step == gpbft.PREPARE_PHASE {
			// Don't allow victims to see dissenting PREPARE.
			if toAnyVictim && !gmsg.Vote.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Vote.Step == gpbft.COMMIT_PHASE {
			// Allow only the main victim to see our COMMIT.
			if !toMainVictim && gmsg.Sender == w.id {
				return false
			}
			// Don't allow the main victim to see any dissenting COMMIts.
			if toMainVictim && !gmsg.Vote.Value.Eq(w.victimValue) {
				return false
			}
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

func (w *WithholdCommit) broadcastHelper(sender gpbft.ActorID) func(gpbft.Payload, *gpbft.Justification) {
	return func(payload gpbft.Payload, justification *gpbft.Justification) {
		pS := payload.MarshalForSigning(w.host.NetworkName())
		_, pubkey := w.powertable.Get(sender)
		sig, err := w.host.Sign(pubkey, pS)
		if err != nil {
			panic(err)
		}

		w.host.BroadcastSynchronous(sender, gpbft.GMessage{
			Sender:        sender,
			Vote:          payload,
			Signature:     sig,
			Justification: justification,
		})
	}
}
