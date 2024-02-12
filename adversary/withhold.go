package adversary

import (
	"slices"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/f3"
	"github.com/filecoin-project/go-f3/sim"
)

// This adversary send its COMMIT message to only a single victim, withholding it from others.
// Against a naive algorithm, when set up with 30% of power, and a victim set with 40%,
// it can cause one victim to decide, while others revert to the base.
type WithholdCommit struct {
	id         f3.ActorID
	host       sim.AdversaryHost
	powertable *f3.PowerTable
	// The first victim is the target, others are those who need to confirm.
	victims     []f3.ActorID
	victimValue f3.ECChain
}

// A participant that never sends anything.
func NewWitholdCommit(id f3.ActorID, host sim.AdversaryHost, powertable *f3.PowerTable) *WithholdCommit {
	return &WithholdCommit{
		id:         id,
		host:       host,
		powertable: powertable,
	}
}

func (w *WithholdCommit) SetVictim(victims []f3.ActorID, victimValue f3.ECChain) {
	w.victims = victims
	w.victimValue = victimValue
}

func (w *WithholdCommit) ID() f3.ActorID {
	return w.id
}

func (w *WithholdCommit) ReceiveCanonicalChain(_ f3.ECChain, _ f3.PowerTable, _ []byte) error {
	return nil
}

func (w *WithholdCommit) ReceiveECChain(_ f3.ECChain) error {
	return nil
}

func (w *WithholdCommit) ReceiveMessage(_ *f3.GMessage) error {
	return nil
}

func (w *WithholdCommit) ReceiveAlarm(_ string) error {
	return nil
}

func (w *WithholdCommit) Begin() {
	broadcast := w.broadcastHelper(w.id)
	// All victims need to see QUALITY and PREPARE in order to send their COMMIT,
	// but only the one victim will see our COMMIT.
	broadcast(f3.Payload{
		Instance: 0,
		Round:    0,
		Step:     f3.QUALITY_PHASE,
		Value:    w.victimValue,
	}, nil)
	preparePayload := f3.Payload{
		Instance: 0,
		Round:    0,
		Step:     f3.PREPARE_PHASE,
		Value:    w.victimValue,
	}
	broadcast(preparePayload, nil)

	commitPayload := f3.Payload{
		Instance: 0,
		Round:    0,
		Step:     f3.COMMIT_PHASE,
		Value:    w.victimValue,
	}

	justification := f3.Justification{
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
	slices.Sort(signers)

	signatures := make([][]byte, 0)
	pubKeys := make([]f3.PubKey, 0)
	prepareMarshalled := preparePayload.MarshalForSigning(f3.TODONetworkName)
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

func (w *WithholdCommit) AllowMessage(_ f3.ActorID, to f3.ActorID, msg f3.Message) bool {
	gmsg, ok := msg.(f3.GMessage)
	if ok {
		toMainVictim := to == w.victims[0]
		toAnyVictim := false
		for _, v := range w.victims {
			if to == v {
				toAnyVictim = true
			}
		}
		if gmsg.Vote.Step == f3.QUALITY_PHASE {
			// Don't allow victims to see dissenting QUALITY.
			if toAnyVictim && !gmsg.Vote.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Vote.Step == f3.PREPARE_PHASE {
			// Don't allow victims to see dissenting PREPARE.
			if toAnyVictim && !gmsg.Vote.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Vote.Step == f3.COMMIT_PHASE {
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

func (w *WithholdCommit) sign(pubkey f3.PubKey, msg []byte) []byte {

	sig, err := w.host.Sign(pubkey, msg)
	if err != nil {
		panic(err)
	}
	return sig
}

func (w *WithholdCommit) broadcastHelper(sender f3.ActorID) func(f3.Payload, *f3.Justification) {
	return func(payload f3.Payload, justification *f3.Justification) {
		pS := payload.MarshalForSigning(f3.TODONetworkName)
		_, pubkey := w.powertable.Get(sender)
		sig, err := w.host.Sign(pubkey, pS)
		if err != nil {
			panic(err)
		}

		var just f3.Justification
		if justification != nil {
			just = *justification
		}

		w.host.BroadcastSynchronous(sender, f3.GMessage{
			Sender:        sender,
			Vote:          payload,
			Signature:     sig,
			Justification: just,
		})
	}
}
