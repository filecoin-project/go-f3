package adversary

import (
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/f3"
	"github.com/filecoin-project/go-f3/sim"
)

// This adversary send its COMMIT message to only a single victim, withholding it from others.
// Against a naive algorithm, when set up with 30% of power, and a victim set with 40%,
// it can cause one victim to decide, while others revert to the base.
type WitholdCommit struct {
	id         f3.ActorID
	host       sim.AdversaryHost
	powertable f3.PowerTable
	// The first victim is the target, others are those who need to confirm.
	victims     []f3.ActorID
	victimValue f3.ECChain
}

// A participant that never sends anything.
func NewWitholdCommit(id f3.ActorID, host sim.AdversaryHost, powertable f3.PowerTable) *WitholdCommit {
	return &WitholdCommit{
		id:         id,
		host:       host,
		powertable: powertable,
	}
}

func (w *WitholdCommit) SetVictim(victims []f3.ActorID, victimValue f3.ECChain) {
	w.victims = victims
	w.victimValue = victimValue
}

func (w *WitholdCommit) ID() f3.ActorID {
	return w.id
}

func (w *WitholdCommit) ReceiveCanonicalChain(_ f3.ECChain, _ f3.PowerTable, _ []byte) error {
	return nil
}

func (w *WitholdCommit) ReceiveECChain(_ f3.ECChain) error {
	return nil
}

func (w *WitholdCommit) ReceiveMessage(_ *f3.GMessage) error {
	return nil
}

func (w *WitholdCommit) ReceiveAlarm(_ string) error {
	return nil
}

func (w *WitholdCommit) Begin() {
	// All victims need to see QUALITY and PREPARE in order to send their COMMIT,
	// but only the one victim will see our COMMIT.
	w.host.BroadcastSynchronous(w.id, f3.GMessage{
		Sender: w.id,
		Current: f3.SignedMessage{
			Instance: 0,
			Round:    0,
			Step:     f3.QUALITY_PHASE,
			Value:    w.victimValue,
		},
		Signature: w.host.Sign(w.id, f3.SignaturePayload(0, 0, f3.QUALITY_PHASE, w.victimValue)),
	})
	w.host.BroadcastSynchronous(w.id, f3.GMessage{
<<<<<<< HEAD
		Sender: w.id,
		Current: f3.SignedMessage{
			Instance: 0,
			Round:    0,
			Step:     f3.PREPARE_PHASE,
			Value:    w.victimValue,
		},
		Signature: w.host.Sign(w.id, f3.SignaturePayload(0, 0, f3.PREPARE_PHASE, w.victimValue)),
	})

	message := f3.GMessage{
		Sender: w.id,
		Current: f3.SignedMessage{
			Instance: 0,
			Round:    0,
			Step:     f3.COMMIT_PHASE,
			Value:    w.victimValue,
		},
		Signature: w.host.Sign(w.id, f3.SignaturePayload(0, 0, f3.COMMIT_PHASE, w.victimValue)),
	}
	payload := f3.SignaturePayload(0, 0, f3.PREPARE_PHASE, w.victimValue)
	justification := f3.Justification{
		Payload: f3.SignedMessage{
			Step:     f3.PREPARE_PHASE,
			Value:    w.victimValue,
			Instance: 0,
			Round:    0,
		},
		QuorumSignature: f3.QuorumSignature{
			Signers:   bitfield.New(),
			Signature: nil,
		},
=======
		Sender:    w.id,
		Instance:  0,
		Round:     0,
		Step:      f3.PREPARE,
		Value:     w.victimValue,
		Signature: w.host.Sign(w.id, f3.SignaturePayload(0, 0, f3.PREPARE, w.victimValue)),
	})

	message := f3.GMessage{
		Sender:    w.id,
		Instance:  0,
		Round:     0,
		Step:      f3.COMMIT,
		Value:     w.victimValue,
		Signature: w.host.Sign(w.id, f3.SignaturePayload(0, 0, f3.COMMIT, w.victimValue)),
	}
	payload := f3.SignaturePayload(0, 0, f3.PREPARE, w.victimValue)
	justification := f3.Justification{
		Step:      f3.PREPARE,
		Value:     w.victimValue,
		Instance:  0,
		Round:     0,
		Signers:   bitfield.New(),
		Signature: nil,
>>>>>>> f944d14 (Update tests and calls to signer/verifier interfaces)
	}
	signatures := make([][]byte, 0)
	for _, actorID := range w.victims {
		signatures = append(signatures, w.host.Sign(actorID, payload))
<<<<<<< HEAD
		justification.QuorumSignature.Signers.Set(uint64(w.powertable.Lookup[actorID]))
	}
	signatures = append(signatures, w.host.Sign(w.id, payload))
	justification.QuorumSignature.Signers.Set(uint64(w.powertable.Lookup[w.id]))
	justification.QuorumSignature.Signature = w.host.Aggregate(signatures, justification.QuorumSignature.Signature)
=======
		justification.Signers.Set(uint64(w.powertable.Lookup[actorID]))
	}
	signatures = append(signatures, w.host.Sign(w.id, payload))
	justification.Signers.Set(uint64(w.powertable.Lookup[w.id]))
	justification.Signature = w.host.Aggregate(signatures, justification.Signature)
>>>>>>> f944d14 (Update tests and calls to signer/verifier interfaces)
	message.Justification = justification
	w.host.BroadcastSynchronous(w.id, message)
}

func (w *WitholdCommit) AllowMessage(_ f3.ActorID, to f3.ActorID, msg f3.Message) bool {
	gmsg, ok := msg.(f3.GMessage)
	if ok {
		toMainVictim := to == w.victims[0]
		toAnyVictim := false
		for _, v := range w.victims {
			if to == v {
				toAnyVictim = true
			}
		}
		if gmsg.Current.Step == f3.QUALITY_PHASE {
			// Don't allow victims to see dissenting QUALITY.
			if toAnyVictim && !gmsg.Current.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Current.Step == f3.PREPARE_PHASE {
			// Don't allow victims to see dissenting PREPARE.
			if toAnyVictim && !gmsg.Current.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Current.Step == f3.COMMIT_PHASE {
			// Allow only the main victim to see our COMMIT.
			if !toMainVictim && gmsg.Sender == w.id {
				return false
			}
			// Don't allow the main victim to see any dissenting COMMIts.
			if toMainVictim && !gmsg.Current.Value.Eq(w.victimValue) {
				return false
			}
		}
	}
	return true
}
