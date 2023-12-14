package adversary

import (
	"github.com/filecoin-project/go-f3/f3"
	"github.com/filecoin-project/go-f3/sim"
)

// This adversary send its COMMIT message to only a single victim, withholding it from others.
// Against a naive algorithm, when set up with 30% of power, and a victim set with 40%,
// it can cause one victim to decide, while others revert to the base.
type WitholdCommit struct {
	id   f3.ActorID
	ntwk sim.AdversaryNetworkSink
	// The first victim is the target, others are those who need to confirm.
	victims     []f3.ActorID
	victimValue f3.ECChain
}

// A participant that never sends anything.
func NewWitholdCommit(id f3.ActorID, ntwk sim.AdversaryNetworkSink) *WitholdCommit {
	return &WitholdCommit{
		id:   id,
		ntwk: ntwk,
	}
}

func (w *WitholdCommit) SetVictim(victims []f3.ActorID, victimValue f3.ECChain) {
	w.victims = victims
	w.victimValue = victimValue
}

func (w *WitholdCommit) ID() f3.ActorID {
	return w.id
}

func (w *WitholdCommit) ReceiveCanonicalChain(_ f3.ECChain, _ f3.PowerTable, _ []byte) {
}

func (w *WitholdCommit) ReceiveMessage(_ f3.ActorID, _ f3.Message) {
}

func (w *WitholdCommit) ReceiveAlarm(_ string) {
}

func (w *WitholdCommit) Begin() {
	// All victims need to see QUALITY and PREPARE in order to send their COMMIT,
	// but only the one victim will see our COMMIT.
	w.ntwk.BroadcastSynchronous(w.id, f3.GMessage{
		Instance: 0,
		Round:    0,
		Sender:   w.id,
		Step:     f3.QUALITY,
		Value:    w.victimValue,
	})
	w.ntwk.BroadcastSynchronous(w.id, f3.GMessage{
		Instance: 0,
		Round:    0,
		Sender:   w.id,
		Step:     f3.PREPARE,
		Value:    w.victimValue,
	})
	w.ntwk.BroadcastSynchronous(w.id, f3.GMessage{
		Instance: 0,
		Round:    0,
		Sender:   w.id,
		Step:     f3.COMMIT,
		Value:    w.victimValue,
	})
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
		if gmsg.Step == f3.QUALITY {
			// Don't allow victims to see dissenting QUALITY.
			if toAnyVictim && !gmsg.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Step == f3.PREPARE {
			// Don't allow victims to see dissenting PREPARE.
			if toAnyVictim && !gmsg.Value.Eq(w.victimValue) {
				return false
			}
		} else if gmsg.Step == f3.COMMIT {
			// Allow only the main victim to see our COMMIT.
			if !toMainVictim && gmsg.Sender == w.id {
				return false
			}
			// Don't allow the main victim to see any dissenting COMMIts.
			if toMainVictim && !gmsg.Value.Eq(w.victimValue) {
				return false
			}
		}
	}
	return true
}
