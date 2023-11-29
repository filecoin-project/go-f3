package adversary

import (
	"github.com/anorth/f3sim/f3"
	"github.com/anorth/f3sim/net"
)

// This adversary send its COMMIT message to only a single victim, withholding it from others.
// Against a naive algorithm, when set up with 30% of power, and a victim set with 40%,
// it can cause one victim to decide, while others revert to the base.
type WitholdCommit struct {
	id   net.ActorID
	ntwk net.AdversaryNetworkSink
	// The first victim is the target, others are those who need to confirm.
	victims     []net.ActorID
	victimValue net.ECChain
}

// A participant that never sends anything.
func NewWitholdCommit(id net.ActorID, ntwk net.AdversaryNetworkSink) *WitholdCommit {
	return &WitholdCommit{
		id:   id,
		ntwk: ntwk,
	}
}

func (w *WitholdCommit) SetVictim(victims []net.ActorID, victimValue net.ECChain) {
	w.victims = victims
	w.victimValue = victimValue
}

func (w *WitholdCommit) ID() net.ActorID {
	return w.id
}

func (w *WitholdCommit) ReceiveCanonicalChain(_ net.ECChain, _ net.PowerTable, _ []byte) {
}

func (w *WitholdCommit) ReceiveMessage(_ net.ActorID, _ net.Message) {
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

func (w *WitholdCommit) AllowMessage(_ net.ActorID, to net.ActorID, msg net.Message) bool {
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
			if toAnyVictim && !gmsg.Value.Eq(&w.victimValue) {
				return false
			}
		} else if gmsg.Step == f3.PREPARE {
			// Don't allow victims to see dissenting PREPARE.
			if toAnyVictim && !gmsg.Value.Eq(&w.victimValue) {
				return false
			}
		} else if gmsg.Step == f3.COMMIT {
			// Allow only the main victim to see our COMMIT.
			if !toMainVictim && gmsg.Sender == w.id {
				return false
			}
			// Don't allow the main victim to see any dissenting COMMIts.
			if toMainVictim && !gmsg.Value.Eq(&w.victimValue) {
				return false
			}
		}
	}
	return true
}
