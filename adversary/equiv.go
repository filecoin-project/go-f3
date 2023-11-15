package adversary

import (
	"github.com/anorth/f3sim/granite"
	"github.com/anorth/f3sim/net"
)

// This adversary send its COMMIT message to only a single victim, withholding it from others.
// Against a naive algorithm, when set up with 30% of power, and a victim set with 40%,
// it can cause one victim to decide, while others revert to the base.
type WitholdCommit struct {
	id   string
	ntwk net.AdversaryNetworkSink
	// The first victim is the target, others are those who need to confirm.
	victims     []string
	victimValue net.ECChain
}

// A participant that never sends anything.
func NewWitholdCommit(id string, ntwk net.AdversaryNetworkSink) *WitholdCommit {
	return &WitholdCommit{
		id:   id,
		ntwk: ntwk,
	}
}

func (w *WitholdCommit) SetVictim(victims []string, victimValue net.ECChain) {
	w.victims = victims
	w.victimValue = victimValue
}

func (w *WitholdCommit) ID() string {
	return w.id
}

func (w *WitholdCommit) ReceiveCanonicalChain(_ net.ECChain) {
}

func (w *WitholdCommit) ReceiveMessage(_ string, _ net.Message) {
}

func (w *WitholdCommit) ReceiveAlarm(_ string) {
}

func (w *WitholdCommit) Begin() {
	// All victims need to see QUALITY and PREPARE in order to send their COMMIT,
	// but only the one victim will see our COMMIT.
	w.ntwk.BroadcastSynchronous(w.id, granite.GMessage{
		Instance: 0,
		Round:    0,
		Sender:   w.id,
		Step:     granite.QUALITY,
		Value:    w.victimValue,
	})
	w.ntwk.BroadcastSynchronous(w.id, granite.GMessage{
		Instance: 0,
		Round:    0,
		Sender:   w.id,
		Step:     granite.PREPARE,
		Value:    w.victimValue,
	})
	w.ntwk.BroadcastSynchronous(w.id, granite.GMessage{
		Instance: 0,
		Round:    0,
		Sender:   w.id,
		Step:     granite.COMMIT,
		Value:    w.victimValue,
	})
}

func (w *WitholdCommit) AllowMessage(_ string, to string, msg net.Message) bool {
	gmsg, ok := msg.(granite.GMessage)
	if ok {
		toMainVictim := to == w.victims[0]
		toAnyVictim := false
		for _, v := range w.victims {
			if to == v {
				toAnyVictim = true
			}
		}
		if gmsg.Step == granite.QUALITY {
			// Don't allow victims to see dissenting QUALITY.
			if toAnyVictim && !gmsg.Value.Eq(&w.victimValue) {
				return false
			}
		} else if gmsg.Step == granite.PREPARE {
			// Don't allow victims to see dissenting PREPARE.
			if toAnyVictim && !gmsg.Value.Eq(&w.victimValue) {
				return false
			}
		} else if gmsg.Step == granite.COMMIT {
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
