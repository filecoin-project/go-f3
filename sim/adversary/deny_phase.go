package adversary

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*DenyPhase)(nil)

// DenyPhase adversary denies messages with given phase to a given set of participants for a
// configured duration of time.
//
// For this adversary to take effect global stabilisation time must be configured
// to be at least as long as the configured deny duration.
//
// See  sim.WithGlobalStabilizationTime.
type DenyPhase struct {
	id          gpbft.ActorID
	host        Host
	targetsByID map[gpbft.ActorID]struct{}
	gst         time.Time
	phase       gpbft.Phase
}

func NewDenyPhase(id gpbft.ActorID, host Host, denialDuration time.Duration, phase gpbft.Phase, targets ...gpbft.ActorID) *DenyPhase {
	targetsByID := make(map[gpbft.ActorID]struct{})
	for _, target := range targets {
		targetsByID[target] = struct{}{}
	}
	return &DenyPhase{
		id:          id,
		host:        host,
		targetsByID: targetsByID,
		gst:         time.Time{}.Add(denialDuration),
		phase:       phase,
	}
}

func NewDenyPhaseGenerator(power gpbft.StoragePower, denialDuration time.Duration, phase gpbft.Phase, targets ...gpbft.ActorID) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewDenyPhase(id, host, denialDuration, phase, targets...),
			Power:    power,
		}
	}
}

func (d *DenyPhase) ID() gpbft.ActorID {
	return d.id
}

func (d *DenyPhase) AllowMessage(from gpbft.ActorID, to gpbft.ActorID, msg gpbft.GMessage) bool {
	// DenyPhase all messages to or from targets until Global Stabilisation Time has
	// elapsed, except messages to self.
	switch {
	case from == to, d.host.Time().After(d.gst):
		return true
	default:
		isAffected := d.isTargeted(to) && msg.Vote.Step == d.phase
		return !isAffected
	}
}

func (d *DenyPhase) isTargeted(id gpbft.ActorID) bool {
	_, found := d.targetsByID[id]
	return found
}

func (*DenyPhase) StartInstanceAt(uint64, time.Time) error { return nil }
func (*DenyPhase) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}
func (*DenyPhase) ReceiveMessage(_ gpbft.ValidatedMessage) error { return nil }
func (*DenyPhase) ReceiveAlarm() error                           { return nil }
