package adversary

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*DenyQuality)(nil)

// DenyQuality adversary denies all messages to/from a given set of participants for a
// configured duration of time.
//
// For this adversary to take effect global stabilisation time must be configured
// to be at least as long as the configured deny duration.
//
// See  sim.WithGlobalStabilizationTime.
type DenyQuality struct {
	id          gpbft.ActorID
	host        Host
	targetsByID map[gpbft.ActorID]struct{}
	gst         time.Time
}

func NewDenyQuality(id gpbft.ActorID, host Host, denialDuration time.Duration, targets ...gpbft.ActorID) *DenyQuality {
	targetsByID := make(map[gpbft.ActorID]struct{})
	for _, target := range targets {
		targetsByID[target] = struct{}{}
	}
	return &DenyQuality{
		id:          id,
		host:        host,
		targetsByID: targetsByID,
		gst:         time.Time{}.Add(denialDuration),
	}
}

func NewDenyQualityGenerator(power gpbft.StoragePower, denialDuration time.Duration, targets ...gpbft.ActorID) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewDenyQuality(id, host, denialDuration, targets...),
			Power:    power,
		}
	}
}

func (d *DenyQuality) ID() gpbft.ActorID {
	return d.id
}

func (d *DenyQuality) AllowMessage(from gpbft.ActorID, to gpbft.ActorID, msg gpbft.GMessage) bool {
	// DenyQuality all messages to or from targets until Global Stabilisation Time has
	// elapsed, except messages to self.
	switch {
	case from == to:
		return true
	default:
		isAffected := d.isTargeted(to) && msg.Vote.Step == gpbft.QUALITY_PHASE
		//fmt.Printf("ADV: %d -> %d, %v\n", from, to, !isAffected)
		return !isAffected
	}
}

func (d *DenyQuality) isTargeted(id gpbft.ActorID) bool {
	_, found := d.targetsByID[id]
	return found
}

func (*DenyQuality) StartInstanceAt(uint64, time.Time) error { return nil }
func (*DenyQuality) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}
func (*DenyQuality) ReceiveMessage(_ gpbft.ValidatedMessage) error { return nil }
func (*DenyQuality) ReceiveAlarm() error                           { return nil }
