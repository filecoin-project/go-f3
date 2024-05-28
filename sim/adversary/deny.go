package adversary

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*Deny)(nil)

// Deny adversary denies all messages to/from a given set of participants for a
// configured duration of time.
type Deny struct {
	id          gpbft.ActorID
	host        Host
	targetsByID map[gpbft.ActorID]struct{}
	gst         time.Time
}

func NewDeny(id gpbft.ActorID, host Host, denialDuration time.Duration, targets ...gpbft.ActorID) *Deny {
	targetsByID := make(map[gpbft.ActorID]struct{})
	for _, target := range targets {
		targetsByID[target] = struct{}{}
	}
	return &Deny{
		id:          id,
		host:        host,
		targetsByID: targetsByID,
		gst:         time.Time{}.Add(denialDuration),
	}
}

func NewDenyGenerator(power *gpbft.StoragePower, denialDuration time.Duration, targets ...gpbft.ActorID) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewDeny(id, host, denialDuration, targets...),
			Power:    power,
		}
	}
}

func (d *Deny) ID() gpbft.ActorID {
	return d.id
}

func (d *Deny) AllowMessage(from gpbft.ActorID, to gpbft.ActorID, msg gpbft.GMessage) bool {
	// Deny all messages to or from targets until Global Stabilisation Time has
	// elapsed, except messages to self.
	switch {
	case from == to, d.host.Time().After(d.gst):
		return true
	default:
		return !(d.isTargeted(from) || d.isTargeted(to))
	}
}

func (d *Deny) isTargeted(id gpbft.ActorID) bool {
	_, found := d.targetsByID[id]
	return found
}

func (*Deny) Start() error                                       { return nil }
func (*Deny) ValidateMessage(*gpbft.GMessage) (bool, error)      { return true, nil }
func (*Deny) ReceiveMessage(*gpbft.GMessage, bool) (bool, error) { return true, nil }
func (*Deny) ReceiveAlarm() error                                { return nil }
