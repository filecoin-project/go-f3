package adversary

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*Absent)(nil)

type Absent struct {
	id   gpbft.ActorID
	host gpbft.Host
}

// A participant that never sends anything.
func NewAbsent(id gpbft.ActorID, host gpbft.Host) *Absent {
	return &Absent{
		id:   id,
		host: host,
	}
}

func NewAbsentGenerator(power gpbft.StoragePower) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewAbsent(id, host),
			Power:    power,
		}
	}
}

func (a *Absent) ID() gpbft.ActorID {
	return a.id
}

func (*Absent) StartInstanceAt(uint64, time.Time) error { return nil }

func (*Absent) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}

func (*Absent) ReceiveMessage(_ gpbft.ValidatedMessage) error {
	return nil
}

func (*Absent) ReceiveAlarm() error {
	return nil
}

func (*Absent) AllowMessage(_ gpbft.ActorID, _ gpbft.ActorID, _ gpbft.GMessage) bool {
	return true
}
