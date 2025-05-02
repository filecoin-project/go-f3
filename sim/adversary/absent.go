package adversary

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*Absent)(nil)

type Absent struct{ allowAll }

func NewAbsentGenerator(power gpbft.StoragePower) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: Absent{},
			Power:    power,
			ID:       id,
		}
	}
}

func (Absent) ValidateMessage(_ context.Context, msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}

func (Absent) StartInstanceAt(uint64, time.Time) error                      { return nil }
func (Absent) ReceiveMessage(context.Context, gpbft.ValidatedMessage) error { return nil }
func (Absent) ReceiveAlarm(context.Context) error                           { return nil }
