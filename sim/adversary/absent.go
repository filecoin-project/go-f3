package adversary

import (
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

func (a *Absent) ID() gpbft.ActorID {
	return a.id
}

func (a *Absent) Start() error {
	return nil
}

func (a *Absent) ReceiveECChain(_ gpbft.ECChain) error {
	return nil
}

func (a *Absent) ValidateMessage(_ *gpbft.GMessage) (bool, error) {
	return true, nil
}

func (a *Absent) ReceiveMessage(_ *gpbft.GMessage, _ bool) (bool, error) {
	return true, nil
}

func (a *Absent) ReceiveAlarm() error {
	return nil
}

func (a *Absent) AllowMessage(_ gpbft.ActorID, _ gpbft.ActorID, _ gpbft.GMessage) bool {
	return true
}
