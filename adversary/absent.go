package adversary

import (
	"github.com/filecoin-project/go-f3/f3"
)

type Absent struct {
	id   f3.ActorID
	host f3.Host
}

// A participant that never sends anything.
func NewAbsent(id f3.ActorID, host f3.Host) *Absent {
	return &Absent{
		id:   id,
		host: host,
	}
}

func (a *Absent) ID() f3.ActorID {
	return a.id
}

func (a *Absent) ReceiveCanonicalChain(_ f3.ECChain, _ f3.PowerTable, _ []byte) error {
	return nil
}

func (a *Absent) ReceiveECChain(_ f3.ECChain) error {
	return nil
}

func (a *Absent) ReceiveMessage(_ *f3.GMessage) error {
	return nil
}

func (a *Absent) ReceiveAlarm(_ *f3.AlarmMsg) error {
	return nil
}

func (a *Absent) AllowMessage(_ f3.ActorID, _ f3.ActorID, _ f3.Message) bool {
	return true
}
