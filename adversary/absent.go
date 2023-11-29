package adversary

import "github.com/anorth/f3sim/net"

type Absent struct {
	id   net.ActorID
	ntwk net.NetworkSink
}

// A participant that never sends anything.
func NewAbsent(id net.ActorID, ntwk net.NetworkSink) *Absent {
	return &Absent{
		id:   id,
		ntwk: ntwk,
	}
}

func (a *Absent) ID() net.ActorID {
	return a.id
}

func (a *Absent) ReceiveCanonicalChain(_ net.ECChain, _ net.PowerTable, _ []byte) {
}

func (a *Absent) ReceiveMessage(_ net.ActorID, _ net.Message) {
}

func (a *Absent) ReceiveAlarm(_ string) {
}

func (a *Absent) AllowMessage(_ net.ActorID, _ net.ActorID, _ net.Message) bool {
	return true
}
