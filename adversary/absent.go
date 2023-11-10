package adversary

import "github.com/anorth/f3sim/net"

type Absent struct {
	id   string
	ntwk net.NetworkSink
}

// A participant that never sends anything.
func NewAbsent(id string, ntwk net.NetworkSink) *Absent {
	return &Absent{
		id:   id,
		ntwk: ntwk,
	}
}

func (a *Absent) ID() string {
	return a.id
}

func (a *Absent) ReceiveCanonicalChain(_ net.ECChain) {
}

func (a *Absent) ReceiveMessage(_ string, _ net.Message) {
}

func (a *Absent) ReceiveAlarm() {
}

func (a *Absent) AllowMessage(_ string, _ string, _ net.Message) bool {
	return true
}
