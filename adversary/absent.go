package adversary

import "github.com/anorth/f3sim/net"

type Absent struct {
	id   string
	ntwk net.NetworkSink
}

// A participant that never sends anything.
func NewAbsent(id string, ntwk net.NetworkSink) net.Receiver {
	return &Absent{
		id:   id,
		ntwk: ntwk,
	}
}

func (a *Absent) ID() string {
	return a.id
}

func (a Absent) ReceiveCanonicalChain(chain net.ECChain) {
}

func (a Absent) ReceiveMessage(sender string, msg net.Message) {
}

func (a Absent) ReceiveAlarm() {
}
