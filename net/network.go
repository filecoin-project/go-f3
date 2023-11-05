package net

import (
	"container/heap"
	"fmt"
	"reflect"
)

// A tipset CID is represented by an opaque string.
type TipSet = string

// An EC chain suffix.
type ECChain struct {
	// The last finalised tipset on which this suffix is based.
	Base TipSet
	// The epoch of the base tipset.
	BaseEpoch int64
	// Chain of tipsets after base.
	Tail []TipSet
}

// Compares two ECChains for equality
func (c *ECChain) Eq(other *ECChain) bool {
	return reflect.DeepEqual(c, other)
}

// Receives an updated EC chain.
type ECReceiver interface {
	ReceiveCanonicalChain(chain ECChain)
}

// A consensus message.
// Opaque to the network, expected to be cast by the receiver.
type Message interface{}

// Receives a consensus message.
type MessageReceiver interface {
	ReceiveMessage(sender string, msg Message)
}

// Interface which network participants must implement.
type Receiver interface {
	ID() string
	ECReceiver
	MessageReceiver
}

// Endpoint to which participants can send messages to others
type NetworkSink interface {
	Broadcast(sender string, msg Message)
}

type Network struct {
	// Participants by ID.
	participants map[string]Receiver
	// Messages received by the network but not yet delivered to all participants.
	queue   messageQueue
	latency LatencyModel
	// Timestamp of last event.
	clock float64
}

func New(latency LatencyModel) *Network {
	return &Network{
		participants: map[string]Receiver{},
		queue:        messageQueue{},
		clock:        0,
		latency:      latency,
	}
}

func (n *Network) AddParticipant(p Receiver) {
	if n.participants[p.ID()] != nil {
		panic("duplicate participant ID")
	}
	n.participants[p.ID()] = p
}

func (n *Network) Broadcast(sender string, msg Message) {
	fmt.Printf("net [%.3f]: received %v from %s\n", n.clock, msg, sender)
	for k := range n.participants {
		if k != sender {
			latency := n.latency.Sample()
			heap.Push(&n.queue,
				messageInFlight{
					source:    sender,
					dest:      k,
					payload:   msg,
					deliverAt: n.clock + latency,
				})
		}
	}
}

func (n *Network) Tick() bool {
	var msg messageInFlight
	msg = heap.Pop(&n.queue).(messageInFlight)
	n.clock = msg.deliverAt
	fmt.Printf("net [%.3f]: delivering %s->%s: %v\n", n.clock, msg.source, msg.dest, msg.payload)
	n.participants[msg.dest].ReceiveMessage(msg.source, msg.payload)
	return n.queue.Len() > 0
}

type messageInFlight struct {
	source    string  // ID of the sender
	dest      string  // ID of the receiver
	payload   Message // Message body
	deliverAt float64 // Timestamp at which to deliver the message
}

// Implements a queue (min-heap) of directed messages.
type messageQueue []messageInFlight

func (h messageQueue) Len() int           { return len(h) }
func (h messageQueue) Less(i, j int) bool { return h[i].deliverAt < (h[j].deliverAt) }
func (h messageQueue) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *messageQueue) Push(x interface{}) {
	*h = append(*h, x.(messageInFlight))
}
func (h *messageQueue) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
