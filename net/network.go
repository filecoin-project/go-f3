package net

import (
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

// Endpoint to which participants can send messages to others
type NetworkSink interface {
	Broadcast(sender string, msg Message)
}

// Receives an updated EC chain.
type ECReceiver interface {
	ReceiveCanonicalChain(chain ECChain)
}

// Receives a consensus message.
type MessageReceiver interface {
	ReceiveMessage(sender string, msg Message)
}

type Receiver interface {
	ECReceiver
	MessageReceiver
	ID() string
}

// A consensus message.
// Opaque to the network, expected to be cast by the receiver.
type Message interface{}

type DirectedMessage struct {
	Sender      string
	Destination string
	Payload     Message
}

type Network struct {
	// Participants by ID.
	participants map[string]Receiver
	queue        []DirectedMessage
}

func New() *Network {
	return &Network{
		participants: map[string]Receiver{},
		queue:        []DirectedMessage{},
	}
}

func (n *Network) AddParticipant(p Receiver) {
	// TODO: reject duplicates
	n.participants[p.ID()] = p
}

func (n *Network) Broadcast(sender string, msg Message) {
	fmt.Printf("net: received %v from %s\n", msg, sender)
	// TODO: interleave new messages with queue.
	for k := range n.participants {
		if k != sender {
			n.queue = append(n.queue, DirectedMessage{
				Sender:      sender,
				Destination: k,
				Payload:     msg,
			})
		}
	}
}

func (n *Network) Tick() bool {
	var msg DirectedMessage
	msg, n.queue = n.queue[0], n.queue[1:]
	fmt.Printf("net: delivering %v to %s\n", msg.Payload, msg.Destination)
	n.participants[msg.Destination].ReceiveMessage(msg.Sender, msg.Payload)
	return len(n.queue) > 0
}
