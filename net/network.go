package net

import (
	"container/heap"
	"fmt"
)

// A consensus message.
// Opaque to the network, expected to be cast by the receiver.
type Message interface{}

// Receives a consensus message.
type MessageReceiver interface {
	ReceiveMessage(sender string, msg Message)
	ReceiveAlarm()
}

// Interface which network participants must implement.
type Receiver interface {
	ID() string
	ECReceiver
	MessageReceiver
}

// Endpoint to which participants can send messages to others
type NetworkSink interface {
	// Sends a message to all other participants.
	Broadcast(sender string, msg Message)
	// Returns the current network time.
	Time() float64
	// Sets an alarm to fire at the given timestamp.
	SetAlarm(sender string, at float64)
	// Logs a message at the "logic" level
	Log(format string, args ...interface{})
}

// Endpoint with which the adversary can control the network
type AdversaryNetworkSink interface {
	NetworkSink
	// Sends a message to a single participant, immediately.
	SendSynchronous(sender string, to string, msg Message)
}

const (
	TraceNone = iota
	TraceSent
	TraceRecvd
	TraceLogic
	TraceAll
)

type Network struct {
	// Participants by ID.
	participants map[string]Receiver
	// Messages received by the network but not yet delivered to all participants.
	queue   messageQueue
	latency LatencyModel
	// Timestamp of last event.
	clock float64
	// Trace level.
	traceLevel int
}

func New(latency LatencyModel, traceLevel int) *Network {
	return &Network{
		participants: map[string]Receiver{},
		queue:        messageQueue{},
		clock:        0,
		latency:      latency,
		traceLevel:   traceLevel,
	}
}

func (n *Network) AddParticipant(p Receiver) {
	if n.participants[p.ID()] != nil {
		panic("duplicate participant ID")
	}
	n.participants[p.ID()] = p
}

func (n *Network) Broadcast(sender string, msg Message) {
	n.log(TraceSent, "%s ↗ %v", sender, msg)
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

func (n *Network) Time() float64 {
	return n.clock
}

func (n *Network) SetAlarm(sender string, at float64) {
	heap.Push(&n.queue,
		messageInFlight{
			source:    sender,
			dest:      sender,
			payload:   "ALARM",
			deliverAt: at,
		})
}

func (n *Network) Log(format string, args ...interface{}) {
	n.log(TraceLogic, format, args...)
}

///// Adversary network interface

func (n *Network) SendSynchronous(sender string, to string, msg Message) {
	heap.Push(&n.queue,
		messageInFlight{
			source:    sender,
			dest:      to,
			payload:   msg,
			deliverAt: n.clock,
		})
}

func (n *Network) Tick() bool {
	var msg messageInFlight
	msg = heap.Pop(&n.queue).(messageInFlight)
	n.clock = msg.deliverAt
	if msg.payload == "ALARM" {
		n.log(TraceRecvd, "%s alarm ", msg.source)
		n.participants[msg.dest].ReceiveAlarm()
	} else {
		n.log(TraceRecvd, "%s ← %s: %v", msg.dest, msg.source, msg.payload)
		n.participants[msg.dest].ReceiveMessage(msg.source, msg.payload)
	}
	return n.queue.Len() > 0
}

func (n *Network) log(level int, format string, args ...interface{}) {
	if level <= n.traceLevel {
		fmt.Printf("net [%.3f]: ", n.clock)
		fmt.Printf(format, args...)
		fmt.Printf("\n")
	}
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
