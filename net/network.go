package net

import (
	"fmt"
	"sort"
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

type AdversaryInterceptor interface {
	Receiver
	AllowMessage(from string, to string, msg Message) bool
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
	// Sends a message to all other participants, immediately.
	BroadcastSynchronous(sender string, msg Message)
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
	// Participant IDs for deterministic iteration
	participantIDs []string
	// Messages received by the network but not yet delivered to all participants.
	queue   messageQueue
	latency LatencyModel
	// Timestamp of last event.
	clock float64
	// Whether global stabilisation time has passed, so adversary can't control network.
	globalStabilisationElapsed bool
	// Trace level.
	traceLevel int
}

func New(latency LatencyModel, traceLevel int) *Network {
	return &Network{
		participants:               map[string]Receiver{},
		participantIDs:             []string{},
		queue:                      messageQueue{},
		clock:                      0,
		latency:                    latency,
		globalStabilisationElapsed: false,
		traceLevel:                 traceLevel,
	}
}

func (n *Network) AddParticipant(p Receiver) {
	if n.participants[p.ID()] != nil {
		panic("duplicate participant ID")
	}
	n.participantIDs = append(n.participantIDs, p.ID())
	n.participants[p.ID()] = p
}

func (n *Network) Broadcast(sender string, msg Message) {
	n.log(TraceSent, "%s ↗ %v", sender, msg)
	for _, k := range n.participantIDs {
		if k != sender {
			latency := n.latency.Sample()
			n.queue.Insert(
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
	n.queue.Insert(messageInFlight{
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
	n.queue.Insert(
		messageInFlight{
			source:    sender,
			dest:      to,
			payload:   msg,
			deliverAt: n.clock,
		})
}

func (n *Network) BroadcastSynchronous(sender string, msg Message) {
	n.log(TraceSent, "%s ↗ %v", sender, msg)
	for _, k := range n.participantIDs {
		if k != sender {
			n.queue.Insert(
				messageInFlight{
					source:    sender,
					dest:      k,
					payload:   msg,
					deliverAt: n.clock,
				})
		}
	}
}

func (n *Network) Tick(adv AdversaryInterceptor) bool {
	// Find first message the adversary will allow.
	i := 0
	if adv != nil && !n.globalStabilisationElapsed {
		for ; i < len(n.queue); i++ {
			msg := n.queue[i]
			if adv.AllowMessage(msg.source, msg.dest, msg.payload) {
				break
			}
		}
		// If adversary blocks everything, assume GST has passed.
		if i == len(n.queue) {
			n.Log("GST elapsed")
			n.globalStabilisationElapsed = true
			i = 0
		}
	}

	msg := n.queue.Remove(i)
	n.clock = msg.deliverAt
	if msg.payload == "ALARM" {
		n.log(TraceRecvd, "%s alarm ", msg.source)
		n.participants[msg.dest].ReceiveAlarm()
	} else {
		n.log(TraceRecvd, "%s ← %s: %v", msg.dest, msg.source, msg.payload)
		n.participants[msg.dest].ReceiveMessage(msg.source, msg.payload)
	}
	return len(n.queue) > 0
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

// A queue of directed messages, maintained as an ordered list.
type messageQueue []messageInFlight

func (h *messageQueue) Insert(x messageInFlight) {
	i := sort.Search(len(*h), func(i int) bool {
		return (*h)[i].deliverAt >= x.deliverAt
	})
	*h = append(*h, messageInFlight{})
	copy((*h)[i+1:], (*h)[i:])
	(*h)[i] = x
}

// Removes an entry from the queue
func (h *messageQueue) Remove(i int) messageInFlight {
	v := (*h)[i]
	copy((*h)[i:], (*h)[i+1:])
	*h = (*h)[:len(*h)-1]
	return v
}
