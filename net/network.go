package net

import (
	"fmt"
	"sort"
	"strings"
)

// A consensus message.
// Opaque to the network, expected to be cast by the receiver.
type Message interface{}

// Receives a consensus message.
type MessageReceiver interface {
	ReceiveMessage(sender ActorID, msg Message)
	ReceiveAlarm(payload string)
}

// Interface which network participants must implement.
type Receiver interface {
	ID() ActorID
	ECReceiver
	MessageReceiver
}

type AdversaryReceiver interface {
	Receiver
	AllowMessage(from ActorID, to ActorID, msg Message) bool
}

// Endpoint to which participants can send messages to others
type NetworkSink interface {
	// Sends a message to all other participants.
	Broadcast(sender ActorID, msg Message)
	// Returns the current network time.
	Time() float64
	// Sets an alarm to fire at the given timestamp.
	SetAlarm(sender ActorID, payload string, at float64)
	// Logs a message at the "logic" level
	Log(format string, args ...interface{})
}

// Endpoint with which the adversary can control the network
type AdversaryNetworkSink interface {
	NetworkSink
	// Sends a message to all other participants, immediately.
	// Note that the adversary can subsequently delay delivery to some participants,
	// before messages are actually received.
	BroadcastSynchronous(sender ActorID, msg Message)
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
	participants map[ActorID]Receiver
	// Participant IDs for deterministic iteration
	participantIDs []ActorID
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
		participants:               map[ActorID]Receiver{},
		participantIDs:             []ActorID{},
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

func (n *Network) Broadcast(sender ActorID, msg Message) {
	n.log(TraceSent, "P%d ↗ %v", sender, msg)
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

func (n *Network) SetAlarm(sender ActorID, payload string, at float64) {
	n.queue.Insert(messageInFlight{
		source:    sender,
		dest:      sender,
		payload:   "ALARM:" + payload,
		deliverAt: at,
	})
}

func (n *Network) Log(format string, args ...interface{}) {
	n.log(TraceLogic, format, args...)
}

///// Adversary network interface

func (n *Network) BroadcastSynchronous(sender ActorID, msg Message) {
	n.log(TraceSent, "P%d ↗ %v", sender, msg)
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

func (n *Network) Tick(adv AdversaryReceiver) bool {
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
	payloadStr, ok := msg.payload.(string)
	if ok && strings.HasPrefix(payloadStr, "ALARM:") {
		n.log(TraceRecvd, "P%d %s", msg.source, payloadStr)
		n.participants[msg.dest].ReceiveAlarm(strings.TrimPrefix(payloadStr, "ALARM:"))
	} else {
		n.log(TraceRecvd, "P%d ← P%d: %v", msg.dest, msg.source, msg.payload)
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
	source    ActorID // ID of the sender
	dest      ActorID // ID of the receiver
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
