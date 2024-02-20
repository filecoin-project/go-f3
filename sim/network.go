package sim

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

type AdversaryReceiver interface {
	gpbft.Receiver
	AllowMessage(from gpbft.ActorID, to gpbft.ActorID, msg gpbft.Message) bool
}

// Endpoint with which the adversary can control the network
type AdversaryHost interface {
	gpbft.Host
	// Sends a message to all other participants, immediately.
	// Note that the adversary can subsequently delay delivery to some participants,
	// before messages are actually received.
	BroadcastSynchronous(sender gpbft.ActorID, msg gpbft.Message)
}

const (
	TraceNone = iota
	TraceSent
	TraceRecvd
	TraceLogic
	TraceAll
)

const _ = TraceAll // Suppress unused constant warning.

type Network struct {
	SigningBacked

	// Participants by ID.
	participants map[gpbft.ActorID]gpbft.Receiver
	// Participant IDs for deterministic iteration
	participantIDs []gpbft.ActorID
	// Messages received by the network but not yet delivered to all participants.
	queue   messageQueue
	latency LatencyModel
	// Timestamp of last event.
	clock time.Time
	// Whether global stabilisation time has passed, so adversary can't control network.
	globalStabilisationElapsed bool
	// Trace level.
	traceLevel int

	actor2PubKey map[gpbft.ActorID]gpbft.PubKey

	networkName gpbft.NetworkName
}

func NewNetwork(latency LatencyModel, traceLevel int, sb SigningBacked, nn gpbft.NetworkName) *Network {
	return &Network{
		SigningBacked:              sb,
		participants:               map[gpbft.ActorID]gpbft.Receiver{},
		participantIDs:             []gpbft.ActorID{},
		queue:                      messageQueue{},
		clock:                      time.Time{},
		latency:                    latency,
		globalStabilisationElapsed: false,
		traceLevel:                 traceLevel,
		actor2PubKey:               map[gpbft.ActorID]gpbft.PubKey{},
		networkName:                nn,
	}
}

func (n *Network) AddParticipant(p gpbft.Receiver, pubKey gpbft.PubKey) {
	if n.participants[p.ID()] != nil {
		panic("duplicate participant ID")
	}
	n.participantIDs = append(n.participantIDs, p.ID())
	n.participants[p.ID()] = p
	n.actor2PubKey[p.ID()] = pubKey
}

////// Network interface

func (n *Network) NetworkName() gpbft.NetworkName {
	return n.networkName
}

func (n *Network) Broadcast(msg *gpbft.GMessage) {
	n.log(TraceSent, "P%d ↗ %v", msg.Sender, msg)
	for _, k := range n.participantIDs {
		if k != msg.Sender {
			latency := n.latency.Sample()
			n.queue.Insert(
				messageInFlight{
					source:    msg.Sender,
					dest:      k,
					payload:   *msg,
					deliverAt: n.clock.Add(latency),
				})
		}
	}
}

///// Clock interface

func (n *Network) Time() time.Time {
	return n.clock
}

func (n *Network) SetAlarm(sender gpbft.ActorID, at time.Time) {
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

func (n *Network) BroadcastSynchronous(sender gpbft.ActorID, msg gpbft.Message) {
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

func (n *Network) Tick(adv AdversaryReceiver) (bool, error) {
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
	if ok && strings.HasPrefix(payloadStr, "ALARM") {
		n.log(TraceRecvd, "P%d %s", msg.source, payloadStr)
		if err := n.participants[msg.dest].ReceiveAlarm(); err != nil {
			return false, fmt.Errorf("failed receiving alarm: %w", err)
		}
	} else {
		n.log(TraceRecvd, "P%d ← P%d: %v", msg.dest, msg.source, msg.payload)
		gmsg := msg.payload.(gpbft.GMessage)
		if err := n.participants[msg.dest].ReceiveMessage(&gmsg); err != nil {
			return false, fmt.Errorf("error receiving message: %w", err)
		}
	}
	return len(n.queue) > 0, nil
}

func (n *Network) log(level int, format string, args ...interface{}) {
	if level <= n.traceLevel {
		fmt.Printf("net [%.3f]: ", n.clock.Sub(time.Time{}).Seconds())
		fmt.Printf(format, args...)
		fmt.Printf("\n")
	}
}

type messageInFlight struct {
	source    gpbft.ActorID  // ID of the sender
	dest      gpbft.ActorID  // ID of the receiver
	payload   interface{} // Message body
	deliverAt time.Time   // Timestamp at which to deliver the message
}

// A queue of directed messages, maintained as an ordered list.
type messageQueue []messageInFlight

func (h *messageQueue) Insert(x messageInFlight) {
	i := sort.Search(len(*h), func(i int) bool {
		ith := (*h)[i].deliverAt
		return ith.After(x.deliverAt) || ith.Equal(x.deliverAt)
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
