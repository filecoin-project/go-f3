package sim

import (
	"fmt"
	"strings"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
	"github.com/filecoin-project/go-f3/sim/signing"
)

const (
	TraceNone = iota
	TraceSent
	TraceRecvd
	TraceLogic
	TraceAll //nolint:unused
)

type Network struct {
	signing.Backend

	// Participants by ID.
	participants map[gpbft.ActorID]gpbft.Receiver
	// Participant IDs for deterministic iteration
	participantIDs []gpbft.ActorID
	// Messages received by the network but not yet delivered to all participants.
	queue   messageQueue
	latency latency.Model
	// Timestamp of last event.
	clock time.Time
	// Whether global stabilisation time has passed, so adversary can't control network.
	globalStabilisationElapsed bool
	// Trace level.
	traceLevel int

	actor2PubKey map[gpbft.ActorID]gpbft.PubKey

	networkName gpbft.NetworkName
}

func NewNetwork(latency latency.Model, traceLevel int, sb signing.Backend, nn gpbft.NetworkName) *Network {
	return &Network{
		Backend:                    sb,
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
	// Remove any existing alarm for the same sender.
	n.queue.RemoveWhere(func(m messageInFlight) bool {
		return m.dest == sender && m.payload == "ALARM"
	})
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

func (n *Network) BroadcastSynchronous(sender gpbft.ActorID, msg gpbft.GMessage) {
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

// Returns whether there are any more messages to process.
func (n *Network) Tick(adv adversary.Receiver) (bool, error) {
	// Find first message the adversary will allow.
	i := 0
	if adv != nil && !n.globalStabilisationElapsed {
		for ; i < len(n.queue); i++ {
			msg := n.queue[i]
			gmsg, ok := msg.payload.(gpbft.GMessage)
			if !ok || adv.AllowMessage(msg.source, msg.dest, gmsg) {
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
	if msg.deliverAt.After(n.clock) {
		n.clock = msg.deliverAt
	}
	payloadStr, ok := msg.payload.(string)
	receiver := n.participants[msg.dest]
	if ok && strings.HasPrefix(payloadStr, "ALARM") {
		n.log(TraceRecvd, "P%d %s", msg.source, payloadStr)
		if err := receiver.ReceiveAlarm(); err != nil {
			return false, fmt.Errorf("failed receiving alarm: %w", err)
		}
	} else {
		gmsg := msg.payload.(gpbft.GMessage)
		validated, err := receiver.ValidateMessage(&gmsg)
		if err != nil {
			return false, fmt.Errorf("invalid message: %w", err)
		}
		n.log(TraceRecvd, "P%d ← P%d: %v", msg.dest, msg.source, msg.payload)
		if _, err := receiver.ReceiveMessage(&gmsg, validated); err != nil {
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
