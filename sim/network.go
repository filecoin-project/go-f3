package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
)

const (
	TraceNone = iota
	TraceSent
	TraceRecvd
	TraceLogic
	TraceAll //nolint:unused
)

type Network struct {
	// Participants by ID.
	participants map[gpbft.ActorID]gpbft.Receiver
	// Participant IDs for deterministic iteration
	participantIDs []gpbft.ActorID
	// Messages received by the network but not yet delivered to all participants.
	queue   *messageQueue
	latency latency.Model
	// Timestamp of last event.
	clock time.Time
	// globalStabilisationElapsed signals whether global stabilisation time has
	// passed, beyond which messages are guaranteed to be delivered.
	globalStabilisationElapsed bool
	// Trace level.
	traceLevel  int
	networkName gpbft.NetworkName
	gst         time.Time
}

func newNetwork(opts *options) *Network {
	return &Network{
		participants: make(map[gpbft.ActorID]gpbft.Receiver),
		latency:      opts.latencyModel,
		traceLevel:   opts.traceLevel,
		networkName:  opts.networkName,
		gst:          time.Time{}.Add(opts.globalStabilizationTime),
		queue:        newMessagePriorityQueue(),
	}
}

// hasGlobalStabilizationTimeElapsed checks whether global stabilisation time has
// passed, beyond which messages are guaranteed to be delivered.
func (n *Network) hasGlobalStabilizationTimeElapsed() bool {
	return n.Time().After(n.gst)
}

func (n *Network) AddParticipant(id gpbft.ActorID, p gpbft.Receiver) {
	if n.participants[id] != nil {
		panic("duplicate participant ID")
	}
	n.participantIDs = append(n.participantIDs, id)
	n.participants[id] = p
}

////// Network interface

func (n *Network) NetworkFor(signer gpbft.SignerWithMarshaler, id gpbft.ActorID) *networkFor {
	return &networkFor{
		ParticipantID: id,
		Signer:        signer,
		Network:       n,
	}
}

type networkFor struct {
	ParticipantID gpbft.ActorID
	Signer        gpbft.SignerWithMarshaler
	*Network
}

func (nf *networkFor) Log(format string, args ...any) {
	nf.Network.log(TraceLogic, "P%d "+format, append([]any{nf.ParticipantID}, args...)...)
}

func (nf *networkFor) RequestBroadcast(mb *gpbft.MessageBuilder) error {
	msg, err := mb.Build(nf.networkName, nf.Signer, nf.ParticipantID)
	if err != nil {
		nf.Log("building message for: %d: %+v", nf.ParticipantID, err)
		return err
	}
	nf.broadcast(msg, false)
	return nil
}

func (n *Network) NetworkName() gpbft.NetworkName {
	return n.networkName
}

func (n *Network) BroadcastSynchronous(msg *gpbft.GMessage) {
	n.broadcast(msg, true)
}

func (n *Network) broadcast(msg *gpbft.GMessage, synchronous bool) {
	n.log(TraceSent, "P%d ↗ %v", msg.Sender, msg)
	for _, dest := range n.participantIDs {
		latencySample := time.Duration(0)
		if !synchronous {
			latencySample = n.latency.Sample(n.Time(), msg.Sender, dest)
		}

		n.queue.Insert(
			&messageInFlight{
				source:    msg.Sender,
				dest:      dest,
				payload:   *msg,
				deliverAt: n.clock.Add(latencySample),
			})
	}
}

func (n *Network) Time() time.Time {
	return n.clock
}

func (n *Network) SetAlarm(sender gpbft.ActorID, at time.Time) {
	// There must be at most one alarm per participant at any given point in time.
	// Update any existing alarm or insert if no such alarm exists.
	n.queue.UpsertFirstWhere(
		func(m *messageInFlight) bool {
			return m.dest == sender && m.payload == "ALARM"
		}, &messageInFlight{
			source:    sender,
			dest:      sender,
			payload:   "ALARM",
			deliverAt: at,
		},
	)
}

// Tick disseminates one message among participants and returns whether there are
// any more messages to process.
func (n *Network) Tick(adv *adversary.Adversary) (bool, error) {
	msg := n.queue.Remove()
	n.clock = msg.deliverAt

	receiver, found := n.participants[msg.dest]
	if !found {
		return false, fmt.Errorf("message destined to unknown participant ID: %d", msg.dest)
	}
	switch payload := msg.payload.(type) {
	case string:
		if payload != "ALARM" {
			return false, fmt.Errorf("unknwon string message payload: %s", payload)
		}
		n.log(TraceRecvd, "P%d %s", msg.source, payload)
		if err := receiver.ReceiveAlarm(); err != nil {
			return false, fmt.Errorf("failed to deliver alarm from %d to %d: %w", msg.source, msg.dest, err)
		}
	case gpbft.GMessage:
		// If GST has not elapsed, check if adversary allows the propagation of message.
		if adv != nil && !n.globalStabilisationElapsed {
			if n.hasGlobalStabilizationTimeElapsed() {
				n.log(TraceRecvd, "GST elapsed")
				n.globalStabilisationElapsed = true
			} else if !adv.AllowMessage(msg.source, msg.dest, payload) {
				// GST has not passed and adversary blocks the delivery of message; proceed to
				// next tick.
				return n.queue.Len() > 0, nil
			}
		}
		validated, err := receiver.ValidateMessage(&payload)
		if err != nil {
			if errors.Is(err, gpbft.ErrValidationTooOld) {
				// Silently drop old messages.
				break
			}
			return false, fmt.Errorf("invalid message from %d to %d: %w", msg.source, msg.dest, err)
		}
		n.log(TraceRecvd, "P%d ← P%d: %v", msg.dest, msg.source, msg.payload)
		if err := receiver.ReceiveMessage(validated); err != nil {
			return false, fmt.Errorf("failed to deliver message from %d to %d: %w", msg.source, msg.dest, err)
		}
	default:
		return false, fmt.Errorf("unknown message payload: %v", payload)
	}
	return n.queue.Len() > 0, nil
}

func (n *Network) log(level int, format string, args ...interface{}) {
	if level <= n.traceLevel {
		fmt.Printf("net [%.3f]: ", n.clock.Sub(time.Time{}).Seconds())
		fmt.Printf(format, args...)
		fmt.Printf("\n")
	}
}
