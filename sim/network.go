package sim

import (
	"bytes"
	"fmt"
	"github.com/filecoin-project/go-f3/f3"
	"io"
<<<<<<< HEAD
<<<<<<< HEAD
=======
	"math"
>>>>>>> 2bf4739 (Implement Aggregator interface for Network)
=======
	"reflect"
>>>>>>> 6ee56ae (Ensure signing and verifying modifies no input)
	"sort"
	"strings"
)

type AdversaryReceiver interface {
	f3.Receiver
	AllowMessage(from f3.ActorID, to f3.ActorID, msg f3.Message) bool
}

// Endpoint with which the adversary can control the network
type AdversaryHost interface {
	f3.Host
	// Sends a message to all other participants, immediately.
	// Note that the adversary can subsequently delay delivery to some participants,
	// before messages are actually received.
	BroadcastSynchronous(sender f3.ActorID, msg f3.Message)
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
	// Participants by ID.
	participants map[f3.ActorID]f3.Receiver
	// Participant IDs for deterministic iteration
	participantIDs []f3.ActorID
	// Messages received by the network but not yet delivered to all participants.
	queue   messageQueue
	latency LatencyModel
	// Timestamp of last event.
	clock float64
	// Whether global stabilisation time has passed, so adversary can't control network.
	globalStabilisationElapsed bool
	// Trace level.
	traceLevel int
<<<<<<< HEAD

=======
	//TODO
>>>>>>> 6ee56ae (Ensure signing and verifying modifies no input)
	actor2PubKey map[f3.ActorID]f3.PubKey
}

func NewNetwork(latency LatencyModel, traceLevel int) *Network {
	return &Network{
		participants:               map[f3.ActorID]f3.Receiver{},
		participantIDs:             []f3.ActorID{},
		queue:                      messageQueue{},
		clock:                      0,
		latency:                    latency,
		globalStabilisationElapsed: false,
		traceLevel:                 traceLevel,
		actor2PubKey:               map[f3.ActorID]f3.PubKey{},
	}
}

func (n *Network) AddParticipant(p f3.Receiver, pubKey f3.PubKey) {
	if n.participants[p.ID()] != nil {
		panic("duplicate participant ID")
	}
	n.participantIDs = append(n.participantIDs, p.ID())
	n.participants[p.ID()] = p
	n.actor2PubKey[p.ID()] = pubKey
}

////// Network interface

func (n *Network) Broadcast(msg *f3.GMessage) {
	n.log(TraceSent, "P%d ↗ %v", msg.Sender, msg)
	for _, k := range n.participantIDs {
		if k != msg.Sender {
			latency := n.latency.Sample()
			n.queue.Insert(
				messageInFlight{
					source:    msg.Sender,
					dest:      k,
					payload:   *msg,
					deliverAt: n.clock + latency,
				})
		}
	}
}

///// Clock interface

func (n *Network) Time() float64 {
	return n.clock
}

func (n *Network) SetAlarm(sender f3.ActorID, payload string, at float64) {
	n.queue.Insert(messageInFlight{
		source:    sender,
		dest:      sender,
		payload:   "ALARM:" + payload,
		deliverAt: at,
	})
}

///// Signer interface

func (n *Network) Sign(sender f3.ActorID, msg []byte) []byte {
	// Fake implementation.
	// Just prepends the pubkey associated with the sender ID to message.
<<<<<<< HEAD
	aux := append([]byte{}, n.actor2PubKey[sender]...)
=======
	aux := append([]byte(nil), n.actor2PubKey[sender]...)
>>>>>>> 6ee56ae (Ensure signing and verifying modifies no input)
	return append(aux, msg...)
}

func (n *Network) Verify(pubKey f3.PubKey, msg, sig []byte) bool {
	// Fake implementation.
	// Just checks that first bytes of the signature match sender ID,
	// and remaining bytes match message.
<<<<<<< HEAD
	aux := append([]byte{}, pubKey...)
	aux = append(aux, msg...)
	return bytes.Equal(aux, sig)
}

func (n *Network) Aggregate(sigs [][]byte, aggSignature []byte) []byte {
	// Fake implementation.
	// Just appends signature to aggregate signature.
	// This fake aggregation is not commutative (order matters)
	// But determinism is preserved by sorting by weight both here and in VerifyAggregate
	// (That contains the sender ID in the signature)

	// Sort the pubKeys based on descending order of actorID
	// take any pubkey from the actor2pubkey
	var pubKeyLen int
	for _, pubKey := range n.actor2PubKey {
		pubKeyLen = len(pubKey)
		break
	}

	msg := sigs[0][pubKeyLen:]
	msgLen := len(msg)

	// Extract existing pubKeys along with their actorIDs
	pubKeys := [][]byte{}
	if len(aggSignature) > 0 {
		buf := bytes.NewReader(aggSignature[msgLen:])
		for {
			existingPubKey := make([]byte, pubKeyLen)
			if _, err := io.ReadFull(buf, existingPubKey); err != nil {
				if err == io.EOF {
					break // End of the aggregate signature.
				} else if err != nil {
					panic(err) // Error in reading the signature.
				}
			}
			pubKeys = append(pubKeys, existingPubKey)
		}
	}

	for i := 0; i < len(sigs); i++ {
		if !bytes.Equal(msg, sigs[i][pubKeyLen:]) {
			panic("Current mismatch")
		}
		pubKeys = append(pubKeys, sigs[i][:pubKeyLen])
	}

	sort.Slice(pubKeys, func(i, j int) bool {
		return bytes.Compare(pubKeys[i], pubKeys[j]) > 0
	})

	for i := 0; i < len(pubKeys)-1; i++ {
		if bytes.Equal(pubKeys[i], pubKeys[i+1]) {
			panic("Duplicate pubkeys")
		}
	}

	// Reconstruct the aggregated signature in sorted order
	updatedAggSignature := append([]byte{}, msg...)
	for _, s := range pubKeys {
		updatedAggSignature = append(updatedAggSignature, s...)
	}

	return updatedAggSignature
}

func (n *Network) VerifyAggregate(payload, aggSig []byte, signers []f3.PubKey) bool {
	sort.Slice(signers, func(i, j int) bool {
		return bytes.Compare(signers[i], signers[j]) > 0
	})

	signersConcat := make([]byte, 0)
	for _, signer := range signers {
		signersConcat = append(signersConcat, signer...)
	}

	aux := append([]byte{}, payload...)
	aux = append(aux, signersConcat...)
	return bytes.Equal(aux, aggSig)
=======
	aux := append([]byte(nil), pubKey...)
	aux = append(aux, msg...)
	return bytes.Equal(aux, sig)
>>>>>>> 6ee56ae (Ensure signing and verifying modifies no input)
}

func (n *Network) Aggregate(sigs [][]byte, aggSignature []byte) []byte {
	// Fake implementation.
	// Just appends signature to aggregate signature.
	// This fake aggregation is not commutative (order matters)
	// But determinism is preserved by sorting by weight both here and in VerifyAggregate
	// (That contains the sender ID in the signature)

	// Sort the pubKeys based on descending order of actorID
	// take any pubkey from the actor2pubkey
	var pubKeyLen int
	for _, pubKey := range n.actor2PubKey {
		pubKeyLen = len(pubKey)
		break
	}

	msg := sigs[0][pubKeyLen:]
	msgLen := len(msg)

	// Extract existing pubKeys along with their actorIDs
	pubKeys := [][]byte{}
	if len(aggSignature) > 0 {
		buf := bytes.NewReader(aggSignature[msgLen:])
		for {
			existingPubKey := make([]byte, pubKeyLen)
			if _, err := io.ReadFull(buf, existingPubKey); err != nil {
				if err == io.EOF {
					break // End of the aggregate signature.
				} else if err != nil {
					panic(err) // Error in reading the signature.
				}
			}
			pubKeys = append(pubKeys, existingPubKey)
		}
	}

	for i := 0; i < len(sigs); i++ {
		if !reflect.DeepEqual(msg, sigs[i][pubKeyLen:]) {
			panic("Payload mismatch")
		}
		pubKeys = append(pubKeys, sigs[i][:pubKeyLen])
	}

	sort.Slice(pubKeys, func(i, j int) bool {
		return bytes.Compare(pubKeys[i], pubKeys[j]) > 0
	})

	for i := 0; i < len(pubKeys)-1; i++ {
		if bytes.Equal(pubKeys[i], pubKeys[i+1]) {
			panic("Duplicate pubkeys")
		}
	}

	// Reconstruct the aggregated signature in sorted order
	updatedAggSignature := append([]byte(nil), msg...)
	for _, s := range pubKeys {
		updatedAggSignature = append(updatedAggSignature, s...)
	}

	return updatedAggSignature
}

func (n *Network) VerifyAggregate(payload, aggSig []byte, signers []f3.PubKey) bool {
	sort.Slice(signers, func(i, j int) bool {
		return bytes.Compare(signers[i], signers[j]) > 0
	})

	signersConcat := make([]byte, 0)
	for _, signer := range signers {
		signersConcat = append(signersConcat, signer...)
	}

	aux := append([]byte(nil), payload...)
	aux = append(aux, signersConcat...)
	return bytes.Equal(aux, aggSig)
}

func (n *Network) Log(format string, args ...interface{}) {
	n.log(TraceLogic, format, args...)
}

///// Adversary network interface

func (n *Network) BroadcastSynchronous(sender f3.ActorID, msg f3.Message) {
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
	if ok && strings.HasPrefix(payloadStr, "ALARM:") {
		n.log(TraceRecvd, "P%d %s", msg.source, payloadStr)
		if err := n.participants[msg.dest].ReceiveAlarm(strings.TrimPrefix(payloadStr, "ALARM:")); err != nil {
			return false, fmt.Errorf("failed receiving alarm: %w", err)
		}
	} else {
		n.log(TraceRecvd, "P%d ← P%d: %v", msg.dest, msg.source, msg.payload)
		gmsg := msg.payload.(f3.GMessage)
		if err := n.participants[msg.dest].ReceiveMessage(&gmsg); err != nil {
			return false, fmt.Errorf("error receiving message: %w", err)
		}
	}
	return len(n.queue) > 0, nil
}

func (n *Network) log(level int, format string, args ...interface{}) {
	if level <= n.traceLevel {
		fmt.Printf("net [%.3f]: ", n.clock)
		fmt.Printf(format, args...)
		fmt.Printf("\n")
	}
}

type messageInFlight struct {
	source    f3.ActorID  // ID of the sender
	dest      f3.ActorID  // ID of the receiver
	payload   interface{} // Message body
	deliverAt float64     // Timestamp at which to deliver the message
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
