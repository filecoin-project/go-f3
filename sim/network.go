package sim

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/f3"
	"io"
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
	}
}

func (n *Network) AddParticipant(p f3.Receiver) {
	if n.participants[p.ID()] != nil {
		panic("duplicate participant ID")
	}
	n.participantIDs = append(n.participantIDs, p.ID())
	n.participants[p.ID()] = p
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
	// Just prepends 8-byte sender ID to message.
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, sender)
	if err != nil {
		panic(err)
	}
	return append(buf.Bytes(), msg...)
}

func (n *Network) Verify(sender f3.ActorID, msg, sig []byte) bool {
	// Fake implementation.
	// Just checks that first 8 bytes of signature match sender ID,
	// and remaining bytes match message.
	buf := bytes.NewReader(sig)
	var recoveredSender uint64
	err := binary.Read(buf, binary.BigEndian, &recoveredSender)
	if err != nil {
		return false
	}
	remainingBytes := sig[8:]
	if recoveredSender != uint64(sender) {
		return false
	}
	if !bytes.Equal(remainingBytes, msg) {
		return false
	}
	return true
}

func (n *Network) Aggregate(sig []byte, actorID f3.ActorID, aggSignature []byte, signers *bitfield.BitField, actor2Index map[f3.ActorID]uint64) ([]byte, *bitfield.BitField) {
	// Fake implementation.
	// Just appends signature to aggregate signature.
	// This fake aggregation is not commutative (order matters)
	// But determinism is preserved by sorting by weight
	// (That contains the sender ID in the signature)
	if _, ok := actor2Index[actorID]; !ok {
		panic("actorID not part of power table")
	}

	// Extract existing signatures along with their actorIDs
	signatures := [][]byte{}
	buf := bytes.NewReader(aggSignature)
	existingSigLen := len(sig)
	for {
		// The length of each existing signature (minus 8 bytes for the actorID)
		existingSig := make([]byte, existingSigLen)
		if _, err := io.ReadFull(buf, existingSig); err != nil {
			if err == io.EOF {
				break // End of the aggregate signature.
			} else if err != nil {
				panic(err) // Error in reading the signature.
			}
		}
		signatures = append(signatures, existingSig)
	}
	signatures = append(signatures, sig) // Append the new signature

	// Sort the signatures based on descending order of actorID's index
	sort.Slice(signatures, func(i, j int) bool {
		actorIDI := binary.BigEndian.Uint64(signatures[i][:8])
		actorIDJ := binary.BigEndian.Uint64(signatures[j][:8])
		return actor2Index[f3.ActorID(actorIDI)] > actor2Index[f3.ActorID(actorIDJ)]
	})

	// Reconstruct the aggregated signature in sorted order
	var updatedAggSignature []byte
	for _, s := range signatures {
		updatedAggSignature = append(updatedAggSignature, s...)
	}

	signers.Set(actor2Index[actorID])

	return updatedAggSignature, signers
}

func (n *Network) VerifyAggregate(msg, aggSig []byte, signers *bitfield.BitField, actor2Index map[f3.ActorID]uint64) bool {
	aggBuf := bytes.NewReader(aggSig)

	verifiedSigners := bitfield.New()
	// Calculate the expected length of each individual signature
	signatureLength := 8 + len(msg) // 8 bytes for sender ID + length of message

	lastActorIndex := uint64(len(actor2Index))
	for {
		// Read the signature corresponding to this sender ID.
		signature := make([]byte, signatureLength)
		if _, err := io.ReadFull(aggBuf, signature); err != nil {
			if err == io.EOF {
				break // End of the aggregate signature.
			} else if err != nil {
				return false // Error in reading the signature.
			}
		}

		buf := bytes.NewReader(signature)

		var senderID uint64
		err := binary.Read(buf, binary.BigEndian, &senderID)
		if err == io.EOF {
			break // End of the aggregate signature.
		} else if err != nil {
			return false // Error in reading sender ID.
		}

		actorID := f3.ActorID(senderID)
		currentActorIndex, ok := actor2Index[actorID]
		if !ok || currentActorIndex >= lastActorIndex {
			return false // ActorID index is not in the correct descending order.
		}
		lastActorIndex = currentActorIndex

		// Verify the signature.
		if !n.Verify(actorID, msg, signature) {
			return false // Signature verification failed.
		}
		verifiedSigners.Set(currentActorIndex)
	}

	// Ensure all signers in the bitset are accounted for.
	verifiedCount, err := verifiedSigners.Count()
	if err != nil {
		panic(err)
	}
	signersCount, err := signers.Count()
	if err != nil {
		panic(err)
	}
	return verifiedCount == signersCount
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
		gmsg := msg.payload.(f3.GMessage)
		n.participants[msg.dest].ReceiveMessage(&gmsg)
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
