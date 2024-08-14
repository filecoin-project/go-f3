package psutil

import (
	"encoding/binary"
	"time"

	"golang.org/x/crypto/blake2b"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Generate a pubsub ID from the message topic + data.
func PubsubMsgIdHashData(m *pubsub_pb.Message) string {
	hasher, err := blake2b.New256(nil)
	if err != nil {
		panic("failed to construct hasher")
	}

	topic := []byte(m.GetTopic())
	if err := binary.Write(hasher, binary.BigEndian, uint32(len(topic))); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(topic); err != nil {
		panic(err)
	}

	hash := blake2b.Sum256(m.Data)
	return string(hash[:])
}

// Generate a pubsub ID from the message topic + sender + data.
func PubsubMsgIdHashDataAndSender(m *pubsub_pb.Message) string {
	hasher, err := blake2b.New256(nil)
	if err != nil {
		panic("failed to construct hasher")
	}

	topic := []byte(m.GetTopic())
	if err := binary.Write(hasher, binary.BigEndian, uint32(len(topic))); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(topic); err != nil {
		panic(err)
	}
	if err := binary.Write(hasher, binary.BigEndian, uint32(len(m.From))); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(m.From); err != nil {
		panic(err)
	}

	hash := blake2b.Sum256(m.Data)
	return string(hash[:])
}

// Borrowed from lotus
var PubsubTopicScoreParams = &pubsub.TopicScoreParams{
	// expected > 400 msgs/second on average.
	//
	TopicWeight: 0.1, // max cap is 5, single invalid message is -100

	// 1 tick per second, maxes at 1 hour
	// XXX
	TimeInMeshWeight:  0.0002778, // ~1/3600
	TimeInMeshQuantum: time.Second,
	TimeInMeshCap:     1,

	// NOTE: Gives weight to the peer that tends to deliver first.
	// deliveries decay after 10min, cap at 100 tx
	FirstMessageDeliveriesWeight: 0.5, // max value is 50
	FirstMessageDeliveriesDecay:  pubsub.ScoreParameterDecay(10 * time.Minute),
	FirstMessageDeliveriesCap:    100, // 100 messages in 10 minutes

	// Mesh Delivery Failure is currently turned off for messages
	// This is on purpose as the network is still too small, which results in
	// asymmetries and potential unmeshing from negative scores.
	// // tracks deliveries in the last minute
	// // penalty activates at 1 min and expects 2.5 txs
	// MeshMessageDeliveriesWeight:     -16, // max penalty is -100
	// MeshMessageDeliveriesDecay:      pubsub.ScoreParameterDecay(time.Minute),
	// MeshMessageDeliveriesCap:        100, // 100 txs in a minute
	// MeshMessageDeliveriesThreshold:  2.5, // 60/12/2 txs/minute
	// MeshMessageDeliveriesWindow:     10 * time.Millisecond,
	// MeshMessageDeliveriesActivation: time.Minute,

	// // decays after 5min
	// MeshFailurePenaltyWeight: -16,
	// MeshFailurePenaltyDecay:  pubsub.ScoreParameterDecay(5 * time.Minute),

	// invalid messages decay after 1 hour
	InvalidMessageDeliveriesWeight: -1000,
	InvalidMessageDeliveriesDecay:  pubsub.ScoreParameterDecay(time.Hour),
}
