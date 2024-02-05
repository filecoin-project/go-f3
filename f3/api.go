package f3

// Receives EC chain values, notifies Lotus of newly finalised chains, and receives power tables
type Finaliser interface {
	// Receives a chain appropriate for use as initial proposals for a Granite instance.
	// The chain's base is assumed to be an appropriate base for the instance.
	// The beacons are the beacon values for the chain.
	ReceiveCanonicalChain(chain ECChain, beacons [][]byte) error

	// Sends a new finalised chain to the host.
	// This call acts a request for Lotus to send an updated power table
	// Lotus will always send a new chain if the head changes so no need to make this call a request for that
	SendNewFinalisedChain() error

	// Receives a power table from the given participant.
	ReceivePowerTable(pt PowerTable, baseID TipSetID) error
}

// A consensus message.
// Opaque to the network, expected to be cast by the receiver.
type Message interface{}

// Receives a Granite protocol message.
type MessageReceiver interface {
	// Receives a message from another participant.
	// No validation may be assumed to have been performed on the message.
	ReceiveMessage(msg *GMessage) error
	ReceiveAlarm(amsg *AlarmMsg) error
}

// Interface which network participants must implement.
type Receiver interface {
	ID() ActorID
	Finaliser
	MessageReceiver
}

// Endpoint to which participants can send messages.
type Network interface {
	// Sends a message to all other participants.
	// The message's sender must be one that the network interface can sign on behalf of.
	Broadcast(msg *GMessage)
}

type Clock interface {
	// Returns the current network time.
	Time() float64
	// Sets an alarm to fire at the given timestamp.
	SetAlarm(sender ActorID, instanceID uint64, payload string, at float64)
}

type Signer interface {
	// Signs a message for the given sender ID.
	Sign(sender ActorID, msg []byte) []byte
}

type Verifier interface {
	// Verifies a signature for the given sender ID.
	Verify(pubKey PubKey, msg, sig []byte) bool
	// Aggregates signatures from a participant to an existing signature (or nil).
	Aggregate(sig [][]byte, aggSignature []byte) []byte
	// VerifyAggregate verifies an aggregate signature.
	VerifyAggregate(payload, aggSig []byte, signers []PubKey) bool
}

// Participant interface to the host system resources.
type Host interface {
	Network
	Clock
	Signer
	Verifier
	// Logs a message at the "logic" level
	Log(format string, args ...interface{})
}
