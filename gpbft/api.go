package gpbft

import "time"

// Receives EC chain values.
type ChainReceiver interface {
	// Receives a chain appropriate for use as initial proposals for a Granite instance.
	// The chain's base is assumed to be an appropriate base for the instance.
	ReceiveCanonicalChain(chain ECChain, power PowerTable, beacon []byte) error

	// Receives a new EC chain, and notifies the current instance if it extends its current acceptable chain.
	// This modifies the set of valid values for the current instance.
	ReceiveECChain(chain ECChain) error
}

// Receives a Granite protocol message.
type MessageReceiver interface {
	// Validates a message received from another participant, if possible.
	// Returns whether the message could be validated, and an error if it was invalid.
	ValidateMessage(msg *GMessage) (bool, error)
	// Receives a message from another participant.
	// The message must already have been validated.
	ReceiveMessage(msg *GMessage) (bool, error)
	ReceiveAlarm() error
}

// Interface which network participants must implement.
type Receiver interface {
	ID() ActorID
	ChainReceiver
	MessageReceiver
}

// Endpoint to which participants can send messages.
type Network interface {
	// Returns the network's name (for signature separation)
	NetworkName() NetworkName
	// Sends a message to all other participants.
	// The message's sender must be one that the network interface can sign on behalf of.
	Broadcast(msg *GMessage)
}

type Clock interface {
	// Returns the current network time.
	Time() time.Time
	// Sets an alarm to fire at the given timestamp.
	SetAlarm(sender ActorID, at time.Time)
}

type Signer interface {
	// GenerateKey is used for testing
	GenerateKey() PubKey
	// Signs a message for the given sender ID.
	Sign(sender PubKey, msg []byte) ([]byte, error)
}

type Verifier interface {
	// Verifies a signature for the given public key
	Verify(pubKey PubKey, msg, sig []byte) error
	// Aggregates signatures from a participants
	Aggregate(pubKeys []PubKey, sigs [][]byte) ([]byte, error)
	// VerifyAggregate verifies an aggregate signature.
	VerifyAggregate(payload, aggSig []byte, signers []PubKey) error
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
