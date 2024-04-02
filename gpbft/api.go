package gpbft

import "time"

// Receives EC chain values.
type ChainReceiver interface {
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
	// The `validated` parameter indicates whether the message has already passed validation.
	ReceiveMessage(msg *GMessage, validated bool) (bool, error)
	ReceiveAlarm() error
}

// Interface which network participants must implement.
type Receiver interface {
	ID() ActorID
	// Begins executing the protocol.
	// The node will request the canonical chain to propose from the host.
	Start() error
	ChainReceiver
	MessageReceiver
}

type Chain interface {
	// Returns the best EC chain, and the power table and beacon value for its base.
	// These will be used as input to a subsequent instance of the protocol.
	GetCanonicalChain() (chain ECChain, power PowerTable, beacon []byte)
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
	// Sets an alarm to fire after the given timestamp.
	// At most one alarm can be set at a time.
	// Setting an alarm replaces any previous alarm that has not yet fired.
	// The timestamp may be in the past, in which case the alarm will fire as soon as possible
	// (but not synchronously).
	SetAlarm(at time.Time)
}

type Signer interface {
	// GenerateKey is used for testing
	GenerateKey() PubKey
	// Signs a message with the secret key corresponding to a public key.
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

type DecisionReceiver interface {
	// Receives a finality decision from the instance, with signatures from a strong quorum
	// of participants justifying it.
	// The decision payload always has round = 0 and step = DECIDE.
	// The notification must return the timestamp at which the next instance should begin,
	// based on the decision received (which may be in the past).
	// E.g. this might be: finalised tipset timestamp + epoch duration + stabilisation delay.
	ReceiveDecision(decision *Justification) time.Time
}

// Tracer collects trace logs that capture logical state changes.
// The primary purpose of Tracer is to aid debugging and simulation.
type Tracer interface {
	Log(format string, args ...any)
}

// Participant interface to the host system resources.
type Host interface {
	Chain
	Network
	Clock
	Signer
	Verifier
	DecisionReceiver
}
