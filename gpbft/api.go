package gpbft

import (
	"errors"
	"time"
)

// Sentinel errors for the message validation and reception APIs.
var (
	ErrValidationTooOld          = errors.New("message is for prior instance")
	ErrValidationNoCommittee     = errors.New("no committee for instance")
	ErrValidationInvalid         = errors.New("message invalid")
	ErrValidationWrongBase       = errors.New("unexpected base chain")
	ErrValidationWrongSupplement = errors.New("unexpected supplemental data")

	ErrReceivedWrongInstance    = errors.New("received message for wrong instance")
	ErrReceivedAfterTermination = errors.New("received message after terminating")
	ErrReceivedInternalError    = errors.New("error processing message")
)

type MessageValidator interface {
	// Validates a Granite message.
	// An invalid message can never become valid, so may be dropped.
	// Returns an error, wrapping (use errors.Is()/Unwrap()):
	// - ErrValidationTooOld if the message is for a prior instance;
	// - both ErrValidationNoCommittee and an error describing the reason;
	//   if there is no committee available with with to validate the message;
	// - both ErrValidationInvalid and a cause if the message is invalid,
	// Returns a validated message if the message is valid.
	//
	// Implementations must be safe for concurrent use.
	ValidateMessage(msg *GMessage) (valid ValidatedMessage, err error)
}

// Opaque type tagging a validated message.
type ValidatedMessage interface {
	// Returns the validated message.
	Message() *GMessage
}

// Receives a Granite protocol message.
type MessageReceiver interface {
	// Receives a validated Granite message from some other participant.
	// Returns an error, wrapping (use errors.Is()/Unwrap()):
	// - ErrValidationTooOld if the message is for a prior instance
	// - ErrValidationWrongBase if the message has an invalid base chain
	// - ErrReceivedAfterTermination if the message is received after the instance has terminated (a programming error)
	// - both ErrReceivedInternalError and a cause if there was an internal error processing the message
	// This method is not safe for concurrent use.
	ReceiveMessage(msg ValidatedMessage) error
	// This method is not safe for concurrent use.
	ReceiveAlarm() error
}

// Interface which network participants must implement.
type Receiver interface {
	// SkipToInstance jumps directly to a given instance.
	// This can be triggered by the reception of a valid finality certificate, or
	// whenever a new instance for a participant want to be started.
	SkipToInstance(uint64)
	MessageValidator
	MessageReceiver
}

type Chain interface {
	// Returns the supplemental data and the chain to propose for a new GPBFT instance.
	// The chain must be a suffix of the chain finalised by the immediately prior instance.
	// The supplemental data must be derived entirely from prior instances and all participants
	// must propose the same supplemental data.
	//
	// Returns an error if the chain for the instance is not available.
	GetProposalForInstance(instance uint64) (data *SupplementalData, chain ECChain, err error)

	// Returns the power table and beacon value to be used for a GPBFT instance.
	// These values should be derived from a chain previously received as final by the host,
	// or known to be final via some other channel (e.g. when bootstrapping the protocol).
	// The offset (how many instances to look back) is determined by the host.
	// Returns an error if the committee for the instance is not available.
	GetCommitteeForInstance(instance uint64) (power *PowerTable, beacon []byte, err error)
}

// Endpoint to which participants can send messages.
type Network interface {
	// Returns the network's name (for signature separation)
	NetworkName() NetworkName
	// Requests that the message is signed and broadcasted, it should also be delivered locally
	RequestBroadcast(mb *MessageBuilder) error
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
	// Signs a message with the secret key corresponding to a public key.
	Sign(sender PubKey, msg []byte) ([]byte, error)
}

type SigningMarshaler interface {
	// MarshalPayloadForSigning marshals the given payload into the bytes that should be signed.
	// This should usually call `Payload.MarshalForSigning(NetworkName)` except when testing as
	// that method is slow (computes a merkle tree that's necessary for testing).
	// Implementations must be safe for concurrent use.
	MarshalPayloadForSigning(NetworkName, *Payload) []byte
}

type Verifier interface {
	// Verifies a signature for the given public key.
	// Implementations must be safe for concurrent use.
	Verify(pubKey PubKey, msg, sig []byte) error
	// Aggregates signatures from a participants.
	Aggregate(pubKeys []PubKey, sigs [][]byte) ([]byte, error)
	// VerifyAggregate verifies an aggregate signature.
	// Implementations must be safe for concurrent use.
	VerifyAggregate(payload, aggSig []byte, signers []PubKey) error
}

type Signatures interface {
	SigningMarshaler
	Signer
	Verifier
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
	Signatures
	DecisionReceiver
}
