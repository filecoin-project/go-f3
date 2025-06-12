package gpbft

import (
	"context"
	"time"
)

// Instant represents a particular moment in the progress of GPBFT, captured by
// instance ID, round and phase.
type Instant struct {
	ID    uint64
	Round uint64
	Phase Phase
}

type MessageValidator interface {
	// ValidateMessage validates a GPBFT message.
	//
	// An invalid message can never become valid, which may be dropped. If a message
	// is invalid, an error of type ValidationError is returned, wrapping the cause.
	// Otherwise, returns a validated message that may be passed onto MessageReceiver
	// for processing.
	//
	// Implementations must be safe for concurrent use.
	ValidateMessage(context.Context, *GMessage) (valid ValidatedMessage, err error)
}

// Opaque type tagging a validated message.
type ValidatedMessage interface {
	// Returns the validated message.
	Message() *GMessage
}

// Receives a Granite protocol message.
// Calls to methods on this interface are expected to be serialized.
// The methods are not safe for concurrent use, and may panic if called concurrently.
type MessageReceiver interface {
	// Receives a validated Granite message from some other participant.
	// Returns an error, wrapping (use errors.Is()/Unwrap()):
	// - ErrValidationTooOld if the message is for a prior instance
	// - ErrValidationWrongBase if the message has an invalid base chain
	// - ErrReceivedAfterTermination if the message is received after the instance has terminated (a programming error)
	// - both ErrReceivedInternalError and a cause if there was an internal error processing the message
	ReceiveMessage(ctx context.Context, msg ValidatedMessage) error
	// ReceiveAlarm signals the trigger of the alarm set by Clock.SetAlarm.
	ReceiveAlarm(ctx context.Context) error
}

// Interface from host to a network participant.
// Calls to methods on this interface are expected to be serialized.
// The methods are not safe for concurrent use, and may panic if called concurrently.
type Receiver interface {
	// Begins executing the protocol from some instance.
	// The node will subsequently request the canonical chain to propose from the host.
	// If the participant is already executing some instance, it will be abandoned.
	StartInstanceAt(uint64, time.Time) error
	MessageValidator
	MessageReceiver
}

// ProposalProvider provides proposal chains for new GPBFT instances along with
// supplemental data.
type ProposalProvider interface {
	// GetProposal returns the supplemental data and the chain to propose for a new
	// GPBFT instance. The returned chain must be a suffix of the chain finalized by
	// the immediately prior instance. The supplemental data must be entirely derived
	// from prior instances, ensuring that all participants propose the same
	// supplemental data.
	//
	// Returns an error if the chain for the specified instance is not available.
	GetProposal(ctx context.Context, instance uint64) (data *SupplementalData, chain *ECChain, err error)
}

// CommitteeProvider defines an interface for retrieving committee information
// required for GPBFT instances.
type CommitteeProvider interface {
	// GetCommittee returns the power table and beacon for a given GPBFT instance.
	// These values should be derived from a chain that is finalized or known to be
	// final, with the offset determined by the host.
	//
	// Returns an error if the committee is unavailable for the specified instance.
	GetCommittee(ctx context.Context, instance uint64) (*Committee, error)
}

// Committee captures the voting power and beacon value associated to an instance
// of GPBFT.
type Committee struct {
	// PowerTable represents the Voting power distribution among committee members.
	PowerTable *PowerTable
	// Beacon is the unique beacon value associated with the committee.
	Beacon []byte
	// AggregateVerifier is used to aggregate and verify aggregate signatures made by the
	// committee.
	AggregateVerifier Aggregate
}

// Endpoint to which participants can send messages.
type Network interface {
	// Returns the network's name (for signature separation)
	NetworkName() NetworkName
	// Requests that the message is signed and broadcasted, it should also be delivered locally
	RequestBroadcast(mb *MessageBuilder) error
	// RequestRebroadcast requests that a message at given instance, round and phase
	// previously broadcasted via RequestBroadcast be rebroadcasted. Rebroadcast
	// requests for messages that have not been broadcasted are silently ignored.
	RequestRebroadcast(instant Instant) error
}

type Clock interface {
	// Returns the current network time.
	Time() time.Time
	// SetAlarm sets an alarm to fire after the given timestamp. At most one alarm
	// can be set at a time. Setting an alarm replaces any previous alarm that has
	// not yet fired. The timestamp may be in the past, in which case the alarm will
	// fire as soon as possible (but not synchronously).
	SetAlarm(at time.Time)
}

type Signer interface {
	// Signs a message with the secret key corresponding to a public key.
	Sign(ctx context.Context, sender PubKey, msg []byte) ([]byte, error)
}

type Aggregate interface {
	// Aggregates signatures from a participants.
	//
	// Implementations must be safe for concurrent use.
	Aggregate(signerMask []int, sigs [][]byte) ([]byte, error)
	// VerifyAggregate verifies an aggregate signature.
	//
	// Implementations must be safe for concurrent use.
	VerifyAggregate(signerMask []int, payload, aggSig []byte) error
}

type Verifier interface {
	// Verifies a signature for the given public key.
	//
	// Implementations must be safe for concurrent use.
	Verify(pubKey PubKey, msg, sig []byte) error
	// Return an Aggregate that can aggregate and verify aggregate signatures made by the given
	// public keys.
	//
	// Implementations must be safe for concurrent use.
	Aggregate(pubKeys []PubKey) (Aggregate, error)
}

type DecisionReceiver interface {
	// Receives a finality decision from the instance, with signatures from a strong quorum
	// of participants justifying it.
	// The decision payload always has round = 0 and phase = DECIDE.
	// The notification must return the timestamp at which the next instance should begin,
	// based on the decision received (which may be in the past).
	// E.g. this might be: finalised tipset timestamp + epoch duration + stabilisation delay.
	ReceiveDecision(ctx context.Context, decision *Justification) (time.Time, error)
}

// Tracer collects trace logs that capture logical state changes.
// The primary purpose of Tracer is to aid debugging and simulation.
type Tracer interface {
	Log(format string, args ...any)
}

// Participant interface to the host system resources.
type Host interface {
	ProposalProvider
	CommitteeProvider
	Network
	Clock
	Verifier
	DecisionReceiver
}
