package f3

// Receives EC chain values.
type ChainReceiver interface {
	// Receives a chain appropriate for use as initial proposals for a Granite instance.
	// The chain's base is assumed to be an appropriate base for the instance.
	ReceiveCanonicalChain(chain ECChain, power PowerTable, beacon []byte) error

	// Receives a new EC chain, and notifies the current instance if it extends its current acceptable chain.
	// This modifies the set of valid values for the current instance.
	ReceiveECChain(chain ECChain) error
}

// A consensus message.
// Opaque to the network, expected to be cast by the receiver.
type Message interface{}

// Receives a Granite protocol message.
type MessageReceiver interface {
	// Receives a message from another participant.
	// No validation may be assumed to have been performed on the message.
	ReceiveMessage(msg *GMessage) error
	ReceiveAlarm(payload string) error
}

// Interface which network participants must implement.
type Receiver interface {
	ID() ActorID
	ChainReceiver
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
	SetAlarm(sender ActorID, payload string, at float64)
}

type Signer interface {
	// Signs a message for the given sender ID.
	Sign(sender ActorID, msg []byte) ([]byte, error)
	// Verifies a signature for the given sender ID.
	Verify(sender ActorID, msg, sig []byte) error
}

// AggSignatureScheme encapsulates signature aggregation functionality.
// The implementation is expected to be aware of a mapping between ActorIDs and public keys.
// A single instance of AggSignatureScheme is expected to be used for each Granite instance.
type AggSignatureScheme interface {

	// NewAggregator returns an aggregator with an "empty" aggregate signature, i.e.,
	// one to which no signatures have yet been added.
	// Signatures can be aggregated by calling the Add method of the returned aggregator.
	NewAggregator() (SigAggregator, error)

	// VerifyAggSig verifies the aggregate signature agg of message msg produced by a call to SigAggregator.Aggregate().
	// Note that the SigAggregator must have been created by an AggSignatureScheme associated with the same power table
	// as this AggSignatureScheme, otherwise the behavior of VerifyAggSig is undefined.
	// VerifyAggSig returns nil if sig is a valid aggregate signature of msg
	// (with respect to the associated power table) and a non-nil error otherwise.
	VerifyAggSig(msg []byte, sig []byte) error
}

// SigAggregator holds the intermediate state of signature aggregation.
// Use one aggregator per aggregated signature.
type SigAggregator interface {

	// Add a simple signature (produced by signer) to the aggregate.
	Add(signer ActorID, sig []byte) error

	// Aggregate aggregates all the signatures previously added using the Add method.
	// The returned byte slice is an opaque encoding of the signature itself,
	// along with references to the identities of the signers with respect to a specific power table
	// associated with instance of AggSignatureScheme that created this SigAggregator.
	// The returned data can be passed to a compatible implementation
	// of AggSignatureScheme.VerifyAggSig for verification.
	// Note that the verifying AggSignatureScheme must have been created using the same power table
	// as the BLSScheme that produced this aggregator. Otherwise, the verification result is undefined.
	Aggregate() ([]byte, error)
}

// Participant interface to the host system resources.
type Host interface {
	Network
	Clock
	Signer

	// Logs a message at the "logic" level
	Log(format string, args ...interface{})
}
