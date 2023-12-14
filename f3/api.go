package f3

// Receives EC chain values.
type ChainReceiver interface {
	// Receives a chain appropriate for use as initial proposals for a Granite instance.
	// The chain's base is assumed to be an appropriate base for the instance.
	ReceiveCanonicalChain(chain ECChain, power PowerTable, beacon []byte)
}

// A consensus message.
// Opaque to the network, expected to be cast by the receiver.
type Message interface{}

// Receives a Granite protocol message.
type MessageReceiver interface {
	ReceiveMessage(sender ActorID, msg Message)
	ReceiveAlarm(payload string)
}

// Interface which network participants must implement.
type Receiver interface {
	ID() ActorID
	ChainReceiver
	MessageReceiver
}

// Endpoint to which participants can interact with the network and context..
type Network interface {
	// Sends a message to all other participants.
	Broadcast(sender ActorID, msg Message)
	// Returns the current network time.
	Time() float64
	// Sets an alarm to fire at the given timestamp.
	SetAlarm(sender ActorID, payload string, at float64)
	// Logs a message at the "logic" level
	Log(format string, args ...interface{})
}
