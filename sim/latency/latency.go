package latency

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

// Model represents a latency model of cross participant communication. The model
// offers the ability for implementation of varying latency across a simulation,
// as well as specialised latency across specific participants.
//
// See LogNormal, Zipf, None.
type Model interface {
	// Sample returns an artificial latency at time t for communications from a
	// participant to another participant.
	//
	// See: gpbft.Host, gpbft.Clock.
	Sample(t time.Time, from, to gpbft.ActorID) time.Duration
}
