package test

import "github.com/anorth/f3sim/granite"

// Configuration constants used across most tests.
// These values are not intended to reflect real-world conditions.
// The latency and delta values are close in order to stress "slow" message paths.
const DELTA = 0.400
const DELTA_RATE = 0.100
const LATENCY_SYNC = 0
const LATENCY_ASYNC = 0.100
const MAX_ROUNDS = 10
const ASYNC_ITERS = 5000

// Returns a default Granite configuration.
func GraniteConfig() granite.Config {
	return granite.Config{
		Delta:     DELTA,
		DeltaRate: DELTA_RATE,
	}
}
