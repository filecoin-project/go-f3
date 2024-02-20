package test

import (
	"github.com/filecoin-project/go-f3/f3"
	"time"
)

// Configuration constants used across most tests.
// These values are not intended to reflect real-world conditions.
// The latency and delta values are similar in order to stress "slow" message paths and interleaving.
// The values are not appropriate for benchmarks.
const DELTA = 400 * time.Millisecond
const DELTA_BACK_OFF_EXPONENT = 1.300

const LATENCY_SYNC = 0
const LATENCY_ASYNC = 100 * time.Millisecond
const MAX_ROUNDS = 10
const ASYNC_ITERS = 5000

// Returns a default Granite configuration.
func GraniteConfig() f3.GraniteConfig {
	return f3.GraniteConfig{
		Delta:                DELTA,
		DeltaBackOffExponent: DELTA_BACK_OFF_EXPONENT,
	}
}
