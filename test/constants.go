package test

import "github.com/filecoin-project/go-f3/f3"

// Configuration constants used across most tests.
// These values are not intended to reflect real-world conditions.
// The latency and delta values are similar in order to stress "slow" message paths and interleaving.
// The values are not appropriate for benchmarks.
const DELTA = 0.400
const DELTA_EXTRA = 0.100
const DELTA_BACK_OFF_EXPONENT = 1.300
const EXTERNAL_CLOCK_RESYNC_PERIOD = 1
const LATENCY_SYNC = 0
const LATENCY_ASYNC = 0.100
const MAX_ROUNDS = 10
const ASYNC_ITERS = 5000

// Returns a default Granite configuration.
func GraniteConfig() f3.GraniteConfig {
	return f3.GraniteConfig{
		Delta:                     DELTA,
		DeltaExtra:                DELTA_EXTRA,
		ExternalClockResyncPeriod: EXTERNAL_CLOCK_RESYNC_PERIOD,
		DeltaBackOffExponent:      DELTA_BACK_OFF_EXPONENT,
	}
}
