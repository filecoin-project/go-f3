package test

import (
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"time"
)

// Configuration constants used across most tests.
// These values are not intended to reflect real-world conditions.
// The latency and delta values are similar in order to stress "slow" message paths and interleaving.
// The values are not appropriate for benchmarks.
// Granite configuration.
const DELTA = 200 * time.Millisecond
const DELTA_BACK_OFF_EXPONENT = 1.300

// Returns a default Granite configuration.
func GraniteConfig() gpbft.GraniteConfig {
	return gpbft.GraniteConfig{
		Delta:                DELTA,
		DeltaBackOffExponent: DELTA_BACK_OFF_EXPONENT,
	}
}

// Simulator configuration.
const LATENCY_SYNC = 0
const LATENCY_ASYNC = 100 * time.Millisecond
const MAX_ROUNDS = 10
const ASYNC_ITERS = 5000
const EC_EPOCH_DURATION = 30 * time.Second
const EC_STABILISATION_DELAY = 3 * time.Second

func SyncConfig(honestCount int) sim.Config {
	return sim.Config{
		HonestCount:          honestCount,
		LatencySeed:          0,
		LatencyMean:          LATENCY_SYNC,
		ECEpochDuration:      EC_EPOCH_DURATION,
		ECStabilisationDelay: EC_STABILISATION_DELAY,
	}
}

func AsyncConfig(honestCount, latencySeed int) sim.Config {
	return sim.Config{
		HonestCount:          honestCount,
		LatencySeed:          int64(latencySeed),
		LatencyMean:          LATENCY_ASYNC,
		ECEpochDuration:      EC_EPOCH_DURATION,
		ECStabilisationDelay: EC_STABILISATION_DELAY,
	}
}
