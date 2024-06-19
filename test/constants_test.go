package test

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/latency"
)

const (
	// tipSetGeneratorSeed is a test random seed from Drand.
	tipSetGeneratorSeed = 0x264803e715714f95

	latencyAsync         = 100 * time.Millisecond
	maxRounds            = 10
	EcEpochDuration      = 30 * time.Second
	EcStabilisationDelay = 3 * time.Second
)

var (
	oneStoragePower        = gpbft.NewStoragePower(1)
	uniformOneStoragePower = sim.UniformStoragePower(oneStoragePower)

	// testGpbftOptions is configuration constants used across most tests.
	// These values are not intended to reflect real-world conditions.
	// The latency and delta values are similar in order to stress "slow" message paths and interleaving.
	// The values are not appropriate for benchmarks.
	testGpbftOptions = []gpbft.Option{
		gpbft.WithDelta(200 * time.Millisecond),
		gpbft.WithDeltaBackOffExponent(1.300),
		gpbft.WithRebroadcastBackoff(1.3, time.Second, 5*time.Second),
	}
)

func syncOptions(o ...sim.Option) []sim.Option {
	return append(o,
		sim.WithLatencyModeler(func() (latency.Model, error) { return latency.None, nil }),
		sim.WithECEpochDuration(EcEpochDuration),
		sim.WitECStabilisationDelay(EcStabilisationDelay),
		sim.WithGpbftOptions(testGpbftOptions...),
	)
}

func asyncOptions(latencySeed int, o ...sim.Option) []sim.Option {
	return append(o,
		sim.WithLatencyModeler(func() (latency.Model, error) {
			return latency.NewLogNormal(int64(latencySeed), latencyAsync), nil
		}),
		sim.WithECEpochDuration(EcEpochDuration),
		sim.WitECStabilisationDelay(EcStabilisationDelay),
		sim.WithGpbftOptions(testGpbftOptions...),
	)
}
