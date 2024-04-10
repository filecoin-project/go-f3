package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
)

func main() {
	iterations := flag.Int("iterations", 1, "number of simulation iterations")
	participantCount := flag.Int("participants", 3, "number of participants")
	latencySeed := flag.Int64("latency-seed", time.Now().UnixMilli(), "random seed for network latency")
	latencyMean := flag.Float64("latency-mean", 0.500, "mean network latency in seconds")
	maxRounds := flag.Uint64("max-rounds", 10, "max rounds to allow before failing")
	traceLevel := flag.Int("trace", sim.TraceNone, "trace verbosity level")

	graniteDelta := flag.Float64("granite-delta", 2.000, "granite delta parameter (bound on message delay)")
	deltaBackOffExponent := flag.Float64("delta-back-off-exponent", 1.300, "exponential factor adjusting the delta value per round")
	flag.Parse()

	for i := 0; i < *iterations; i++ {
		// Increment seed for successive iterations.
		seed := *latencySeed + int64(i)
		fmt.Printf("Iteration %d: seed=%d, mean=%f\n", i, seed, *latencyMean)

		simConfig := sim.Config{
			HonestCount:          *participantCount,
			LatencySeed:          *latencySeed,
			LatencyMean:          time.Duration(*latencyMean * float64(time.Second)),
			ECEpochDuration:      30 * time.Second,
			ECStabilisationDelay: 0,
		}
		graniteConfig := gpbft.GraniteConfig{
			Delta:                time.Duration(*graniteDelta * float64(time.Second)),
			DeltaBackOffExponent: *deltaBackOffExponent,
		}
		sm := sim.NewSimulation(simConfig, graniteConfig, *traceLevel)

		// Same chain for everyone.
		candidate := sm.Base(0).Extend(sm.TipGen.Sample())
		sm.SetChains(sim.ChainCount{Count: *participantCount, Chain: candidate})

		err := sm.Run(1, *maxRounds)
		if err != nil {
			sm.PrintResults()
		}
	}
}
