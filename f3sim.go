package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/f3"
	"github.com/filecoin-project/go-f3/sim"
)

func main() {
	iterations := flag.Int("iterations", 1, "number of simulation iterations")
	participantCount := flag.Int("participants", 3, "number of participants")
	latencySeed := flag.Int64("latency-seed", time.Now().UnixMilli(), "random seed for network latency")
	latencyMean := flag.Float64("latency-mean", 0.500, "mean network latency")
	maxRounds := flag.Uint64("max-rounds", 10, "max rounds to allow before failing")
	traceLevel := flag.Int("trace", sim.TraceNone, "trace verbosity level")

	graniteDelta := flag.Float64("granite-delta", 2.000, "granite delta parameter")
	graniteDeltaRate := flag.Float64("granite-delta-rate", 1.300, "change in delta for each round")
	graniteDeltaExtra := flag.Float64("granite-delta-extra", 1.000, "extra delta for each round")
	clockTickDelta := flag.Float64("clock-tick-delta", 30.000, "clock tick delta in seconds")
	flag.Parse()

	for i := 0; i < *iterations; i++ {
		// Increment seed for successive iterations.
		seed := *latencySeed + int64(i)
		fmt.Printf("Iteration %d: seed=%d, mean=%f\n", i, seed, *latencyMean)

		simConfig := sim.Config{
			HonestCount: *participantCount,
			LatencySeed: *latencySeed,
			LatencyMean: *latencyMean,
		}
		graniteConfig := f3.GraniteConfig{
			Delta:          *graniteDelta,
			DeltaRate:      *graniteDeltaRate,
			DeltaExtra:     *graniteDeltaExtra,
			ClockTickDelta: *clockTickDelta,
		}
		sm := sim.NewSimulation(simConfig, graniteConfig, *traceLevel)

		// Same chain for everyone.
		candidate := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{Count: *participantCount, Chain: candidate})

		err := sm.Run(*maxRounds)
		if err != nil {
			sm.PrintResults()
		}
	}
}
