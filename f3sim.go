package main

import (
	"flag"
	"fmt"
	"github.com/anorth/f3sim/net"
	"github.com/anorth/f3sim/sim"
	"time"
)

func main() {
	iterations := flag.Int("iterations", 1, "number of simulation iterations")
	participantCount := flag.Int("participants", 3, "number of participants")
	latencySeed := flag.Int64("latency-seed", time.Now().UnixMilli(), "random seed for network latency")
	latencyMean := flag.Float64("latency-mean", 0.500, "mean network latency")
	graniteDelta := flag.Float64("granite-delta", 3.000, "granite delta parameter")
	traceLevel := flag.Int("trace", net.TraceNone, "trace verbosity level")
	flag.Parse()

	for i := 0; i < *iterations; i++ {
		// Increment seed for successive iterations.
		seed := *latencySeed + int64(i)
		fmt.Printf("Iteration %d: seed=%d, mean=%f\n", i, seed, *latencyMean)

		cfg := sim.Config{
			HonestCount:  *participantCount,
			LatencySeed:  *latencySeed,
			LatencyMean:  *latencyMean,
			GraniteDelta: *graniteDelta,
		}
		sm := sim.NewSimulation(&cfg, *traceLevel)

		// Same chain for everyone.
		candidate := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{Count: *participantCount, Chain: *candidate})

		ok := sm.Run()
		if !ok {
			sm.PrintResults()
		}
	}
}
