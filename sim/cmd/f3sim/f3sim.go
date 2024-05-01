package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/latency"
	"github.com/filecoin-project/go-f3/sim/signing"
)

func main() {
	iterations := flag.Int("iterations", 1, "number of simulation iterations")
	participantCount := flag.Int("participants", 3, "number of participants")
	latencySeed := flag.Int64("latency-seed", time.Now().UnixMilli(), "random seed for network latency")
	latencyMean := flag.Float64("latency-mean", 0.500, "mean network latency in seconds")
	maxRounds := flag.Uint64("max-rounds", 10, "max rounds to allow before failing")
	traceLevel := flag.Int("trace", sim.TraceNone, "trace verbosity level")

	delta := flag.Duration("delta", 2*time.Second, "bound on message delay")
	deltaBackOffExponent := flag.Float64("delta-back-off-exponent", 1.300, "exponential factor adjusting the delta value per round")
	flag.Parse()

	for i := 0; i < *iterations; i++ {
		// Increment seed for successive iterations.
		seed := *latencySeed + int64(i)
		fmt.Printf("Iteration %d: seed=%d, mean=%f\n", i, seed, *latencyMean)

		latencyModel, err := latency.NewLogNormal(*latencySeed, time.Duration(*latencyMean*float64(time.Second)))
		if err != nil {
			log.Panicf("failed to instantiate log normal latency model: %c\n", err)
		}

		tsg := sim.NewTipSetGenerator(uint64(seed))
		baseChain, err := gpbft.NewChain(tsg.Sample())
		if err != nil {
			log.Fatalf("failed to generate base chain: %v\n", err)
		}

		options := []sim.Option{
			sim.WithHonestParticipantCount(*participantCount),
			sim.WithLatencyModel(latencyModel),
			sim.WithECEpochDuration(30 * time.Second),
			sim.WithECStabilisationDelay(0),
			sim.WithTipSetGenerator(tsg),
			sim.WithBaseChain(&baseChain),
			sim.WithTraceLevel(*traceLevel),
			sim.WithGpbftOptions(
				gpbft.WithDelta(*delta),
				gpbft.WithDeltaBackOffExponent(*deltaBackOffExponent),
			),
		}

		if os.Getenv("F3_TEST_USE_BLS") == "1" {
			options = append(options, sim.WithSigningBackend(signing.NewBLSBackend()))
		}

		sm, err := sim.NewSimulation(options...)
		if err != nil {
			return
		}
		if err != nil {
			log.Panicf("failed to instantiate simulation: %v\n", err)
		}

		// Same chain for everyone.
		candidate := baseChain.Extend(tsg.Sample())
		sm.SetChains(sim.ChainCount{Count: *participantCount, Chain: candidate})

		if err := sm.Run(1, *maxRounds); err != nil {
			sm.PrintResults()
			os.Exit(1)
		}
	}
}
