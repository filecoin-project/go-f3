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

		options := []sim.Option{
			sim.WithLatencyModel(latency.NewLogNormal(*latencySeed, time.Duration(*latencyMean*float64(time.Second)))),
			sim.WithECEpochDuration(30 * time.Second),
			sim.WithECStabilisationDelay(0),
			sim.AddHonestParticipants(
				*participantCount,
				sim.NewUniformECChainGenerator(uint64(seed), 1, 10),
				sim.UniformStoragePower(gpbft.NewStoragePower(1))),
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
			log.Panicf("failed to instantiate simulation: %v\n", err)
		}

		if err := sm.Run(1, *maxRounds); err != nil {
			sm.GetInstance(0).Print()
			os.Exit(1)
		}
	}
}
