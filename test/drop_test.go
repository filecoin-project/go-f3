package test

import (
	"fmt"
	"math"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestDrop_ReachesConsensusDespiteMessageLoss(t *testing.T) {
	SkipInRaceMode(t)
	t.Parallel()
	const (
		instanceCount       = 5000
		gst                 = 1000 * EcEpochDuration
		dropAdversaryTarget = 0
	)
	messageLossProbabilities := []float64{0.01, 0.05, 0.1, 0.2, 0.5, 0.8, 1.0}
	tests := []struct {
		name    string
		options []sim.Option
	}{
		{
			name:    "sync",
			options: syncOptions(),
		},
		{
			name:    "async",
			options: asyncOptions(-9856210),
		},
	}
	for _, test := range tests {
		for _, lossProbability := range messageLossProbabilities {
			name := fmt.Sprintf("%s %.0f%% loss", test.name, lossProbability*100)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				ecChainGenerator := sim.NewUniformECChainGenerator(54445, 1, 1)
				var opts []sim.Option
				opts = append(opts, test.options...)
				opts = append(opts,
					sim.AddHonestParticipants(5, ecChainGenerator, uniformOneStoragePower),
					sim.WithAdversary(adversary.NewDropGenerator(oneStoragePower, 25, lossProbability, gst, dropAdversaryTarget)),
					sim.WithGlobalStabilizationTime(gst),
					sim.WithIgnoreConsensusFor(dropAdversaryTarget))
				sm, err := sim.NewSimulation(opts...)
				require.NoError(t, err)
				require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
				chain := ecChainGenerator.GenerateECChain(instanceCount-1, gpbft.TipSet{}, math.MaxUint64)
				requireConsensusAtInstance(t, sm, instanceCount-1, *chain.Head())
			})
		}
	}
}
