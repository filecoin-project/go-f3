package test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
	"github.com/stretchr/testify/require"
)

func TestSpamAdversary(t *testing.T) {
	t.Parallel()
	const (
		instanceCount = 2000
		maxRounds     = 30
	)
	honestCounts := []int{3, 4, 5, 6, 7, 8, 9}
	tests := []struct {
		name               string
		maxLookaheadRounds uint64
		spamAheadRounds    uint64
	}{
		{
			name:            "zero lookahead 10x spam",
			spamAheadRounds: 10,
		},
		{
			name:               "10 lookahead 3x spam",
			maxLookaheadRounds: 10,
			spamAheadRounds:    30,
		},
	}
	for _, test := range tests {
		for _, hc := range honestCounts {
			test := test
			hc := hc
			lessThanOneThirdAdversaryStoragePower := gpbft.NewStoragePower(int64(max(hc/3-1, 1)))
			name := fmt.Sprintf("%s honest count %d", test.name, hc)
			t.Run(name, func(t *testing.T) {
				ecChainGenerator := sim.NewUniformECChainGenerator(651651, 1, 10)
				sm, err := sim.NewSimulation(
					sim.WithLatencyModeler(func() (latency.Model, error) {
						return latency.NewLogNormal(455454, time.Second), nil
					}),
					sim.WithECEpochDuration(EcEpochDuration),
					sim.WitECStabilisationDelay(EcStabilisationDelay),
					sim.WithGpbftOptions(testGpbftOptions...),
					sim.AddHonestParticipants(hc, ecChainGenerator, uniformOneStoragePower),
					sim.WithGpbftOptions(gpbft.WithMaxLookaheadRounds(test.maxLookaheadRounds)),
					sim.WithAdversary(adversary.NewSpamGenerator(lessThanOneThirdAdversaryStoragePower, test.spamAheadRounds)),
				)
				require.NoError(t, err)
				require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())

				instance := sm.GetInstance(0)
				require.NotNil(t, instance, "instance 0")
				latestBaseECChain := instance.BaseChain
				for i := uint64(0); i < instanceCount; i++ {
					instance = sm.GetInstance(i + 1)
					require.NotNil(t, instance, "instance %d", i)
					wantDecision := ecChainGenerator.GenerateECChain(i, *latestBaseECChain.Head(), math.MaxUint64)

					// Sanity check that the expected decision is progressed from the base chain
					require.Equal(t, wantDecision.Base(), latestBaseECChain.Head())
					require.NotEqual(t, wantDecision.Suffix(), latestBaseECChain.Suffix())

					// Assert the consensus is reached at the head of expected chain despite the spam.
					requireConsensusAtInstance(t, sm, i, wantDecision...)
					latestBaseECChain = instance.BaseChain
				}

				// TODO: We have no good way of asserting that the spam is mitigated as DoS
				//       vector via maxLookaheadRounds. Expand assertions of this test via
				//       metrics once observability/telemetry is implemented.
			})
		}
	}
}
