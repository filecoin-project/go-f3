package test

import (
	"math"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestDeny_SkipsToFuture(t *testing.T) {
	t.Parallel()
	const (
		instanceCount = 2000
		maxRounds     = 20
		denialTarget  = 0
		gst           = 100 * EcEpochDuration
	)

	ecChainGenerator := sim.NewUniformECChainGenerator(54445, 1, 5)
	sm, err := sim.NewSimulation(
		asyncOptions(2342342,
			sim.AddHonestParticipants(6, ecChainGenerator, uniformOneStoragePower),
			sim.WithAdversary(adversary.NewDenyGenerator(oneStoragePower, gst, denialTarget)),
			sim.WithGlobalStabilizationTime(gst),
			sim.WithIgnoreConsensusFor(denialTarget),
		)...,
	)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	chain := ecChainGenerator.GenerateECChain(instanceCount-1, gpbft.TipSet{}, math.MaxUint64)
	requireConsensusAtInstance(t, sm, instanceCount-1, chain...)
}
