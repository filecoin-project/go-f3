package test

import (
	"fmt"
	"math"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
)

func TestDeny_SkipsToFuture(t *testing.T) {
	t.Parallel()
	const (
		instanceCount = 2000
		maxRounds     = 30
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

func TestDenyQuality(t *testing.T) {
	t.Parallel()
	const (
		instanceCount = 2000
		maxRounds     = 30
		denialTarget  = 0
		gst           = 100 * EcEpochDuration
	)

	logging.SetLogLevel("f3/gpbft", "DEBUG")
	for i := 0; i < 1; i++ {
		fmt.Printf("seed: %d\n", i)
		ecChainGenerator := sim.NewUniformECChainGenerator(uint64(i), 1, 5)
		sm, err := sim.NewSimulation(
			syncOptions(
				sim.AddHonestParticipants(4, ecChainGenerator, uniformOneStoragePower),
				sim.WithAdversary(adversary.NewDenyQualityGenerator(oneStoragePower, gst, []gpbft.ActorID{2, 3}...)),
				sim.WithGlobalStabilizationTime(gst*1000),
				sim.WithTraceLevel(10),
			)...,
		)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		//chain := ecChainGenerator.GenerateECChain(instanceCount-1, gpbft.TipSet{}, math.MaxUint64)
		//requireConsensusAtInstance(t, sm, instanceCount-1, chain...)
	}
}
