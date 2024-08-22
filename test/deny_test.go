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
		instanceCount = 20
		maxRounds     = 30
		gst           = 10 * EcEpochDuration
		participants  = 50
	)

	ecGen := sim.NewUniformECChainGenerator(4332432, 1, 5)

	attacked := []gpbft.ActorID{}
	for i := gpbft.ActorID(1); i <= participants; i++ {
		attacked = append(attacked, i)
	}

	sm, err := sim.NewSimulation(
		syncOptions(
			sim.AddHonestParticipants(1, ecGen, sim.UniformStoragePower(gpbft.NewStoragePower(100*participants))),
			sim.AddHonestParticipants(participants, ecGen, sim.UniformStoragePower(gpbft.NewStoragePower(100))),
			sim.WithAdversary(adversary.NewDenyPhaseGenerator(gpbft.NewStoragePower(1), gst, gpbft.QUALITY_PHASE, attacked...)),
			sim.WithTraceLevel(1),
			sim.WithGlobalStabilizationTime(gst),
		)...,
	)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	chain := ecGen.GenerateECChain(instanceCount-1, gpbft.TipSet{}, math.MaxUint64)
	requireConsensusAtInstance(t, sm, instanceCount-1, chain...)
}
