package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

///// Tests for multiple chained instances of the protocol, no adversaries.

const instanceCount = 4000

func TestMultiSingleton(t *testing.T) {
	sm, err := sim.NewSimulation(syncOptions(
		sim.AddHonestParticipants(1, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10)),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	instance := sm.GetInstance(instanceCount)
	require.NotNil(t, instance)
	expected := instance.BaseChain
	requireConsensusAtInstance(t, sm, instanceCount-1, expected.Head())
}

func TestMultiSyncPair(t *testing.T) {
	sm, err := sim.NewSimulation(syncOptions(
		sim.AddHonestParticipants(2, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10)),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	instance := sm.GetInstance(instanceCount)
	require.NotNil(t, instance)
	expected := instance.BaseChain
	requireConsensusAtInstance(t, sm, instanceCount-1, expected.Head())
}

func TestMultiASyncPair(t *testing.T) {
	sm, err := sim.NewSimulation(asyncOptions(t, 1413,
		sim.AddHonestParticipants(2, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10)),
	)...)
	require.NoError(t, err)

	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	// Note: when async, the decision is not always the latest possible value,
	// but should be something recent.
	// This expectation may need to be relaxed.
	instance := sm.GetInstance(instanceCount)
	require.NotNil(t, instance)
	expected := instance.BaseChain
	requireConsensusAtInstance(t, sm, instanceCount-1, expected...)
}

func TestMultiSyncAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()
	repeatInParallel(t, 9, func(t *testing.T, repetition int) {
		honestCount := repetition + 3
		sm, err := sim.NewSimulation(syncOptions(
			sim.AddHonestParticipants(honestCount, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10)),
		)...)
		require.NoError(t, err)
		// All nodes start with the same chain and will observe the same extensions of that chain
		// in subsequent instances.
		require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
		// Synchronous, agreeing groups always decide the candidate.
		instance := sm.GetInstance(instanceCount)
		require.NotNil(t, instance)
		expected := instance.BaseChain
		requireConsensusAtInstance(t, sm, instanceCount-1, expected...)
	})
}

func TestMultiAsyncAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()
	repeatInParallel(t, 9, func(t *testing.T, repetition int) {
		honestCount := repetition + 3
		sm, err := sim.NewSimulation(asyncOptions(t, 1414,
			sim.AddHonestParticipants(honestCount, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10)),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
		// Note: The expected decision only needs to be something recent.
		// Relax this expectation when the simEC chain is less clean.
		instance := sm.GetInstance(instanceCount)
		require.NotNil(t, instance)
		expected := instance.BaseChain
		requireConsensusAtInstance(t, sm, instanceCount-1, expected...)
	})
}
