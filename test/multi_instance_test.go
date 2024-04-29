package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

///// Tests for multiple chained instances of the protocol, no adversaries.

const instanceCount = 4000

func TestMultiSingleton(t *testing.T) {
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	sm, err := sim.NewSimulation(syncOptions(
		sim.WithHonestParticipantCount(1),
		sim.WithTipSetGenerator(tsg),
		sim.WithBaseChain(&baseChain))...)
	require.NoError(t, err)
	a := baseChain.Extend(tsg.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a})

	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	instance := sm.GetInstance(instanceCount)
	require.NotNil(t, instance)
	expected := instance.Base
	requireConsensusAtInstance(t, sm, instanceCount-1, expected.Head())
}

func TestMultiSyncPair(t *testing.T) {
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	sm, err := sim.NewSimulation(syncOptions(
		sim.WithHonestParticipantCount(2),
		sim.WithTipSetGenerator(tsg),
		sim.WithBaseChain(&baseChain))...)
	require.NoError(t, err)
	a := baseChain.Extend(tsg.Sample())
	sm.SetChains(sim.ChainCount{Count: sm.HonestParticipantsCount(), Chain: a})

	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	expected := sm.GetInstance(instanceCount).Base
	requireConsensusAtInstance(t, sm, instanceCount-1, expected.Head())
}

func TestMultiASyncPair(t *testing.T) {
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	sm, err := sim.NewSimulation(asyncOptions(t, 1413,
		sim.WithHonestParticipantCount(2),
		sim.WithTipSetGenerator(tsg),
		sim.WithBaseChain(&baseChain))...)
	require.NoError(t, err)
	a := baseChain.Extend(tsg.Sample())
	sm.SetChains(sim.ChainCount{Count: sm.HonestParticipantsCount(), Chain: a})

	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	// Note: when async, the decision is not always the latest possible value,
	// but should be something recent.
	// This expectation may need to be relaxed.
	expected := sm.GetInstance(instanceCount).Base
	requireConsensusAtInstance(t, sm, instanceCount-1, expected...)
}

func TestMultiSyncAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()
	repeatInParallel(t, 9, func(t *testing.T, repetition int) {
		honestCount := repetition + 3
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		sm, err := sim.NewSimulation(syncOptions(
			sim.WithHonestParticipantCount(honestCount),
			sim.WithTipSetGenerator(tsg),
			sim.WithBaseChain(&baseChain))...)
		require.NoError(t, err)
		a := baseChain.Extend(tsg.Sample())
		// All nodes start with the same chain and will observe the same extensions of that chain
		// in subsequent instances.
		sm.SetChains(sim.ChainCount{Count: sm.HonestParticipantsCount(), Chain: a})
		require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
		// Synchronous, agreeing groups always decide the candidate.
		expected := sm.GetInstance(instanceCount).Base
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
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		sm, err := sim.NewSimulation(asyncOptions(t, 1413,
			sim.WithHonestParticipantCount(honestCount),
			sim.WithTipSetGenerator(tsg),
			sim.WithBaseChain(&baseChain))...)
		require.NoError(t, err)
		a := baseChain.Extend(tsg.Sample())
		sm.SetChains(sim.ChainCount{Count: honestCount, Chain: a})

		require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
		// Note: The expected decision only needs to be something recent.
		// Relax this expectation when the EC chain is less clean.
		expected := sm.GetInstance(instanceCount).Base
		requireConsensusAtInstance(t, sm, instanceCount-1, expected...)
	})
}
