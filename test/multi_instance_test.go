package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

///// Tests for multiple chained instances of the protocol, no adversaries.

const instanceCount = 1000

func TestMultiSingleton(t *testing.T) {
	sm, err := sim.NewSimulation(syncOptions(
		sim.AddHonestParticipants(1, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10), uniformOneStoragePower),
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
		sim.AddHonestParticipants(2, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10), uniformOneStoragePower),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	instance := sm.GetInstance(instanceCount)
	require.NotNil(t, instance)
	expected := instance.BaseChain
	requireConsensusAtInstance(t, sm, instanceCount-1, expected.Head())
}

func TestMultiASyncPair(t *testing.T) {
	sm, err := sim.NewSimulation(asyncOptions(1413,
		sim.AddHonestParticipants(2, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10), uniformOneStoragePower),
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

func TestMultiAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

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
			options: asyncOptions(-52),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			multiAgreementTest(t, 4455454, 10, test.options...)
		})
	}
}

func TestMultiAsyncAgreement(t *testing.T) {
	multiAgreementTest(t, 4455454, 10, asyncOptions(-52)...)
}

func FuzzMultiSyncAgreement(f *testing.F) {
	f.Fuzz(func(t *testing.T, seed int) {
		multiAgreementTest(t, seed, 10, syncOptions()...)
	})
}

func FuzzMultiAsyncAgreement(f *testing.F) {
	f.Fuzz(func(t *testing.T, seed int) {
		multiAgreementTest(t, seed, 10, asyncOptions(seed)...)
	})
}

func multiAgreementTest(t *testing.T, seed int, honestCount int, opts ...sim.Option) {
	t.Parallel()
	sm, err := sim.NewSimulation(append(opts,
		sim.AddHonestParticipants(
			honestCount,
			sim.NewUniformECChainGenerator(uint64(seed*7), 1, 1), uniformOneStoragePower),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	// Note: The expected decision only needs to be something recent.
	// Relax this expectation when the simEC chain is less clean.
	instance := sm.GetInstance(instanceCount)
	require.NotNil(t, instance)
	expected := instance.BaseChain
	requireConsensusAtInstance(t, sm, instanceCount-1, expected...)
}
