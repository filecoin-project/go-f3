package test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

// TestHonestMultiInstance_Agreement tests for multiple chained instances of the protocol with no adversaries.
func TestHonestMultiInstance_Agreement(t *testing.T) {
	SkipInRaceMode(t)
	t.Parallel()
	const (
		instanceCount  = 4000
		testRNGSeed    = 8965130
		latencySeed    = testRNGSeed * 7
		maxHonestCount = 10
	)
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
			options: asyncOptions(latencySeed),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for hc := 1; hc <= maxHonestCount; hc++ {
				hc := hc
				t.Run(fmt.Sprintf("%d", hc), func(t *testing.T) {
					multiAgreementTest(t, testRNGSeed, hc, instanceCount, maxRounds, test.options...)
				})
			}
		})
	}
}

// FuzzHonestMultiInstance_AsyncDisagreement tests a scenario where two groups of equal
// participants, both in terms of count and power, vote for randomly generated
// chains at each instance, where the chain is uniform among all participants
// within the same group. It then asserts that over multiple instances the
// consensus is never reached and all nodes converge on the base chain of the
// first instance.
func FuzzHonestMultiInstance_AsyncDisagreement(f *testing.F) {
	const (
		instanceCount = 1000
		honestCount   = 6
	)
	f.Add(981)
	f.Fuzz(func(t *testing.T, seed int) {
		t.Parallel()
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		sm, err := sim.NewSimulation(asyncOptions(seed,
			sim.WithBaseChain(&baseChain),
			sim.AddHonestParticipants(honestCount/2, sim.NewUniformECChainGenerator(rand.Uint64(), 1, 3), uniformOneStoragePower),
			sim.AddHonestParticipants(honestCount/2, sim.NewUniformECChainGenerator(rand.Uint64(), 2, 4), uniformOneStoragePower),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
		// Insufficient majority means all should decide on base
		requireConsensusAtFirstInstance(t, sm, *baseChain.Base())
	})
}

func FuzzHonestMultiInstance_SyncAgreement(f *testing.F) {
	const (
		instanceCount = 4000
		honestCount   = 4
	)
	f.Add(-47)
	f.Fuzz(func(t *testing.T, seed int) {
		multiAgreementTest(t, seed, honestCount, instanceCount, maxRounds, syncOptions()...)
	})
}

func FuzzHonestMultiInstance_AsyncAgreement(f *testing.F) {
	const (
		instanceCount = 2000
		honestCount   = 4
	)
	f.Add(-7)
	f.Add(31)
	f.Add(33)
	f.Fuzz(func(t *testing.T, seed int) {
		multiAgreementTest(t, seed, honestCount, instanceCount, maxRounds*2, asyncOptions(seed)...)
	})
}

func multiAgreementTest(t *testing.T, seed int, honestCount int, instanceCount uint64, maxRounds uint64, opts ...sim.Option) {
	t.Parallel()
	rng := rand.New(rand.NewSource(int64(seed)))
	sm, err := sim.NewSimulation(append(opts,
		sim.AddHonestParticipants(
			honestCount,
			// Generate a random EC chain for all participant that changes randomly at each
			// instance.
			sim.NewUniformECChainGenerator(rng.Uint64(), 1, 4), uniformOneStoragePower),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())
	instance := sm.GetInstance(instanceCount)
	require.NotNil(t, instance)
	expected := instance.BaseChain
	// Assert that the network reaches a decision at last completed instance, and the
	// decision always matches the head of instance after it, which is initialised
	// but not executed by the simulation due to hitting the instanceCount limit.
	requireConsensusAtInstance(t, sm, instanceCount-1, *expected.Head())
}
