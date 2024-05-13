package test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/stretchr/testify/require"
)

func TestHonest_ChainAgreement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		options          []sim.Option
		participantCount int
	}{
		{
			name:             "sync singleton no signing",
			options:          syncOptions(),
			participantCount: 1,
		},
		{
			name:             "sync singleton bls",
			options:          syncOptions(sim.WithSigningBackend(signing.NewBLSBackend())),
			participantCount: 1,
		},
		{
			name:             "sync pair no signing",
			options:          syncOptions(),
			participantCount: 2,
		},
		{
			name:             "sync pair bls",
			options:          syncOptions(sim.WithSigningBackend(signing.NewBLSBackend())),
			participantCount: 2,
		},
		{
			name:             "async pair no signing",
			options:          asyncOptions(t, 1413),
			participantCount: 2,
		},
		{
			name:             "async pair bls",
			options:          asyncOptions(t, 1413, sim.WithSigningBackend(signing.NewBLSBackend())),
			participantCount: 2,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
			baseChain := generateECChain(t, tsg)
			targetChain := baseChain.Extend(tsg.Sample())
			test.options = append(test.options,
				sim.WithBaseChain(&baseChain),
				sim.AddHonestParticipants(test.participantCount, sim.NewFixedECChainGenerator(targetChain)))
			sm, err := sim.NewSimulation(test.options...)
			require.NoError(t, err)

			require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
			requireConsensusAtFirstInstance(t, sm, targetChain.Head())
		})
	}
}

func TestHonest_ChainDisagreement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		options []sim.Option
	}{
		{
			name:    "sync pair no signing",
			options: syncOptions(),
		},
		{
			name:    "sync pair bls",
			options: syncOptions(sim.WithSigningBackend(signing.NewBLSBackend())),
		},
		{
			name:    "async pair no signing",
			options: asyncOptions(t, 1413),
		},
		{
			name:    "async pair bls",
			options: asyncOptions(t, 1413, sim.WithSigningBackend(signing.NewBLSBackend())),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
			baseChain := generateECChain(t, tsg)
			oneChain := baseChain.Extend(tsg.Sample())
			anotherChain := baseChain.Extend(tsg.Sample())
			test.options = append(test.options,
				sim.WithBaseChain(&baseChain),
				sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(oneChain)),
				sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(anotherChain)),
			)
			sm, err := sim.NewSimulation(test.options...)
			require.NoError(t, err)
			require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
			requireConsensusAtFirstInstance(t, sm, baseChain...)
		})
	}
}

func TestSync_AgreementWithRepetition(t *testing.T) {
	repeatInParallel(t, 50, func(t *testing.T, repetition int) {
		honestCount := 3 + repetition
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		someChain := baseChain.Extend(tsg.Sample())
		sm, err := sim.NewSimulation(syncOptions(
			sim.WithBaseChain(&baseChain),
			sim.AddHonestParticipants(honestCount, sim.NewFixedECChainGenerator(someChain)),
		)...)
		require.NoError(t, err)

		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Synchronous, agreeing groups always decide the candidate.
		requireConsensusAtFirstInstance(t, sm, someChain.Head())
	})
}

func TestAsyncAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Parallel()
	// These iterations are much slower, so we can't test as many participants.
	for n := 3; n <= 16; n++ {
		honestCount := n
		t.Run(fmt.Sprintf("honest count %d", honestCount), func(t *testing.T) {
			repeatInParallel(t, asyncIterations, func(t *testing.T, repetition int) {
				tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
				baseChain := generateECChain(t, tsg)
				someChain := baseChain.Extend(tsg.Sample())
				sm, err := sim.NewSimulation(asyncOptions(t, repetition,
					sim.WithBaseChain(&baseChain),
					sim.AddHonestParticipants(honestCount, sim.NewFixedECChainGenerator(someChain)),
				)...)
				require.NoError(t, err)
				require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
				requireConsensusAtFirstInstance(t, sm, baseChain.Head(), someChain.Head())
			})
		})
	}
}

func TestSyncHalves(t *testing.T) {
	t.Parallel()

	repeatInParallel(t, 15, func(t *testing.T, repetition int) {
		honestCount := repetition*2 + 2
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		oneChain := baseChain.Extend(tsg.Sample())
		anotherChain := baseChain.Extend(tsg.Sample())
		sm, err := sim.NewSimulation(syncOptions(
			sim.WithBaseChain(&baseChain),
			sim.AddHonestParticipants(honestCount/2, sim.NewFixedECChainGenerator(oneChain)),
			sim.AddHonestParticipants(honestCount/2, sim.NewFixedECChainGenerator(anotherChain)),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		requireConsensusAtFirstInstance(t, sm, baseChain.Head())
	})
}

func TestSyncHalvesBLS(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()
	repeatInParallel(t, 3, func(t *testing.T, repetition int) {
		honestCount := repetition*2 + 2
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		oneChain := baseChain.Extend(tsg.Sample())
		anotherChain := baseChain.Extend(tsg.Sample())
		sm, err := sim.NewSimulation(syncOptions(
			sim.WithSigningBackend(signing.NewBLSBackend()),
			sim.WithBaseChain(&baseChain),
			sim.AddHonestParticipants(honestCount/2, sim.NewFixedECChainGenerator(oneChain)),
			sim.AddHonestParticipants(honestCount/2, sim.NewFixedECChainGenerator(anotherChain)),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		requireConsensusAtFirstInstance(t, sm, baseChain.Head())
	})
}

func TestAsyncHalves(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Parallel()
	for n := 4; n <= 20; n += 2 {
		honestCount := n
		t.Run(fmt.Sprintf("honest count %d", honestCount), func(t *testing.T) {
			repeatInParallel(t, asyncIterations, func(t *testing.T, repetition int) {
				tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
				baseChain := generateECChain(t, tsg)
				oneChain := baseChain.Extend(tsg.Sample())
				anotherChain := baseChain.Extend(tsg.Sample())
				sm, err := sim.NewSimulation(asyncOptions(t, repetition,
					sim.WithBaseChain(&baseChain),
					sim.AddHonestParticipants(honestCount/2, sim.NewFixedECChainGenerator(oneChain)),
					sim.AddHonestParticipants(honestCount/2, sim.NewFixedECChainGenerator(anotherChain)),
				)...)
				require.NoError(t, err)
				require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
				// Groups split 50/50 always decide the base.
				requireConsensusAtFirstInstance(t, sm, baseChain.Head())
			})
		})
	}
}

func TestRequireStrongQuorumToProgress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Parallel()
	repeatInParallel(t, asyncIterations, func(t *testing.T, repetition int) {
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		oneChain := baseChain.Extend(tsg.Sample())
		anotherChain := baseChain.Extend(tsg.Sample())
		sm, err := sim.NewSimulation(asyncOptions(t, repetition,
			sim.WithBaseChain(&baseChain),
			sim.AddHonestParticipants(10, sim.NewFixedECChainGenerator(oneChain)),
			sim.AddHonestParticipants(20, sim.NewFixedECChainGenerator(anotherChain)),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Must decide base.
		requireConsensusAtFirstInstance(t, sm, baseChain.Head())
	})
}

func TestHonest_FixedLongestCommonPrefix(t *testing.T) {
	// This test uses a synchronous configuration to ensure timely message delivery.
	// If async, it is possible to decide the base chain if QUALITY messages are delayed.
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	commonPrefix := baseChain.Extend(tsg.Sample())
	abc := commonPrefix.Extend(tsg.Sample())
	abd := commonPrefix.Extend(tsg.Sample())
	abe := commonPrefix.Extend(tsg.Sample())
	abf := commonPrefix.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(syncOptions(
		sim.WithBaseChain(&baseChain),
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abc)),
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abd)),
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abe)),
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abf)),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
	// Must decide ab, the longest common prefix.
	requireConsensusAtFirstInstance(t, sm, commonPrefix.Head())
}

// TestHonest_MajorityCommonPrefix tests that in a network of honest participants, where there is a majority
// that vote for a chain with un-identical but some common prefix, the common prefix of the chain proposed by
// the majority is decided.
func TestHonest_MajorityCommonPrefix(t *testing.T) {
	const instanceCount = 10
	majorityCommonPrefixGenerator := sim.NewUniformECChainGenerator(8484, 10, 20)
	sm, err := sim.NewSimulation(syncOptions(
		sim.AddHonestParticipants(20, sim.NewAppendingECChainGenerator(
			majorityCommonPrefixGenerator,
			sim.NewRandomECChainGenerator(23002354, 1, 8),
		)),
		sim.AddHonestParticipants(5, sim.NewAppendingECChainGenerator(
			sim.NewUniformECChainGenerator(84154, 1, 20),
			sim.NewRandomECChainGenerator(8965741, 5, 8),
		)),
		sim.AddHonestParticipants(1, sim.NewAppendingECChainGenerator(
			sim.NewUniformECChainGenerator(2158, 10, 20),
			sim.NewRandomECChainGenerator(154787878, 2, 8),
		)),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())

	// Must decide the longest common prefix proposed by the majority at every instance.
	for i := 0; i < instanceCount; i++ {
		ii := uint64(i)
		instance := sm.GetInstance(ii)
		commonPrefix := majorityCommonPrefixGenerator.GenerateECChain(ii, instance.BaseChain.Base(), 0)
		requireConsensusAtInstance(t, sm, ii, commonPrefix.Head())
	}
}
