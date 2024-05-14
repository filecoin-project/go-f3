package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/stretchr/testify/require"
)

func TestHonest_Agreement(t *testing.T) {
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
			options:          asyncOptions(1413),
			participantCount: 2,
		},
		{
			name:             "async pair bls",
			options:          asyncOptions(1413, sim.WithSigningBackend(signing.NewBLSBackend())),
			participantCount: 2,
		},
		{
			name:             "sync 100",
			options:          syncOptions(),
			participantCount: 100,
		},
		{
			name:             "async 100",
			options:          asyncOptions(1413),
			participantCount: 100,
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
				sim.AddHonestParticipants(test.participantCount, sim.NewFixedECChainGenerator(targetChain), uniformOneStoragePower))
			sm, err := sim.NewSimulation(test.options...)
			require.NoError(t, err)

			require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
			requireConsensusAtFirstInstance(t, sm, targetChain.Head())
		})
	}
}

func TestHonest_Disagreement(t *testing.T) {
	tests := []struct {
		name        string
		options     []sim.Option
		honestCount int
	}{
		{
			name:        "sync pair no signing",
			options:     syncOptions(),
			honestCount: 2,
		},
		{
			name:        "sync pair bls",
			options:     syncOptions(sim.WithSigningBackend(signing.NewBLSBackend())),
			honestCount: 2,
		},
		{
			name:        "async pair no signing",
			options:     asyncOptions(1413),
			honestCount: 2,
		},
		{
			name:        "async pair bls",
			options:     asyncOptions(1413, sim.WithSigningBackend(signing.NewBLSBackend())),
			honestCount: 2,
		},
		{
			name:        "sync 100",
			options:     syncOptions(),
			honestCount: 100,
		},
		{
			name:        "async 100",
			options:     asyncOptions(1413),
			honestCount: 100,
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
				sim.AddHonestParticipants(test.honestCount/2, sim.NewFixedECChainGenerator(oneChain), uniformOneStoragePower),
				sim.AddHonestParticipants(test.honestCount/2, sim.NewFixedECChainGenerator(anotherChain), uniformOneStoragePower),
			)
			sm, err := sim.NewSimulation(test.options...)
			require.NoError(t, err)
			require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
			// Insufficient majority means all should decide on base
			requireConsensusAtFirstInstance(t, sm, baseChain.Base())
		})
	}
}

func TestHonest_RequireStrongQuorumToProgress(t *testing.T) {
	requireStrongQuorumToProgressTest(t, 8956230)
}

func FuzzHonest_RequireStrongQuorumToProgress(f *testing.F) {
	f.Fuzz(requireStrongQuorumToProgressTest)
}

func requireStrongQuorumToProgressTest(t *testing.T, seed int) {
	tsg := sim.NewTipSetGenerator(uint64(seed * 3))
	baseChain := generateECChain(t, tsg)
	oneChain := baseChain.Extend(tsg.Sample())
	anotherChain := baseChain.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(asyncOptions(seed*5,
		sim.WithBaseChain(&baseChain),
		sim.AddHonestParticipants(10, sim.NewFixedECChainGenerator(oneChain), uniformOneStoragePower),
		sim.AddHonestParticipants(20, sim.NewFixedECChainGenerator(anotherChain), uniformOneStoragePower),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())

	// Must decide baseChain's head, i.e. the longest common prefix since there is no strong quorum.
	requireConsensusAtFirstInstance(t, sm, baseChain.Head())
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
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abc), uniformOneStoragePower),
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abd), uniformOneStoragePower),
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abe), uniformOneStoragePower),
		sim.AddHonestParticipants(1, sim.NewFixedECChainGenerator(abf), uniformOneStoragePower),
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
		), uniformOneStoragePower),
		sim.AddHonestParticipants(5, sim.NewAppendingECChainGenerator(
			sim.NewUniformECChainGenerator(84154, 1, 20),
			sim.NewRandomECChainGenerator(8965741, 5, 8),
		), uniformOneStoragePower),
		sim.AddHonestParticipants(1, sim.NewAppendingECChainGenerator(
			sim.NewUniformECChainGenerator(2158, 10, 20),
			sim.NewRandomECChainGenerator(154787878, 2, 8),
		), uniformOneStoragePower),
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
