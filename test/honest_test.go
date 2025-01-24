package test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/stretchr/testify/require"
)

func TestHonest_Agreement(t *testing.T) {
	t.Parallel()
	maxParticipantCases := 10
	if testing.Short() {
		// Reduce max participants to 5 during short test for faster completion.
		maxParticipantCases = 5
	}

	// Tests with BLS are super slow; try fewer participant counts for them.
	blsParticipantCount := []int{4, 5, 6}

	// The number of honest participants for which every table test is executed.
	participantCounts := make([]int, maxParticipantCases)
	for i := range participantCounts {
		participantCounts[i] = i + 3
	}
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	targetChain := baseChain.Extend(tsg.Sample())

	tests := []struct {
		name                 string
		options              []sim.Option
		useBLS               bool
		participantCounts    []int
		wantConsensusOnAnyOf []*gpbft.TipSet
	}{
		{
			name:                 "sync no signing",
			options:              syncOptions(),
			participantCounts:    participantCounts,
			wantConsensusOnAnyOf: []*gpbft.TipSet{targetChain.Head()},
		},
		{
			name:                 "sync bls",
			options:              syncOptions(),
			useBLS:               true,
			participantCounts:    blsParticipantCount,
			wantConsensusOnAnyOf: []*gpbft.TipSet{targetChain.Head()},
		},
		{
			name:                 "async no signing",
			options:              asyncOptions(21),
			participantCounts:    participantCounts,
			wantConsensusOnAnyOf: []*gpbft.TipSet{baseChain.Head(), targetChain.Head()},
		},
		{
			name:                 "async pair bls",
			options:              asyncOptions(1413),
			useBLS:               true,
			participantCounts:    blsParticipantCount,
			wantConsensusOnAnyOf: []*gpbft.TipSet{baseChain.Head(), targetChain.Head()},
		},
	}
	for _, test := range tests {
		for _, participantCount := range test.participantCounts {
			name := fmt.Sprintf("%s %d", test.name, participantCount)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				var opts []sim.Option
				opts = append(opts, test.options...)
				opts = append(opts,
					sim.WithBaseChain(baseChain),
					sim.AddHonestParticipants(participantCount, sim.NewFixedECChainGenerator(targetChain), uniformOneStoragePower))
				if test.useBLS {
					// Initialise a new BLS backend for each test since it's not concurrent-safe.
					opts = append(opts, sim.WithSigningBackend(signing.NewBLSBackend()))
				}
				sm, err := sim.NewSimulation(opts...)
				require.NoError(t, err)
				require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
				requireConsensusAtFirstInstance(t, sm, test.wantConsensusOnAnyOf...)
			})
		}
	}
}

func TestHonest_Disagreement(t *testing.T) {
	t.Parallel()
	maxParticipantCases := 10
	if testing.Short() {
		// Reduce max participants to 5
		maxParticipantCases = 5
	}

	// Tests with BLS are super slow; try fewer participant counts for them.
	blsParticipantCount := []int{2, 4, 10}

	participantCounts := make([]int, maxParticipantCases)
	for i := range participantCounts {
		participantCounts[i] = (i + 1) * 2
	}

	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	oneChain := baseChain.Extend(tsg.Sample())
	anotherChain := baseChain.Extend(tsg.Sample())

	tests := []struct {
		name              string
		options           []sim.Option
		participantCounts []int
	}{
		{
			name:              "sync no signing",
			options:           syncOptions(),
			participantCounts: participantCounts,
		},
		{
			name:              "sync bls",
			options:           syncOptions(sim.WithSigningBackend(signing.NewBLSBackend())),
			participantCounts: blsParticipantCount,
		},
		{
			name:              "async no signing",
			options:           asyncOptions(1413),
			participantCounts: participantCounts,
		},
		{
			name:              "async bls",
			options:           asyncOptions(1413, sim.WithSigningBackend(signing.NewBLSBackend())),
			participantCounts: blsParticipantCount,
		},
	}

	for _, test := range tests {
		for _, participantCount := range test.participantCounts {
			name := fmt.Sprintf("%s %d", test.name, participantCount)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				var opts []sim.Option
				opts = append(opts, test.options...)
				opts = append(opts,
					sim.WithBaseChain(baseChain),
					sim.AddHonestParticipants(participantCount/2, sim.NewFixedECChainGenerator(oneChain), uniformOneStoragePower),
					sim.AddHonestParticipants(participantCount/2, sim.NewFixedECChainGenerator(anotherChain), uniformOneStoragePower),
				)
				sm, err := sim.NewSimulation(opts...)
				require.NoError(t, err)
				require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
				// Insufficient majority means all should decide on base
				requireConsensusAtFirstInstance(t, sm, baseChain.Base())
			})
		}
	}
}

func FuzzHonest_AsyncRequireStrongQuorumToProgress(f *testing.F) {
	f.Add(8956230)
	f.Add(-54546)
	f.Add(565)
	f.Add(4)
	f.Add(-88)
	f.Fuzz(func(t *testing.T, seed int) {
		t.Parallel()
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		oneChain := baseChain.Extend(tsg.Sample())
		anotherChain := baseChain.Extend(tsg.Sample())
		sm, err := sim.NewSimulation(asyncOptions(seed,
			sim.WithBaseChain(baseChain),
			sim.AddHonestParticipants(10, sim.NewFixedECChainGenerator(oneChain), uniformOneStoragePower),
			sim.AddHonestParticipants(20, sim.NewFixedECChainGenerator(anotherChain), uniformOneStoragePower),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())

		// Must decide baseChain's head, i.e. the longest common prefix since there is no strong quorum.
		requireConsensusAtFirstInstance(t, sm, baseChain.Head())
	})
}

func TestHonest_FixedLongestCommonPrefix(t *testing.T) {
	t.Parallel()
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
		sim.WithBaseChain(baseChain),
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

func FuzzHonest_SyncMajorityCommonPrefix(f *testing.F) {
	const instanceCount = 10
	f.Add(69846)
	f.Add(2)
	f.Add(545)
	f.Add(-955846)
	f.Add(28)
	f.Fuzz(func(t *testing.T, seed int) {
		t.Parallel()
		rng := rand.New(rand.NewSource(int64(seed)))
		majorityCommonPrefixGenerator := sim.NewUniformECChainGenerator(rng.Uint64(), 1, 5)
		sm, err := sim.NewSimulation(append(syncOptions(),
			sim.AddHonestParticipants(20, sim.NewAppendingECChainGenerator(
				majorityCommonPrefixGenerator,
				sim.NewRandomECChainGenerator(rng.Uint64(), 1, 3),
			), uniformOneStoragePower),
			sim.AddHonestParticipants(5, sim.NewAppendingECChainGenerator(
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 4),
				sim.NewRandomECChainGenerator(rng.Uint64(), 2, 4),
			), uniformOneStoragePower),
			sim.AddHonestParticipants(1, sim.NewAppendingECChainGenerator(
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 5),
				sim.NewRandomECChainGenerator(rng.Uint64(), 1, 3),
			), uniformOneStoragePower),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())

		// Must decide the longest common prefix's head proposed by the majority at every instance.
		for i := uint64(0); i < instanceCount; i++ {
			instance := sm.GetInstance(i)
			commonPrefix := majorityCommonPrefixGenerator.GenerateECChain(i, instance.BaseChain.Base(), 0)
			requireConsensusAtInstance(t, sm, i, commonPrefix.Head())
		}
	})
}

// FuzzHonest_AsyncMajorityCommonPrefix tests that in a network of honest
// participants, where there is a majority that vote for a chain with
// un-identical but some common prefix, the final decision is one of the tipsets
// of that common prefix. Since due to latency it is possible that not all
// participant would decide on the head of the common prefix.
func FuzzHonest_AsyncMajorityCommonPrefix(f *testing.F) {
	const instanceCount = 10
	f.Add(5465)
	f.Add(54)
	f.Add(321565)
	f.Add(-7)
	f.Add(5)
	f.Fuzz(func(t *testing.T, seed int) {
		t.Parallel()
		rng := rand.New(rand.NewSource(int64(seed)))
		majorityCommonPrefixGenerator := sim.NewUniformECChainGenerator(rng.Uint64(), 1, 3)
		sm, err := sim.NewSimulation(append(asyncOptions(rng.Int()),
			sim.AddHonestParticipants(20, sim.NewAppendingECChainGenerator(
				majorityCommonPrefixGenerator,
				sim.NewRandomECChainGenerator(rng.Uint64(), 1, 4),
			), uniformOneStoragePower),
			sim.AddHonestParticipants(5, sim.NewAppendingECChainGenerator(
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 4),
				sim.NewRandomECChainGenerator(rng.Uint64(), 2, 3),
			), uniformOneStoragePower),
			sim.AddHonestParticipants(1, sim.NewAppendingECChainGenerator(
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 3),
				sim.NewRandomECChainGenerator(rng.Uint64(), 2, 3),
			), uniformOneStoragePower),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(instanceCount, maxRounds*2), "%s", sm.Describe())

		// Must decide on any one of common prefix tipsets. Since, unlike
		// FuzzHonest_SyncMajorityCommonPrefix, due to latency not all nodes would
		// coverage exactly on common prefix head
		for i := uint64(0); i < instanceCount; i++ {
			instance := sm.GetInstance(i)
			commonPrefix := majorityCommonPrefixGenerator.GenerateECChain(i, instance.BaseChain.Base(), 0)
			requireConsensusAtInstance(t, sm, i, commonPrefix.TipSets...)
		}
	})
}
