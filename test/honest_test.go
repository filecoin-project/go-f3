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
		name    string
		options []sim.Option
	}{
		{
			name:    "sync singleton no signing",
			options: syncOptions(sim.WithHonestParticipantCount(1)),
		},
		{
			name: "sync singleton bls",
			options: syncOptions(
				sim.WithHonestParticipantCount(1),
				sim.WithSigningBackend(signing.NewBLSBackend())),
		},
		{
			name:    "sync pair no signing",
			options: syncOptions(sim.WithHonestParticipantCount(2)),
		},
		{
			name: "sync pair bls",
			options: syncOptions(
				sim.WithHonestParticipantCount(2),
				sim.WithSigningBackend(signing.NewBLSBackend())),
		},
		{
			name:    "async pair no signing",
			options: asyncOptions(t, 1413, sim.WithHonestParticipantCount(2)),
		},
		{
			name: "async pair bls",
			options: asyncOptions(t, 1413,
				sim.WithHonestParticipantCount(2),
				sim.WithSigningBackend(signing.NewBLSBackend())),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
			baseChain := generateECChain(t, tsg)
			targetChain := baseChain.Extend(tsg.Sample())
			test.options = append(test.options, sim.WithBaseChain(&baseChain), sim.WithTipSetGenerator(tsg))
			sm, err := sim.NewSimulation(test.options...)
			require.NoError(t, err)
			sm.SetChains(sim.ChainCount{Count: sm.HonestParticipantsCount(), Chain: targetChain})

			require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
			requireConsensus(t, sm, targetChain.Head())
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
			options: syncOptions(sim.WithHonestParticipantCount(2)),
		},
		{
			name: "sync pair bls",
			options: syncOptions(
				sim.WithHonestParticipantCount(2),
				sim.WithSigningBackend(signing.NewBLSBackend())),
		},
		{
			name:    "async pair no signing",
			options: asyncOptions(t, 1413, sim.WithHonestParticipantCount(2)),
		},
		{
			name: "async pair bls",
			options: asyncOptions(t, 1413,
				sim.WithHonestParticipantCount(2),
				sim.WithSigningBackend(signing.NewBLSBackend())),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
			baseChain := generateECChain(t, tsg)
			test.options = append(test.options, sim.WithBaseChain(&baseChain), sim.WithTipSetGenerator(tsg))
			sm, err := sim.NewSimulation(test.options...)
			require.NoError(t, err)
			oneChain := baseChain.Extend(tsg.Sample())
			anotherChain := baseChain.Extend(tsg.Sample())
			participantsCount := sm.HonestParticipantsCount()
			sm.SetChains(sim.ChainCount{Count: participantsCount / 2, Chain: oneChain}, sim.ChainCount{Count: participantsCount / 2, Chain: anotherChain})

			require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
			requireConsensus(t, sm, baseChain...)
		})
	}
}

func TestSync_AgreementWithRepetition(t *testing.T) {
	repeatInParallel(t, 50, func(t *testing.T, repetition int) {
		honestCount := 3 + repetition
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		sm, err := sim.NewSimulation(syncOptions(
			sim.WithHonestParticipantCount(honestCount),
			sim.WithTipSetGenerator(tsg),
			sim.WithBaseChain(&baseChain))...)
		require.NoError(t, err)
		a := baseChain.Extend(tsg.Sample())
		sm.SetChains(sim.ChainCount{Count: sm.HonestParticipantsCount(), Chain: a})
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Synchronous, agreeing groups always decide the candidate.
		requireConsensus(t, sm, a.Head())
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
			repeatInParallel(t, asyncInterations, func(t *testing.T, repetition int) {
				tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
				baseChain := generateECChain(t, tsg)
				sm, err := sim.NewSimulation(asyncOptions(t, repetition,
					sim.WithHonestParticipantCount(honestCount),
					sim.WithTipSetGenerator(tsg),
					sim.WithBaseChain(&baseChain),
				)...)
				require.NoError(t, err)
				a := baseChain.Extend(tsg.Sample())
				sm.SetChains(sim.ChainCount{Count: sm.HonestParticipantsCount(), Chain: a})

				require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
				requireConsensus(t, sm, baseChain.Head(), a.Head())
			})
		})
	}
}

func TestAsyncAgreementXXX(t *testing.T) {
	n := 3
	iters := 100
	//for n := 3; n <= 16; n++ {
	honestCount := n
	t.Run(fmt.Sprintf("honest count %d", honestCount), func(t *testing.T) {
		for repetition := 1; repetition < iters; repetition++ {
			t.Log("iteration", repetition)
			tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
			baseChain := generateECChain(t, tsg)
			sm, err := sim.NewSimulation(asyncOptions(t, repetition,
				sim.WithTraceLevel(sim.TraceAll), // TRACE
				sim.WithHonestParticipantCount(honestCount),
				sim.WithTipSetGenerator(tsg),
				sim.WithBaseChain(&baseChain),
			)...)
			require.NoError(t, err)
			a := baseChain.Extend(tsg.Sample())
			sm.SetChains(sim.ChainCount{Count: sm.HonestParticipantsCount(), Chain: a})

			require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
			requireConsensus(t, sm, baseChain.Head(), a.Head())
		}
		//repeatInParallel(t, iters, func(t *testing.T, repetition int) {
		//})
	})
	//}
}

func TestSyncHalves(t *testing.T) {
	t.Parallel()

	repeatInParallel(t, 15, func(t *testing.T, repetition int) {
		honestCount := repetition*2 + 2
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		sm, err := sim.NewSimulation(syncOptions(
			sim.WithHonestParticipantCount(honestCount),
			sim.WithTipSetGenerator(tsg),
			sim.WithBaseChain(&baseChain))...)
		require.NoError(t, err)
		a := baseChain.Extend(tsg.Sample())
		b := baseChain.Extend(tsg.Sample())
		sm.SetChains(sim.ChainCount{Count: honestCount / 2, Chain: a}, sim.ChainCount{Count: honestCount / 2, Chain: b})

		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		requireConsensus(t, sm, baseChain.Head())
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
		sm, err := sim.NewSimulation(syncOptions(
			sim.WithHonestParticipantCount(honestCount),
			sim.WithTipSetGenerator(tsg),
			sim.WithSigningBackend(signing.NewBLSBackend()),
			sim.WithBaseChain(&baseChain))...)
		require.NoError(t, err)
		a := baseChain.Extend(tsg.Sample())
		b := baseChain.Extend(tsg.Sample())
		sm.SetChains(sim.ChainCount{Count: honestCount / 2, Chain: a}, sim.ChainCount{Count: honestCount / 2, Chain: b})

		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		requireConsensus(t, sm, baseChain.Head())
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
			repeatInParallel(t, asyncInterations, func(t *testing.T, repetition int) {
				tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
				baseChain := generateECChain(t, tsg)
				sm, err := sim.NewSimulation(asyncOptions(t, repetition,
					sim.WithHonestParticipantCount(honestCount),
					sim.WithTipSetGenerator(tsg),
					sim.WithBaseChain(&baseChain))...)
				require.NoError(t, err)
				a := baseChain.Extend(tsg.Sample())
				b := baseChain.Extend(tsg.Sample())
				sm.SetChains(sim.ChainCount{Count: honestCount / 2, Chain: a}, sim.ChainCount{Count: honestCount / 2, Chain: b})

				require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
				// Groups split 50/50 always decide the base.
				requireConsensus(t, sm, baseChain.Head())
			})
		})
	}
}

func TestRequireStrongQuorumToProgress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Parallel()
	repeatInParallel(t, asyncInterations, func(t *testing.T, repetition int) {
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		sm, err := sim.NewSimulation(asyncOptions(t, repetition,
			sim.WithHonestParticipantCount(30),
			sim.WithTipSetGenerator(tsg),
			sim.WithBaseChain(&baseChain))...)
		require.NoError(t, err)
		a := baseChain.Extend(tsg.Sample())
		b := baseChain.Extend(tsg.Sample())
		// No strict > quorum.
		sm.SetChains(sim.ChainCount{Count: 20, Chain: a}, sim.ChainCount{Count: 10, Chain: b})

		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
		// Must decide base.
		requireConsensus(t, sm, baseChain.Head())
	})
}

func TestLongestCommonPrefix(t *testing.T) {
	// This test uses a synchronous configuration to ensure timely message delivery.
	// If async, it is possible to decide the base chain if QUALITY messages are delayed.
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	sm, err := sim.NewSimulation(syncOptions(
		sim.WithHonestParticipantCount(4),
		sim.WithTipSetGenerator(tsg),
		sim.WithBaseChain(&baseChain),
	)...)
	require.NoError(t, err)
	ab := baseChain.Extend(tsg.Sample())
	abc := ab.Extend(tsg.Sample())
	abd := ab.Extend(tsg.Sample())
	abe := ab.Extend(tsg.Sample())
	abf := ab.Extend(tsg.Sample())
	sm.SetChains(
		sim.ChainCount{Count: 1, Chain: abc},
		sim.ChainCount{Count: 1, Chain: abd},
		sim.ChainCount{Count: 1, Chain: abe},
		sim.ChainCount{Count: 1, Chain: abf},
	)

	require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
	// Must decide ab, the longest common prefix.
	requireConsensus(t, sm, ab.Head())
}
