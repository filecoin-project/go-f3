package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestAbsent(t *testing.T) {
	t.Parallel()

	const honestParticipantsCount = 3
	repeatInParallel(t, 1, func(t *testing.T, repetition int) {
		// Total network size of 3 + 1, where the adversary has 1/4 of power.
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		sm, err := sim.NewSimulation(asyncOptions(t, repetition,
			sim.WithHonestParticipantCount(honestParticipantsCount),
			sim.WithTipSetGenerator(tsg),
			sim.WithBaseChain(&baseChain),
			sim.WithAdversary(adversary.NewAbsentGenerator(oneStoragePower)),
		)...)
		require.NoError(t, err)

		a := baseChain.Extend(tsg.Sample())
		sm.SetChains(sim.ChainCount{Count: honestParticipantsCount, Chain: a})

		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
	})
}
