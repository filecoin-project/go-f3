package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestAbsent(t *testing.T) {
	t.Parallel()

	repeatInParallel(t, 1, func(t *testing.T, repetition int) {
		// Total network size of 3 + 1, where the adversary has 1/4 of power.
		sm, err := sim.NewSimulation(asyncOptions(t, repetition,
			sim.AddHonestParticipants(
				3,
				sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10),
				uniformOneStoragePower),
			sim.WithAdversary(adversary.NewAbsentGenerator(oneStoragePower)),
		)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
	})
}
