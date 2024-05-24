package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func FuzzAbsentAdversary(f *testing.F) {
	f.Add(98465230)
	f.Add(651)
	f.Add(-56)
	f.Add(22)
	f.Add(0)
	f.Fuzz(func(t *testing.T, latencySeed int) {
		t.Parallel()
		sm, err := sim.NewSimulation(
			asyncOptions(latencySeed,
				// Total network size of 3 + 1, where the adversary has 1/4 of power.
				sim.AddHonestParticipants(
					3,
					sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10),
					uniformOneStoragePower),
				sim.WithAdversary(adversary.NewAbsentGenerator(oneStoragePower)),
			)...)
		require.NoError(t, err)
		require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
	},
	)
}
