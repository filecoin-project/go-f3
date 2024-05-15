package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestAbsentAdversary(t *testing.T) {
	absentAdversaryTest(t, 98465230)
}

func FuzzAbsentAdversary(f *testing.F) {
	f.Fuzz(absentAdversaryTest)
}

func absentAdversaryTest(t *testing.T, latencySeed int) {
	t.Parallel()
	sm, err := sim.NewSimulation(
		asyncOptions(latencySeed,
			sim.AddHonestParticipants(
				3,
				sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10),
				uniformOneStoragePower),
			sim.WithAdversary(adversary.NewAbsentGenerator(oneStoragePower)),
		)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
}
