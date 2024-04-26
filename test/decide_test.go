package test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestImmediateDecide(t *testing.T) {
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	adversaryValue := baseChain.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(asyncOptions(t, 1413,
		sim.WithHonestParticipantCount(1),
		sim.WithBaseChain(&baseChain),
		sim.WithTipSetGenerator(tsg),
		// Add the adversary to the simulation with 3/4 of total power.
		sim.WithAdversary(adversary.NewImmediateDecideGenerator(adversaryValue, gpbft.NewStoragePower(3))),
	)...)
	require.NoError(t, err)

	// The honest node starts with a different chain (on the same base).
	sm.SetChains(sim.ChainCount{Count: 1, Chain: baseChain.Extend(tsg.Sample())})
	err = sm.Run(1, maxRounds)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.PrintResults()
	}
	require.NoError(t, err)

	decision, ok := sm.GetDecision(0, 0)
	require.True(t, ok, "no decision")
	require.Equal(t, adversaryValue.Head(), decision.Head(), "honest node did not decide the right value")
}
