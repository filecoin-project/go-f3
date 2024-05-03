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
	tsg := sim.NewTipSetGenerator(984615)
	baseChain := generateECChain(t, tsg)
	adversaryValue := baseChain.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(asyncOptions(t, 1413,
		sim.AddHonestParticipants(1, sim.NewUniformECChainGenerator(tipSetGeneratorSeed, 1, 10)),
		sim.WithBaseChain(&baseChain),
		// Add the adversary to the simulation with 3/4 of total power.
		sim.WithAdversary(adversary.NewImmediateDecideGenerator(adversaryValue, gpbft.NewStoragePower(3))),
	)...)
	require.NoError(t, err)

	err = sm.Run(1, maxRounds)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.GetInstance(0).Print()
	}
	require.NoError(t, err)

	decision := sm.GetInstance(0).GetDecision(0)
	require.NotNil(t, decision, "no decision")
	require.Equal(t, adversaryValue.Head(), decision.Head(), "honest node did not decide the right value")
}
