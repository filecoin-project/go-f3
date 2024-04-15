package test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestImmediateDecide(t *testing.T) {
	sm := sim.NewSimulation(AsyncConfig(1, 0), GraniteConfig(), sim.TraceNone)

	// Create adversarial node
	value := sm.Base(0).Extend(sm.TipGen.Sample())
	adv := adversary.NewImmediateDecide(99, sm.HostFor(99), value)

	// Add the adversary to the simulation with 3/4 of total power.
	sm.SetAdversary(adv, 3)

	// The honest node starts with a different chain (on the same base).
	sm.SetChains(sim.ChainCount{Count: 1, Chain: sm.Base(0).Extend(sm.TipGen.Sample())})
	adv.Begin()
	err := sm.Run(1, MAX_ROUNDS)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.PrintResults()
	}
	require.NoError(t, err)

	decision, ok := sm.GetDecision(0, 0)
	require.True(t, ok, "no decision")
	require.Equal(t, value.Head(), decision.Head(), "honest node did not decide the right value")
}
