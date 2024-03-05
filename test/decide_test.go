package test

import (
	"fmt"
	"github.com/filecoin-project/go-f3/adversary"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestImmediateDecide(t *testing.T) {
	sm := sim.NewSimulation(sim.Config{
		HonestCount: 1,
		LatencyMean: LATENCY_ASYNC,
	}, GraniteConfig(), sim.TraceNone)

	// Create adversarial node
	value := sm.Base.Extend(sm.CIDGen.Sample())
	adv := adversary.NewImmediateDecide(99, sm.Network, sm.PowerTable, value)

	// Add the adversary to the simulation with 3/4 of total power.
	sm.SetAdversary(adv, 3)

	// The honest node starts with a different chain (on the same base).
	sm.ReceiveChains(sim.ChainCount{Count: 1, Chain: sm.Base.Extend(sm.CIDGen.Sample())})
	adv.Begin()
	err := sm.Run(MAX_ROUNDS)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.PrintResults()
	}
	require.NoError(t, err)

	decision, ok := sm.GetDecision(0, 0)
	require.True(t, ok, "no decision")
	require.Equal(t, value.Head(), decision.Head(), "honest node did not decide the right value")
}
