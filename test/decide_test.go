package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/adversary"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

func TestImmediateDecide(t *testing.T) {
	sm := sim.NewSimulation(sim.Config{
		HonestCount: 1,
		LatencyMean: 10 * time.Millisecond, // Near-synchrony
	}, GraniteConfig(), sim.TraceNone)

	// Create adversarial node
	value := sm.Base.Extend(sm.CIDGen.Sample())
	adv := adversary.NewImmediateDecide(99, sm.Network, sm.PowerTable, value)

	// Add the adversarial nodes to the simulation
	sm.SetAdversary(adv, 3) // Adversary has 30% of 10 total power.

	adv.Begin()
	sm.ReceiveChains(sim.ChainCount{Count: 1, Chain: sm.Base.Extend(sm.CIDGen.Sample())})
	fmt.Printf("Running test: \n")
	err := sm.Run(MAX_ROUNDS)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.PrintResults()
	}

	require.NoError(t, err)

	decision, ok := sm.GetDecision(0, 0)
	require.True(t, ok, "no decision")
	require.Equal(t, value.Head(), decision.Head(), "The honest node did not decide on the same value")
}
