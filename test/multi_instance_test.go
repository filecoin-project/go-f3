package test

import (
	"fmt"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

///// Tests for multiple chained instances of the protocol.

func TestMultiInstanceAgreement(t *testing.T) {
	sm := sim.NewSimulation(sim.Config{
		HonestCount: 10,
		LatencyMean: LATENCY_ASYNC,
	}, GraniteConfig(), sim.TraceNone)

	// All nodes start with the same chain and will observe the same extensions of that chain
	// in subsequent instances.
	sm.SetChains(sim.ChainCount{Count: 10, Chain: sm.Base(0).Extend(sm.CIDGen.Sample())})

	// NOTE: This test fails with values much higher than this as the participants
	// get too far out of sync.
	// See https://github.com/filecoin-project/go-f3/issues/113
	instances := uint64(1750)
	lastInstance := instances - 1
	err := sm.Run(instances, MAX_ROUNDS)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.PrintResults()
	}
	require.NoError(t, err)

	// The expected decision is the base for the next instance.
	expected := sm.EC.Instances[lastInstance+1].Base
	for _, participant := range sm.Participants {
		decision, ok := sm.GetDecision(lastInstance, participant.ID())
		require.True(t, ok, "no decision for participant %d", participant.ID())
		require.Equal(t, expected.Head(), decision.Head())
	}
}
