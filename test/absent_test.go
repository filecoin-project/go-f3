package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/adversary"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

func TestAbsent(t *testing.T) {
	for i := 0; i < 1000; i++ {
		//fmt.Println("Iteration", i)
		sm, err := sim.NewSimulation(sim.Config{
			HonestCount: 3,
			LatencySeed: int64(i),
			LatencyMean: LATENCY_ASYNC,
		}, GraniteConfig(), sim.TraceNone)
		require.NoError(t, err)
		// Adversary has 1/4 of power.
		err = sm.SetAdversary(adversary.NewAbsent(99, sm.Network), 1)
		require.NoError(t, err)

		a := sm.Base.Extend(sm.CIDGen.Sample())
		err = sm.ReceiveChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})
		require.NoError(t, err)

		require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
	}
}
