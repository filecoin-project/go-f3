package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/adversary"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

func TestAbsent(t *testing.T) {
	for i := 0; i < 1000; i++ {
		// fmt.Println("Iteration", i)
		sm := sim.NewSimulation(AsyncConfig(3, i), GraniteConfig(), sim.TraceNone)
		// Adversary has 1/4 of power.
		sm.SetAdversary(adversary.NewAbsent(99, sm.HostFor(99)), 1)

		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

		require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
	}
}
