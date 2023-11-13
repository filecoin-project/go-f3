package test

import (
	"github.com/anorth/f3sim/adversary"
	"github.com/anorth/f3sim/net"
	"github.com/anorth/f3sim/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWitholdCommit1(t *testing.T) {
	i := 0
	sm := sim.NewSimulation(&sim.Config{
		HonestCount:  7,
		LatencySeed:  int64(i),
		LatencyMean:  0.01, // Near-synchrony
		GraniteDelta: 0.200,
	}, net.TraceNone)
	adv := adversary.NewWitholdCommit("A", sm.Network)
	sm.SetAdversary(adv, 3) // Adversary has 30% of 10 total power.

	a := sm.Base.Extend(sm.CIDGen.Sample())
	b := sm.Base.Extend(sm.CIDGen.Sample())
	// Of 7 nodes, 4 victims will prefer chain a, 3 others will prefer chain b.
	// The adversary will target the first, and withhold COMMIT from the rest.
	victims := []string{"P0", "P1", "P2", "P3"}
	adv.SetVictim(victims, *a)

	adv.Begin()
	sm.ReceiveChains(sim.ChainCount{4, *a}, sim.ChainCount{3, *b})
	ok := sm.Run()
	if !ok {
		sm.PrintResults()
	}
	// The adversary could convince the victim to decide a, so all must decide a.
	require.True(t, ok)
	decision, _ := sm.Participants[0].Finalised()
	require.Equal(t, *a.Head(), decision)
}
