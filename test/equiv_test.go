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
	sm := sim.NewSimulation(sim.Config{
		HonestCount: 7,
		LatencySeed: int64(i),
		LatencyMean: 0.01, // Near-synchrony
	}, GraniteConfig(), net.TraceNone)
	adv := adversary.NewWitholdCommit("A", sm.Network)
	sm.SetAdversary(adv, 3) // Adversary has 30% of 10 total power.

	a := sm.Base.Extend(sm.CIDGen.Sample())
	b := sm.Base.Extend(sm.CIDGen.Sample())
	// Of 7 nodes, 4 victims will prefer chain A, 3 others will prefer chain B.
	// The adversary will target the first to decide, and withhold COMMIT from the rest.
	// After the victim decides in round 0, the adversary stops participating.
	// Now there are 3 nodes on each side (and one decided), with total power 6/10, less than quorum.
	// The B side must be swayed to the A side by observing that some nodes on the A side reached a COMMIT.
	victims := []string{"P0", "P1", "P2", "P3"}
	adv.SetVictim(victims, *a)

	adv.Begin()
	sm.ReceiveChains(sim.ChainCount{4, *a}, sim.ChainCount{3, *b})
	ok := sm.Run(MAX_ROUNDS)
	if !ok {
		sm.PrintResults()
	}
	// The adversary could convince the victim to decide a, so all must decide a.
	require.True(t, ok)
	decision, _ := sm.Participants[0].Finalised()
	require.Equal(t, *a.Head(), decision)
}
