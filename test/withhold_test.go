package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestWitholdCommit1(t *testing.T) {
	i := 0
	simConfig := AsyncConfig(7, i)
	simConfig.LatencyMean = 10 * time.Millisecond // Near-synchrony
	sm, err := sim.NewSimulation(simConfig, GraniteConfig(), sim.TraceNone)
	require.NoError(t, err)
	adv := adversary.NewWitholdCommit(99, sm.HostFor(99))
	sm.SetAdversary(adv, 3) // Adversary has 30% of 10 total power.

	a := sm.Base(0).Extend(sm.TipGen.Sample())
	b := sm.Base(0).Extend(sm.TipGen.Sample())
	// Of 7 nodes, 4 victims will prefer chain A, 3 others will prefer chain B.
	// The adversary will target the first to decide, and withhold COMMIT from the rest.
	// After the victim decides in round 0, the adversary stops participating.
	// Now there are 3 nodes on each side (and one decided), with total power 6/10, less than quorum.
	// The B side must be swayed to the A side by observing that some nodes on the A side reached a COMMIT.
	victims := []gpbft.ActorID{0, 1, 2, 3}
	adv.SetVictim(victims, a)

	adv.Begin()
	sm.SetChains(sim.ChainCount{Count: 4, Chain: a}, sim.ChainCount{Count: 3, Chain: b})
	err = sm.Run(1, MAX_ROUNDS)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.PrintResults()
	}
	// The adversary could convince the victim to decide a, so all must decide a.
	require.NoError(t, err)
	decision, ok := sm.GetDecision(0, 0)
	require.True(t, ok)
	require.Equal(t, a, decision)
}
