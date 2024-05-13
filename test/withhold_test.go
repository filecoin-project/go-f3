package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
	"github.com/stretchr/testify/require"
)

func TestWitholdCommit1(t *testing.T) {
	nearSynchrony, err := latency.NewLogNormal(1413, 10*time.Millisecond)
	require.NoError(t, err)
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	a := baseChain.Extend(tsg.Sample())
	b := baseChain.Extend(tsg.Sample())
	victims := []gpbft.ActorID{0, 1, 2, 3}
	sm, err := sim.NewSimulation(
		sim.WithLatencyModel(nearSynchrony),
		sim.WithECEpochDuration(EcEpochDuration),
		sim.WitECStabilisationDelay(EcStabilisationDelay),
		sim.WithGpbftOptions(testGpbftOptions...),
		sim.WithBaseChain(&baseChain),
		// Adversary has 30% of 10 total power.
		// Of 7 nodes, 4 victims will prefer chain A, 3 others will prefer chain B.
		// The adversary will target the first to decide, and withhold COMMIT from the rest.
		// After the victim decides in round 0, the adversary stops participating.
		// Now there are 3 nodes on each side (and one decided), with total power 6/10, less than quorum.
		// The B side must be swayed to the A side by observing that some nodes on the A side reached a COMMIT.
		sim.WithAdversary(adversary.NewWitholdCommitGenerator(gpbft.NewStoragePower(3), victims, a)),
		sim.AddHonestParticipants(4, sim.NewFixedECChainGenerator(a)),
		sim.AddHonestParticipants(3, sim.NewFixedECChainGenerator(b)),
	)
	require.NoError(t, err)

	err = sm.Run(1, maxRounds)
	if err != nil {
		fmt.Printf("%s", sm.Describe())
		sm.GetInstance(0).Print()
	}
	// The adversary could convince the victim to decide a, so all must decide a.
	require.NoError(t, err)
	decision := sm.GetInstance(0).GetDecision(0)
	require.NotNil(t, decision)
	require.Equal(t, &a, decision)
}
