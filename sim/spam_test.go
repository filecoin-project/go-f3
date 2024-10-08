package sim_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
	"github.com/stretchr/testify/require"
)

func TestSpamAdversary(t *testing.T) {
	SkipInRaceMode(t)
	t.Parallel()
	const (
		instanceCount = 2000
		maxRounds     = 30
	)
	honestCounts := []int{3, 4, 5, 6, 7, 8, 9}
	if testing.Short() {
		honestCounts = []int{3, 4, 5}
	}
	tests := []struct {
		name               string
		maxLookaheadRounds uint64
		spamAheadRounds    uint64
	}{
		{
			name:            "zero lookahead 10x spam",
			spamAheadRounds: 10,
		},
		{
			name:               "10 lookahead 3x spam",
			maxLookaheadRounds: 10,
			spamAheadRounds:    30,
		},
	}
	for _, test := range tests {
		for _, hc := range honestCounts {
			name := fmt.Sprintf("%s honest count %d", test.name, hc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testSpamAdversary(t, 8976, hc, test.maxLookaheadRounds, test.spamAheadRounds, instanceCount, maxRounds)

				// TODO: We have no good way of asserting that the spam is mitigated as DoS
				//       vector via maxLookaheadRounds. Expand assertions of this test via
				//       metrics once observability/telemetry is implemented.
			})
		}
	}
}

func FuzzSpamAdversary(f *testing.F) {
	const (
		instanceCount = 500
		maxRounds     = 10
	)

	f.Add(int64(22098))
	f.Add(int64(8))
	f.Add(int64(65))

	f.Fuzz(func(t *testing.T, seed int64) {
		t.Parallel()
		rng := rand.New(rand.NewSource(seed))
		hc := rng.Intn(2) + 3
		maxLookaheadRounds := uint64(rng.Intn(10))
		spamAheadRounds := uint64((rng.Intn(3) + 1) * 10)
		t.Log(maxLookaheadRounds)
		t.Log(spamAheadRounds)
		t.Log(hc)
		testSpamAdversary(t, rng.Int63(), hc, maxLookaheadRounds, spamAheadRounds, instanceCount, maxRounds)
	})
}

func testSpamAdversary(t *testing.T, seed int64, hc int, maxLookaheadRounds, spamAheadRounds, instanceCount, maxRounds uint64) {
	rng := rand.New(rand.NewSource(seed))
	ecChainGenerator := sim.NewUniformECChainGenerator(rng.Uint64(), 1, 10)
	lessThanOneThirdAdversaryStoragePower := gpbft.NewStoragePower(int64(max(hc/3-1, 1)))
	var opts []gpbft.Option
	opts = append(opts, testGpbftOptions...)
	opts = append(opts, gpbft.WithMaxLookaheadRounds(maxLookaheadRounds))
	sm, err := sim.NewSimulation(
		sim.WithLatencyModeler(func() (latency.Model, error) {
			return latency.NewLogNormal(rng.Int63(), time.Microsecond), nil
		}),
		sim.WithECEpochDuration(EcEpochDuration),
		sim.WitECStabilisationDelay(EcStabilisationDelay),
		sim.WithGpbftOptions(opts...),
		sim.AddHonestParticipants(hc, ecChainGenerator, uniformOneStoragePower),
		sim.WithAdversary(adversary.NewSpamGenerator(lessThanOneThirdAdversaryStoragePower, spamAheadRounds)),
		//sim.WithTraceLevel(sim.TraceAll),
	)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(instanceCount, maxRounds), "%s", sm.Describe())

	instance := sm.GetInstance(0)
	require.NotNil(t, instance, "instance 0")
	latestBaseECChain := instance.BaseChain
	for i := uint64(0); i < instanceCount; i++ {
		instance = sm.GetInstance(i + 1)
		require.NotNil(t, instance, "instance %d", i)
		wantDecision := ecChainGenerator.GenerateECChain(i, *latestBaseECChain.Head(), math.MaxUint64)

		// Sanity check that the expected decision is progressed from the base chain
		require.Equal(t, wantDecision.Base(), latestBaseECChain.Head())
		require.NotEqual(t, wantDecision.Suffix(), latestBaseECChain.Suffix())

		// Assert the consensus is reached at the head of expected chain despite the spam.
		requireConsensusAtInstance(t, sm, i, wantDecision...)
		latestBaseECChain = instance.BaseChain
	}
}
