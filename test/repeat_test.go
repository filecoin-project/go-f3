package test

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

var (
	repeatOnce = func(int) adversary.RepetitionSampler {
		return func(*gpbft.GMessage) int {
			return 1
		}
	}
	repeatBoundedRandom = func(seed int) adversary.RepetitionSampler {
		return newBoundedRepeater(int64(seed), 10, 50)
	}
	repeatZipF = func(seed int) adversary.RepetitionSampler {
		rng := rand.New(rand.NewSource(int64(seed)))
		zipf := rand.NewZipf(rng, 1.2, 1.0, 100)
		return func(*gpbft.GMessage) int {
			return int(zipf.Uint64())
		}
	}
	repeatBoundedQuality = func(seed int) adversary.RepetitionSampler {
		boundedRepeater := newBoundedRepeater(int64(seed), 10, 50)
		return func(msg *gpbft.GMessage) int {
			if msg.Vote.Step != gpbft.QUALITY_PHASE {
				return 0
			}
			return boundedRepeater(msg)
		}
	}
	repeatBoundedCommit = func(seed int) adversary.RepetitionSampler {
		boundedRepeater := newBoundedRepeater(int64(seed), 10, 50)
		return func(msg *gpbft.GMessage) int {
			if msg.Vote.Step != gpbft.COMMIT_PHASE {
				return 0
			}
			return boundedRepeater(msg)
		}
	}
	repeatAdversaryTestHonestCounts = []int{
		2, // 1/3 adversary power
		3, // 1/4 adversary power
		4, // 1/5 adversary power
	}
)

func TestRepeatAdversary(t *testing.T) {
	tests := []struct {
		name              string
		repetitionSampler func(int) adversary.RepetitionSampler
	}{
		{
			name:              "once",
			repetitionSampler: repeatOnce,
		},
		{
			name:              "bounded uniform random",
			repetitionSampler: repeatBoundedRandom,
		},
		{
			name:              "zipf",
			repetitionSampler: repeatZipF,
		},
		{
			name:              "QUALITY Repeater",
			repetitionSampler: repeatBoundedQuality,
		},
		{
			name:              "COMMIT Repeater",
			repetitionSampler: repeatBoundedCommit,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for _, hc := range repeatAdversaryTestHonestCounts {
				repeatAdversaryTest(t, 98461, hc, maxRounds, test.repetitionSampler)
			}
		})
	}
}

func FuzzRepeatAdversary(f *testing.F) {
	f.Add(68465)
	f.Add(-5)
	f.Add(-5454)
	f.Fuzz(func(t *testing.T, seed int) {
		for _, hc := range repeatAdversaryTestHonestCounts {
			repeatAdversaryTest(t, seed, hc, maxRounds, repeatOnce)
			repeatAdversaryTest(t, seed, hc, maxRounds, repeatZipF)
			repeatAdversaryTest(t, seed, hc, maxRounds, repeatBoundedRandom)
			repeatAdversaryTest(t, seed, hc, maxRounds*2, repeatBoundedQuality)
			repeatAdversaryTest(t, seed, hc, maxRounds*2, repeatBoundedCommit)
		}
	})
}

func repeatAdversaryTest(t *testing.T, seed int, honestCount int, maxRounds uint64, repetitionSampler func(int) adversary.RepetitionSampler) {
	rng := rand.New(rand.NewSource(int64(seed)))
	dist := repetitionSampler(seed)
	sm, err := sim.NewSimulation(asyncOptions(rng.Int(),
		sim.AddHonestParticipants(
			honestCount,
			sim.NewUniformECChainGenerator(rng.Uint64(), 1, 10),
			uniformOneStoragePower),
		sim.WithAdversary(adversary.NewRepeatGenerator(oneStoragePower, dist)),
	)...)
	require.NoError(t, err)
	require.NoErrorf(t, sm.Run(1, maxRounds), "%s", sm.Describe())
}

func newBoundedRepeater(rngSeed int64, min, max int) adversary.RepetitionSampler {
	rng := rand.New(rand.NewSource(rngSeed))
	return func(*gpbft.GMessage) int {
		return int(rng.Uint64())%(max-min+1) + min
	}
}
