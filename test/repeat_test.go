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

func FuzzRepeatAdversary(f *testing.F) {
	tests := []struct {
		name              string
		repetitionSampler func(int) adversary.RepetitionSampler
		maxRounds         uint64
	}{
		{
			name:              "once",
			repetitionSampler: repeatOnce,
			maxRounds:         maxRounds,
		},
		{
			name:              "bounded uniform random",
			repetitionSampler: repeatBoundedRandom,
			maxRounds:         maxRounds,
		},
		{
			name:              "zipf",
			repetitionSampler: repeatZipF,
			maxRounds:         maxRounds,
		},
		{
			name:              "QUALITY Repeater",
			repetitionSampler: repeatBoundedQuality,
			maxRounds:         maxRounds * 2,
		},
		{
			name:              "COMMIT Repeater",
			repetitionSampler: repeatBoundedCommit,
			maxRounds:         maxRounds * 2,
		},
	}
	f.Add(68465)
	f.Add(-5)
	f.Add(-5454)
	f.Fuzz(func(t *testing.T, seed int) {
		t.Parallel()
		for _, hc := range repeatAdversaryTestHonestCounts {
			hc := hc
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()
					rng := rand.New(rand.NewSource(int64(seed)))
					dist := test.repetitionSampler(seed)
					sm, err := sim.NewSimulation(asyncOptions(rng.Int(),
						sim.AddHonestParticipants(
							hc,
							sim.NewUniformECChainGenerator(rng.Uint64(), 1, 4),
							uniformOneStoragePower),
						sim.WithAdversary(adversary.NewRepeatGenerator(oneStoragePower, dist)),
					)...)
					require.NoError(t, err)
					require.NoErrorf(t, sm.Run(1, test.maxRounds), "%s", sm.Describe())
				})
			}
		}
	})
}

func newBoundedRepeater(rngSeed int64, min, max int) adversary.RepetitionSampler {
	rng := rand.New(rand.NewSource(rngSeed))
	return func(*gpbft.GMessage) int {
		return int(rng.Uint64())%(max-min+1) + min
	}
}
