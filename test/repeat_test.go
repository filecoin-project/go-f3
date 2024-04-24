package test

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func TestRepeat(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	honestCounts := []int{
		2, // 1/3 adversary power
		3, // 1/4 adversary power
		4, // 1/5 adversary power
	}
	tests := []struct {
		name              string
		repetitionSampler func(int) adversary.RepetitionSampler
		maxRounds         uint64
	}{
		{
			name: "once",
			repetitionSampler: func(int) adversary.RepetitionSampler {
				return func(*gpbft.GMessage) int {
					return 1
				}
			},
			maxRounds: MAX_ROUNDS,
		},
		{
			name: "bounded uniform random",
			repetitionSampler: func(repetition int) adversary.RepetitionSampler {
				return newBoundedRepeater(int64(repetition), 10, 50)
			},
			maxRounds: MAX_ROUNDS,
		},
		{
			name: "zipf",
			repetitionSampler: func(repetition int) adversary.RepetitionSampler {
				rng := rand.New(rand.NewSource(int64(repetition)))
				zipf := rand.NewZipf(rng, 1.2, 1.0, 100)
				return func(*gpbft.GMessage) int {
					return int(zipf.Uint64())
				}
			},
			maxRounds: MAX_ROUNDS,
		},
		{
			name: "QUALITY Repeater",
			repetitionSampler: func(repetition int) adversary.RepetitionSampler {
				boundedRepeater := newBoundedRepeater(int64(repetition), 10, 50)
				return func(msg *gpbft.GMessage) int {
					if msg.Vote.Step != gpbft.QUALITY_PHASE {
						return 0
					}
					return boundedRepeater(msg)
				}
			},
			maxRounds: MAX_ROUNDS,
		},
		{
			name: "COMMIT Repeater",
			repetitionSampler: func(repetition int) adversary.RepetitionSampler {
				boundedRepeater := newBoundedRepeater(int64(repetition), 10, 50)
				return func(msg *gpbft.GMessage) int {
					if msg.Vote.Step != gpbft.COMMIT_PHASE {
						return 0
					}
					return boundedRepeater(msg)
				}
			},
			maxRounds: 100,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for _, hc := range honestCounts {
				repeatInParallel(t, ASYNC_ITERS, func(t *testing.T, repetition int) {
					sm := sim.NewSimulation(AsyncConfig(hc, repetition), GraniteConfig(), sim.TraceNone)
					dist := test.repetitionSampler(repetition)
					repeat := adversary.NewRepeat(99, sm.HostFor(99), dist)
					sm.SetAdversary(repeat, 1)

					a := sm.Base(0).Extend(sm.TipGen.Sample())
					sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

					require.NoErrorf(t, sm.Run(1, test.maxRounds), "%s", sm.Describe())
				})
			}

		})
	}
}

func newBoundedRepeater(rngSeed int64, min, max int) adversary.RepetitionSampler {
	rng := rand.New(rand.NewSource(rngSeed))
	return func(*gpbft.GMessage) int {
		return int(rng.Uint64())%(max-min+1) + min
	}
}
