package test

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/adversary"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

func TestRepeat(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	// Repeated messages should not interfere with consensus. Hence, test with larger than 1/3 adversary power.
	honestCounts := []int{
		1, // 1/2 adversary power
		2, // 1/3 adversary power
		3, // 1/4 adversary power
	}
	tests := []struct {
		name          string
		honestCount   []int
		echoCountDist func(int) adversary.CountSampler // Distribution from which the number of echos is drawn.
	}{
		{
			name: "uniform random",
			echoCountDist: func(repetition int) adversary.CountSampler {
				return uniformRandomRange{
					rng: rand.New(rand.NewSource(int64(repetition))),
					min: 10,
					max: 50,
				}
			},
		},
		{
			name: "zipf",
			echoCountDist: func(repetition int) adversary.CountSampler {
				rng := rand.New(rand.NewSource(int64(repetition)))
				return rand.NewZipf(rng, 1.2, 1.0, 100)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for _, hc := range honestCounts {
				repeatInParallel(t, ASYNC_ITERS, func(t *testing.T, repetition int) {
					sm := sim.NewSimulation(AsyncConfig(hc, repetition), GraniteConfig(), sim.TraceNone)
					dist := test.echoCountDist(repetition)
					repeat := adversary.NewRepeat(99, sm.HostFor(99), dist)
					sm.SetAdversary(repeat, 1)

					a := sm.Base(0).Extend(sm.TipGen.Sample())
					sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

					require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
				})
			}

		})
	}
}

var _ adversary.CountSampler = (*uniformRandomRange)(nil)

type uniformRandomRange struct {
	rng      *rand.Rand
	min, max uint64
}

func (r uniformRandomRange) Uint64() uint64 {
	return r.rng.Uint64()%(r.max-r.min+1) + r.min
}
