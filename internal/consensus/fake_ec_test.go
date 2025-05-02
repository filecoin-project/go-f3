package consensus_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/consensus"
	"github.com/stretchr/testify/require"
)

func TestTipsetString(t *testing.T) {
	ctx := context.Background()
	subject := consensus.NewFakeEC()
	ts, err := subject.GetHead(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, ts.String())
}

func TestTipsetReproducibility(t *testing.T) {
	const sampleSize = 3
	sampleTipsets := func(opts ...consensus.FakeECOption) []ec.TipSet {
		ctx, clk := clock.WithMockClock(context.Background())
		subject := consensus.NewFakeEC(append(opts, consensus.WithClock(clk))...)
		var samples []ec.TipSet
		for i := 0; i < sampleSize; i++ {
			ts, err := subject.GetHead(ctx)
			require.NoError(t, err)
			samples = append(samples, ts)
			clk.Add(5 * time.Minute)
		}
		return samples
	}

	for _, test := range []struct {
		name            string
		opts            []consensus.FakeECOption
		anotherOpts     []consensus.FakeECOption
		wantConsistency bool
	}{
		{
			name:            "seeded with no forkiness",
			opts:            []consensus.FakeECOption{consensus.WithSeed(1413)},
			wantConsistency: true,
		},
		{
			name: "seeded with forkiness",
			opts: []consensus.FakeECOption{
				consensus.WithSeed(1413),
				consensus.WithForkSeed(1414),
				consensus.WithForkAfterEpochs(3),
			},
			wantConsistency: true,
		},
		{
			name: "seeded with different forkiness",
			opts: []consensus.FakeECOption{
				consensus.WithSeed(1413),
				consensus.WithForkSeed(1414),
				consensus.WithForkAfterEpochs(3),
			},
			anotherOpts: []consensus.FakeECOption{
				consensus.WithSeed(1413),
				consensus.WithForkSeed(1415),
				consensus.WithForkAfterEpochs(3),
			},
			wantConsistency: false,
		},
		{
			name: "different seed with same forkiness",
			opts: []consensus.FakeECOption{
				consensus.WithSeed(1413),
				consensus.WithForkSeed(1414),
				consensus.WithForkAfterEpochs(3),
			},
			anotherOpts: []consensus.FakeECOption{
				consensus.WithSeed(1414),
				consensus.WithForkSeed(1414),
				consensus.WithForkAfterEpochs(3),
			},
			wantConsistency: false,
		},
		{
			name:            "unseeded",
			wantConsistency: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.anotherOpts == nil {
				test.anotherOpts = test.opts
			}
			one := sampleTipsets(test.opts...)
			another := sampleTipsets(test.anotherOpts...)
			require.Len(t, one, sampleSize)
			require.Len(t, another, sampleSize)
			if test.wantConsistency {
				for i, got := range one {
					want := another[i]
					require.Equal(t, got.String(), want.String(), "tipset %d", i)
				}
			} else {
				require.NotEqual(t, one, another)
			}
		})
	}
}

func TestFakeEC_Finalization(t *testing.T) {
	ctx, clk := clock.WithMockClock(context.Background())
	const (
		seed            = 1413
		bootstrapEpoch  = 1
		ecPeriod        = 1 * time.Second
		ecElapsed       = time.Minute
		forkAfterEpochs = 3
		finalizedEpoch  = 30
	)
	subject := consensus.NewFakeEC(
		consensus.WithClock(clk),
		consensus.WithSeed(seed),
		consensus.WithECPeriod(ecPeriod),
		consensus.WithBootstrapEpoch(bootstrapEpoch),
		consensus.WithForkSeed(seed*7),
		consensus.WithForkAfterEpochs(forkAfterEpochs),
	)
	alternative := consensus.NewFakeEC(
		consensus.WithClock(clk),
		consensus.WithSeed(seed),
		consensus.WithECPeriod(ecPeriod),
		consensus.WithForkSeed(seed*23),
		consensus.WithBootstrapEpoch(bootstrapEpoch),
		consensus.WithForkSeed(seed*31),
		consensus.WithForkAfterEpochs(1),
	)
	clk.Add(ecElapsed)

	// Assert that alternative EC generates different tipsets.
	for i := range finalizedEpoch + forkAfterEpochs {

		epoch := int64(i)
		one, err := subject.GetTipsetByEpoch(ctx, epoch)
		require.NoError(t, err)
		other, err := alternative.GetTipsetByEpoch(ctx, epoch)
		require.NoError(t, err)
		if epoch <= bootstrapEpoch {
			require.Equal(t, one, other)
		} else {
			require.NotEqual(t, one, other, "epoch %d", epoch)
		}
	}

	// Now generate an alternative tipset at epoch 30 and assert that the subject obeys the finalized tipset according to fork after epochs.
	finalizer, err := alternative.GetTipsetByEpoch(ctx, finalizedEpoch)
	require.NoError(t, err)
	require.NoError(t, subject.Finalize(ctx, finalizer.Key()))
	for i := range ecElapsed / ecPeriod {
		epoch := int64(i)
		one, err := subject.GetTipsetByEpoch(ctx, epoch)
		require.NoError(t, err)
		other, err := alternative.GetTipsetByEpoch(ctx, epoch)
		require.NoError(t, err)
		if epoch <= finalizedEpoch {
			require.Equal(t, one, other, "epoch %d", epoch)
		}
	}
}
