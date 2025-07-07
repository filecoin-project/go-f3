package f3

import (
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/manifest"
)

var _ ec.TipSet = (*tipset)(nil)

func TestComputeBootstrapDelay(t *testing.T) {
	period := 30 * time.Second
	bootstrapEpoch := 1000
	m := manifest.Manifest{
		BootstrapEpoch: int64(bootstrapEpoch),
		EC: manifest.EcConfig{
			Period: period,
		},
	}

	clock := clock.NewMock()
	genesis := time.Date(2020, time.January, 12, 01, 01, 01, 00, time.UTC)

	tt := []struct {
		name string
		time time.Time
		ts   tipset
		want time.Duration
	}{
		{
			name: "in sync - right before bootstrap",
			time: genesis.Add(time.Duration(bootstrapEpoch-1) * period),
			ts:   tipset{genesis: genesis, epoch: int64(bootstrapEpoch - 1), period: period},
			want: period,
		},
		{
			name: "in sync - right at bootstrap",
			time: genesis.Add(time.Duration(bootstrapEpoch) * period),
			ts:   tipset{genesis: genesis, epoch: int64(bootstrapEpoch), period: period},
			want: 0,
		},
		{
			name: "in sync - right after bootstrap",
			time: genesis.Add(time.Duration(bootstrapEpoch+1) * period),
			ts:   tipset{genesis: genesis, epoch: int64(bootstrapEpoch + 1), period: period},
			want: 0,
		},
		{
			name: "in sync - right before bootstrap (offset)",
			time: genesis.Add(time.Duration(bootstrapEpoch-1)*period + 15*time.Second),
			ts:   tipset{genesis: genesis, epoch: int64(bootstrapEpoch - 1), period: period},
			want: 15 * time.Second,
		},
		{
			name: "in sync - right after bootstrap (offset)",
			time: genesis.Add(time.Duration(bootstrapEpoch)*period + 1*time.Second),
			ts:   tipset{genesis: genesis, epoch: int64(bootstrapEpoch), period: period},
			want: 0 * time.Second,
		},
		{
			name: "out of sync - way after bootstrap",
			time: genesis.Add(time.Duration(bootstrapEpoch+100)*period + 1*time.Second),
			ts:   tipset{genesis: genesis, epoch: int64(bootstrapEpoch - 100), period: period},
			want: 1 * time.Nanosecond, // we don't start immediately as the tipset we need is not available yet
		},
		{
			name: "out of sync - way before bootstrap",
			time: genesis.Add(time.Duration(bootstrapEpoch-30)*period + 1*time.Second),
			ts:   tipset{genesis: genesis, epoch: int64(bootstrapEpoch - 100), period: period},
			want: 30*period - 1*time.Second,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			clock.Set(tc.time)
			got := computeBootstrapDelay(&tc.ts, clock, m)
			if got != tc.want {
				t.Errorf("computeBootstrapDelay(%s, %v, %v) = %v, want %v", &tc.ts, tc.time, period, got, tc.want)
			}
		})
	}
}
