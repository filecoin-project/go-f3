package ec

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
)

func WithModifiedPower(backend Backend, delta []certs.PowerTableDelta) Backend {
	return &withModifiedPower{
		Backend: backend,
		delta:   delta,
	}
}

type withModifiedPower struct {
	Backend
	delta []certs.PowerTableDelta
}

func (b *withModifiedPower) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	pt, err := b.Backend.GetPowerTable(ctx, ts)
	if err != nil {
		return nil, fmt.Errorf("getting power table: %w", err)
	}
	return certs.ApplyPowerTableDiffs(pt, b.delta)
}
