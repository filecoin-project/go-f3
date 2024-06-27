package ec

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

type Backend interface {
	// GetTipsetByEpoch should return the tipset immediately before the one requested.
	// If the epoch requested is null, it returns the latest not-null one.
	GetTipsetByEpoch(ctx context.Context, epoch int64) (TipSet, error)
	// GetTipset returns the tipset with the given key.
	GetTipset(context.Context, gpbft.TipSetKey) (TipSet, error)
	// GetHead returns the current head tipset of the chain
	GetHead(context.Context) (TipSet, error)
	// GetParent returns the parent of the current tipset.
	GetParent(context.Context, TipSet) (TipSet, error)
	// GetPowerTable returns the power table at the tipset given as an argument.
	GetPowerTable(context.Context, gpbft.TipSetKey) (gpbft.PowerEntries, error)
}

type TipSet interface {
	Key() gpbft.TipSetKey
	Beacon() []byte
	Epoch() int64
	Timestamp() time.Time
}
