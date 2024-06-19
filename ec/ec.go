package ec

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

type Backend interface {
	// GetTipsetByEpoch should return a tipset before the one requested if the requested
	// tipset does not exist due to null epochs
	GetTipsetByEpoch(ctx context.Context, epoch int64) (TipSet, error)
	GetTipset(context.Context, gpbft.TipSetKey) (TipSet, error)
	GetHead(context.Context) (TipSet, error)
	GetParent(context.Context, TipSet) (TipSet, error)

	GetPowerTable(context.Context, gpbft.TipSetKey) (gpbft.PowerEntries, error)
}

type TipSet interface {
	Key() gpbft.TipSetKey
	Beacon() []byte
	Epoch() int64
	Timestamp() time.Time
}
