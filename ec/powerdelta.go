package ec

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/filecoin-project/go-f3/gpbft"
)

func WithModifiedPower(backend Backend, explicitPower gpbft.PowerEntries, ignoreEcPower bool) Backend {
	if len(explicitPower) == 0 && !ignoreEcPower {
		return backend
	}

	explicitPower = slices.Clone(explicitPower)
	sort.Sort(explicitPower)

	if ignoreEcPower {
		return &withReplacedPower{
			Backend: backend,
			power:   trimPowerTable(explicitPower),
		}
	}

	index := make(map[gpbft.ActorID]int, len(explicitPower))
	for i, entry := range explicitPower {
		index[entry.ID] = i
	}
	return &withModifiedPower{
		Backend:       backend,
		explicit:      explicitPower,
		explicitIndex: index,
	}
}

type withReplacedPower struct {
	Backend
	power gpbft.PowerEntries
}

func (b *withReplacedPower) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return b.power, nil
}

type withModifiedPower struct {
	Backend
	explicit      gpbft.PowerEntries
	explicitIndex map[gpbft.ActorID]int
}

func (b *withModifiedPower) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	pt, err := b.Backend.GetPowerTable(ctx, ts)
	if err != nil {
		return nil, fmt.Errorf("getting power table: %w", err)
	}
	pt = slices.Clone(pt)
	index := maps.Clone(b.explicitIndex)
	for i := range pt {
		e := &pt[i]
		if idx, ok := index[e.ID]; ok {
			*e = b.explicit[idx]
			delete(index, e.ID)
		}
	}
	for _, idx := range index {
		pt = append(pt, b.explicit[idx])
	}
	sort.Sort(pt)
	return trimPowerTable(pt), nil
}

func trimPowerTable(pt gpbft.PowerEntries) gpbft.PowerEntries {
	newLen := len(pt)
	for newLen > 0 && pt[newLen-1].Power.Sign() == 0 {
		newLen--
	}
	return pt[:newLen]
}
