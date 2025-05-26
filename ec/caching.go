package ec

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/singleflight"
	lru "github.com/hashicorp/golang-lru/v2"
)

type PowerCachingECWrapper struct {
	Backend

	cache *lru.Cache[string, gpbft.PowerEntries]

	smaphore chan struct{}
	dedup    singleflight.Group[gpbft.PowerEntries]
}

func NewPowerCachingECWrapper(backend Backend, concurrency int, cacheSize int) *PowerCachingECWrapper {
	cache, err := lru.New[string, gpbft.PowerEntries](cacheSize)
	if err != nil {
		panic(err)
	}
	smaphore := make(chan struct{}, concurrency)

	return &PowerCachingECWrapper{
		Backend:  backend,
		cache:    cache,
		smaphore: smaphore,
	}
}

func (p *PowerCachingECWrapper) GetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	entry, ok := p.cache.Get(string(tsk))
	if ok {
		return entry, nil
	}

	ch := p.dedup.DoChan(string(tsk),
		// break context cancellation chain as the dedup group might start with short context and then get called with longer one
		func() (gpbft.PowerEntries, error) { return p.executeGetPowerTable(context.WithoutCancel(ctx), tsk) })

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val, nil
	}
}

func (p *PowerCachingECWrapper) executeGetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	// take semaphore
	p.smaphore <- struct{}{}
	defer func() { <-p.smaphore }()

	res, err := p.Backend.GetPowerTable(ctx, tsk)
	if err != nil {
		return nil, fmt.Errorf("getting power table: %w", err)
	}

	p.cache.Add(string(tsk), res)
	return res, nil
}
