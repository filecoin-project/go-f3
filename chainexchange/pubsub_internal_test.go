package chainexchange

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
)

func newTestChainExchange() *PubSubChainExchange {
	return &PubSubChainExchange{
		options: &options{
			maxDiscoveredChainsPerInstance: 10,
			maxWantedChainsPerInstance:     10,
		},
		chainsWanted:     make(map[uint64]*lru.Cache[gpbft.ECChainKey, *chainPortion]),
		chainsDiscovered: make(map[uint64]*lru.Cache[gpbft.ECChainKey, *chainPortion]),
	}
}

// TestCacheAsDiscoveredChain_PromotesWantedPlaceholder asserts the intended
// contract of cacheAsDiscoveredChain: a chain discovery that matches a wanted
// placeholder must replace that placeholder in the wanted cache, and must not
// be stored in the discovered cache, since the wanted cache already tracks it.
func TestCacheAsDiscoveredChain_PromotesWantedPlaceholder(t *testing.T) {
	ctx := context.Background()
	const instance = uint64(7)

	subject := newTestChainExchange()
	chain := &gpbft.ECChain{
		TipSets: []*gpbft.TipSet{
			{Epoch: 1, Key: []byte("a"), PowerTable: gpbft.MakeCid([]byte("pt"))},
			{Epoch: 2, Key: []byte("b"), PowerTable: gpbft.MakeCid([]byte("pt"))},
		},
	}
	key := chain.Key()

	wanted := subject.getChainsWantedAt(ctx, instance)
	wanted.Add(key, chainPortionPlaceHolder)

	subject.cacheAsDiscoveredChain(ctx, Message{Instance: instance, Chain: chain})

	portion, found := wanted.Get(key)
	require.True(t, found, "wanted key must remain in the wanted cache")
	require.False(t, portion.IsPlaceholder(), "wanted placeholder must be replaced with the discovered chain")

	discovered := subject.getChainsDiscoveredAt(ctx, instance)
	_, foundInDiscovered := discovered.Peek(key)
	require.False(t, foundInDiscovered, "wanted chain must not also be cached as discovered")
}
