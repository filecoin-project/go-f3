package chainexchange

import (
	"context"

	"github.com/filecoin-project/go-f3/gpbft"
)

type Message struct {
	Instance  uint64
	Chain     *gpbft.ECChain
	Timestamp int64
}

type ChainExchange interface {
	Broadcast(context.Context, Message) error
	GetChainByInstance(context.Context, uint64, gpbft.ECChainKey) (*gpbft.ECChain, bool)
	RemoveChainsByInstance(context.Context, uint64) error
}

type Listener interface {
	NotifyChainDiscovered(ctx context.Context, instance uint64, chain *gpbft.ECChain)
}
