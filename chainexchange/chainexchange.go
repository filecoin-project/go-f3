package chainexchange

import (
	"context"

	"github.com/filecoin-project/go-f3/gpbft"
)

type Key []byte

type Keyer interface {
	Key(gpbft.ECChain) Key
}

type Message struct {
	Instance uint64
	Chain    gpbft.ECChain
}

type ChainExchange interface {
	Keyer
	Broadcast(context.Context, Message) error
	GetChainByInstance(context.Context, uint64, Key) (gpbft.ECChain, bool)
	RemoveChainsByInstance(context.Context, uint64) error
}

func (k Key) IsZero() bool { return len(k) == 0 }
