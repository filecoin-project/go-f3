package chainexchange_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/chainexchange"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/stretchr/testify/require"
)

func TestPubSubChainExchange_Broadcast(t *testing.T) {
	const topicName = "fish"
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	var testInstant gpbft.Instant
	var testListener listener
	host, err := libp2p.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, host.Close())
	})

	ps, err := pubsub.NewGossipSub(ctx, host, pubsub.WithFloodPublish(true))
	require.NoError(t, err)

	subject, err := chainexchange.NewPubSubChainExchange(
		chainexchange.WithProgress(func() (instant gpbft.Instant) {
			return testInstant
		}),
		chainexchange.WithPubSub(ps),
		chainexchange.WithTopicName(topicName),
		chainexchange.WithTopicScoreParams(nil),
		chainexchange.WithListener(&testListener),
	)
	require.NoError(t, err)
	require.NotNil(t, subject)

	err = subject.Start(ctx)
	require.NoError(t, err)

	instance := uint64(1)
	ecChain := gpbft.ECChain{
		{Epoch: 0, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
		{Epoch: 1, Key: []byte("barreleye"), PowerTable: gpbft.MakeCid([]byte("pt"))},
	}

	key := subject.Key(ecChain)
	chain, found := subject.GetChainByInstance(ctx, instance, key)
	require.False(t, found)
	require.Nil(t, chain)
	require.Empty(t, testListener.notifications)

	require.NoError(t, subject.Broadcast(ctx, chainexchange.Message{
		Instance: instance,
		Chain:    ecChain,
	}))

	chain, found = subject.GetChainByInstance(ctx, instance, key)
	require.True(t, found)
	require.Equal(t, ecChain, chain)

	baseChain := ecChain.BaseChain()
	baseKey := subject.Key(baseChain)
	chain, found = subject.GetChainByInstance(ctx, instance, baseKey)
	require.True(t, found)
	require.Equal(t, baseChain, chain)

	// Assert that we have received 2 notifications, because ecChain has 2 tipsets.
	// First should be the ecChain, second should be the baseChain.
	require.Len(t, testListener.notifications, 2)
	require.Equal(t, instance, testListener.notifications[1].instance)
	require.Equal(t, baseKey, testListener.notifications[1].key)
	require.Equal(t, baseChain, testListener.notifications[1].chain)
	require.Equal(t, instance, testListener.notifications[0].instance)
	require.Equal(t, key, testListener.notifications[0].key)
	require.Equal(t, ecChain, testListener.notifications[0].chain)

	require.NoError(t, subject.Shutdown(ctx))
}

type notification struct {
	key      chainexchange.Key
	instance uint64
	chain    gpbft.ECChain
}
type listener struct {
	notifications []notification
}

func (l *listener) NotifyChainDiscovered(_ context.Context, key chainexchange.Key, instance uint64, chain gpbft.ECChain) {
	l.notifications = append(l.notifications, notification{key: key, instance: instance, chain: chain})
}

// TODO: Add more tests, specifically:
//        - validation
//        - discovery through other chainexchange instance
//        - cache eviction/fixed memory footprint.
//        - fulfilment of chain from discovery to wanted in any order.
//        - spam
//        - fuzz
