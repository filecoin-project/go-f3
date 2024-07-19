package manifest

import (
	"context"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	mocknetwork "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestDynamicManifest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Cleanup(cancel)

	mocknet := mocknetwork.New()
	initialManifest := LocalDevnetManifest()

	var (
		sender   *ManifestSender
		provider *DynamicManifestProvider
	)

	{
		host, err := mocknet.GenPeer()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, host.Close()) })

		pubSub, err := pubsub.NewGossipSub(ctx, host, pubsub.WithPeerExchange(true))
		require.NoError(t, err)
		sender, err = NewManifestSender(ctx, host, pubSub, initialManifest, 10*time.Millisecond)
		require.NoError(t, err)
	}

	{
		host, err := mocknet.GenPeer()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, host.Close()) })

		pubSub, err := pubsub.NewGossipSub(ctx, host, pubsub.WithPeerExchange(true))
		require.NoError(t, err)

		provider = NewDynamicManifestProvider(initialManifest, pubSub, sender.SenderID())
	}

	err := mocknet.LinkAll()
	require.NoError(t, err)
	err = mocknet.ConnectAllButSelf()
	require.NoError(t, err)

	waitSender := make(chan error, 1)
	senderCtx, cancelSender := context.WithCancel(ctx)
	go func() { waitSender <- sender.Run(senderCtx) }()

	require.NoError(t, provider.Start(ctx))
	t.Cleanup(func() { require.NoError(t, provider.Stop(context.Background())) })

	// Should receive the initial manifest.
	require.True(t, initialManifest.Equal(<-provider.ManifestUpdates()))

	// Pausing should send nil.
	sender.Pause()
	require.Nil(t, <-provider.ManifestUpdates())

	// Should get the initial manifest again.
	sender.Resume()
	require.True(t, initialManifest.Equal(<-provider.ManifestUpdates()))

	cancelSender()
	require.Nil(t, <-waitSender)

	// Re-start the sender. The client shouldn't see an update.
	senderCtx, cancelSender = context.WithCancel(ctx)
	go func() { waitSender <- sender.Run(senderCtx) }()

	select {
	case <-provider.ManifestUpdates():
		t.Fatal("did not expect a manifest update when restarting manifest sender")
	case <-time.After(1 * time.Second):
	}
	newManifest := *initialManifest
	newManifest.NetworkName = "updated-name"
	sender.UpdateManifest(&newManifest)

	require.True(t, newManifest.Equal(<-provider.ManifestUpdates()))

	cancelSender()
	require.NoError(t, <-waitSender)
}
