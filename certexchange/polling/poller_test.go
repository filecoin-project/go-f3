package polling_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certexchange/polling"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/sim/signing"

	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	mocknetwork "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestPoller(t *testing.T) {
	backend := signing.NewFakeBackend()
	rng := rand.New(rand.NewSource(1234))

	cg := polling.MakeCertificates(t, rng, backend)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mocknet := mocknetwork.New()

	clientHost, err := mocknet.GenPeer()
	require.NoError(t, err)

	serverHost, err := mocknet.GenPeer()
	require.NoError(t, err)

	require.NoError(t, mocknet.LinkAll())

	serverDs := ds_sync.MutexWrap(datastore.NewMapDatastore())

	serverCs, err := certstore.CreateStore(ctx, serverDs, 0, cg.PowerTable)
	require.NoError(t, err)

	for cg.NextInstance < 10 {
		require.NoError(t, serverCs.Put(ctx, cg.MakeCertificate()))
	}

	server := certexchange.Server{
		NetworkName: polling.TestNetworkName,
		Host:        serverHost,
		Store:       serverCs,
	}

	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() { require.NoError(t, server.Stop(context.Background())) })

	clientDs := ds_sync.MutexWrap(datastore.NewMapDatastore())
	clientCs, err := certstore.CreateStore(ctx, clientDs, 0, cg.PowerTable)
	require.NoError(t, err)

	client := certexchange.Client{
		Host:        clientHost,
		NetworkName: polling.TestNetworkName,
	}

	poller, err := polling.NewPoller(ctx, &client, clientCs, backend)
	require.NoError(t, err)

	require.NoError(t, mocknet.ConnectAllButSelf())

	// Client should start with a clean slate.
	{
		i, err := poller.CatchUp(ctx)
		require.NoError(t, err)
		require.Zero(t, i)
		require.Zero(t, poller.NextInstance)
	}

	// Should catch up. Doing this twice should be considered a hit both times because the
	// server is at least as caught up as us.
	for i := 0; i < 2; i++ {
		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollHit, res.Status)
		require.Equal(t, cg.NextInstance, poller.NextInstance)
	}

	// If we put a certificate on the client, we should call it a _miss_
	{
		cert := cg.MakeCertificate()
		require.NoError(t, clientCs.Put(ctx, cert))

		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollMiss, res.Status)

		// Add that cert to the server.
		require.NoError(t, serverCs.Put(ctx, cert))
	}

	// And now it's a hit!
	{
		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollHit, res.Status)
	}

	// Add more than the request maximum (up till the last cert)
	for cg.NextInstance < 500 {
		require.NoError(t, serverCs.Put(ctx, cg.MakeCertificate()))
	}

	// We should poll multiple times and completely catch up.
	{
		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollHit, res.Status)
		require.Equal(t, cg.NextInstance, poller.NextInstance)
	}

	// We catch evil servers!
	{
		badCert := cg.MakeCertificate()
		badCert.Signature = []byte("bad sig")
		require.NoError(t, serverCs.Put(ctx, badCert))

		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollIllegal, res.Status)

		// And we don't store certificates from them!
		require.Equal(t, badCert.GPBFTInstance, poller.NextInstance)
		_, err = clientCs.Get(ctx, badCert.GPBFTInstance)
		require.ErrorIs(t, err, certstore.ErrCertNotFound)
	}

	// Stop the server, and make sure we get a failure.
	require.NoError(t, server.Stop(ctx))

	{
		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollFailed, res.Status)
	}
}
