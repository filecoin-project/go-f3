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

	certificates, powerTable := makeCertificates(t, rng, backend)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mocknet := mocknetwork.New()
	const serverCount = 100

	clientHost, err := mocknet.GenPeer()
	require.NoError(t, err)

	serverHost, err := mocknet.GenPeer()
	require.NoError(t, err)

	require.NoError(t, mocknet.LinkAll())

	serverDs := ds_sync.MutexWrap(datastore.NewMapDatastore())

	serverCs, err := certstore.CreateStore(ctx, serverDs, 0, powerTable)
	require.NoError(t, err)

	certificatesAdded := 10
	for _, cert := range certificates[:certificatesAdded] {
		require.NoError(t, serverCs.Put(ctx, cert))
	}

	server := certexchange.Server{
		NetworkName: testNetworkName,
		Host:        serverHost,
		Store:       serverCs,
		Log:         log,
	}

	require.NoError(t, server.Start())
	defer server.Stop()

	clientDs := ds_sync.MutexWrap(datastore.NewMapDatastore())
	clientCs, err := certstore.CreateStore(ctx, clientDs, 0, powerTable)
	require.NoError(t, err)

	client := certexchange.Client{
		Host:        clientHost,
		NetworkName: testNetworkName,
		Log:         log,
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
		require.Equal(t, polling.PollHit, res)
		require.Equal(t, uint64(certificatesAdded), poller.NextInstance)
	}

	// If we put a certificate on the client, we should call it a _miss_
	{
		require.NoError(t, clientCs.Put(ctx, certificates[certificatesAdded]))

		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollMiss, res)
	}

	// Add that cert to the server.
	require.NoError(t, serverCs.Put(ctx, certificates[certificatesAdded]))
	certificatesAdded++

	// And now it's a hit!
	{
		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollHit, res)
	}

	// Add more than the request maximum (up till the last cert)
	for ; certificatesAdded < len(certificates)-1; certificatesAdded++ {
		require.NoError(t, serverCs.Put(ctx, certificates[certificatesAdded]))
	}

	// We should poll multiple times and completely catch up.
	{
		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollHit, res)
		require.Equal(t, uint64(certificatesAdded), poller.NextInstance)
	}

	// We catch evil servers!
	{
		lastCert := certificates[certificatesAdded]
		lastCert.Signature = []byte("bad sig")
		require.NoError(t, serverCs.Put(ctx, lastCert))

		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollIllegal, res)

		// And we don't store certificates from them!
		require.Equal(t, uint64(certificatesAdded), poller.NextInstance)
		_, err = clientCs.Get(ctx, lastCert.GPBFTInstance)
		require.ErrorIs(t, err, certstore.ErrCertNotFound)
	}

	// Stop the server, and make sure we get a failure.
	server.Stop()

	{
		res, err := poller.Poll(ctx, serverHost.ID())
		require.NoError(t, err)
		require.Equal(t, polling.PollFailed, res)
	}
}
