package polling_test

import (
	"context"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certexchange/polling"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/sim/signing"

	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	mocknetwork "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestSubscriber(t *testing.T) {
	backend := signing.NewFakeBackend()
	rng := rand.New(rand.NewSource(1234))

	certificates, powerTable := polling.MakeCertificates(t, rng, backend)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mocknet := mocknetwork.New()

	clientHost, err := mocknet.GenPeer()
	require.NoError(t, err)

	servers := make([]*certexchange.Server, 100)
	for i := range servers {
		h, err := mocknet.GenPeer()
		require.NoError(t, err)

		ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
		cs, err := certstore.CreateStore(ctx, ds, 0, powerTable)
		require.NoError(t, err)

		servers[i] = &certexchange.Server{
			NetworkName: polling.TestNetworkName,
			Host:        h,
			Store:       cs,
			Log:         polling.TestLog,
		}
	}

	require.NoError(t, mocknet.LinkAll())

	for _, server := range servers {
		require.NoError(t, server.Start())
		t.Cleanup(func() { require.NoError(t, server.Stop()) })
	}

	clientDs := ds_sync.MutexWrap(datastore.NewMapDatastore())
	clientCs, err := certstore.CreateStore(ctx, clientDs, 0, powerTable)
	require.NoError(t, err)

	client := certexchange.Client{
		Host:        clientHost,
		NetworkName: polling.TestNetworkName,
		Log:         polling.TestLog,
	}

	subscriber := polling.Subscriber{
		Client:              client,
		Store:               clientCs,
		SignatureVerifier:   backend,
		MinimumPollInterval: time.Millisecond,
		MaximumPollInterval: time.Second,
		InitialPollInterval: 100 * time.Millisecond,
	}

	require.NoError(t, subscriber.Start())

	t.Cleanup(func() { require.NoError(t, subscriber.Stop()) })

	require.NoError(t, mocknet.ConnectAllButSelf())

	for _, s := range servers {
		require.NoError(t, s.Store.Put(ctx, certificates[0]))
	}

	polling.MockClock.Add(100 * time.Millisecond)

	require.Eventually(t, func() bool {
		return clientCs.Latest() != nil
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, uint64(0), clientCs.Latest().GPBFTInstance)

	// Slowly drop servers from the network
	liveServers := slices.Clone(servers)

	// Let the network run fine for a few rounds.
	nextInstance := uint64(1)
	for ; nextInstance < 10; nextInstance++ {
		for _, s := range liveServers {
			require.NoError(t, s.Store.Put(ctx, certificates[nextInstance]))
		}

		i := 0
		require.Eventually(t, func() bool {
			i += 10
			polling.MockClock.Add(10 * time.Millisecond)
			return clientCs.Latest().GPBFTInstance == nextInstance
		}, 10*time.Second, time.Millisecond)

		polling.MockClock.Add(time.Duration(max(0, 100-i)) * time.Millisecond)
	}

	// Then kill 20% of the network every three instances.
	for ; len(liveServers) > 0; nextInstance++ {
		for _, s := range liveServers {
			require.NoError(t, s.Store.Put(ctx, certificates[nextInstance]))
		}

		i := 0
		require.Eventually(t, func() bool {
			i += 10
			polling.MockClock.Add(10 * time.Millisecond)
			return clientCs.Latest().GPBFTInstance == uint64(nextInstance)
		}, 10*time.Second, time.Millisecond)

		polling.MockClock.Add(time.Duration(max(0, 100-i)) * time.Millisecond)

		// Every 4 instances, stop updating 20% of the network.
		if nextInstance%4 == 0 {
			rand.Shuffle(len(liveServers), func(a, b int) {
				liveServers[a], liveServers[b] = liveServers[b], liveServers[a]
			})
			liveServers = liveServers[:8*len(liveServers)/10]
		}

	}
}
