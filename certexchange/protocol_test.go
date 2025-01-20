package certexchange_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/gpbft"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	mocknetwork "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

const testNetworkName gpbft.NetworkName = "testnet"

func testPowerTable(entries int64) (gpbft.PowerEntries, cid.Cid) {
	powerTable := make(gpbft.PowerEntries, entries)

	for i := range powerTable {
		powerTable[i] = gpbft.PowerEntry{
			ID:     gpbft.ActorID(i + 1),
			Power:  gpbft.NewStoragePower(int64(len(powerTable)*2 - i/2)),
			PubKey: []byte("fake key"),
		}
	}
	k, err := certs.MakePowerTableCID(powerTable)
	if err != nil {
		panic(err)
	}
	return powerTable, k
}

func TestClientServer(t *testing.T) {
	mocknet := mocknetwork.New()
	h1, err := mocknet.GenPeer()
	require.NoError(t, err)
	h2, err := mocknet.GenPeer()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, mocknet.LinkAll())

	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
	pt, pcid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: pcid}

	cs, err := certstore.CreateStore(ctx, ds, 0, pt)
	require.NoError(t, err)

	cert := &certs.FinalityCertificate{GPBFTInstance: 0, SupplementalData: supp,
		ECChain: &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: gpbft.TipSetKey("tsk0"), PowerTable: pcid},
			},
		},
	}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	cert = &certs.FinalityCertificate{GPBFTInstance: 1, SupplementalData: supp,
		ECChain: &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: gpbft.TipSetKey("tsk0"), PowerTable: pcid},
			},
		},
	}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	server := certexchange.Server{
		NetworkName: testNetworkName,
		Host:        h1,
		Store:       cs,
	}

	client := certexchange.Client{
		Host:        h2,
		NetworkName: testNetworkName,
	}

	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() { require.NoError(t, server.Stop(context.Background())) })

	require.NoError(t, mocknet.ConnectAllButSelf())

	// No certificates, but get the power table.
	{
		head, certs, err := client.Request(ctx, h1.ID(), &certexchange.Request{
			FirstInstance:     0,
			Limit:             0,
			IncludePowerTable: true,
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, head.PendingInstance)
		require.EqualValues(t, pt, head.PowerTable)
		require.Empty(t, certs) // limit == 0
	}

	// All certificates and no power table.
	{
		head, certs, err := client.Request(ctx, h1.ID(), &certexchange.Request{
			FirstInstance:     0,
			Limit:             certexchange.NoLimit,
			IncludePowerTable: false,
		})

		require.NoError(t, err)
		require.EqualValues(t, 2, head.PendingInstance)
		require.Nil(t, head.PowerTable)

		expectInstance := uint64(0)
		for c := range certs {
			require.Equal(t, expectInstance, c.GPBFTInstance)
			expectInstance++
		}
		require.Equal(t, expectInstance, head.PendingInstance)
	}

	// Just one certificate
	{
		_, certs, err := client.Request(ctx, h1.ID(), &certexchange.Request{
			FirstInstance: 0,
			Limit:         1,
		})
		require.NoError(t, err)

		require.Equal(t, uint64(0), (<-certs).GPBFTInstance)
		require.Empty(t, certs)
	}

	// Should be able to get the power table beyond the last instance.
	{
		head, certs, err := client.Request(ctx, h1.ID(), &certexchange.Request{
			FirstInstance:     2,
			Limit:             certexchange.NoLimit,
			IncludePowerTable: true,
		})
		require.NoError(t, err)
		require.EqualValues(t, pt, head.PowerTable)
		require.Empty(t, certs)
	}

	// Should get nothing beyond that
	{
		head, certs, err := client.Request(ctx, h1.ID(), &certexchange.Request{
			FirstInstance:     3,
			Limit:             certexchange.NoLimit,
			IncludePowerTable: true,
		})
		require.NoError(t, err)
		require.Nil(t, head.PowerTable)
		require.Empty(t, certs)
	}

	// Until we've added a new certificate.
	cert = &certs.FinalityCertificate{GPBFTInstance: 2, SupplementalData: supp,
		ECChain: &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: gpbft.TipSetKey("tsk0"), PowerTable: pcid},
			},
		},
	}
	require.NoError(t, cs.Put(ctx, cert))

	{
		_, certs, err := client.Request(ctx, h1.ID(), &certexchange.Request{
			FirstInstance: 2,
			Limit:         certexchange.NoLimit,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(2), (<-certs).GPBFTInstance)
		require.Empty(t, certs)
	}

	{
		head, certs, err := client.Request(ctx, h1.ID(), &certexchange.Request{
			FirstInstance:     3,
			Limit:             certexchange.NoLimit,
			IncludePowerTable: true,
		})
		require.NoError(t, err)
		require.EqualValues(t, pt, head.PowerTable)
		require.Empty(t, certs)
	}

	{
		ptCid, err := certs.MakePowerTableCID(pt)
		require.NoError(t, err)

		pt2, err := certexchange.FindInitialPowerTable(ctx, client, ptCid, 1*time.Second)
		require.NoError(t, err)
		require.EqualValues(t, pt, pt2)
	}
}
