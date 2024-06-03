package certstore

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-f3/certs"
	datastore "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func TestNewCertStore(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	_, err := NewStore(ctx, ds)
	require.NoError(t, err)
}

func TestLatest(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewStore(ctx, ds)
	require.NoError(t, err)

	latest := cs.Latest()
	require.Nil(t, latest)
}

func TestPut(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewStore(ctx, ds)
	require.NoError(t, err)

	cert := &certs.FinalityCertificate{GPBFTInstance: 1}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	cert = &certs.FinalityCertificate{GPBFTInstance: 3}
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewStore(ctx, ds)
	require.NoError(t, err)

	_, err = cs.Get(ctx, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertNotFound)

	cert := &certs.FinalityCertificate{GPBFTInstance: 1}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	fetchedCert, err := cs.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), fetchedCert.GPBFTInstance)
}

func TestGetRange(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewStore(ctx, ds)
	require.NoError(t, err)

	_, err = cs.GetRange(ctx, 1, 5)
	require.Error(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &certs.FinalityCertificate{GPBFTInstance: i}
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	certs, err := cs.GetRange(ctx, 1, 5)
	require.NoError(t, err)
	require.Len(t, certs, 5)
}

func TestSubscribeForNewCerts(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewStore(ctx, ds)
	require.NoError(t, err)

	ch := make(chan *certs.FinalityCertificate, 1)
	_, closer := cs.SubscribeForNewCerts(ch)
	defer closer()

	cert := &certs.FinalityCertificate{GPBFTInstance: 1}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	select {
	case received, ok := <-ch:
		require.True(t, ok, "channel should not close")
		require.Equal(t, cert, received)
	default:
		t.FailNow()
	}
}

func TestLatestAfterPut(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewStore(ctx, ds)
	require.NoError(t, err)

	cert := &certs.FinalityCertificate{GPBFTInstance: 1}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	cert = &certs.FinalityCertificate{GPBFTInstance: 2}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest = cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(2), latest.GPBFTInstance)
}

func TestPutSequential(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewStore(ctx, ds)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &certs.FinalityCertificate{GPBFTInstance: i}
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	cert := &certs.FinalityCertificate{GPBFTInstance: 7}
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestPersistency(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs1, err := NewStore(ctx, ds)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &certs.FinalityCertificate{GPBFTInstance: i}
		err := cs1.Put(ctx, cert)
		require.NoError(t, err)
	}

	cs2, err := NewStore(ctx, ds)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert, err := cs2.Get(ctx, i)
		require.NoError(t, err)
		require.Equal(t, i, cert.GPBFTInstance)
	}

	require.Equal(t, cs1.Latest().GPBFTInstance, cs2.Latest().GPBFTInstance)
}
