package certs

import (
	"context"
	"testing"

	datastore "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func TestNewCertStore(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	_, err := NewCertStore(ctx, ds)
	require.NoError(t, err)
}

func TestLatest(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	latest := cs.Latest()
	require.Nil(t, latest)
}

func TestPut(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	cert := &Cert{Instance: 1}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.Instance)

	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	cert = &Cert{Instance: 3}
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	_, err = cs.Get(ctx, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertNotFound)

	cert := &Cert{Instance: 1}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	fetchedCert, err := cs.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), fetchedCert.Instance)
}

func TestGetRange(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	_, err = cs.GetRange(ctx, 1, 5)
	require.Error(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &Cert{Instance: i}
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

	cs, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	ch := make(chan *Cert, 1)
	_, closer := cs.SubscribeForNewCerts(ch)
	defer closer()

	cert := &Cert{Instance: 1}
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

	cs, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	cert := &Cert{Instance: 1}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.Instance)

	cert = &Cert{Instance: 2}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest = cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(2), latest.Instance)
}

func TestPutSequential(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &Cert{Instance: i}
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	cert := &Cert{Instance: 7}
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestPersistency(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	cs1, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &Cert{Instance: i}
		err := cs1.Put(ctx, cert)
		require.NoError(t, err)
	}

	cs2, err := NewCertStore(ctx, ds)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert, err := cs2.Get(ctx, i)
		require.NoError(t, err)
		require.Equal(t, i, cert.Instance)
	}

	require.Equal(t, cs1.Latest().Instance, cs2.Latest().Instance)
}
