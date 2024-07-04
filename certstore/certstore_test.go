package certstore

import (
	"context"
	"slices"
	"testing"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	datastore "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func testPowerTable(entries int64) (gpbft.PowerEntries, gpbft.CID) {
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

func TestNewCertStore(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, _ := testPowerTable(10)
	_, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)
}

func TestLatest(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, _ := testPowerTable(10)
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	latest := cs.Latest()
	require.Nil(t, latest)
}

func TestPut(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	cert := &certs.FinalityCertificate{GPBFTInstance: 0, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.Error(t, err)

	cert = &certs.FinalityCertificate{GPBFTInstance: 1, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	cert = &certs.FinalityCertificate{GPBFTInstance: 3, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	_, err = cs.Get(ctx, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertNotFound)

	cert := &certs.FinalityCertificate{GPBFTInstance: 1, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	fetchedCert, err := cs.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), fetchedCert.GPBFTInstance)
}

func TestGetRange(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	_, err = cs.GetRange(ctx, 1, 5)
	require.Error(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &certs.FinalityCertificate{GPBFTInstance: i, SupplementalData: supp}
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

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	ch := make(chan *certs.FinalityCertificate, 1)
	_, closer := cs.SubscribeForNewCerts(ch)
	defer closer()

	cert := &certs.FinalityCertificate{GPBFTInstance: 1, SupplementalData: supp}
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

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	cert := &certs.FinalityCertificate{GPBFTInstance: 1, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	cert = &certs.FinalityCertificate{GPBFTInstance: 2, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest = cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(2), latest.GPBFTInstance)
}

func TestPutSequential(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &certs.FinalityCertificate{GPBFTInstance: i, SupplementalData: supp}
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	cert := &certs.FinalityCertificate{GPBFTInstance: 7, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestPersistency(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs1, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := &certs.FinalityCertificate{GPBFTInstance: i, SupplementalData: supp}
		err := cs1.Put(ctx, cert)
		require.NoError(t, err)
	}

	cs2, err := OpenStore(ctx, ds)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert, err := cs2.Get(ctx, i)
		require.NoError(t, err)
		require.Equal(t, i, cert.GPBFTInstance)
	}

	require.Equal(t, cs1.Latest().GPBFTInstance, cs2.Latest().GPBFTInstance)
}

func TestClear(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	cert := &certs.FinalityCertificate{GPBFTInstance: 1, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	cert = &certs.FinalityCertificate{GPBFTInstance: 2, SupplementalData: supp}
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest = cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(2), latest.GPBFTInstance)

	err = cs.Delete(ctx, 1)
	require.NoError(t, err)
	_, err = cs.Get(ctx, 1)
	require.Error(t, err, ErrCertNotFound)

	err = cs.DeleteAll(ctx)
	require.NoError(t, err)
	_, err = OpenStore(ctx, ds)
	require.Error(t, err)
}

func TestCreateOpen(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, _ := testPowerTable(20)
	newPt := slices.Clone(pt)
	newPt[0].PubKey = []byte("other key")

	// Cannot open if it doesn't exist.
	_, err := OpenStore(ctx, ds)
	require.Error(t, err)

	// Can create with create-or-open
	_, err = OpenOrCreateStore(ctx, ds, 0, pt)
	require.NoError(t, err)

	// Cannot re-create with "create"
	_, err = CreateStore(ctx, ds, 0, pt)
	require.Error(t, err)

	// Can re-open with "open or create".
	_, err = OpenOrCreateStore(ctx, ds, 0, pt)
	require.NoError(t, err)

	// Instance must agree
	_, err = OpenOrCreateStore(ctx, ds, 1, pt)
	require.Error(t, err)

	// Power table must agree
	_, err = OpenOrCreateStore(ctx, ds, 0, newPt)
	require.Error(t, err)
}

func TestPowerNoData(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
	pt, _ := testPowerTable(20)

	cs, err := CreateStore(ctx, ds, 0, pt)
	require.NoError(t, err)
	cs.powerTableFrequency = 5
	// clobber the datastore.
	cs.ds = ds_sync.MutexWrap(datastore.NewMapDatastore())

	// Should fail to find any power tables.
	_, err = cs.GetPowerTable(ctx, 0)
	require.ErrorContains(t, err, "failed to load power table")
	_, err = cs.GetPowerTable(ctx, 1)
	require.ErrorContains(t, err, "cannot return future power table")

}

func TestPowerEmptyPowerTable(t *testing.T) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
	pt, _ := testPowerTable(5)

	_, err := CreateStore(ctx, ds, 0, nil)
	require.ErrorContains(t, err, "empty initial power table")

	_, err = OpenOrCreateStore(ctx, ds, 0, nil)
	require.ErrorContains(t, err, "empty initial power table")

	cs, err := CreateStore(ctx, ds, 0, pt)
	require.NoError(t, err)

	emptyPt := gpbft.PowerEntries{}
	emptyPtCid, err := certs.MakePowerTableCID(emptyPt)
	require.NoError(t, err)

	cert := &certs.FinalityCertificate{
		GPBFTInstance:    0,
		PowerTableDelta:  certs.MakePowerTableDiff(pt, emptyPt),
		SupplementalData: gpbft.SupplementalData{PowerTable: emptyPtCid},
	}
	err = cs.Put(ctx, cert)
	require.ErrorContains(t, err, "empty the power table")

}

func TestPower(t *testing.T) {
	t.Run("Offset", func(t *testing.T) {
		testPowerInner(t, 2)
	})
	t.Run("NoOffset", func(t *testing.T) {
		testPowerInner(t, 0)
	})
}
func testPowerInner(t *testing.T, firstInstance uint64) {
	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(20)
	newPt := slices.Clone(pt)
	newPt[0].PubKey = []byte("other key")
	newPtCid, err := certs.MakePowerTableCID(newPt)
	require.NoError(t, err)

	cs, err := CreateStore(ctx, ds, firstInstance, pt)
	require.NoError(t, err)

	cs.powerTableFrequency = 5

	// Before we insert any finality certificates.
	{
		actualPt, err := cs.GetPowerTable(ctx, firstInstance)
		require.NoError(t, err)
		require.Equal(t, pt, actualPt)

		_, err = cs.GetPowerTable(ctx, firstInstance+1)
		require.ErrorContains(t, err, "cannot return future power table")
	}

	for i := uint64(firstInstance); i < 8; i++ {
		cert := &certs.FinalityCertificate{
			GPBFTInstance:    i,
			SupplementalData: gpbft.SupplementalData{PowerTable: ptCid},
		}
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	{
		cert := &certs.FinalityCertificate{
			GPBFTInstance:    8,
			PowerTableDelta:  certs.MakePowerTableDiff(pt, newPt),
			SupplementalData: gpbft.SupplementalData{PowerTable: newPtCid},
		}
		err = cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	for i := uint64(9); i < 13; i++ {
		cert := &certs.FinalityCertificate{
			GPBFTInstance:    i,
			SupplementalData: gpbft.SupplementalData{PowerTable: newPtCid},
		}
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	for i := uint64(firstInstance); i < 7; i += 2 {
		actualPt, err := cs.GetPowerTable(ctx, i)
		require.NoError(t, err)
		require.Equal(t, pt, actualPt)
	}

	for i := uint64(9); i <= 13; i++ {
		actualPt, err := cs.GetPowerTable(ctx, i)
		require.NoError(t, err)
		require.NotEqual(t, pt, actualPt)
		require.Equal(t, newPt, actualPt)
	}

	// Future power table.
	{
		// Doesn't exist.
		_, err = cs.GetPowerTable(ctx, 14)
		require.ErrorContains(t, err, "cannot return future power table")

		// Insert a finality certificate.
		cert := &certs.FinalityCertificate{
			GPBFTInstance:    13,
			SupplementalData: gpbft.SupplementalData{PowerTable: newPtCid},
		}
		err = cs.Put(ctx, cert)
		require.NoError(t, err)

		// Exists now
		actualPt, err := cs.GetPowerTable(ctx, 14)
		require.NoError(t, err)
		require.Equal(t, newPt, actualPt)
	}

	if firstInstance > 0 {
		_, err = cs.GetPowerTable(ctx, firstInstance-1)
		require.ErrorContains(t, err, "cannot return a power table before the first instance")
		_, err = cs.GetPowerTable(ctx, 0)
		require.ErrorContains(t, err, "cannot return a power table before the first instance")
	}
}
