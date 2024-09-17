package certstore

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func makeCert(instance uint64, supp gpbft.SupplementalData) *certs.FinalityCertificate {
	return &certs.FinalityCertificate{
		GPBFTInstance:    instance,
		SupplementalData: supp,
		ECChain:          gpbft.ECChain{{Epoch: 0, Key: gpbft.TipSetKey("tsk0"), PowerTable: supp.PowerTable}},
	}
}

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

func TestNewCertStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, _ := testPowerTable(10)
	_, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)
}

func TestLatest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, _ := testPowerTable(10)
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	latest := cs.Latest()
	require.Nil(t, latest)
}

func TestPut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	cert := makeCert(0, supp)
	err = cs.Put(ctx, cert)
	require.Error(t, err)

	cert = makeCert(1, supp)
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	cert = makeCert(3, supp)
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestPutWrongPowerDelta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 0, pt)
	require.NoError(t, err)

	// Make sure we sanity check the produced power table.
	cert := makeCert(0, supp)
	cert.PowerTableDelta = certs.PowerTableDiff{{
		ParticipantID: 0,
		PowerDelta:    gpbft.NewStoragePower(10),
		SigningKey:    []byte("testkey"),
	}}

	err = cs.Put(ctx, cert)
	require.ErrorContains(t, err, "new power table differs from expected")
}

func TestGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	_, err = cs.Get(ctx, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertNotFound)

	cert := makeCert(1, supp)
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	fetchedCert, err := cs.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), fetchedCert.GPBFTInstance)
}

func TestGetRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	_, err = cs.GetRange(ctx, 1, 5)
	require.Error(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := makeCert(i, supp)
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	certs, err := cs.GetRange(ctx, 1, 5)
	require.NoError(t, err)
	require.Len(t, certs, 5)

	// Can get one cert
	certs, err = cs.GetRange(ctx, 1, 1)
	require.NoError(t, err)
	require.Len(t, certs, 1)

	// Start cannot be after end.
	_, err = cs.GetRange(ctx, 1, 0)
	require.ErrorContains(t, err, "start is larger than end")

	// Make sure we don't have any overflow issues.
	_, err = cs.GetRange(ctx, 0, math.MaxInt)
	require.ErrorContains(t, err, "is too large")
}

func TestDeleteAll(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := makeCert(i, supp)
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	require.NoError(t, cs.DeleteAll(ctx))
	_, err = OpenStore(ctx, ds)
	require.ErrorIs(t, err, ErrNotInitialized)

	verifyEmpty := func() {
		res, err := ds.Query(ctx, query.Query{})
		require.NoError(t, err)
		_, ok := res.NextSync()
		require.False(t, ok, "there should be nothing in the datastore")
	}
	verifyEmpty()

	// create it again
	_, err = CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	// put in the tombstone, as if we crashed during DeleteAll
	require.NoError(t, ds.Put(ctx, tombstoneKey, []byte("AAAAAAAA")))
	// re-opening should delete and fail
	_, err = OpenStore(ctx, ds)
	require.ErrorIs(t, err, ErrNotInitialized)
	verifyEmpty()
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	ch, closer := cs.Subscribe()
	defer closer()

	cert := makeCert(1, supp)
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
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	cert := makeCert(1, supp)
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	cert = makeCert(2, supp)
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest = cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(2), latest.GPBFTInstance)
}

func TestPutSequential(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := makeCert(i, supp)
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
	}

	cert := makeCert(7, supp)
	err = cs.Put(ctx, cert)
	require.Error(t, err)
}

func TestPersistency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs1, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		cert := makeCert(i, supp)
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
	t.Parallel()

	ctx := context.Background()
	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())

	pt, ptCid := testPowerTable(10)
	supp := gpbft.SupplementalData{PowerTable: ptCid}
	cs, err := CreateStore(ctx, ds, 1, pt)
	require.NoError(t, err)

	cert := makeCert(1, supp)
	err = cs.Put(ctx, cert)
	require.NoError(t, err)

	latest := cs.Latest()
	require.NotNil(t, latest)
	require.Equal(t, uint64(1), latest.GPBFTInstance)

	cert = makeCert(2, supp)
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
	t.Parallel()

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
	t.Parallel()
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
	t.Parallel()
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

	cert := makeCert(0, gpbft.SupplementalData{PowerTable: emptyPtCid})
	cert.PowerTableDelta = certs.MakePowerTableDiff(pt, emptyPt)
	err = cs.Put(ctx, cert)
	require.ErrorContains(t, err, "empty the power table")

}

func TestPower(t *testing.T) {
	t.Parallel()
	t.Run("Offset", func(t *testing.T) {
		t.Parallel()
		testPowerInner(t, 2)
	})
	t.Run("NoOffset", func(t *testing.T) {
		t.Parallel()
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
		cert := makeCert(i, gpbft.SupplementalData{PowerTable: ptCid})
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
		_, err = cs.GetPowerTable(ctx, i+1)
		require.NoError(t, err)
	}

	{
		cert := makeCert(8, gpbft.SupplementalData{PowerTable: newPtCid})
		cert.PowerTableDelta = certs.MakePowerTableDiff(pt, newPt)
		err = cs.Put(ctx, cert)
		require.NoError(t, err)
		_, err = cs.GetPowerTable(ctx, cert.GPBFTInstance+1)
		require.NoError(t, err)
	}

	for i := uint64(9); i < 13; i++ {
		cert := makeCert(i, gpbft.SupplementalData{PowerTable: newPtCid})
		err := cs.Put(ctx, cert)
		require.NoError(t, err)
		_, err = cs.GetPowerTable(ctx, i+1)
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
		cert := makeCert(13, gpbft.SupplementalData{PowerTable: newPtCid})
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
