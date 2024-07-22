package powerstore_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/powerstore"
	"github.com/filecoin-project/go-f3/manifest"

	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

type forgetfulEC struct {
	*ec.FakeEC

	ecFinality int64
}

// GetPowerTable implements ec.Backend.
func (f *forgetfulEC) GetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	ts, err := f.GetTipset(ctx, tsk)
	if err != nil {
		return nil, err
	}
	head, err := f.GetHead(ctx)
	if err != nil {
		return nil, err
	}
	if ts.Epoch() < head.Epoch()-2*f.ecFinality {
		return nil, fmt.Errorf("oops, we forgot that power table, head %d, epoch %d", head.Epoch(), ts.Epoch())
	}
	pt, err := f.FakeEC.GetPowerTable(ctx, tsk)
	if err != nil {
		return nil, err
	}

	// make sure power changes over time by adding the current epoch to the first entry.
	pt = slices.Clone(pt)
	newPower := gpbft.NewStoragePower(ts.Epoch())
	pt[0].Power = newPower.Add(newPower, pt[0].Power)

	return pt, nil
}

var _ ec.Backend = (*forgetfulEC)(nil)

var basePowerTable = gpbft.PowerEntries{
	{ID: 1, Power: gpbft.NewStoragePower(50), PubKey: gpbft.PubKey("1")},
	{ID: 3, Power: gpbft.NewStoragePower(10), PubKey: gpbft.PubKey("2")},
	{ID: 4, Power: gpbft.NewStoragePower(4), PubKey: gpbft.PubKey("3")},
}

func TestPowerStore(t *testing.T) {
	ctx, clk := clock.WithMockClock(context.Background())

	m := manifest.LocalDevnetManifest()

	ec := &forgetfulEC{
		FakeEC:     ec.NewFakeEC(ctx, 1234, m.BootstrapEpoch, m.ECPeriod, basePowerTable, true),
		ecFinality: m.ECFinality,
	}

	head, err := ec.GetHead(ctx)
	require.NoError(t, err)
	require.Equal(t, m.BootstrapEpoch, head.Epoch())

	bsTs, err := ec.GetTipsetByEpoch(ctx, m.BootstrapEpoch-m.ECFinality)
	require.NoError(t, err)

	pt, err := ec.GetPowerTable(ctx, bsTs.Key())
	require.NoError(t, err)

	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
	cs, err := certstore.CreateStore(ctx, ds, m.InitialInstance, pt)
	require.NoError(t, err)

	ps, err := powerstore.New(ctx, ec, ds, cs, m)
	require.NoError(t, err)
	require.NoError(t, ps.Start(ctx))
	t.Cleanup(func() { require.NoError(t, ps.Stop(context.Background())) })

	oldTs, err := ec.GetTipsetByEpoch(ctx, 200)
	require.NoError(t, err)

	expectedPt, err := ec.GetPowerTable(ctx, oldTs.Key())
	require.NoError(t, err)

	clk.Add(m.ECPeriod * time.Duration(m.ECFinality/2))
	time.Sleep(10 * time.Millisecond)
	clk.Add(m.ECPeriod * time.Duration(m.ECFinality))

	head, err = ec.GetHead(ctx)
	require.NoError(t, err)
	require.Equal(t, m.BootstrapEpoch+(3*m.ECFinality)/2, head.Epoch())

	_, err = ec.GetPowerTable(ctx, oldTs.Key())
	require.Error(t, err)

	actualPt, err := ps.GetPowerTable(ctx, oldTs.Key())
	require.NoError(t, err)
	require.Equal(t, expectedPt, actualPt)

	// Stop/restart.
	require.NoError(t, ps.Stop(ctx))
	ps, err = powerstore.New(ctx, ec, ds, cs, m)
	require.NoError(t, err)
	require.NoError(t, ps.Start(ctx))
	time.Sleep(10 * time.Millisecond)

	// Advance time a bit making sure we can still get old power tables.
	for i := 0; i < 5; i++ {
		head, err = ec.GetHead(ctx)
		require.NoError(t, err)
		oldTs2, err := ec.GetTipsetByEpoch(ctx, head.Epoch()-m.ECFinality-1)
		require.NoError(t, err)
		expectedPt2, err := ps.GetPowerTable(ctx, oldTs2.Key())
		require.NoError(t, err)

		clk.Add(m.ECPeriod * time.Duration(m.ECFinality))
		time.Sleep(10 * time.Millisecond)

		// We should still be able to get the og old power table.
		actualPt, err = ps.GetPowerTable(ctx, oldTs.Key())
		require.NoError(t, err)
		require.Equal(t, expectedPt, actualPt)

		// And the bootstrap power table
		actualPt, err = ps.GetPowerTable(ctx, bsTs.Key())
		require.NoError(t, err)
		require.Equal(t, pt, actualPt)

		// And the "new" old power table.
		actualPt2, err := ps.GetPowerTable(ctx, oldTs2.Key())
		require.NoError(t, err)
		require.Equal(t, expectedPt2, actualPt2)

		// Which EC has now forgotten.
		_, err = ec.GetPowerTable(ctx, oldTs2.Key())
		require.Error(t, err)
	}

	// But if we go _way_ back to before F3 finality, it fails.

	veryEarlyTs, err := ec.GetTipsetByEpoch(ctx, 50)
	require.NoError(t, err)
	_, err = ps.GetPowerTable(ctx, veryEarlyTs.Key())
	require.Error(t, err)
}
