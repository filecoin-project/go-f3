package powerstore_test

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/consensus"
	"github.com/filecoin-project/go-f3/internal/powerstore"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

var basePowerTable = gpbft.PowerEntries{
	{ID: 1, Power: gpbft.NewStoragePower(50), PubKey: gpbft.PubKey("1")},
	{ID: 3, Power: gpbft.NewStoragePower(10), PubKey: gpbft.PubKey("2")},
	{ID: 4, Power: gpbft.NewStoragePower(4), PubKey: gpbft.PubKey("3")},
}

func TestPowerStore(t *testing.T) {
	ctx, clk := clock.WithMockClock(context.Background())

	m := manifest.LocalDevnetManifest()

	ec := consensus.NewFakeEC(ctx,
		consensus.WithNullTipsetProbablity(0),
		consensus.WithMaxLookback(2*m.EC.Finality),
		consensus.WithBootstrapEpoch(m.BootstrapEpoch),
		consensus.WithECPeriod(m.EC.Period),
		consensus.WithInitialPowerTable(basePowerTable),
		consensus.WithEvolvingPowerTable(func(epoch int64, pt gpbft.PowerEntries) gpbft.PowerEntries {
			pt = slices.Clone(pt)
			newPower := gpbft.NewStoragePower(epoch)
			pt[0].Power = big.Add(newPower, pt[0].Power)
			return pt
		}),
	)

	head, err := ec.GetHead(ctx)
	require.NoError(t, err)
	require.Equal(t, m.BootstrapEpoch, head.Epoch())

	bsTs, err := ec.GetTipsetByEpoch(ctx, m.BootstrapEpoch-m.EC.Finality)
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

	clk.Add(m.EC.Period * time.Duration(m.EC.Finality/2))
	time.Sleep(10 * time.Millisecond)
	clk.Add(m.EC.Period * time.Duration(m.EC.Finality))

	head, err = ec.GetHead(ctx)
	require.NoError(t, err)
	require.Equal(t, m.BootstrapEpoch+(3*m.EC.Finality)/2, head.Epoch())

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

	// We should still be able to get the og old power table.
	actualPt, err = ps.GetPowerTable(ctx, oldTs.Key())
	require.NoError(t, err)
	require.Equal(t, expectedPt, actualPt)

	// And the bootstrap power table
	actualPt, err = ps.GetPowerTable(ctx, bsTs.Key())
	require.NoError(t, err)
	require.Equal(t, pt, actualPt)

	// But if we go _way_ back to before F3 finality, it fails.
	veryEarlyTs, err := ec.GetTipsetByEpoch(ctx, 50)
	require.NoError(t, err)
	_, err = ps.GetPowerTable(ctx, veryEarlyTs.Key())
	require.Error(t, err)

	// Ok, now we're going to try falling behind and catching up a few times.

	isDsEmpty := func() bool {
		res, err := ds.Query(ctx, query.Query{Prefix: "/ohshitstore"})
		require.NoError(t, err)
		defer res.Close()
		_, ok := res.NextSync()
		return !ok
	}

	epochsPerCert := 1
	for i := 0; i < 3; i++ {
		// Advance a few finalities.
		for j := 0; j < 3; j++ {
			clk.Add(m.EC.Period * time.Duration(m.EC.Finality))
			time.Sleep(10 * time.Millisecond)
		}

		// We should have power diffs in the datastore.
		require.False(t, isDsEmpty())

		// Now catch up. I need the current power base epoch to be within 0.5
		// finality of head.
		head, err := ec.GetHead(ctx)
		require.NoError(t, err)
		oldEnough, err := ec.GetTipsetByEpoch(ctx, head.Epoch()-m.EC.Finality/3)
		require.NoError(t, err)

		advanceF3(t, m, ps, cs, oldEnough.Epoch(), epochsPerCert)
		clk.Add(4 * m.EC.Period)

		// On the first two passes, we should eventually clear the datastore. On the last
		// pass, base is 990 epochs behind which is more than the max 450 to cleanup. So we
		// need to advance a bit more with smaller instances. That'll bring the base back
		// into a reasonable range.
		if i == 2 {
			clk.Add(m.EC.Period * time.Duration(m.EC.Finality))
			advanceF3(t, m, ps, cs, oldEnough.Epoch()+m.EC.Finality, 10)
			clk.Add(4 * m.EC.Period)
		}
		require.Eventually(t, func() bool { return isDsEmpty() }, 10*time.Second, 10*time.Millisecond)

		// by 1, 10, then 100 (well, 99).
		epochsPerCert *= 10
	}

}

func advanceF3(t *testing.T, m manifest.Manifest, ps *powerstore.Store, cs *certstore.Store, until int64, epochsPerCert int) {
	instance := uint64(0)
	base := m.BootstrapEpoch - m.EC.Finality
	if latest := cs.Latest(); latest != nil {
		instance = latest.GPBFTInstance + 1
		base = latest.ECChain.Head().Epoch
	}

	ctx := context.Background()
	ts, err := ps.GetTipsetByEpoch(ctx, until)
	require.NoError(t, err)

	chain := make([]ec.TipSet, 0, ts.Epoch())
	chain = append(chain, ts)
	for ts.Epoch() > base {
		ts, err = ps.GetParent(ctx, ts)
		require.NoError(t, err)
		chain = append(chain, ts)
	}
	slices.Reverse(chain)

	if len(chain) == 0 {
		return
	}

	require.Equal(t, base, chain[0].Epoch())

	gpbftChain := &gpbft.ECChain{}
	for _, ts := range chain {
		pt, err := ps.GetPowerTable(ctx, ts.Key())
		require.NoError(t, err)
		ptcid, err := certs.MakePowerTableCID(pt)
		require.NoError(t, err)
		gpbftChain = gpbftChain.Append(&gpbft.TipSet{
			Epoch:      ts.Epoch(),
			Key:        ts.Key(),
			PowerTable: ptcid,
		})
	}

	basePt, err := cs.GetPowerTable(ctx, instance)
	require.NoError(t, err)
	for gpbftChain.Len() > 1 {
		count := min(gpbftChain.Len(), rand.IntN(epochsPerCert+1)+1, gpbft.ChainDefaultLen)
		newChain := gpbftChain.Prefix(count)

		nextPt := basePt
		if instance+1 >= m.InitialInstance+m.CommitteeLookback {
			ptCert, err := cs.Get(ctx, instance+1-m.CommitteeLookback)
			require.NoError(t, err)
			nextPt, err = ps.GetPowerTable(ctx, ptCert.ECChain.Head().Key)
			require.NoError(t, err)
		}
		ptCid, err := certs.MakePowerTableCID(nextPt)
		require.NoError(t, err)

		supp := gpbft.SupplementalData{PowerTable: ptCid}

		cert := &certs.FinalityCertificate{
			GPBFTInstance:    instance,
			ECChain:          newChain,
			SupplementalData: supp,
			PowerTableDelta:  certs.MakePowerTableDiff(basePt, nextPt),
		}
		require.NoError(t, cs.Put(ctx, cert))

		basePt = nextPt
		gpbftChain = &gpbft.ECChain{TipSets: gpbftChain.TipSets[count-1:]}
		instance++
	}
}
