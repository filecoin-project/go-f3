package f3

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

// openCertstore opens the certificate store for the specific manifest (namespaced by the network
// name).
func openCertstore(ctx context.Context, ec ec.Backend, ds datastore.Datastore,
	m *manifest.Manifest, certClient certexchange.Client) (*certstore.Store, error) {
	ds = namespace.Wrap(ds, m.DatastorePrefix())

	if cs, err := certstore.OpenStore(ctx, ds); err == nil {
		return cs, nil
	} else if !errors.Is(err, certstore.ErrNotInitialized) {
		return nil, err
	}

	var initialPowerTable gpbft.PowerEntries
	initialPowerTable, err := loadInitialPowerTable(ctx, ec, m, certClient)
	if err != nil {
		return nil, fmt.Errorf("getting initial power table: %w", err)
	}

	return certstore.CreateStore(ctx, ds, m.InitialInstance, initialPowerTable)
}

func loadInitialPowerTable(ctx context.Context, ec ec.Backend, m *manifest.Manifest, certClient certexchange.Client) (gpbft.PowerEntries, error) {
	epoch := m.BootstrapEpoch - m.EC.Finality
	if ts, err := ec.GetTipsetByEpoch(ctx, epoch); err != nil {
		// This is odd because we usually keep the entire chain, just not the state.
		// Odd but not fatal here.
		log.Warnw("failed to find bootstrap tipset for F3", "error", err, "epoch", epoch)
	} else if pt, err := ec.GetPowerTable(ctx, ts.Key()); err != nil {
		log.Debugw("failed to load the bootstrap power table for F3 from state", "error", err)
	} else {
		if ptCid, err := certs.MakePowerTableCID(pt); err == nil {
			log.Infof("loaded initial power table at epoch %d: %s", epoch, ptCid)
		}
		return pt, nil
	}
	if !m.InitialPowerTable.Defined() {
		return nil, fmt.Errorf("failed to load the F3 bootstrap power table and none is specified in the manifest")
	}

	log.Infow("loading the F3 bootstrap power table", "epoch", epoch, "cid", m.InitialPowerTable)

	pt, err := certexchange.FindInitialPowerTable(ctx, certClient, m.InitialPowerTable, m.EC.Period)
	if err != nil {
		return nil, fmt.Errorf("could not get initial power table from finality exchange: %w", err)
	}
	return pt, nil
}
