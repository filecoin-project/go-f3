package f3

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/go-f3/certexchange"
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

	ts, err := ec.GetTipsetByEpoch(ctx, m.BootstrapEpoch-m.EC.Finality)
	var initialPowerTable gpbft.PowerEntries

	if err == nil {
		initialPowerTable, err = ec.GetPowerTable(ctx, ts.Key())
		if err != nil {
			return nil, fmt.Errorf("getting initial power table: %w", err)
		}
	} else if m.InitialPowerTable.Defined() {
		log.Errorw("could not get initial power table from EC, trying finality exchange", "error", err)
		initialPowerTable, err = certexchange.FindInitialPowerTable(ctx, certClient,
			m.InitialPowerTable, m.EC.Period)

		if err != nil {
			log.Errorw("could not get initial power table from finality exchange", "error", err)
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("getting initial power tipset: %w", err)
	}

	return certstore.CreateStore(ctx, ds, m.InitialInstance, initialPowerTable)
}
