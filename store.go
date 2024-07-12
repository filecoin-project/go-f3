package f3

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/manifest"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

// openCertstore opens the certificate store for the specific manifest (namespaced by the network
// name).
func openCertstore(ctx context.Context, ec ec.Backend, ds datastore.Datastore, m *manifest.Manifest) (*certstore.Store, error) {

	ds = namespace.Wrap(ds, m.DatastorePrefix())

	ts, err := ec.GetTipsetByEpoch(ctx, m.BootstrapEpoch-m.ECFinality)
	if err != nil {
		return nil, fmt.Errorf("getting initial power tipset: %w", err)
	}

	initialPowerTable, err := ec.GetPowerTable(ctx, ts.Key())
	if err != nil {
		return nil, fmt.Errorf("getting initial power table: %w", err)
	}

	return certstore.OpenOrCreateStore(ctx, ds, m.InitialInstance, initialPowerTable)
}
