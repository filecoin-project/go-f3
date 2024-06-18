package manifest

import (
	"context"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-datastore"
)

// Static manifest provider that doesn't allow any changes
// in runtime to the initial manifest set in the provider
type Static struct {
	manifest *Manifest
}

var _ ManifestProvider = (*Static)(nil)

func NewStaticManifest(m *Manifest) ManifestProvider {
	return &Static{manifest: m}
}

func (m *Static) GpbftOptions() []gpbft.Option {
	return m.manifest.GpbftOptions()
}

func (m *Static) DatastorePrefix() datastore.Key {
	return m.manifest.DatastorePrefix()
}

func (m *Static) EcConfig() *EcConfig {
	return m.manifest.EcConfig
}

func (m *Static) BootstrapEpoch() int64 {
	return m.manifest.BootstrapEpoch
}

func (m *Static) NetworkName() gpbft.NetworkName {
	return m.manifest.NetworkName
}

func (m *Static) InitialPowerTable() []gpbft.PowerEntry {
	return m.manifest.InitialPowerTable
}

func (m *Static) Subscribe() <-chan struct{} {
	return nil
}

func (m *Static) MsgPubSubTopic() string {
	return m.manifest.PubSubTopic()
}

func (m *Static) Run(ctx context.Context, errCh chan error) {}

func (m *Static) SetManifestChangeCb(mc OnManifestChange) {}
