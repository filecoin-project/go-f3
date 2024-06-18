package passive

import (
	"context"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("f3-dynamic-manifest")

const (
	ManifestPubSubTopicName = "/f3/manifests/0.0.1"
)

var _ manifest.ManifestProvider = (*DynamicManifest)(nil)

// DynamicManifest is a manifest provider that allows
// the manifest to be changed at runtime.
type DynamicManifest struct {
	manifest         *manifest.Manifest
	pubsub           *pubsub.PubSub
	ec               ec.ECBackend
	manifestServerID peer.ID

	// these are populate in runtime
	onManifestChange manifest.OnManifestChange
	// nextManifest     *manifest.Manifest
	manifestUpdates <-chan struct{}
	// manifestTopic    *pubsub.Topic
}

func NewDynamicManifest(manifest *manifest.Manifest, pubsub *pubsub.PubSub, ec ec.ECBackend, manifestServerID peer.ID) manifest.ManifestProvider {
	return &DynamicManifest{
		manifest:         manifest,
		pubsub:           pubsub,
		ec:               ec,
		manifestServerID: manifestServerID,
	}
}

func (m *DynamicManifest) GpbftOptions() []gpbft.Option {
	return m.manifest.GpbftOptions()
}

func (m *DynamicManifest) DatastorePrefix() datastore.Key {
	return m.manifest.DatastorePrefix()
}

func (m *DynamicManifest) EcConfig() *manifest.EcConfig {
	return m.manifest.EcConfig
}

func (m *DynamicManifest) BootstrapEpoch() int64 {
	return m.manifest.BootstrapEpoch
}

func (m *DynamicManifest) NetworkName() gpbft.NetworkName {
	return m.manifest.NetworkName
}

func (m *DynamicManifest) InitialPowerTable() []gpbft.PowerEntry {
	return m.manifest.InitialPowerTable
}

func (m *DynamicManifest) Subscribe() <-chan struct{} {
	return m.manifestUpdates
}

func (m *DynamicManifest) SetManifestChangeCb(mc manifest.OnManifestChange) {
	m.onManifestChange = mc
}

// Returns the pubsub topic name for the manifest
// which includes a version subpath that allows to unique
// identify the configuration manifest used for the network.
func (m *DynamicManifest) MsgPubSubTopic() string {
	v, _ := m.manifest.Version()
	return m.manifest.PubSubTopic() + string(v)
}

func (m *DynamicManifest) Run(context.Context, chan error) {}
