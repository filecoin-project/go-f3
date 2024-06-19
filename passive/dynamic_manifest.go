package passive

import (
	"context"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("f3-dynamic-manifest")

const (
	ManifestPubSubTopicName = "/f3/manifests/0.0.1"
)

var _ manifest.ManifestProvider = (*DynamicManifestProvider)(nil)

// DynamicManifestProvider is a manifest provider that allows
// the manifest to be changed at runtime.
type DynamicManifestProvider struct {
	manifest         manifest.Manifest
	pubsub           *pubsub.PubSub
	ec               ec.Backend
	manifestServerID peer.ID

	// these are populate in runtime
	onManifestChange manifest.OnManifestChange
	// nextManifest     *manifest.Manifest
	manifestUpdates <-chan struct{}
	// manifestTopic    *pubsub.Topic
}

func NewDynamicManifestProvider(manifest manifest.Manifest, pubsub *pubsub.PubSub, ec ec.Backend, manifestServerID peer.ID) manifest.ManifestProvider {
	return &DynamicManifestProvider{
		manifest:         manifest,
		pubsub:           pubsub,
		ec:               ec,
		manifestServerID: manifestServerID,
	}
}

func (m *DynamicManifestProvider) Manifest() manifest.Manifest {
	return m.manifest
}

func (m *DynamicManifestProvider) GpbftOptions() []gpbft.Option {
	return m.manifest.GpbftOptions()
}

func (m *DynamicManifestProvider) ManifestQueue() <-chan struct{} {
	return m.manifestUpdates
}

func (m *DynamicManifestProvider) SetManifestChangeCallback(mc manifest.OnManifestChange) {
	m.onManifestChange = mc
}

// Returns the pubsub topic name for the manifest
// which includes a version subpath that allows to unique
// identify the configuration manifest used for the network.
func (m *DynamicManifestProvider) PubSubTopic() string {
	v, _ := m.manifest.Version()
	return m.manifest.PubSubTopic() + string(v)
}

func (m *DynamicManifestProvider) Run(context.Context, chan error) {
	log.Debug("running dynamic manifest")
	// TODO: Implementation coming in the next PR.
}
