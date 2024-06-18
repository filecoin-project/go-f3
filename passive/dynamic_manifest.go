package passive

import (
	"bytes"
	"context"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

var log = logging.Logger("f3-dynamic-manifest")

const (
	ManifestPubSubTopicName = "/f3/manifests/0.0.1"
)

var _ manifest.ManifestProvider = (*DynamicManifest)(nil)

type DynamicManifest struct {
	manifest         *manifest.Manifest
	pubsub           *pubsub.PubSub
	ec               ec.ECBackend
	manifestServerID peer.ID

	// these are populate in runtime
	onManifestChange manifest.OnManifestChange
	nextManifest     *manifest.Manifest
	manifestUpdates  <-chan struct{}
	manifestTopic    *pubsub.Topic
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

func (m *DynamicManifest) Run(ctx context.Context, errCh chan error) {
	if m.onManifestChange == nil {
		errCh <- xerrors.New("onManifestChange is nil. Callback for manifest change required")
	}
	manifestQueue := make(chan struct{}, 5)
	m.manifestUpdates = manifestQueue
	m.handleIncomingManifests(ctx, manifestQueue, errCh)
}

func (m *DynamicManifest) handleIncomingManifests(ctx context.Context, manifestQueue chan struct{}, errCh chan error) {
	if err := m.setupManifestPubsub(); err != nil {
		errCh <- xerrors.Errorf("setting up pubsub: %w", err)
		return
	}

	manifestSub, err := m.manifestTopic.Subscribe()
	if err != nil {
		errCh <- xerrors.Errorf("subscribing to topic: %w", err)
		return
	}

	// FIXME; This is a stub and should be replaced with whatever
	// function allow us to subscribe to new epochs coming from EC.
	// ecSub, err := m.ec.ChainHead(ctx)
	// if err != nil {
	// 	errCh <- xerrors.Errorf("subscribing to chain events: %w", err)
	// 	return
	// }
	ecSub := make(chan ec.TipSet)

loop:
	for {
		select {
		// Check first if there is a new configuration manifest that needs to be applied.
		case ts := <-ecSub:
			if m.nextManifest != nil {
				// if the upgrade epoch is reached or already passed.
				if ts.Epoch() >= m.nextManifest.BootstrapEpoch {
					// update the current manifest
					m.manifest = m.nextManifest
					m.nextManifest = nil
					// stop existing pubsub and subscribe to the new one
					// if the re-bootstrap flag is enabled, it will setup a new runner with the new config.
					go m.onManifestChange(ctx, uint64(m.manifest.BootstrapEpoch), m.nextManifest.ReBootstrap, errCh)
					if !m.nextManifest.ReBootstrap {
						// TODO: If the manifest doesn't have the re-bootstrap flagged
						// enabled, no new runner is setup, we reuse the existing one.
						// We need to pass the manifest to the runner to notify
						// that there is a new configuration to be applied without
						// restarting the runner.
						// Some of the configurations that we expect to run in this way are
						// - Updates to the power table
						// - ECStabilisationDelay
						// - more?
						manifestQueue <- struct{}{}
					}
					continue
				}
			}
		case <-ctx.Done():
			break loop

		default:
			var msg *pubsub.Message
			msg, err = manifestSub.Next(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				log.Errorf("manifestPubsub subscription.Next() returned an error: %+v", err)
				break
			}
			manifest, ok := msg.ValidatorData.(*manifest.Manifest)
			if !ok {
				log.Errorf("invalid manifestValidatorData: %+v", msg.ValidatorData)
				continue
			}

			if !m.acceptNextManifest(manifest) {
				continue
			}

			m.nextManifest = manifest
		}
	}

	manifestSub.Cancel()
	if err := m.teardownManifestPubsub(); err != nil {
		errCh <- xerrors.Errorf("shutting down manifest pubsub: %w", err)
	}
}

func (m *DynamicManifest) teardownPubsub(topic *pubsub.Topic, topicName string) error {
	return multierr.Combine(
		m.pubsub.UnregisterTopicValidator(topicName),
		topic.Close(),
	)
}

func (m *DynamicManifest) teardownManifestPubsub() error {
	return m.teardownPubsub(m.manifestTopic, ManifestPubSubTopicName)
}

// Checks if we should accept the manifest that we received through pubsub
func (m *DynamicManifest) acceptNextManifest(manifest *manifest.Manifest) bool {
	// if the manifest is older, skip it
	if manifest.Sequence <= m.manifest.Sequence ||
		manifest.BootstrapEpoch < m.manifest.BootstrapEpoch {
		return false
	}

	return true
}

func (m *DynamicManifest) setupManifestPubsub() (err error) {
	topicName := ManifestPubSubTopicName
	// using the same validator approach used for the message pubsub
	// to be homogeneous.
	var validator pubsub.ValidatorEx = func(ctx context.Context, pID peer.ID,
		msg *pubsub.Message) pubsub.ValidationResult {
		var manifest manifest.Manifest
		err := manifest.Unmarshal(bytes.NewReader(msg.Data))
		if err != nil {
			return pubsub.ValidationReject
		}

		// manifest should come from the expected diagnostics server
		if pID != m.manifestServerID {
			return pubsub.ValidationReject
		}

		// TODO: Any additional validation?
		// Expect a sequence number that is over our current sequence number.
		// Expect an BootstrapEpoch over the BootstrapEpoch of the current manifests?
		// These should probably not be ValidationRejects to avoid banning in gossipsub
		// the centralized server in case of misconfigurations or bugs.
		msg.ValidatorData = &manifest
		return pubsub.ValidationAccept
	}

	err = m.pubsub.RegisterTopicValidator(topicName, validator)
	if err != nil {
		return xerrors.Errorf("registering topic validator: %w", err)
	}

	m.manifestTopic, err = m.pubsub.Join(topicName)
	if err != nil {
		return xerrors.Errorf("could not join on pubsub topic: %s: %w", topicName, err)
	}
	return
}
