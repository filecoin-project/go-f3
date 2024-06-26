package manifest

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

var _ ManifestProvider = (*DynamicManifestProvider)(nil)

var log = logging.Logger("f3-dynamic-manifest")

const (
	ManifestPubSubTopicName = "/f3/manifests/0.0.1"
	ManifestCheckTick       = 5 * time.Second
)

// DynamicManifestProvider is a manifest provider that allows
// the manifest to be changed at runtime.
type DynamicManifestProvider struct {
	pubsub           *pubsub.PubSub
	ec               ec.Backend
	manifestServerID peer.ID
	manifestTopic    *pubsub.Topic

	// the lk guards all dynamic manifest-specific fields
	lk               sync.RWMutex
	manifest         Manifest
	onManifestChange OnManifestChange
	nextManifest     *Manifest
}

func NewDynamicManifestProvider(manifest Manifest, pubsub *pubsub.PubSub, ec ec.Backend, manifestServerID peer.ID) ManifestProvider {
	return &DynamicManifestProvider{
		manifest:         manifest,
		pubsub:           pubsub,
		ec:               ec,
		manifestServerID: manifestServerID,
	}
}

func (m *DynamicManifestProvider) Manifest() Manifest {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return m.manifest
}

func (m *DynamicManifestProvider) GpbftOptions() []gpbft.Option {
	return m.manifest.GpbftOptions()
}

func (m *DynamicManifestProvider) SetManifestChangeCallback(mc OnManifestChange) {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.onManifestChange = mc
}

// When a manifest configuration changes, a new network name
// is set that depends on the manifest version of the previous version to avoid
// overlapping previous configurations.
func (m *DynamicManifestProvider) networkNameOnChange() gpbft.NetworkName {
	return gpbft.NetworkName(string(m.manifest.NetworkName) + "/" + fmt.Sprintf("%d", m.manifest.Sequence))
}

func (m *DynamicManifestProvider) Run(ctx context.Context, errCh chan error) {
	if m.onManifestChange == nil {
		errCh <- xerrors.New("onManifestChange is nil. Callback for manifest change required")
	}
	go m.handleIncomingManifests(ctx, errCh)
	m.handleApplyManifest(ctx, errCh)
}

func (m *DynamicManifestProvider) handleApplyManifest(ctx context.Context, errCh chan error) {
	// add a timer for EC period
	ticker := time.NewTicker(ManifestCheckTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.lk.Lock()
			if m.nextManifest != nil {
				ts, err := m.ec.GetHead(ctx)
				if err != nil {
					log.Errorf("error fetching chain head: %+v", err)
					m.lk.Unlock()
					continue
				}

				// if the upgrade epoch is reached or already passed.
				if ts.Epoch() >= m.nextManifest.BootstrapEpoch {
					log.Debugf("reached bootstrap epoch, triggering manifest change: %d", ts.Epoch())
					// update the current manifest
					prevManifest := m.manifest
					m.manifest = *m.nextManifest
					nn := m.networkNameOnChange()
					m.manifest.NetworkName = nn
					m.nextManifest = nil
					// trigger manifest change callback.
					go m.onManifestChange(ctx, prevManifest, errCh)
					m.lk.Unlock()
					continue
				}
			}
			m.lk.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// listen to manifests being broadcast through the network.
func (m *DynamicManifestProvider) handleIncomingManifests(ctx context.Context, errCh chan error) {
	if err := m.setupManifestPubsub(); err != nil {
		errCh <- xerrors.Errorf("setting up pubsub: %w", err)
		return
	}

	manifestSub, err := m.manifestTopic.Subscribe()
	if err != nil {
		errCh <- xerrors.Errorf("subscribing to topic: %w", err)
		return
	}

loop:
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			break loop

		default:
			var msg *pubsub.Message
			msg, err = manifestSub.Next(ctx)
			if err != nil {
				log.Errorf("manifestPubsub subscription.Next() returned an error: %+v", err)
				break
			}
			manifest, ok := msg.ValidatorData.(*Manifest)
			if !ok {
				log.Errorf("invalid manifestValidatorData: %+v", msg.ValidatorData)
				continue
			}

			m.acceptNextManifest(manifest)
		}
	}

	manifestSub.Cancel()
	if err := m.teardownManifestPubsub(); err != nil {
		errCh <- xerrors.Errorf("shutting down manifest pubsub: %w", err)
	}
}

func (m *DynamicManifestProvider) teardownPubsub(topic *pubsub.Topic, topicName string) error {
	return multierr.Combine(
		m.pubsub.UnregisterTopicValidator(topicName),
		topic.Close(),
	)
}

func (m *DynamicManifestProvider) teardownManifestPubsub() error {
	return m.teardownPubsub(m.manifestTopic, ManifestPubSubTopicName)
}

// Checks if we should accept the manifest that we received through pubsub
// and sets nextManifest if it is the case
func (m *DynamicManifestProvider) acceptNextManifest(manifest *Manifest) {
	m.lk.Lock()
	defer m.lk.Unlock()

	if manifest.Sequence <= m.manifest.Sequence {
		return
	}
	// TODO: Any additional logic here to determine what manifests to accept or not?

	m.nextManifest = manifest
}

func (m *DynamicManifestProvider) setupManifestPubsub() (err error) {
	topicName := ManifestPubSubTopicName
	// using the same validator approach used for the message pubsub
	// to be homogeneous.
	var validator pubsub.ValidatorEx = func(ctx context.Context, pID peer.ID,
		msg *pubsub.Message) pubsub.ValidationResult {
		var manifest Manifest
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
