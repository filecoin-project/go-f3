package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/filecoin-project/go-f3/internal/measurements"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("f3/manifest-provider")

var _ ManifestProvider = (*DynamicManifestProvider)(nil)

const ManifestPubSubTopicName = "/f3/manifests/0.0.2"

// DynamicManifestProvider is a manifest provider that allows
// the manifest to be changed at runtime.
type DynamicManifestProvider struct {
	pubsub           *pubsub.PubSub
	ds               datastore.Datastore
	filter           func(*Manifest) error
	manifestServerID peer.ID

	runningCtx context.Context
	errgrp     *errgroup.Group
	cancel     context.CancelFunc

	initialManifest *Manifest
	manifestChanges chan *Manifest
	sequenceNumber  atomic.Uint64
}

// ManifestUpdateMessage updates the GPBFT manifest.
type ManifestUpdateMessage struct {
	// An increasing sequence number for ordering manifest updates received over the network.
	MessageSequence uint64
	// The manifest to apply or nil to pause the network.
	Manifest Manifest
}

func (m ManifestUpdateMessage) Marshal() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshaling JSON: %w", err)
	}
	return b, nil
}

func (m *ManifestUpdateMessage) Unmarshal(r io.Reader) error {
	err := json.NewDecoder(r).Decode(&m)
	if err != nil {
		return fmt.Errorf("decoding JSON: %w", err)
	}
	return nil
}

type dynamicManifestProviderConfig DynamicManifestProvider
type DynamicManifestProviderOption func(cfg *dynamicManifestProviderConfig) error

// DynamicManifestProviderWithDatastore specifies the datastore in which to store/retrieve received
// dynamic manifest updates. If unspecified, no state is persisted.
func DynamicManifestProviderWithDatastore(ds datastore.Datastore) func(cfg *dynamicManifestProviderConfig) error {
	return func(cfg *dynamicManifestProviderConfig) error {
		cfg.ds = measurements.NewMeteredDatastore(meter, "f3_manifest_datastore_", ds)
		return nil
	}
}

// DynamicManifestProviderWithFilter specifies a filter for incoming manifests.
func DynamicManifestProviderWithFilter(filter func(*Manifest) error) func(cfg *dynamicManifestProviderConfig) error {
	return func(cfg *dynamicManifestProviderConfig) error {
		cfg.filter = filter
		return nil
	}
}

// DynamicManifestProviderWithInitialManifest specifies the initial manifest to use (unless one is
// available in the datastore).
func DynamicManifestProviderWithInitialManifest(m *Manifest) func(cfg *dynamicManifestProviderConfig) error {
	return func(cfg *dynamicManifestProviderConfig) error {
		if err := m.Validate(); err != nil {
			return err
		}
		cfg.initialManifest = m
		return nil
	}
}

func NewDynamicManifestProvider(pubsub *pubsub.PubSub, manifestServerID peer.ID,
	options ...DynamicManifestProviderOption) (*DynamicManifestProvider, error) {

	ctx, cancel := context.WithCancel(context.Background())
	errgrp, ctx := errgroup.WithContext(ctx)

	m := &dynamicManifestProviderConfig{
		pubsub:           pubsub,
		manifestServerID: manifestServerID,
		runningCtx:       ctx,
		errgrp:           errgrp,
		cancel:           cancel,
		manifestChanges:  make(chan *Manifest, 1),
		filter:           func(*Manifest) error { return nil },
	}
	for _, opt := range options {
		if err := opt(m); err != nil {
			return nil, err
		}
	}
	return (*DynamicManifestProvider)(m), nil
}

var latestManifestKey = datastore.NewKey("latestManifest")

func (m *DynamicManifestProvider) ManifestUpdates() <-chan *Manifest {
	return m.manifestChanges
}

func (m *DynamicManifestProvider) Stop(ctx context.Context) error {
	m.cancel()
	return m.errgrp.Wait()
}

func (m *DynamicManifestProvider) Start(startCtx context.Context) error {
	log.Infow("starting a dynamic manifest provider", "manifestServerID", m.manifestServerID)
	if err := m.registerTopicValidator(); err != nil {
		return err
	}

	// Use the message hash as the message ID to reduce the chances of routing cycles. We ensure
	// our rebroadcast interval is greater than our cache timeout.
	manifestTopic, err := m.pubsub.Join(ManifestPubSubTopicName, pubsub.WithTopicMessageIdFn(psutil.ManifestMessageIdFn))
	if err != nil {
		return fmt.Errorf("could not join manifest pubsub topic: %w", err)
	}

	if err := manifestTopic.SetScoreParams(psutil.PubsubTopicScoreParams); err != nil {
		log.Infow("failed to set topic score params", "error", err)
	}

	manifestSub, err := manifestTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("subscribing to manifest pubsub topic: %w", err)
	}

	var currentManifest *Manifest
	if m.ds == nil {
		currentManifest = m.initialManifest
	} else if mBytes, err := m.ds.Get(startCtx, latestManifestKey); errors.Is(err, datastore.ErrNotFound) {
		currentManifest = m.initialManifest
	} else if err != nil {
		return fmt.Errorf("error while checking saved manifest")
	} else {
		var update ManifestUpdateMessage
		err := update.Unmarshal(bytes.NewReader(mBytes))
		if err != nil {
			return fmt.Errorf("decoding saved manifest: %w", err)
		}

		m.sequenceNumber.Store(update.MessageSequence)
		currentManifest = &update.Manifest
	}

	if currentManifest != nil {
		if err := currentManifest.Validate(); err != nil {
			log.Errorw("invalid initial manifest, ignoring", "error", err)
		} else {
			m.updateManifest(currentManifest)
		}
	}

	m.errgrp.Go(func() (_err error) {
		defer func() {
			manifestSub.Cancel()
			err := multierr.Combine(
				manifestTopic.Close(),
				m.unregisterTopicValidator(),
			)
			// Pubsub likes to return context canceled errors if/when we unregister after
			// closing pubsub. Ignore it.
			if err != nil && !errors.Is(err, context.Canceled) {
				_err = multierr.Append(_err, err)
			}
			if _err != nil {
				log.Error("exited manifest subscription early: %+v", _err)
			}
		}()

		for m.runningCtx.Err() == nil {
			msg, err := manifestSub.Next(m.runningCtx)
			if err != nil {
				if m.runningCtx.Err() == nil {
					return fmt.Errorf("error from manifest subscription: %w", err)
				}
				return nil
			}
			update, ok := msg.ValidatorData.(*ManifestUpdateMessage)
			if !ok {
				log.Errorf("invalid manifestValidatorData: %+v", msg.ValidatorData)
				continue
			}

			oldSeq := m.sequenceNumber.Load()

			if update.MessageSequence <= oldSeq {
				log.Debugw("discarded manifest update", "newSeqNo", update.MessageSequence, "oldSeqNo", oldSeq)
				continue
			}
			m.sequenceNumber.Store(update.MessageSequence)

			if err := update.Manifest.Validate(); err != nil {
				log.Errorw("received invalid manifest, discarded", "error", err)
				continue
			}

			mcid, err := update.Manifest.Cid()
			if err != nil {
				log.Errorw("Failed to determine manifest CID", "error", err)
				continue
			}

			if err := m.filter(&update.Manifest); err != nil {
				log.Errorw("received filtered manifest, discarded", "error", err)
				continue
			}

			if m.ds != nil {
				err = m.ds.Put(m.runningCtx, latestManifestKey, msg.Data)
				if err != nil {
					log.Errorw("saving new manifest", "error", err)
				}
			}

			log.Infow("received manifest update", "seqNo", update.MessageSequence, "cid", mcid)

			oldManifest := currentManifest
			manifestCopy := update.Manifest
			currentManifest = &manifestCopy

			// If we're receiving the same manifest multiple times (manifest publisher
			// could have restarted), don't re-apply it.
			if oldManifest.Equal(currentManifest) {
				continue
			}

			m.updateManifest(currentManifest)
		}
		return nil
	})

	return nil
}

func (m *DynamicManifestProvider) updateManifest(update *Manifest) {
	drain(m.manifestChanges)
	m.manifestChanges <- update
}

func (m *DynamicManifestProvider) registerTopicValidator() error {
	// using the same validator approach used for the message pubsub
	// to be homogeneous.
	var validator pubsub.ValidatorEx = func(ctx context.Context, pID peer.ID,
		msg *pubsub.Message) pubsub.ValidationResult {
		// manifest should come from the expected diagnostics server
		originID, err := peer.IDFromBytes(msg.From)
		if err != nil {
			log.Debugw("decoding msg.From ID", "error", err)
			return pubsub.ValidationReject
		}
		if originID != m.manifestServerID {
			log.Debugw("rejected manifest from unknown peer", "from", msg.From, "manifestServerID", m.manifestServerID)
			return pubsub.ValidationReject
		}

		var update ManifestUpdateMessage
		err = update.Unmarshal(bytes.NewReader(msg.Data))
		if err != nil {
			log.Debugw("failed to unmarshal manifest", "from", msg.From, "error", err)
			return pubsub.ValidationReject
		}

		// Only allow the latest sequence number through.
		if update.MessageSequence < m.sequenceNumber.Load() {
			return pubsub.ValidationIgnore
		}

		// TODO: Any additional validation?
		// Expect a sequence number that is over our current sequence number.
		// Expect an BootstrapEpoch over the BootstrapEpoch of the current manifests?
		// These should probably not be ValidationRejects to avoid banning in gossipsub
		// the centralized server in case of misconfigurations or bugs.
		msg.ValidatorData = &update
		return pubsub.ValidationAccept
	}

	err := m.pubsub.RegisterTopicValidator(ManifestPubSubTopicName, validator)
	if err != nil {
		return fmt.Errorf("registering topic validator: %w", err)
	}
	return nil
}

func (m *DynamicManifestProvider) unregisterTopicValidator() error {
	return m.pubsub.UnregisterTopicValidator(ManifestPubSubTopicName)
}
