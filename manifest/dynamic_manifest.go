package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("f3-dynamic-manifest")

var _ ManifestProvider = (*DynamicManifestProvider)(nil)

const ManifestPubSubTopicName = "/f3/manifests/0.0.1"

// DynamicManifestProvider is a manifest provider that allows
// the manifest to be changed at runtime.
type DynamicManifestProvider struct {
	pubsub           *pubsub.PubSub
	ec               ec.Backend
	manifestServerID peer.ID

	runningCtx context.Context
	errgrp     *errgroup.Group
	cancel     context.CancelFunc

	initialManifest *Manifest
	manifestChanges chan *Manifest
}

// ManifestUpdateMessage updates the GPBFT manifest.
type ManifestUpdateMessage struct {
	// A monotonically increasing sequence number for ordering manifest
	// updates received over the network.
	MessageSequence uint64
	// The manifest version changes each time we distribute a new manifest. Pausing/resuming
	// does not update the version number.
	ManifestVersion uint64
	// The manifest to apply or nil to pause the network.
	Manifest *Manifest
}

func (mu ManifestUpdateMessage) toManifest() *Manifest {
	if mu.Manifest == nil {
		return nil
	}

	// When a manifest configuration changes, a new network name is set that depends on the
	// manifest version of the previous version to avoid overlapping previous configurations.
	cpy := *mu.Manifest
	newName := fmt.Sprintf("%s/%d", string(cpy.NetworkName), mu.ManifestVersion)
	cpy.NetworkName = gpbft.NetworkName(newName)
	return &cpy
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

func NewDynamicManifestProvider(initialManifest *Manifest, pubsub *pubsub.PubSub, ec ec.Backend, manifestServerID peer.ID) ManifestProvider {
	ctx, cancel := context.WithCancel(context.Background())
	errgrp, ctx := errgroup.WithContext(ctx)

	return &DynamicManifestProvider{
		pubsub:           pubsub,
		ec:               ec,
		manifestServerID: manifestServerID,
		runningCtx:       ctx,
		errgrp:           errgrp,
		cancel:           cancel,
		initialManifest:  initialManifest,
		manifestChanges:  make(chan *Manifest, 1),
	}
}

func (m *DynamicManifestProvider) ManifestUpdates() <-chan *Manifest {
	return m.manifestChanges
}

func (m *DynamicManifestProvider) Stop(ctx context.Context) (_err error) {
	m.cancel()
	return m.errgrp.Wait()
}

func (m *DynamicManifestProvider) Start(startCtx context.Context) (_err error) {
	if err := m.registerTopicValidator(); err != nil {
		return err
	}

	manifestTopic, err := m.pubsub.Join(ManifestPubSubTopicName)
	if err != nil {
		return fmt.Errorf("could not join manifest pubsub topic: %w", err)
	}

	manifestSub, err := manifestTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("subscribing to manifest pubsub topic: %w", err)
	}

	// XXX: load the initial manifest from disk!
	// And save it!

	m.manifestChanges <- ManifestUpdateMessage{
		MessageSequence: 0,
		ManifestVersion: 0,
		Manifest:        m.initialManifest,
	}.toManifest()

	m.errgrp.Go(func() error {
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

		var msgSeqNumber uint64
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

			if update.MessageSequence <= msgSeqNumber {
				log.Warnf("discarded manifest update %d", update.MessageSequence)
				continue
			}
			log.Infof("received manifest update %d", update.MessageSequence)
			msgSeqNumber = update.MessageSequence
			select {
			case m.manifestChanges <- update.toManifest():
			case <-m.runningCtx.Done():
				return nil
			}
		}
		return nil
	})

	return nil
}

func (m *DynamicManifestProvider) registerTopicValidator() error {
	// using the same validator approach used for the message pubsub
	// to be homogeneous.
	var validator pubsub.ValidatorEx = func(ctx context.Context, pID peer.ID,
		msg *pubsub.Message) pubsub.ValidationResult {
		var update ManifestUpdateMessage
		err := update.Unmarshal(bytes.NewReader(msg.Data))
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
