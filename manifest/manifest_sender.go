package manifest

import (
	"context"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"
)

// ManifestSender is responsible for periodically broadcasting
// the current manifest for the network through the corresponding pubsub
type ManifestSender struct {
	h             host.Host
	pubsub        *pubsub.PubSub
	manifestTopic *pubsub.Topic
	interval      time.Duration

	// lock to that guards the update of the manifest.
	lk       sync.RWMutex
	manifest Manifest
	// Used to stop publishing manifests
	cancel context.CancelFunc
}

func NewManifestSender(h host.Host, pubsub *pubsub.PubSub, firstManifest Manifest, pubishInterval time.Duration) (*ManifestSender, error) {
	topicName := ManifestPubSubTopicName
	m := &ManifestSender{
		manifest: firstManifest,
		h:        h,
		pubsub:   pubsub,
		interval: pubishInterval,
	}

	var err error
	m.manifestTopic, err = m.pubsub.Join(topicName)
	if err != nil {
		return nil, xerrors.Errorf("could not join on pubsub topic: %s: %w", topicName, err)
	}

	return m, nil
}

func (m *ManifestSender) SenderID() peer.ID {
	return m.h.ID()
}

func (m *ManifestSender) PeerInfo() peer.AddrInfo {
	return m.h.Peerstore().PeerInfo(m.h.ID())
}

func (m *ManifestSender) Start(ctx context.Context) {
	if m.cancel != nil {
		log.Warn("manifest sender already running")
		return
	}
	ctx, m.cancel = context.WithCancel(ctx)

	ticker := time.NewTicker(m.interval)
	for {
		select {
		case <-ticker.C:
			err := m.publishManifest(ctx)
			if err != nil {
				log.Error("error publishing manifest: %w", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *ManifestSender) publishManifest(ctx context.Context) error {
	m.lk.RLock()
	defer m.lk.RUnlock()

	b, err := m.manifest.Marshal()
	if err != nil {
		return err
	}
	return m.manifestTopic.Publish(ctx, b)
}

func (m *ManifestSender) UpdateManifest(manifest Manifest) {
	m.lk.Lock()
	m.manifest = manifest
	m.lk.Unlock()
}

func (m *ManifestSender) Stop() {
	m.cancel()
	m.cancel = nil
}
