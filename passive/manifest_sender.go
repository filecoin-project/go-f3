package passive

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/manifest"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
)

// ManifestSender is responsible for periodically broadcasting
// the current manifest for the network through the corresponding pubsub
type ManifestSender struct {
	h             host.Host
	pubsub        *pubsub.PubSub
	manifestTopic *pubsub.Topic

	lk       sync.RWMutex
	manifest *manifest.Manifest
	period   time.Duration

	// Used to stop publishing manifests
	cancel func()
}

func NewManifestSender(h host.Host, pubsub *pubsub.PubSub, firstManifest *manifest.Manifest, publicationPeriod time.Duration) (*ManifestSender, error) {
	topicName := ManifestPubSubTopicName
	m := &ManifestSender{
		manifest: firstManifest,
		h:        h,
		pubsub:   pubsub,
		period:   publicationPeriod,
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

func (m *ManifestSender) Addrs() []multiaddr.Multiaddr {
	return m.h.Addrs()
}

func (m *ManifestSender) Start(ctx context.Context) {
	if m.cancel != nil {
		log.Warn("manifest sender already running")
		return
	}
	ctx, m.cancel = context.WithCancel(ctx)

	ticker := time.NewTicker(m.period)
	for {
		select {
		case <-ticker.C:
			m.lk.RLock()
			err := m.publishManifest(ctx)
			if err != nil {
				log.Error("error publishing manifest: %w", err)
			}
			m.lk.RUnlock()
		case <-ctx.Done():
			return
		}
	}
}

func (m *ManifestSender) publishManifest(ctx context.Context) error {
	b, err := m.manifest.Marshal()
	if err != nil {
		return err
	}
	return m.manifestTopic.Publish(ctx, b)
}

func (m *ManifestSender) UpdateManifest(manifest *manifest.Manifest) {
	m.lk.Lock()
	m.manifest = manifest
	m.lk.Unlock()
}

func (m *ManifestSender) Stop() {
	m.cancel()
	m.cancel = nil
}
