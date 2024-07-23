package manifest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/internal/clock"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ManifestSender is responsible for periodically broadcasting
// the current manifest for the network through the corresponding pubsub
type ManifestSender struct {
	h             host.Host
	pubsub        *pubsub.PubSub
	manifestTopic *pubsub.Topic
	interval      time.Duration
	clock         clock.Clock

	// lock to that guards the update of the manifest.
	lk       sync.Mutex
	manifest *Manifest
	msgSeq   uint64
	paused   bool
}

func NewManifestSender(ctx context.Context, h host.Host, ps *pubsub.PubSub, firstManifest *Manifest, pubishInterval time.Duration) (*ManifestSender, error) {
	clk := clock.GetClock(ctx)
	m := &ManifestSender{
		manifest: firstManifest,
		h:        h,
		pubsub:   ps,
		interval: pubishInterval,
		// seed the sequence number with nanoseconds so we can restart and don't have to
		// remember the last sequence number.
		msgSeq: uint64(clk.Now().UnixNano()),
		clock:  clk,
	}

	var err error
	m.manifestTopic, err = m.pubsub.Join(ManifestPubSubTopicName, pubsub.WithTopicMessageIdFn(pubsub.DefaultMsgIdFn))
	if err != nil {
		return nil, fmt.Errorf("could not join on pubsub topic: %s: %w", ManifestPubSubTopicName, err)
	}

	return m, nil
}

func (m *ManifestSender) SenderID() peer.ID {
	return m.h.ID()
}

func (m *ManifestSender) PeerInfo() peer.AddrInfo {
	return m.h.Peerstore().PeerInfo(m.h.ID())
}

func (m *ManifestSender) Run(ctx context.Context) error {
	ticker := m.clock.Ticker(m.interval)
	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			err := m.publishManifest(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("error publishing manifest: %w", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (m *ManifestSender) publishManifest(ctx context.Context) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	update := ManifestUpdateMessage{
		MessageSequence: m.msgSeq,
	}
	if !m.paused {
		update.Manifest = m.manifest
	}

	b, err := update.Marshal()
	if err != nil {
		return err
	}
	return m.manifestTopic.Publish(ctx, b)
}

func (m *ManifestSender) UpdateManifest(manifest *Manifest) {
	m.lk.Lock()
	m.manifest = manifest
	m.msgSeq++
	m.paused = false
	m.lk.Unlock()
}

func (m *ManifestSender) Pause() {
	m.lk.Lock()
	if !m.paused {
		m.paused = true
		m.msgSeq++
	}
	m.lk.Unlock()
}

func (m *ManifestSender) Resume() {
	m.lk.Lock()
	if m.paused {
		m.paused = false
		m.msgSeq++
	}
	m.lk.Unlock()
}
