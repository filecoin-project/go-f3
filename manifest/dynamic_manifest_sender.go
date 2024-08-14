package manifest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/psutil"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
}

func NewManifestSender(ctx context.Context, h host.Host, ps *pubsub.PubSub, firstManifest *Manifest, publishInterval time.Duration) (*ManifestSender, error) {
	// The rebroadcast interval must be larger than the time cache duration. Default to 2x just in case.
	if minInterval := 2 * pubsub.TimeCacheDuration; publishInterval < minInterval {
		log.Warnf("manifest sender publish interval is too short (%s), increasing to 2x the time-cache duration %s",
			publishInterval, minInterval,
		)
		publishInterval = minInterval
	}

	clk := clock.GetClock(ctx)
	m := &ManifestSender{
		manifest: firstManifest,
		h:        h,
		pubsub:   ps,
		interval: publishInterval,
		// seed the sequence number with nanoseconds so we can restart and don't have to
		// remember the last sequence number.
		msgSeq: uint64(clk.Now().UnixNano()),
		clock:  clk,
	}

	var err error
	m.manifestTopic, err = m.pubsub.Join(ManifestPubSubTopicName, pubsub.WithTopicMessageIdFn(psutil.ManifestMessageIdFn))
	if err != nil {
		return nil, fmt.Errorf("could not join on pubsub topic: %s: %w", ManifestPubSubTopicName, err)
	}
	if err := m.manifestTopic.SetScoreParams(psutil.PubsubTopicScoreParams); err != nil {
		log.Infow("could not set topic score params", "error", err)
	}

	// Record one-off attributes about the sender for easier runtime debugging.
	metrics.senderInfo.Record(ctx, 1, metric.WithAttributes(
		attribute.String("id", m.SenderID().String()),
		attribute.String("topic", ManifestPubSubTopicName),
		attribute.String("publish_interval", publishInterval.String()),
	))

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

func (m *ManifestSender) publishManifest(ctx context.Context) (_err error) {
	m.lk.Lock()
	defer func() {
		m.lk.Unlock()
		recordSenderPublishManifest(ctx, _err)
	}()

	// Manifest sender itself is paused.
	if m.manifest == nil {
		return nil
	}

	update := ManifestUpdateMessage{
		MessageSequence: m.msgSeq,
		Manifest:        *m.manifest,
	}

	b, err := update.Marshal()
	if err != nil {
		return err
	}
	return m.manifestTopic.Publish(ctx, b)
}

func (m *ManifestSender) UpdateManifest(manifest *Manifest) {
	var seq uint64
	m.lk.Lock()
	m.manifest = manifest
	m.msgSeq++
	seq = m.msgSeq
	m.lk.Unlock()
	recordSenderManifestInfo(context.TODO(), seq, manifest)
	metrics.senderManifestUpdated.Add(context.TODO(), 1)
}
