package polling

import (
	"context"
	"slices"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/gpbft"
)

func discoverPeers(ctx context.Context, h host.Host, nn gpbft.NetworkName) (<-chan peer.ID, error) {
	out := make(chan peer.ID, 256)
	discoveryEvents, err := h.EventBus().Subscribe([]any{
		new(event.EvtPeerIdentificationCompleted),
		new(event.EvtPeerProtocolsUpdated),
	})
	if err != nil {
		return nil, err
	}

	targetProtocol := certexchange.FetchProtocolName(nn)

	// record existing peers.
fillInitialPeers:
	for _, p := range h.Network().Peers() {
		if proto, err := h.Peerstore().FirstSupportedProtocol(p, targetProtocol); err == nil && proto == targetProtocol {
			select {
			case out <- p:
			default:
				// Don't block because we've subscribed to libp2p events.
				break fillInitialPeers
			}
		}
	}

	// Then start listening for new peers
	go func() {
		defer close(out)
		defer discoveryEvents.Close()

		for {
			var (
				evt any
				ok  bool
			)
			select {
			case evt, ok = <-discoveryEvents.Out():
			case <-ctx.Done():
			}
			if !ok {
				return
			}

			var protos []protocol.ID
			var peer peer.ID
			switch e := evt.(type) {
			case *event.EvtPeerIdentificationCompleted:
				protos = e.Protocols
				peer = e.Peer
			case *event.EvtPeerProtocolsUpdated:
				protos = e.Added
				peer = e.Peer
			default:
				continue
			}
			if slices.Contains(protos, targetProtocol) {
				// If the channel is full, ignore newly discovered peers. We
				// likely have enough anyways and we'll drain the channel
				// eventually.
				select {
				case out <- peer:
				default:
				}
			}
		}
	}()
	return out, nil
}
