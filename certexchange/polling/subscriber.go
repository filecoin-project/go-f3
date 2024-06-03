package polling

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/gpbft"
)

const maxRequestLength = 256

// A polling Subscriber will continuously poll the network for new finality certificates.
type Subscriber struct {
	certexchange.Client

	Store               *certstore.Store
	SignatureVerifier   gpbft.Verifier
	InitialPollInterval time.Duration
	MaximumPollInterval time.Duration
	MinimumPollInterval time.Duration

	peerTracker peerTracker

	wg sync.WaitGroup

	ctx  context.Context
	stop context.CancelFunc
}

func (s *Subscriber) Start() error {
	s.wg.Add(1)
	s.ctx, s.stop = context.WithCancel(context.Background())
	go func() {
		defer s.wg.Done()

		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()
		if err := s.run(ctx); err != nil && s.ctx.Err() != nil {
			s.Log.Errorf("polling certificate exchange subscriber exited early: %w", err)
		}
	}()

	return nil
}

func (s *Subscriber) Stop() error {
	s.stop()
	s.wg.Wait()

	return nil
}

// Discover new peers.
func (s *Subscriber) libp2pDiscover(ctx context.Context) (<-chan peer.ID, error) {
	out := make(chan peer.ID, 256)
	discoveryEvents, err := s.Host.EventBus().Subscribe([]any{
		new(event.EvtPeerIdentificationCompleted),
		new(event.EvtPeerProtocolsUpdated),
	})
	if err != nil {
		return nil, err
	}

	targetProtocol := certexchange.FetchProtocolName(s.NetworkName)

	// Mark already connected peers as "seen".
	for _, p := range s.Host.Network().Peers() {
		if proto, err := s.Host.Peerstore().FirstSupportedProtocol(p, targetProtocol); err == nil && proto == targetProtocol {
			s.peerTracker.peerSeen(p)
		}
	}

	// Then start listening for new peers
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
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

func (s *Subscriber) run(ctx context.Context) error {
	timer := time.NewTimer(s.InitialPollInterval)
	defer timer.Stop()

	discoveredPeers, err := s.libp2pDiscover(ctx)
	if err != nil {
		return err
	}

	predictor := newPredictor(
		0.05,
		s.MinimumPollInterval,
		s.InitialPollInterval,
		s.MaximumPollInterval,
	)

	poller, err := NewPoller(ctx, &s.Client, s.Store, s.SignatureVerifier)
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		var err error

		// Always handle newly discovered peers and new certificates from the certificate
		// store _first_. Then check the timer to see if we should poll.
		select {
		case p := <-discoveredPeers:
			s.peerTracker.peerSeen(p)
		default:
			select {
			case p := <-discoveredPeers:
				s.peerTracker.peerSeen(p)
			case <-timer.C:
				// First, see if we made progress locally. If we have, update
				// interval prediction based on that local progress. If our interval
				// was accurate, we'll keep predicting the same interval and we'll
				// never make any network requests. If we stop making local
				// progress, we'll start making network requests again.
				var progress uint64
				progress, err = poller.CatchUp(ctx)
				if err != nil {
					break
				}
				if progress > 0 {
					timer.Reset(predictor.update(progress))
					break
				}

				progress, err = s.poll(ctx, poller)
				if err != nil {
					break
				}
				timer.Reset(predictor.update(progress))
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *Subscriber) poll(ctx context.Context, poller *Poller) (uint64, error) {
	var (
		misses []peer.ID
		hits   []peer.ID
	)

	start := poller.NextInstance
	for _, peer := range s.peerTracker.suggestPeers() {
		oldInstance := poller.NextInstance
		res, err := poller.Poll(ctx, peer)
		if err != nil {
			return poller.NextInstance - start, err
		}
		// If we manage to advance, all old "hits" are actually misses.
		if oldInstance < poller.NextInstance {
			misses = append(misses, hits...)
			hits = hits[:0]
		}

		switch res {
		case PollMiss:
			misses = append(misses, peer)
		case PollHit:
			hits = append(hits, peer)
		case PollFailed:
			s.peerTracker.recordFailure(peer)
		case PollIllegal:
			s.peerTracker.recordInvalid(peer)
		default:
			panic(fmt.Sprintf("unexpected polling.PollResult: %#v", res))
		}
	}

	// If we've made progress, record hits/misses. Otherwise, we just have to assume that we
	// asked too soon.
	progress := poller.NextInstance - start
	if progress > 0 {
		for _, p := range misses {
			s.peerTracker.recordMiss(p)
		}
		for _, p := range hits {
			s.peerTracker.recordHit(p)
		}
	}

	return progress, nil
}
