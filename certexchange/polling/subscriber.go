package polling

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

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

	peerTracker *peerTracker
	poller      *Poller
	discoverCh  <-chan peer.ID

	wg   sync.WaitGroup
	stop context.CancelFunc
}

func (s *Subscriber) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.stop = cancel

	var err error

	s.peerTracker = newPeerTracker()
	s.poller, err = NewPoller(ctx, &s.Client, s.Store, s.SignatureVerifier)
	if err != nil {
		return err
	}

	s.discoverCh, err = discoverPeers(ctx, s.Host, s.NetworkName)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer func() {
			// in case we return early, cancel.
			s.stop()
			// and wait for discovery to exit.
			for range s.discoverCh {
			}

			// then we're done
			s.wg.Done()
		}()

		if err := s.run(ctx); err != nil && ctx.Err() == nil {
			s.Log.Errorf("polling certificate exchange subscriber exited early: %s", err)
		}
	}()

	return nil
}

func (s *Subscriber) Stop() error {
	if s.stop != nil {
		s.stop()
		s.wg.Wait()
	}

	return nil
}

func (s *Subscriber) run(ctx context.Context) error {
	timer := time.NewTimer(s.InitialPollInterval)
	defer timer.Stop()

	predictor := newPredictor(
		s.MinimumPollInterval,
		s.InitialPollInterval,
		s.MaximumPollInterval,
	)

	for ctx.Err() == nil {
		select {
		case p := <-s.discoverCh:
			s.peerTracker.peerSeen(p)
		case pollTime := <-timer.C:
			// First, see if we made progress locally. If we have, update
			// interval prediction based on that local progress. If our interval
			// was accurate, we'll keep predicting the same interval and we'll
			// never make any network requests. If we stop making local
			// progress, we'll start making network requests again.
			progress, err := s.poller.CatchUp(ctx)
			if err != nil {
				return err
			}
			// Otherwise, poll the network.
			if progress == 0 {
				progress, err = s.poll(ctx)
				if err != nil {
					return err
				}
			}

			nextInterval := predictor.update(progress)
			nextPollTime := pollTime.Add(nextInterval)
			timer.Reset(max(time.Until(nextPollTime), 0))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func (s *Subscriber) poll(ctx context.Context) (uint64, error) {
	var (
		misses []peer.ID
		hits   []peer.ID
	)

	peers := s.peerTracker.suggestPeers()
	start := s.poller.NextInstance
	s.Log.Debugf("polling %d peers for instance %d", len(peers), s.poller.NextInstance)
	for _, peer := range peers {
		oldInstance := s.poller.NextInstance
		res, err := s.poller.Poll(ctx, peer)
		if err != nil {
			return s.poller.NextInstance - start, err
		}
		s.Log.Debugf("polled %s for instance %d, got %s", peer, s.poller.NextInstance, res)
		// If we manage to advance, all old "hits" are actually misses.
		if oldInstance < s.poller.NextInstance {
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
	progress := s.poller.NextInstance - start
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
