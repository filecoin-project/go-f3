package polling

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/internal/measurements"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/metric"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
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
	clock       clock.Clock

	wg   sync.WaitGroup
	stop context.CancelFunc
}

func (s *Subscriber) Start(startCtx context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	s.stop = cancel
	s.clock = clock.GetClock(startCtx)

	var err error

	s.peerTracker = newPeerTracker(s.clock)
	s.poller, err = NewPoller(startCtx, &s.Client, s.Store, s.SignatureVerifier)
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
			log.Errorf("polling certificate exchange subscriber exited early: %s", err)
		}
	}()

	return nil
}

func (s *Subscriber) Stop(stopCtx context.Context) error {
	if s.stop != nil {
		s.stop()
		s.wg.Wait()
	}

	return nil
}

func (s *Subscriber) run(ctx context.Context) error {
	timer := s.clock.Timer(s.InitialPollInterval)
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
			delay := max(s.clock.Until(nextPollTime), 0)
			log.Debugf("predicted interval is %s (waiting %s)", nextInterval, delay)
			timer.Reset(delay)

			metrics.predictedPollingInterval.Record(ctx, delay.Seconds())
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func (s *Subscriber) poll(ctx context.Context) (_progress uint64, _err error) {
	var (
		misses []peer.ID
		hits   []peer.ID
	)

	defer func(startTime time.Time) {
		status := measurements.AttrStatusSuccess
		if _err != nil {
			// All errors here are internal.
			status = measurements.AttrStatusInternalError
		}
		metrics.pollDuration.Record(ctx, time.Since(startTime).Seconds(), metric.WithAttributes(
			status,
			attrMadeProgress.Bool(_progress > 0),
		))
	}(time.Now())

	peers := s.peerTracker.suggestPeers(ctx)
	start := s.poller.NextInstance

	log.Debugf("polling %d peers for instance %d", len(peers), s.poller.NextInstance)
	pollsSinceLastProgress := 0
	for _, peer := range peers {
		oldInstance := s.poller.NextInstance
		res, err := s.poller.Poll(ctx, peer)
		if err != nil {
			return s.poller.NextInstance - start, err
		}
		log.Debugf("polled %s for instance %d, got %+v", peer, s.poller.NextInstance, res)
		// If we manage to advance, all old "hits" are actually misses.
		if oldInstance < s.poller.NextInstance {
			misses = append(misses, hits...)
			hits = hits[:0]
		}

		switch res.Status {
		case PollMiss:
			misses = append(misses, peer)
			s.peerTracker.updateLatency(peer, res.Latency)
		case PollHit:
			hits = append(hits, peer)
			s.peerTracker.updateLatency(peer, res.Latency)
		case PollFailed:
			s.peerTracker.recordFailure(peer)
		case PollIllegal:
			s.peerTracker.recordInvalid(peer)
		default:
			panic(fmt.Sprintf("unexpected polling.PollResult: %#v", res))
		}

		if res.Progress == 0 {
			pollsSinceLastProgress++
		} else {
			pollsSinceLastProgress = 0
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

	// Record our metrics.
	metrics.peersPolled.Record(ctx, int64(len(peers)),
		metric.WithAttributes(attrMadeProgress.Bool(progress > 0)),
	)
	if len(peers) > 0 && pollsSinceLastProgress < len(peers) {
		required := len(peers) - pollsSinceLastProgress
		metrics.peersRequiredPerPoll.Record(ctx, int64(required))
		efficiency := float64(required) / float64(len(peers))
		metrics.pollEfficiency.Record(ctx, efficiency)
	}

	return progress, nil
}
