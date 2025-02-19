package manifest

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/internal/clock"
	"golang.org/x/sync/errgroup"
)

// HeadGetter is the minimal subset of ec.Backend required by the
// FusingManifestProvider.
type HeadGetter interface {
	GetHead(context.Context) (ec.TipSet, error)
}

var _ ManifestProvider = (*FusingManifestProvider)(nil)

// FusingManifestProvider is a ManifestProvider that starts by providing secondary manifest updates
// then switches to a primary manifest when we get within finality of said manifest's bootstrap
// epoch.
type FusingManifestProvider struct {
	ec        HeadGetter
	secondary ManifestProvider
	primary   ManifestProvider

	manifestCh chan *Manifest

	errgrp     *errgroup.Group
	cancel     context.CancelFunc
	runningCtx context.Context
	clock      clock.Clock
}

// NewFusingManifestProvider creates a provider that will lock into the primary manifest onces it reaches BootstrapEpoch-Finality of primary manifest
// the primary ManifestProvider needs to provide at least one manifest (or nil), a sign of life, to enable forwarding of secondary manifests.
func NewFusingManifestProvider(ctx context.Context, ec HeadGetter, secondary ManifestProvider, primary ManifestProvider) (*FusingManifestProvider, error) {
	clk := clock.GetClock(ctx)
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	errgrp, ctx := errgroup.WithContext(ctx)

	return &FusingManifestProvider{
		ec:         ec,
		secondary:  secondary,
		primary:    primary,
		errgrp:     errgrp,
		cancel:     cancel,
		runningCtx: ctx,
		clock:      clk,
		manifestCh: make(chan *Manifest, 1),
	}, nil
}

func (m *FusingManifestProvider) ManifestUpdates() <-chan *Manifest {
	return m.manifestCh
}

func (m *FusingManifestProvider) Start(ctx context.Context) error {
	if err := m.primary.Start(ctx); err != nil {
		return err
	}

	if err := m.secondary.Start(ctx); err != nil {
		return err
	}

	m.errgrp.Go(func() error {
		defer m.primary.Stop(context.Background())
		defer m.secondary.Stop(context.Background())

		startTimeOfPriority := func(head ec.TipSet, mani *Manifest) time.Time {
			headEpoch := head.Epoch()
			switchEpoch := mani.BootstrapEpoch - mani.EC.Finality
			epochDelay := switchEpoch - headEpoch
			start := head.Timestamp().Add(time.Duration(epochDelay) * mani.EC.Period)
			return start
		}

		var primaryManifest *Manifest
		// create a stopped timer
		timer := m.clock.Timer(time.Hour)
		timer.Stop()

		first := true
		for m.runningCtx.Err() == nil {
			if !first {
				m.clock.Sleep(5 * time.Second)
				first = false
			}

			select {
			case primaryManifest = <-m.primary.ManifestUpdates():
			case <-m.runningCtx.Done():
				// we were stopped, clean exit
				return nil
			}

			head, err := m.ec.GetHead(m.runningCtx)
			if err != nil {
				log.Errorf("failed to determine current head epoch: %w", err)
				continue
			}
			headEpoch := head.Epoch()
			// exit early if primaryManifest is relevant right now
			if primaryManifest != nil && headEpoch >= primaryManifest.BootstrapEpoch-primaryManifest.EC.Finality {
				m.manifestCh <- primaryManifest
				return nil
			}

			if primaryManifest == nil {
				// init with stopped timer
				break
			}
			startTime := startTimeOfPriority(head, primaryManifest)
			log.Infof("starting the fusing manifest provider, will switch to the primary manifest at %s",
				startTime)
			if err != nil {
				log.Errorf("trying to compute start time: %w", err)
				continue
			}
			timer.Reset(m.clock.Until(startTime))
			break
		}

		if m.runningCtx.Err() != nil {
			return nil
		}

		defer timer.Stop()

		for m.runningCtx.Err() == nil {
			select {
			case primaryManifest = <-m.primary.ManifestUpdates():
				if primaryManifest == nil {
					timer.Stop()
					continue
				}
				head, err := m.ec.GetHead(m.runningCtx)
				if err != nil {
					log.Errorf("getting head in fusing manifest: %+v", err)
				}
				startTime := startTimeOfPriority(head, primaryManifest)
				if err != nil {
					log.Errorf("trying to compute start time: %+v", err)
					// set timer in one epoch, shouldn't happen but be defensive
					timer.Reset(primaryManifest.EC.Period)
					continue
				}

				log.Infof("got new primaryManifest, will switch to the primary manifest at %s",
					startTime)
				timer.Reset(m.clock.Until(startTime))
			case <-timer.C:
				if primaryManifest == nil {
					// just a consistency check, timer might have fired before it was stopped
					continue
				}

				// Make sure we're actually at the target epoch. This shouldn't be
				// an issue unless our clocks are really funky, the network is
				// behind, or we're in a lotus integration test
				// (https://github.com/filecoin-project/lotus/issues/12557).
				head, err := m.ec.GetHead(m.runningCtx)
				switchEpoch := primaryManifest.BootstrapEpoch - primaryManifest.EC.Finality
				switch {
				case err != nil:
					log.Errorw("failed to get head in fusing manifest provider", "error", err)
					fallthrough
				case head.Epoch() < switchEpoch:
					log.Infow("delaying fusing manifest switch-over because head is behind the target epoch",
						"head", head.Epoch(),
						"target epoch", switchEpoch,
						"bootstrap epoch", primaryManifest.BootstrapEpoch,
					)
					timer.Reset(primaryManifest.EC.Period)
					continue
				}

				log.Infow(
					"fusing to the primary manifest, stopping the secondary manifest provider",
					"network", primaryManifest.NetworkName,
					"bootstrap epoch", primaryManifest.BootstrapEpoch,
					"current epoch", head.Epoch(),
				)
				m.updateManifest(primaryManifest)
				return nil
			case update := <-m.secondary.ManifestUpdates():
				m.updateManifest(update)
			case <-m.runningCtx.Done():
			}
		}
		return nil
	})

	return nil
}

func (m *FusingManifestProvider) updateManifest(update *Manifest) {
	drain(m.manifestCh)
	m.manifestCh <- update
}

func (m *FusingManifestProvider) Stop(ctx context.Context) error {
	m.cancel()
	return m.errgrp.Wait()
}
