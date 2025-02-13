package manifest

import (
	"context"
	"fmt"
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

// FusingManifestProvider is a ManifestProvider that starts by providing dynamic manifest updates
// then switches to a priority manifest when we get within finality of said manifest's bootstrap
// epoch.
type FusingManifestProvider struct {
	ec       HeadGetter
	dynamic  ManifestProvider
	priority ManifestProvider

	manifestCh chan *Manifest

	errgrp     *errgroup.Group
	cancel     context.CancelFunc
	runningCtx context.Context
	clock      clock.Clock
}

// NewFusingManifestProvider creates a provider that will lock into the priority manifest onces it reaches BootstrapEpoch of priority manifest
// the priority ManifestProvider needs to provide at least one manifest (or nil), a sign of life, to enable forwarding of dynamic manifests
func NewFusingManifestProvider(ctx context.Context, ec HeadGetter, dynamic ManifestProvider, priority ManifestProvider) (*FusingManifestProvider, error) {
	clk := clock.GetClock(ctx)
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	errgrp, ctx := errgroup.WithContext(ctx)

	return &FusingManifestProvider{
		ec:         ec,
		dynamic:    dynamic,
		priority:   priority,
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
	if err := m.priority.Start(ctx); err != nil {
		return err
	}

	priorityManifest := <-m.priority.ManifestUpdates()
	var timer *clock.Timer
	startTimeOfPriority := func(mani *Manifest) (time.Time, error) {
		head, err := m.ec.GetHead(ctx)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to determine current head epoch: %w", err)
		}
		headEpoch := head.Epoch()
		switchEpoch := mani.BootstrapEpoch - mani.EC.Finality
		epochDelay := switchEpoch - headEpoch
		start := head.Timestamp().Add(time.Duration(epochDelay) * mani.EC.Period)
		return start, nil
	}

	{
		head, err := m.ec.GetHead(ctx)
		if err != nil {
			return fmt.Errorf("failed to determine current head epoch: %w", err)
		}
		headEpoch := head.Epoch()
		// exit early if priorityManifest is relevant right now
		if priorityManifest != nil && headEpoch >= priorityManifest.BootstrapEpoch-priorityManifest.EC.Finality {
			m.priority.Stop(ctx)
			m.manifestCh <- priorityManifest
			return nil
		}

		if priorityManifest != nil {
			startTime, err := startTimeOfPriority(priorityManifest)
			log.Infof("starting the fusing manifest provider, will switch to the priority manifest at %s,(now %s)",
				startTime, m.clock.Now())
			if err != nil {
				return fmt.Errorf("trying to compute start time: %w", err)
			}
			timer = m.clock.Timer(m.clock.Until(startTime))
		} else {
			// create a stopped timer
			timer = m.clock.Timer(time.Hour)
			timer.Stop()
		}
	}

	if err := m.dynamic.Start(ctx); err != nil {
		return err
	}

	m.errgrp.Go(func() error {
		defer timer.Stop()
		defer m.priority.Stop(context.Background())

		for m.runningCtx.Err() == nil {
			select {
			case priorityManifest = <-m.priority.ManifestUpdates():
				if priorityManifest == nil {
					timer.Stop()
					continue
				}
				startTime, err := startTimeOfPriority(priorityManifest)
				if err != nil {
					log.Errorf("trying to compute start time: %+v", err)
					// set timer in one epoch, shouldn't happen but be defensive
					timer.Reset(priorityManifest.EC.Period)
					continue
				}

				log.Infof("got new priorityManifest, will switch to the priority manifest at %s",
					startTime)
				timer.Reset(m.clock.Until(startTime))
			case <-timer.C:
				log.Errorf("timer fired")
				if priorityManifest == nil {
					log.Errorf("nil priorityManifest")
					// just a consistency check, timer might have fired before it was stopped
					continue
				}

				// Make sure we're actually at the target epoch. This shouldn't be
				// an issue unless our clocks are really funky, the network is
				// behind, or we're in a lotus integration test
				// (https://github.com/filecoin-project/lotus/issues/12557).
				head, err := m.ec.GetHead(m.runningCtx)
				switchEpoch := priorityManifest.BootstrapEpoch - priorityManifest.EC.Finality
				switch {
				case err != nil:
					log.Errorw("failed to get head in fusing manifest provider", "error", err)
					fallthrough
				case head.Epoch() < switchEpoch:
					log.Infow("delaying fusing manifest switch-over because head is behind the target epoch",
						"head", head.Epoch(),
						"target epoch", switchEpoch,
						"bootstrap epoch", priorityManifest.BootstrapEpoch,
					)
					timer.Reset(priorityManifest.EC.Period)
					continue
				}

				log.Infow(
					"fusing to the priority manifest, stopping the dynamic manifest provider",
					"network", priorityManifest.NetworkName,
					"bootstrap epoch", priorityManifest.BootstrapEpoch,
					"current epoch", head.Epoch(),
				)
				m.updateManifest(priorityManifest)
				// Log any errors and move on. We don't bubble it because we don't
				// want to stop everything if shutting down the dynamic manifest
				// provider fails when switching over to a priority manifest.
				if err := m.dynamic.Stop(context.Background()); err != nil {
					log.Errorw("failure when stopping dynamic manifest provider", "error", err)
				}
				return nil
			case update := <-m.dynamic.ManifestUpdates():
				m.updateManifest(update)
			case <-m.runningCtx.Done():
			}
		}
		return m.dynamic.Stop(context.Background())
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
