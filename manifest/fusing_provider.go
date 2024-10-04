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
// then switches to a static manifest when we get within finality of said manifest's bootstrap
// epoch.
type FusingManifestProvider struct {
	ec      HeadGetter
	dynamic ManifestProvider
	static  *Manifest

	manifestCh chan *Manifest

	errgrp     *errgroup.Group
	cancel     context.CancelFunc
	runningCtx context.Context
	clock      clock.Clock
}

func NewFusingManifestProvider(ctx context.Context, ec HeadGetter, dynamic ManifestProvider, static *Manifest) (*FusingManifestProvider, error) {
	if err := static.Validate(); err != nil {
		return nil, err
	}

	clk := clock.GetClock(ctx)
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	errgrp, ctx := errgroup.WithContext(ctx)

	return &FusingManifestProvider{
		ec:         ec,
		dynamic:    dynamic,
		static:     static,
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
	head, err := m.ec.GetHead(ctx)
	if err != nil {
		return fmt.Errorf("failed to determine current head epoch")
	}

	switchEpoch := m.static.BootstrapEpoch - m.static.EC.Finality
	headEpoch := head.Epoch()

	if headEpoch >= switchEpoch {
		m.manifestCh <- m.static
		return nil
	}

	epochDelay := switchEpoch - headEpoch
	start := head.Timestamp().Add(time.Duration(epochDelay) * m.static.EC.Period)

	if err := m.dynamic.Start(ctx); err != nil {
		return err
	}

	log.Infof("starting the fusing manifest provider, will switch to the static manifest at %s", start)

	m.errgrp.Go(func() error {
		dynamicUpdates := m.dynamic.ManifestUpdates()
		timer := m.clock.Timer(m.clock.Until(start))
		defer timer.Stop()

		for m.runningCtx.Err() == nil {
			select {
			case <-timer.C:
				// Make sure we're actually at the target epoch. This shouldn't be
				// an issue unless our clocks are really funky, the network is
				// behind, or we're in a lotus integration test
				// (https://github.com/filecoin-project/lotus/issues/12557).
				head, err := m.ec.GetHead(m.runningCtx)
				switch {
				case err != nil:
					log.Errorw("failed to get head in fusing manifest provider", "error", err)
					fallthrough
				case head.Epoch() < switchEpoch:
					log.Infow("delaying fusing manifest switch-over because head is behind the target epoch",
						"head", head.Epoch(),
						"target epoch", switchEpoch,
						"bootstrap epoch", m.static.BootstrapEpoch,
					)
					timer.Reset(m.static.EC.Period)
					continue
				}

				log.Infow(
					"fusing to the static manifest, stopping the dynamic manifest provider",
					"network", m.static.NetworkName,
					"bootstrap epoch", m.static.BootstrapEpoch,
					"current epoch", head.Epoch(),
				)
				m.updateManifest(m.static)
				// Log any errors and move on. We don't bubble it because we don't
				// want to stop everything if shutting down the dynamic manifest
				// provider fails when switching over to a static manifest.
				if err := m.dynamic.Stop(context.Background()); err != nil {
					log.Errorw("failure when stopping dynamic manifest provider", "error", err)
				}
				return nil
			case update := <-dynamicUpdates:
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
