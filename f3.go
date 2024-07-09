package f3

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/certexchange"
	certexpoll "github.com/filecoin-project/go-f3/certexchange/polling"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"

	"github.com/Kubuxu/go-broadcast"
	"github.com/ipfs/go-datastore"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type F3 struct {
	verifier         gpbft.Verifier
	manifestProvider manifest.ManifestProvider

	busBroadcast broadcast.Channel[*gpbft.MessageBuilder]

	host   host.Host
	ds     datastore.Datastore
	ec     ec.Backend
	pubsub *pubsub.PubSub

	runningCtx context.Context
	cancelCtx  context.CancelFunc
	errgrp     *errgroup.Group

	mu       sync.Mutex
	cs       *certstore.Store
	manifest *manifest.Manifest
	runner   *gpbftRunner
	certsub  *certexpoll.Subscriber
	certserv *certexchange.Server
}

// New creates and setups f3 with libp2p
// The context is used for initialization not runtime.
// signingMarshaller can be nil for default SigningMarshaler
func New(_ctx context.Context, manifest manifest.ManifestProvider, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, verif gpbft.Verifier, ec ec.Backend) (*F3, error) {
	runningCtx, cancel := context.WithCancel(context.Background())
	errgrp, runningCtx := errgroup.WithContext(runningCtx)

	return &F3{
		verifier:         verif,
		manifestProvider: manifest,
		host:             h,
		ds:               ds,
		ec:               ec,
		pubsub:           ps,
		runningCtx:       runningCtx,
		cancelCtx:        cancel,
		errgrp:           errgrp,
	}, nil
}

// SubscribeForMessagesToSign is used to subscribe to the message broadcast channel.
// After perparing inputs and signing over them, Broadcast should be called.
//
// If the passed channel is full at any point, it will be dropped from subscription and closed.
// To stop subscribing, either the closer function can be used, or the channel can be abandoned.
// Passing a channel multiple times to the Subscribe function will result in a panic.
func (m *F3) SubscribeForMessagesToSign(ch chan<- *gpbft.MessageBuilder) (closer func()) {
	_, closer = m.busBroadcast.Subscribe(ch)
	return closer
}

func (m *F3) Manifest() *manifest.Manifest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.manifest
}

func (m *F3) Broadcast(ctx context.Context, signatureBuilder *gpbft.SignatureBuilder, msgSig []byte, vrf []byte) {
	msg := signatureBuilder.Build(msgSig, vrf)

	m.mu.Lock()
	runner := m.runner
	m.mu.Unlock()

	if runner == nil {
		log.Error("attempted to broadcast message while F3 wasn't running")
		return
	}

	err := runner.BroadcastMessage(msg)
	if err != nil {
		log.Errorf("failed to broadcast message: %+v", err)
	}
}

func (m *F3) GetLatestCert(ctx context.Context) (*certs.FinalityCertificate, error) {
	m.mu.Lock()
	cs := m.cs
	m.mu.Unlock()

	if cs == nil {
		return nil, xerrors.Errorf("F3 is not running")
	}
	return cs.Latest(), nil
}

func (m *F3) GetCert(ctx context.Context, instance uint64) (*certs.FinalityCertificate, error) {
	m.mu.Lock()
	cs := m.cs
	m.mu.Unlock()

	if cs == nil {
		return nil, xerrors.Errorf("F3 is not running")
	}
	return cs.Get(ctx, instance)
}

// Returns the time at which the F3 instance specified by the passed manifest should be started, or
// 0 if the passed manifest is nil.
func (m *F3) computeBootstrapDelay(manifest *manifest.Manifest) (time.Duration, error) {
	if manifest == nil {
		return 0, nil
	}

	ts, err := m.ec.GetHead(m.runningCtx)
	if err != nil {
		err := xerrors.Errorf("failed to get the EC chain head: %w", err)
		return 0, err
	}

	currentEpoch := ts.Epoch()
	if currentEpoch >= manifest.BootstrapEpoch {
		return 0, nil
	}
	epochDelay := manifest.BootstrapEpoch - currentEpoch
	start := ts.Timestamp().Add(time.Duration(epochDelay) * manifest.ECPeriod)
	delay := time.Until(start)
	// Add additional delay to skip over null epochs. That way we wait the full 900 epochs.
	if delay <= 0 {
		delay = manifest.ECPeriod + delay%manifest.ECPeriod
	}
	return delay, nil
}

// Run start the module. It will exit when context is cancelled.
// Or if there is an error from the message handling routines.
func (m *F3) Start(startCtx context.Context) (_err error) {
	err := m.manifestProvider.Start(startCtx)
	if err != nil {
		return err
	}

	// Try to get an initial manifest immediately if possible so uses can query it immediately.
	var pendingManifest *manifest.Manifest
	select {
	case pendingManifest = <-m.manifestProvider.ManifestUpdates():
		m.mu.Lock()
		m.manifest = pendingManifest
		m.mu.Unlock()
	default:
	}

	initialDelay, err := m.computeBootstrapDelay(pendingManifest)
	if err != nil {
		return err
	}

	// Try to start immediately if we have a manifest available and don't have to wait to
	// bootstrap. That way, we'll be fully started when we return from Start.
	if initialDelay == 0 {
		if err := m.reconfigure(startCtx, pendingManifest); err != nil {
			return xerrors.Errorf("failed to start GPBFT: %w", err)
		}
		pendingManifest = nil
	}

	m.errgrp.Go(func() (_err error) {
		defer func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.stopInternal(context.Background())
		}()

		manifestChangeTimer := time.NewTimer(initialDelay)
		if pendingManifest == nil && !manifestChangeTimer.Stop() {
			<-manifestChangeTimer.C
		}

		defer manifestChangeTimer.Stop()
		for m.runningCtx.Err() == nil {
			select {
			case update := <-m.manifestProvider.ManifestUpdates():
				if pendingManifest != nil && !manifestChangeTimer.Stop() {
					<-manifestChangeTimer.C
				}
				pendingManifest = update
			case <-manifestChangeTimer.C:
			case <-m.runningCtx.Done():
				return nil
			}

			if delay, err := m.computeBootstrapDelay(pendingManifest); err != nil {
				return err
			} else if delay > 0 {
				manifestChangeTimer.Reset(delay)
			} else {
				if err := m.reconfigure(m.runningCtx, pendingManifest); err != nil {
					return xerrors.Errorf("failed to reconfigure F3: %w", err)
				}
				pendingManifest = nil
			}
		}
		return nil
	})

	return nil
}

func (m *F3) Stop(stopCtx context.Context) (_err error) {
	m.cancelCtx()
	return multierr.Combine(
		m.manifestProvider.Stop(stopCtx),
		m.errgrp.Wait(),
	)
}

func (m *F3) stopInternal(ctx context.Context) {
	if m.runner != nil {
		// Log and ignore shutdown errors.
		if err := m.runner.Stop(ctx); err != nil {
			log.Errorw("failed to stop gpbft", "error", err)
		}
		m.runner = nil
	}
	if m.certsub != nil {
		if err := m.certsub.Stop(ctx); err != nil {
			log.Errorw("failed to stop certificate exchange subscriber", "error", err)
		}
		m.certsub = nil
	}
	if m.certserv != nil {
		if err := m.certserv.Stop(ctx); err != nil {
			log.Errorw("failed to stop certificate exchange server", "error", err)
		}
		m.certserv = nil
	}
}

func (m *F3) reconfigure(ctx context.Context, manifest *manifest.Manifest) (_err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopInternal(ctx)

	if manifest == nil {
		return nil
	}

	runnerEc := m.ec
	if len(manifest.PowerUpdate) > 0 {
		runnerEc = ec.WithModifiedPower(m.ec, manifest.PowerUpdate)
	}

	cs, err := openCertstore(m.runningCtx, runnerEc, m.ds, manifest)
	if err != nil {
		return xerrors.Errorf("failed to open certstore: %w", err)
	}

	m.cs = cs
	m.manifest = manifest
	m.certserv = &certexchange.Server{
		NetworkName:    m.manifest.NetworkName,
		RequestTimeout: m.manifest.ServerRequestTimeout,
		Host:           m.host,
		Store:          m.cs,
	}
	if err := m.certserv.Start(ctx); err != nil {
		return err
	}

	m.certsub = &certexpoll.Subscriber{
		Client: certexchange.Client{
			Host:           m.host,
			NetworkName:    m.manifest.NetworkName,
			RequestTimeout: m.manifest.ClientRequestTimeout,
		},
		Store:               m.cs,
		SignatureVerifier:   m.verifier,
		InitialPollInterval: m.manifest.ECPeriod,
		MaximumPollInterval: m.manifest.MaximumPollInterval,
		MinimumPollInterval: m.manifest.MinimumPollInterval,
	}
	if err := m.certsub.Start(ctx); err != nil {
		return err
	}

	if runner, err := newRunner(
		ctx, m.cs, runnerEc, m.pubsub, m.verifier,
		m.busBroadcast.Publish, m.manifest,
	); err != nil {
		return err
	} else {
		m.runner = runner
	}

	return m.runner.Start(ctx)
}

type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
}

// IsRunning returns true if gpbft is running
// Used mainly for testing purposes
func (m *F3) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runner != nil
}

// GetPowerTable returns the power table for the given tipset
// Used mainly for testing purposes
func (m *F3) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	m.mu.Lock()
	manifest := m.manifest
	m.mu.Unlock()

	if manifest == nil {
		return nil, xerrors.Errorf("no known network manifest")
	}

	return ec.WithModifiedPower(m.ec, manifest.PowerUpdate).GetPowerTable(ctx, ts)
}
