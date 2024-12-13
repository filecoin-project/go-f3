package f3

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-f3/certexchange"
	certexpoll "github.com/filecoin-project/go-f3/certexchange/polling"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/measurements"
	"github.com/filecoin-project/go-f3/internal/powerstore"
	"github.com/filecoin-project/go-f3/internal/writeaheadlog"
	"github.com/filecoin-project/go-f3/manifest"

	"github.com/ipfs/go-datastore"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"

	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

type f3State struct {
	cs       *certstore.Store
	runner   *gpbftRunner
	ps       *powerstore.Store
	certsub  *certexpoll.Subscriber
	certserv *certexchange.Server
	manifest *manifest.Manifest
}

type F3 struct {
	verifier         gpbft.Verifier
	manifestProvider manifest.ManifestProvider
	diskPath         string

	outboundMessages chan *gpbft.MessageBuilder

	host   host.Host
	ds     datastore.Datastore
	ec     ec.Backend
	pubsub *pubsub.PubSub
	clock  clock.Clock

	runningCtx context.Context
	cancelCtx  context.CancelFunc
	errgrp     *errgroup.Group

	manifest atomic.Pointer[manifest.Manifest]
	state    atomic.Pointer[f3State]
}

// New creates and setups f3 with libp2p
// The context is used for initialization not runtime.
func New(_ctx context.Context, manifest manifest.ManifestProvider, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, verif gpbft.Verifier, ec ec.Backend, diskPath string) (*F3, error) {
	runningCtx, cancel := context.WithCancel(context.WithoutCancel(_ctx))
	errgrp, runningCtx := errgroup.WithContext(runningCtx)

	return &F3{
		verifier:         verif,
		manifestProvider: manifest,
		diskPath:         diskPath,
		outboundMessages: make(chan *gpbft.MessageBuilder, 128),
		host:             h,
		ds:               ds,
		ec:               ec,
		pubsub:           ps,
		clock:            clock.GetClock(runningCtx),
		runningCtx:       runningCtx,
		cancelCtx:        cancel,
		errgrp:           errgrp,
	}, nil
}

// MessageStoSign returns a channel of outbound messages that need to be signed by the client(s).
// - The same channel is shared between all callers and will never be closed.
// - GPBFT will block if this channel is not read from.
func (m *F3) MessagesToSign() <-chan *gpbft.MessageBuilder {
	return m.outboundMessages
}

func (m *F3) Manifest() *manifest.Manifest {
	return m.manifest.Load()
}

func (m *F3) Broadcast(ctx context.Context, signatureBuilder *gpbft.SignatureBuilder, msgSig []byte, vrf []byte) {
	state := m.state.Load()
	if state == nil {
		log.Error("attempted to broadcast message while F3 wasn't running")
		return
	}

	if state.manifest.NetworkName != signatureBuilder.NetworkName {
		log.Errorw("attempted to broadcast message for a wrong network",
			"manifestNetwork", state.manifest.NetworkName, "messageNetwork", signatureBuilder.NetworkName)
		return
	}

	msg := signatureBuilder.Build(msgSig, vrf)
	err := state.runner.BroadcastMessage(ctx, msg)
	if err != nil {
		log.Warnf("failed to broadcast message: %+v", err)
	}
}

func (m *F3) GetLatestCert(ctx context.Context) (*certs.FinalityCertificate, error) {
	if state := m.state.Load(); state != nil {
		return state.cs.Latest(), nil
	}
	return nil, fmt.Errorf("f3 is not running")
}

func (m *F3) GetCert(ctx context.Context, instance uint64) (*certs.FinalityCertificate, error) {
	if state := m.state.Load(); state != nil {
		return state.cs.Get(ctx, instance)
	}
	return nil, fmt.Errorf("f3 is not running")
}

// Returns the time at which the F3 instance specified by the passed manifest should be started, or
// 0 if the passed manifest is nil.
func (m *F3) computeBootstrapDelay() (time.Duration, error) {
	manifest := m.manifest.Load()
	if manifest == nil || manifest.Pause {
		return 0, nil
	}

	ts, err := m.ec.GetHead(m.runningCtx)
	if err != nil {
		err := fmt.Errorf("failed to get the EC chain head: %w", err)
		return 0, err
	}

	currentEpoch := ts.Epoch()
	if currentEpoch >= manifest.BootstrapEpoch {
		return 0, nil
	}
	epochDelay := manifest.BootstrapEpoch - currentEpoch
	start := ts.Timestamp().Add(time.Duration(epochDelay) * manifest.EC.Period)
	delay := m.clock.Until(start)
	// Add additional delay to skip over null epochs. That way we wait the full 900 epochs.
	if delay <= 0 {
		delay = manifest.EC.Period + delay%manifest.EC.Period
	}
	return delay, nil
}

// Start the module, call Stop to exit. Canceling the past context will cancel the request to start
// F3, it won't stop the service after it has started.
func (m *F3) Start(startCtx context.Context) (_err error) {
	err := m.manifestProvider.Start(startCtx)
	if err != nil {
		return err
	}

	// Try to get an initial manifest immediately if possible so uses can query it immediately.
	var hasPendingManifest bool
	select {
	case pendingManifest := <-m.manifestProvider.ManifestUpdates():
		metrics.manifestsReceived.Add(m.runningCtx, 1)
		m.manifest.Store(pendingManifest)
		hasPendingManifest = true
	default:
	}

	initialDelay, err := m.computeBootstrapDelay()
	if err != nil {
		return err
	}

	// Try to start immediately if we have a manifest available and don't have to wait to
	// bootstrap. That way, we'll be fully started when we return from Start.
	if initialDelay == 0 {
		if err := m.startInternal(startCtx); err != nil {
			log.Errorw("failed to reconfigure GPBFT", "error", err)
		}
		hasPendingManifest = false
	}

	if m := m.manifest.Load(); m != nil {
		log.Infow("F3 is starting", "initialDelay", initialDelay, "hasPendingManifest", hasPendingManifest,
			"NetworkName", m.NetworkName, "BootstrapEpoch", m.BootstrapEpoch, "Finality", m.EC.Finality,
			"InitialPowerTable", m.InitialPowerTable, "CommitteeLookback", m.CommitteeLookback)
	} else {
		log.Infow("F3 is starting", "initialDelay", initialDelay, "hasPendingManifest", hasPendingManifest)
	}

	m.errgrp.Go(func() (_err error) {
		defer func() {
			if err := m.stopInternal(context.Background()); err != nil {
				_err = multierr.Append(_err, err)
			}
		}()

		manifestChangeTimer := m.clock.Timer(initialDelay)
		if !hasPendingManifest && !manifestChangeTimer.Stop() {
			<-manifestChangeTimer.C
		}

		defer manifestChangeTimer.Stop()
		for m.runningCtx.Err() == nil {
			select {
			case update := <-m.manifestProvider.ManifestUpdates():
				metrics.manifestsReceived.Add(m.runningCtx, 1)
				if hasPendingManifest && !manifestChangeTimer.Stop() {
					<-manifestChangeTimer.C
				}
				if err := m.stopInternal(m.runningCtx); err != nil {
					// Don't fail here, just log and move on.
					log.Errorw("failed to stop running F3 instance", "error", err)
				}
				metrics.manifestsReceived.Add(m.runningCtx, 1)
				m.manifest.Store(update)
			case <-manifestChangeTimer.C:
			case <-m.runningCtx.Done():
				return nil
			}

			if delay, err := m.computeBootstrapDelay(); err != nil {
				return err
			} else if delay > 0 {
				hasPendingManifest = true
				manifestChangeTimer.Reset(delay)
				log.Infow("waiting for bootstrap epoch", "duration", delay.String())
			} else {
				if err := m.startInternal(m.runningCtx); err != nil {
					// There's not much we can do here but complain and wait for
					// a new manifest. This matches the behavior when we
					// initially start as well.
					log.Errorw("failed to start F3", "error", err)
				}
				hasPendingManifest = false
			}
		}
		return nil
	})

	return nil
}

// Stop F3.
func (m *F3) Stop(stopCtx context.Context) (_err error) {
	m.cancelCtx()
	return multierr.Combine(
		m.manifestProvider.Stop(stopCtx),
		m.errgrp.Wait(),
	)
}

func (s *f3State) stop(ctx context.Context) (err error) {
	log.Info("stopping F3 internals")
	if serr := s.ps.Stop(ctx); serr != nil {
		err = multierr.Append(err, fmt.Errorf("failed to stop ohshitstore: %w", serr))
	}
	if serr := s.runner.Stop(ctx); serr != nil {
		err = multierr.Append(err, fmt.Errorf("failed to stop gpbft: %w", serr))
	}
	if serr := s.certsub.Stop(ctx); serr != nil {
		err = multierr.Append(err, fmt.Errorf("failed to stop certificate exchange subscriber: %w", serr))
	}
	if serr := s.certserv.Stop(ctx); serr != nil {
		err = multierr.Append(err, fmt.Errorf("failed to stop certificate exchange server: %w", serr))
	}
	return err
}

func (s *f3State) start(ctx context.Context) error {
	if err := s.ps.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the ohshitstore: %w", err)
	}
	if err := s.certsub.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the certificate subscriber: %w", err)
	}
	if err := s.certserv.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the certificate server: %w", err)
	}
	if err := s.runner.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the gpbft runner: %w", err)
	}
	return nil
}

func (m *F3) stopInternal(ctx context.Context) error {
	if st := m.state.Swap(nil); st != nil {
		if err := st.stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *F3) startInternal(ctx context.Context) error {
	log.Info("resuming F3 internals")

	var (
		state f3State
		err   error
	)

	state.manifest = m.manifest.Load()
	// Pause if we we have no manifest, the manifest is paused, and/or we don't meet the minimum
	// version requirement.
	if state.manifest == nil ||
		state.manifest.Pause ||
		state.manifest.ProtocolVersion > manifest.VersionCapability {
		return nil
	}

	mPowerEc := ec.WithModifiedPower(m.ec, state.manifest.ExplicitPower, state.manifest.IgnoreECPower)

	// We don't reset these fields if we only pause/resume.
	certClient := certexchange.Client{
		Host:           m.host,
		NetworkName:    state.manifest.NetworkName,
		RequestTimeout: state.manifest.CertificateExchange.ClientRequestTimeout,
	}
	cds := measurements.NewMeteredDatastore(meter, "f3_certstore_datastore_", m.ds)
	state.cs, err = openCertstore(ctx, mPowerEc, cds, state.manifest, certClient)
	if err != nil {
		return fmt.Errorf("failed to open certstore: %w", err)
	}

	pds := measurements.NewMeteredDatastore(meter, "f3_ohshitstore_datastore_", m.ds)
	state.ps, err = powerstore.New(ctx, mPowerEc, pds, state.cs, state.manifest)
	if err != nil {
		return fmt.Errorf("failed to construct the oshitstore: %w", err)
	}

	state.certserv = &certexchange.Server{
		NetworkName:    state.manifest.NetworkName,
		RequestTimeout: state.manifest.CertificateExchange.ServerRequestTimeout,
		Host:           m.host,
		Store:          state.cs,
	}

	state.certsub = &certexpoll.Subscriber{
		Client: certexchange.Client{
			Host:           m.host,
			NetworkName:    state.manifest.NetworkName,
			RequestTimeout: state.manifest.CertificateExchange.ClientRequestTimeout,
		},
		Store:               state.cs,
		SignatureVerifier:   m.verifier,
		InitialPollInterval: state.manifest.EC.Period,
		MaximumPollInterval: state.manifest.CertificateExchange.MaximumPollInterval,
		MinimumPollInterval: state.manifest.CertificateExchange.MinimumPollInterval,
	}
	cleanName := strings.ReplaceAll(string(state.manifest.NetworkName), "/", "-")
	cleanName = strings.ReplaceAll(cleanName, ".", "")
	cleanName = strings.ReplaceAll(cleanName, "\u0000", "")

	walPath := filepath.Join(m.diskPath, "wal", cleanName)
	wal, err := writeaheadlog.Open[walEntry](walPath)
	if err != nil {
		return fmt.Errorf("opening WAL: %w", err)
	}

	state.runner, err = newRunner(
		ctx, state.cs, state.ps, m.pubsub, m.verifier,
		m.outboundMessages, state.manifest, wal, m.host.ID(),
	)
	if err != nil {
		return err
	}

	if err := state.start(ctx); err != nil {
		return err
	}

	if !m.state.CompareAndSwap(nil, &state) {
		return fmt.Errorf("concurrent start")
	}

	return nil
}

// IsRunning returns true if gpbft is running
// Used mainly for testing purposes
func (m *F3) IsRunning() bool {
	return m.state.Load() != nil
}

// GetPowerTable returns the power table for the given tipset
// Used mainly for testing purposes
func (m *F3) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	if st := m.state.Load(); st != nil {
		return st.ps.GetPowerTable(ctx, ts)
	}
	if manif := m.manifest.Load(); manif != nil {
		return ec.WithModifiedPower(m.ec, manif.ExplicitPower, manif.IgnoreECPower).
			GetPowerTable(ctx, ts)
	}
	return nil, fmt.Errorf("no known network manifest")
}

func (m *F3) Progress() (instant gpbft.Instant) {
	if st := m.state.Load(); st != nil && st.runner != nil {
		instant = st.runner.Progress()
	}
	return
}
