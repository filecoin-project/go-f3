package f3

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
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
)

// ErrF3NotRunning is returned when an operation is attempted on a non-running F3
// instance.
var ErrF3NotRunning = errors.New("f3 is not running")

type f3State struct {
	cs       *certstore.Store
	runner   *gpbftRunner
	ps       *powerstore.Store
	certsub  *certexpoll.Subscriber
	certserv *certexchange.Server
}

type F3 struct {
	verifier gpbft.Verifier
	mfst     manifest.Manifest
	diskPath string

	outboundMessages chan *gpbft.MessageBuilder

	host   host.Host
	ds     datastore.Datastore
	ec     ec.Backend
	pubsub *pubsub.PubSub
	clock  clock.Clock

	runningCtx context.Context
	cancelCtx  context.CancelFunc

	state atomic.Pointer[f3State]
}

// New creates and setups f3 with libp2p
// The context is used for initialization not runtime.
func New(_ctx context.Context, manifest manifest.Manifest, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, verif gpbft.Verifier, ecBackend ec.Backend, diskPath string) (*F3, error) {
	runningCtx, cancel := context.WithCancel(context.WithoutCancel(_ctx))

	// concurrency is limited to half of the number of CPUs, and cache size is set to 256 which is more than 2x max ECChain size
	ecBackend = ec.NewPowerCachingECWrapper(ecBackend, max(runtime.NumCPU()/2, 8), 256)
	return &F3{
		verifier:         verif,
		mfst:             manifest,
		diskPath:         diskPath,
		outboundMessages: make(chan *gpbft.MessageBuilder, 128),
		host:             h,
		ds:               ds,
		ec:               ecBackend,
		pubsub:           ps,
		clock:            clock.GetClock(runningCtx),
		runningCtx:       runningCtx,
		cancelCtx:        cancel,
	}, nil
}

// MessagesToSign returns a channel of outbound messages that need to be signed by the client(s).
// - The same channel is shared between all callers and will never be closed.
// - GPBFT will block if this channel is not read from.
func (m *F3) MessagesToSign() <-chan *gpbft.MessageBuilder {
	return m.outboundMessages
}

func (m *F3) Manifest() manifest.Manifest { return m.mfst }

func (m *F3) Broadcast(ctx context.Context, signatureBuilder *gpbft.SignatureBuilder, msgSig []byte, vrf []byte) {
	state := m.state.Load()
	if state == nil {
		log.Error("attempted to broadcast message while F3 wasn't running")
		return
	}

	if m.mfst.NetworkName != signatureBuilder.NetworkName {
		log.Errorw("attempted to broadcast message for a wrong network",
			"manifestNetwork", m.mfst.NetworkName, "messageNetwork", signatureBuilder.NetworkName)
		return
	}

	msg := signatureBuilder.Build(msgSig, vrf)
	err := state.runner.BroadcastMessage(ctx, msg)
	if err != nil {
		log.Warnf("failed to broadcast message: %+v", err)
	}
}

func (m *F3) GetLatestCert(context.Context) (*certs.FinalityCertificate, error) {
	if state := m.state.Load(); state != nil {
		return state.cs.Latest(), nil
	}
	return nil, ErrF3NotRunning
}

func (m *F3) GetCert(ctx context.Context, instance uint64) (*certs.FinalityCertificate, error) {
	if state := m.state.Load(); state != nil {
		return state.cs.Get(ctx, instance)
	}
	return nil, ErrF3NotRunning
}

// computeBootstrapDelay returns the time at which the F3 instance specified by
// the passed manifest should be started.
func (m *F3) computeBootstrapDelay() (time.Duration, error) {
	ts, err := m.ec.GetHead(m.runningCtx)
	if err != nil {
		return 0, fmt.Errorf("failed to get the EC chain head: %w", err)
	}

	currentEpoch := ts.Epoch()
	if currentEpoch >= m.mfst.BootstrapEpoch {
		return 0, nil
	}
	epochDelay := m.mfst.BootstrapEpoch - currentEpoch
	start := ts.Timestamp().Add(time.Duration(epochDelay) * m.mfst.EC.Period)
	delay := m.clock.Until(start)
	// Add additional delay to skip over null epochs. That way we wait the full 900 epochs.
	if delay <= 0 {
		delay = m.mfst.EC.Period + delay%m.mfst.EC.Period
	}
	return delay, nil
}

// Start the module, call Stop to exit. Canceling the past context will cancel the request to start
// F3, it won't stop the service after it has started.
func (m *F3) Start(startCtx context.Context) (_err error) {

	initialDelay, err := m.computeBootstrapDelay()
	if err != nil {
		return fmt.Errorf("failed to compute bootstrap delay: %w", err)
	}

	// Try to start immediately if there's no initial delay and pass on any start
	// errors directly.
	if initialDelay == 0 {
		return m.startInternal(startCtx)
	}

	log.Infow("F3 is scheduled to start with initial delay", "initialDelay", initialDelay,
		"NetworkName", m.mfst.NetworkName, "BootstrapEpoch", m.mfst.BootstrapEpoch, "Finality", m.mfst.EC.Finality,
		"InitialPowerTable", m.mfst.InitialPowerTable, "CommitteeLookback", m.mfst.CommitteeLookback)

	go func() {
		startTimer := m.clock.Timer(initialDelay)
		defer startTimer.Stop()
		select {
		case <-m.runningCtx.Done():
			log.Debugw("F3 start disrupted", "cause", m.runningCtx.Err())
		case startTime := <-startTimer.C:
			if err := m.startInternal(m.runningCtx); err != nil {
				log.Errorw("Failed to start F3 after initial delay", "scheduledStartTime", startTime, "err", err)
			}
		}
	}()

	return nil
}

// Stop F3.
func (m *F3) Stop(context.Context) (_err error) {
	m.cancelCtx()
	return nil
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

	if m.mfst.ProtocolVersion > manifest.VersionCapability {
		return fmt.Errorf("manifest version %d is higher than current capability %d", m.mfst.ProtocolVersion, manifest.VersionCapability)
	}

	// We don't reset these fields if we only pause/resume.
	certClient := certexchange.Client{
		Host:           m.host,
		NetworkName:    m.mfst.NetworkName,
		RequestTimeout: m.mfst.CertificateExchange.ClientRequestTimeout,
	}
	cds := measurements.NewMeteredDatastore(meter, "f3_certstore_datastore_", m.ds)
	state.cs, err = openCertstore(ctx, m.ec, cds, m.mfst, certClient)
	if err != nil {
		return fmt.Errorf("failed to open certstore: %w", err)
	}

	pds := measurements.NewMeteredDatastore(meter, "f3_ohshitstore_datastore_", m.ds)
	state.ps, err = powerstore.New(ctx, m.ec, pds, state.cs, m.mfst)
	if err != nil {
		return fmt.Errorf("failed to construct the oshitstore: %w", err)
	}

	state.certserv = &certexchange.Server{
		NetworkName:    m.mfst.NetworkName,
		RequestTimeout: m.mfst.CertificateExchange.ServerRequestTimeout,
		Host:           m.host,
		Store:          state.cs,
	}

	state.certsub = &certexpoll.Subscriber{
		Client: certexchange.Client{
			Host:           m.host,
			NetworkName:    m.mfst.NetworkName,
			RequestTimeout: m.mfst.CertificateExchange.ClientRequestTimeout,
		},
		Store:               state.cs,
		SignatureVerifier:   m.verifier,
		InitialPollInterval: m.mfst.EC.Period,
		MaximumPollInterval: m.mfst.CertificateExchange.MaximumPollInterval,
		MinimumPollInterval: m.mfst.CertificateExchange.MinimumPollInterval,
	}
	cleanName := strings.ReplaceAll(string(m.mfst.NetworkName), "/", "-")
	cleanName = strings.ReplaceAll(cleanName, ".", "")
	cleanName = strings.ReplaceAll(cleanName, "\u0000", "")

	walPath := filepath.Join(m.diskPath, "wal", cleanName)
	wal, err := writeaheadlog.Open[walEntry](walPath)
	if err != nil {
		return fmt.Errorf("opening WAL: %w", err)
	}

	state.runner, err = newRunner(
		ctx, state.cs, state.ps, m.pubsub, m.verifier,
		m.outboundMessages, m.mfst, wal, m.host.ID(),
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
	return m.ec.GetPowerTable(ctx, ts)
}

func (m *F3) Progress() (instant gpbft.InstanceProgress) {
	if st := m.state.Load(); st != nil && st.runner != nil {
		instant = st.runner.Progress()
	}
	return
}
