package f3

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Kubuxu/go-broadcast"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

type F3 struct {
	// mLk protects the runner and certStore
	mLk    sync.Mutex
	runner *gpbftRunner
	// certStore is nil until Run is called on the F3
	csLk      sync.Mutex
	certStore *certstore.Store

	cancelCtx context.CancelFunc
	host      host.Host
	ds        datastore.Datastore
	ec        ec.Backend
	log       Logger

	// psLk protects pubsub. We need an independent one
	// due to its use in Broadcast
	psLk   sync.Mutex
	pubsub *pubsub.PubSub
	msgSub *pubsub.Subscription

	client *client
}

type client struct {
	// certStore is nil until Run is called on the F3
	certStore *certstore.Store
	manifest  manifest.ManifestProvider
	ec        ec.Backend

	signingMarshaller gpbft.SigningMarshaler

	busBroadcast broadcast.Channel[*gpbft.MessageBuilder]

	gpbft.Verifier
	logger         Logger
	loggerWithSkip Logger

	// Populated after Run is called
	messageQueue <-chan gpbft.ValidatedMessage
	topic        *pubsub.Topic

	// Notifies manifest updates
	manifestUpdate <-chan uint64
	// Triggers the cancellation of the incoming message
	// routine to avoid delivering any outstanding messages.
	incomingCancel func()
}

func (mc *client) BroadcastMessage(ctx context.Context, mb *gpbft.MessageBuilder) error {
	mb.SetNetworkName(mc.manifest.Manifest().NetworkName)
	mb.SetSigningMarshaler(mc.signingMarshaller)
	mc.busBroadcast.Publish(mb)
	return nil
}

func (mc *client) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	// Apply power table deltas from the manifest
	pt, err := mc.ec.GetPowerTable(ctx, ts)
	if err != nil {
		return nil, xerrors.Errorf("getting power table: %w", err)
	}
	return certs.ApplyPowerTableDiffs(pt, mc.manifest.Manifest().PowerUpdate)
}

func (mc *client) IncomingMessages() <-chan gpbft.ValidatedMessage {
	return mc.messageQueue
}

var _ gpbft.Tracer = (*client)(nil)

// Log fulfills the gpbft.Tracer interface
func (mc *client) Log(fmt string, args ...any) {
	mc.loggerWithSkip.Debugf(fmt, args...)
}

func (mc *client) Logger() Logger {
	return mc.logger
}

// New creates and setups f3 with libp2p
// The context is used for initialization not runtime.
// signingMarshaller can be nil for default SigningMarshaler
func New(ctx context.Context, manifest manifest.ManifestProvider, ds datastore.Datastore, h host.Host, manifestServer peer.ID,
	ps *pubsub.PubSub, verif gpbft.Verifier, ec ec.Backend, log Logger, signingMarshaller gpbft.SigningMarshaler) (*F3, error) {
	ds = namespace.Wrap(ds, manifest.Manifest().DatastorePrefix())
	loggerWithSkip := log
	if zapLogger, ok := log.(*logging.ZapEventLogger); ok {
		loggerWithSkip = logging.WithSkip(zapLogger, 1)
	}
	if signingMarshaller == nil {
		signingMarshaller = gpbft.DefaultSigningMarshaller
	}

	m := F3{
		ds:     ds,
		host:   h,
		pubsub: ps,
		ec:     ec,
		log:    log,

		client: &client{
			ec:                ec,
			manifest:          manifest,
			Verifier:          verif,
			logger:            log,
			loggerWithSkip:    loggerWithSkip,
			signingMarshaller: signingMarshaller,
		},
	}

	return &m, nil
}

// SubscribeForMessagesToSign is used to subscribe to the message broadcast channel.
// After perparing inputs and signing over them, Broadcast should be called.
//
// If the passed channel is full at any point, it will be dropped from subscription and closed.
// To stop subscribing, either the closer function can be used, or the channel can be abandoned.
// Passing a channel multiple times to the Subscribe function will result in a panic.
func (m *F3) SubscribeForMessagesToSign(ch chan<- *gpbft.MessageBuilder) (closer func()) {
	_, closer = m.client.busBroadcast.Subscribe(ch)
	return closer
}

func (m *F3) Manifest() manifest.Manifest {
	return m.client.manifest.Manifest()
}

func (m *F3) Broadcast(ctx context.Context, signatureBuilder *gpbft.SignatureBuilder, msgSig []byte, vrf []byte) {
	msg := signatureBuilder.Build(msgSig, vrf)

	var bw bytes.Buffer
	err := msg.MarshalCBOR(&bw)
	if err != nil {
		m.log.Errorf("marshalling GMessage: %+v", err)
		return
	}

	m.psLk.Lock()
	if m.client.topic != nil {
		err = m.client.topic.Publish(ctx, bw.Bytes())
		if err != nil {
			m.log.Errorf("publishing on topic: %w", err)
		}
	}
	m.psLk.Unlock()
}

func (m *F3) setCertStore(cs *certstore.Store) {
	m.csLk.Lock()
	defer m.csLk.Unlock()
	m.certStore = cs
	m.client.certStore = cs
}

func (m *F3) setupPubsub(runner *gpbftRunner) error {
	pubsubTopicName := m.Manifest().PubSubTopic()

	// explicit type to typecheck the anonymous function defintion
	// a bit ugly but I don't want gpbftRunner to know about pubsub
	var validator pubsub.ValidatorEx = func(ctx context.Context, pID peer.ID,
		msg *pubsub.Message) pubsub.ValidationResult {

		var gmsg gpbft.GMessage
		err := gmsg.UnmarshalCBOR(bytes.NewReader(msg.Data))
		if err != nil {
			return pubsub.ValidationReject
		}
		validatedMessage, err := runner.ValidateMessage(&gmsg)
		if errors.Is(err, gpbft.ErrValidationInvalid) {
			m.log.Debugf("validation error during validation: %+v", err)
			return pubsub.ValidationReject
		}
		if err != nil {
			m.log.Warnf("unknown error during validation: %+v", err)
			return pubsub.ValidationIgnore
		}
		msg.ValidatorData = validatedMessage
		return pubsub.ValidationAccept
	}

	err := m.pubsub.RegisterTopicValidator(pubsubTopicName, validator)
	if err != nil {
		return xerrors.Errorf("registering topic validator: %w", err)
	}

	topic, err := m.pubsub.Join(pubsubTopicName)
	if err != nil {
		return xerrors.Errorf("could not join on pubsub topic: %s: %w", pubsubTopicName, err)
	}
	m.client.topic = topic
	return nil
}

func (m *F3) teardownPubsub(manifest manifest.Manifest) (err error) {
	m.psLk.Lock()
	defer m.psLk.Unlock()
	if m.client.topic != nil {
		m.msgSub.Cancel()
		err = multierr.Combine(
			m.pubsub.UnregisterTopicValidator(manifest.PubSubTopic()),
			m.client.topic.Close(),
		)
		m.client.topic = nil
	}
	return err
}

func (m *F3) initGpbftRunner(ctx context.Context) error {
	m.mLk.Lock()
	defer m.mLk.Unlock()

	cs, err := certstore.OpenStore(ctx, m.ds)
	if err == nil {
		m.setCertStore(cs)
	} else if errors.Is(err, certstore.ErrNotInitialized) {
		err := m.boostrap(ctx)
		if err != nil {
			return xerrors.Errorf("failed to boostrap: %w", err)
		}
	} else {
		return xerrors.Errorf("opening certstore: %w", err)
	}

	m.runner, err = newRunner(m.client.manifest, m.client)
	if err != nil {
		return xerrors.Errorf("creating gpbft host: %w", err)
	}

	m.psLk.Lock()
	if err := m.setupPubsub(m.runner); err != nil {
		return xerrors.Errorf("setting up pubsub: %w", err)
	}
	m.msgSub, err = m.client.topic.Subscribe()
	if err != nil {
		m.psLk.Unlock()
		return xerrors.Errorf("subscribing to topic: %w", err)
	}
	m.psLk.Unlock()
	return nil
}

// Sets up the gpbft runner, this is triggered at initialization
func (m *F3) startGpbftRunner(ctx context.Context, errCh chan error) {
	if err := m.initGpbftRunner(ctx); err != nil {
		errCh <- xerrors.Errorf("initializing gpbft host: %w", err)
		return
	}

	// the size of the buffer is set to prevent message spamming from pubsub,
	// so it is a big enough buffer to be able to accommodate new messages
	// at "high-rate", but small enough to avoid spamming or clogging the node.
	messageQueue := make(chan gpbft.ValidatedMessage, 20)
	m.client.messageQueue = messageQueue

	go func() {
		m.mLk.Lock()
		latest := m.certStore.Latest()
		m.mLk.Unlock()
		startInstance := m.Manifest().InitialInstance
		if latest != nil {
			startInstance = latest.GPBFTInstance + 1
		}
		// Check context before starting the instance
		select {
		case <-ctx.Done():
			// If the context is cancelled before starting, return immediately
			errCh <- ctx.Err()
			return
		default:
			// Proceed with running the instance
			err := m.runner.Run(startInstance, ctx)
			if err != nil {
				m.log.Errorf("error returned while running host: %+v", err)
			}
			errCh <- err
		}
	}()

	m.handleIncomingMessages(ctx, messageQueue)
}

func (m *F3) boostrap(ctx context.Context) error {
	head, err := m.ec.GetHead(ctx)
	if err != nil {
		return xerrors.Errorf("failed to get the head: %w", err)
	}

	if head.Epoch() < m.Manifest().BootstrapEpoch {
		// wait for bootstrap epoch
		for {
			head, err := m.ec.GetHead(ctx)
			if err != nil {
				return xerrors.Errorf("getting head: %w", err)
			}
			if head.Epoch() >= m.Manifest().BootstrapEpoch {
				break
			}

			m.log.Infof("wating for bootstrap epoch (%d): currently at epoch %d", m.Manifest().BootstrapEpoch, head.Epoch())
			aim := time.Until(head.Timestamp().Add(m.Manifest().ECPeriod))
			// correct for null epochs
			for aim < 0 {
				aim += m.Manifest().ECPeriod
			}

			select {
			case <-time.After(aim):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	ts, err := m.ec.GetTipsetByEpoch(ctx, m.Manifest().BootstrapEpoch-m.Manifest().ECFinality)
	if err != nil {
		return xerrors.Errorf("getting initial power tipset: %w", err)
	}

	initialPowerTable, err := m.ec.GetPowerTable(ctx, ts.Key())
	if err != nil {
		return xerrors.Errorf("getting initial power table: %w", err)
	}

	cs, err := certstore.CreateStore(ctx, m.ds, m.Manifest().InitialInstance, initialPowerTable)
	if err != nil {
		return xerrors.Errorf("creating certstore: %w", err)
	}
	m.setCertStore(cs)
	return nil
}

func (m *F3) GetLatestCert(ctx context.Context) (*certs.FinalityCertificate, error) {
	m.mLk.Lock()
	defer m.mLk.Unlock()
	if m.certStore == nil {
		return nil, xerrors.Errorf("F3 is not running")
	}
	return m.certStore.Latest(), nil
}
func (m *F3) GetCert(ctx context.Context, instance uint64) (*certs.FinalityCertificate, error) {
	m.mLk.Lock()
	defer m.mLk.Unlock()
	if m.certStore == nil {
		return nil, xerrors.Errorf("F3 is not running")
	}
	return m.certStore.Get(ctx, instance)
}

// Run start the module. It will exit when context is cancelled.
// Or if there is an error from the message handling routines.
func (m *F3) Run(ctx context.Context) error {
	ctx, m.cancelCtx = context.WithCancel(ctx)
	defer m.cancelCtx()

	runnerErrCh := make(chan error, 1)
	manifestErrCh := make(chan error, 1)

	// bootstrap runner for the initial manifest
	go m.startGpbftRunner(ctx, runnerErrCh)

	// run manifest provider. This runs a background goroutine that will
	// handle dynamic manifest updates if this is a dynamic manifest provider.
	// If it is a static manifest it does nothing.
	go m.client.manifest.Run(ctx, manifestErrCh)

	// teardown pubsub on shutdown
	var err error
	defer func() {
		teardownErr := m.teardownPubsub(m.Manifest())
		err = multierr.Append(err, teardownErr)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err = <-runnerErrCh:
		return err
	case err = <-manifestErrCh:
		return err
	}
}

func (m *F3) Stop() {
	m.mLk.Lock()
	defer m.mLk.Unlock()

	m.pauseRunner()

	if m.cancelCtx != nil {
		m.cancelCtx()
	}
}

func (m *F3) pauseRunner() {
	if m.runner != nil {
		m.runner.Stop()
	}
	m.runner = nil
}

func (m *F3) handleIncomingMessages(ctx context.Context, queue chan gpbft.ValidatedMessage) {
	ctx, m.client.incomingCancel = context.WithCancel(ctx)
loop:
	for {
		var msg *pubsub.Message
		msg, err := m.msgSub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			m.log.Errorf("msgPubsub subscription.Next() returned an error: %+v", err)
			break
		}
		gmsg, ok := msg.ValidatorData.(gpbft.ValidatedMessage)
		if !ok {
			m.log.Errorf("invalid msgValidatorData: %+v", msg.ValidatorData)
			continue
		}
		select {
		case queue <- gmsg:
		case <-ctx.Done():
			break loop
		}
	}
}

// Callback to be triggered when there is a dynamic manifest change
// If the manifest triggers a rebootstrap it starts a new runner with the new configuration.
// If there is no rebootstrap it only starts a new pubsub topic with a new network name that
// depends on the manifest version so there is no overlap between different configuration instances
func ManifestChangeCallback(m *F3) manifest.OnManifestChange {
	// We can only accommodate 1 manifest update at a time, thus
	// the buffer size.
	manifestUpdate := make(chan uint64, 1)
	m.client.manifestUpdate = manifestUpdate
	return func(ctx context.Context, prevManifest manifest.Manifest, errCh chan error) {
		// Tear down pubsub.
		if err := m.teardownPubsub(prevManifest); err != nil {
			// for now we just log the error and continue.
			// This is not critical, but alternative approaches welcome.
			m.log.Errorf("error stopping gpbft runner: %+v", err)
		}
		// empty message queue from outstanding messages
		m.emptyMessageQueue()

		if m.Manifest().Pause {
			m.pauseCallback()
		} else {

			if m.Manifest().ReBootstrap {
				m.withRebootstrapCallback(ctx, errCh)
			} else {
				m.withoutRebootstrapCallback(ctx, manifestUpdate, errCh)
			}
		}

	}
}

func (m *F3) withRebootstrapCallback(ctx context.Context, errCh chan error) {
	m.mLk.Lock()
	m.log.Infof("triggering manifest (seq=%d) change with rebootstrap", m.Manifest().Sequence)

	// if runner still up
	if m.runner != nil {
		m.runner.Stop()
	}

	// clear the certstore
	if err := m.certStore.DeleteAll(ctx); err != nil {
		errCh <- xerrors.Errorf("clearing certstore: %w", err)
		m.mLk.Unlock()
		return
	}
	m.mLk.Unlock()
	m.startGpbftRunner(ctx, errCh)
}

func (m *F3) withoutRebootstrapCallback(ctx context.Context, manifestUpdate chan uint64, errCh chan error) {
	m.mLk.Lock()
	m.log.Infof("triggering manifest (seq=%d) change without rebootstrap", m.Manifest().Sequence)

	if m.runner != nil {
		m.log.Error("cannot trigger a callback without rebootstrap if the runner is stop")
	}

	// immediately stop listening to network messages
	m.client.incomingCancel()
	m.psLk.Lock()
	if err := m.setupPubsub(m.runner); err != nil {
		errCh <- xerrors.Errorf("setting up pubsub: %w", err)
		return
	}
	var err error
	m.msgSub, err = m.client.topic.Subscribe()
	if err != nil {
		errCh <- xerrors.Errorf("subscribing to topic: %w", err)
		return
	}
	m.psLk.Unlock()

	messageQueue := make(chan gpbft.ValidatedMessage, 20)
	m.client.messageQueue = messageQueue
	// notify update to host to pick up the new message queue
	fc := m.certStore.Latest()
	manifestUpdate <- fc.GPBFTInstance
	m.mLk.Unlock()
	m.handleIncomingMessages(ctx, messageQueue)
}

func (m *F3) pauseCallback() {
	m.mLk.Lock()
	defer m.mLk.Unlock()
	m.log.Infof("triggering manifest (seq=%d) change with pause", m.Manifest().Sequence)
	m.pauseRunner()
}

func (m *F3) emptyMessageQueue() {
	for {
		select {
		case <-m.client.messageQueue:
			m.log.Debug("emptying message queue")
		default:
			return
		}
	}
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
	m.mLk.Lock()
	defer m.mLk.Unlock()
	return m.runner != nil
}

// GetPowerTable returns the power table for the given tipset
// Used mainly for testing purposes
func (m *F3) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return m.client.GetPowerTable(ctx, ts)
}
