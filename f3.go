package f3

import (
	"bytes"
	"context"
	"errors"

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
	Manifest  manifest.ManifestProvider
	CertStore *certstore.Store

	runner *gpbftRunner
	msgSub *pubsub.Subscription
	ds     datastore.Datastore
	host   host.Host
	pubsub *pubsub.PubSub
	ec     ec.Backend
	log    Logger

	client *client
}

type client struct {
	certstore *certstore.Store
	id        gpbft.ActorID
	manifest  manifest.ManifestProvider
	ec        ec.Backend

	gpbft.Verifier
	gpbft.SignerWithMarshaler
	logger         Logger
	loggerWithSkip Logger

	// Populated after Run is called
	messageQueue <-chan gpbft.ValidatedMessage
	topic        *pubsub.Topic

	// Notifies manifest updates
	manifestUpdate <-chan struct{}
	// Triggers the cancellation of the incoming message
	// routine to avoid delivering any outstanding messages.
	incomingCancel func()
}

func (mc *client) BroadcastMessage(ctx context.Context, mb *gpbft.MessageBuilder) error {
	msg, err := mb.Build(mc.manifest.Manifest().NetworkName, mc.SignerWithMarshaler, mc.id)
	if err != nil {
		if errors.Is(err, gpbft.ErrNoPower) {
			return nil
		}
		mc.Log("building message for: %d: %+v", mc.id, err)
		return err
	}
	var bw bytes.Buffer
	err = msg.MarshalCBOR(&bw)
	if err != nil {
		mc.Log("marshalling GMessage: %+v", err)
	}
	err = mc.topic.Publish(ctx, bw.Bytes())
	if err != nil {
		if err == pubsub.ErrTopicClosed {
			return nil
		}
		return xerrors.Errorf("publishing on topic: %w", err)
	}
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
func New(ctx context.Context, id gpbft.ActorID, manifest manifest.ManifestProvider, ds datastore.Datastore, h host.Host, manifestServer peer.ID,
	ps *pubsub.PubSub, sigs gpbft.SignerWithMarshaler, verif gpbft.Verifier, ec ec.Backend, log Logger) (*F3, error) {
	ds = namespace.Wrap(ds, manifest.Manifest().DatastorePrefix())
	cs, err := certstore.OpenOrCreateStore(ctx, ds, 0, manifest.Manifest().InitialPowerTable)
	if err != nil {
		return nil, xerrors.Errorf("creating CertStore: %w", err)
	}
	loggerWithSkip := log
	if zapLogger, ok := log.(*logging.ZapEventLogger); ok {
		loggerWithSkip = logging.WithSkip(zapLogger, 1)
	}

	m := F3{
		Manifest:  manifest,
		CertStore: cs,

		ds:     ds,
		host:   h,
		pubsub: ps,
		ec:     ec,
		log:    log,

		client: &client{
			certstore:           cs,
			ec:                  ec,
			manifest:            manifest,
			id:                  id,
			Verifier:            verif,
			SignerWithMarshaler: sigs,
			logger:              log,
			loggerWithSkip:      loggerWithSkip,
		},
	}

	return &m, nil
}
func (m *F3) setupPubsub(runner *gpbftRunner) error {
	pubsubTopicName := m.Manifest.Manifest().PubSubTopic()

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

func (m *F3) teardownPubsub(manifest manifest.Manifest) error {
	m.msgSub.Cancel()
	return multierr.Combine(
		m.pubsub.UnregisterTopicValidator(manifest.PubSubTopic()),
		m.client.topic.Close(),
	)
}

// Sets up the gpbft runner, this is triggered at initialization
func (m *F3) setupGpbftRunner(ctx context.Context, initialInstance uint64, errCh chan error) {
	var err error
	m.runner, err = newRunner(m.client.id, m.Manifest, m.client)
	if err != nil {
		errCh <- xerrors.Errorf("creating gpbft host: %w", err)
		return
	}

	if err := m.setupPubsub(m.runner); err != nil {
		errCh <- xerrors.Errorf("setting up pubsub: %w", err)
		return
	}

	m.msgSub, err = m.client.topic.Subscribe()
	if err != nil {
		errCh <- xerrors.Errorf("subscribing to topic: %w", err)
		return
	}

	messageQueue := make(chan gpbft.ValidatedMessage, 20)
	m.client.messageQueue = messageQueue

	go func() {
		err := m.runner.Run(initialInstance, ctx)
		if err != nil {
			m.log.Errorf("eror returned while running host: %+v", err)
		}
		errCh <- err
	}()

	m.handleIncomingMessages(ctx, messageQueue)
}

// Run start the module. It will exit when context is cancelled.
// Or if there is an error from the message handling routines.
func (m *F3) Run(initialInstance uint64, ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	runnerErrCh := make(chan error, 1)
	manifestErrCh := make(chan error, 1)

	// bootstrap runner for the initial manifest
	go m.setupGpbftRunner(ctx, initialInstance, runnerErrCh)

	// run manifest provider. This runs a background goroutine that will
	// handle dynamic manifest updates if this is a dynamic manifest provider.
	// If it is a static manifest it does nothing.
	go m.Manifest.Run(ctx, manifestErrCh)

	// teardown pubsub on shutdown
	var err error
	defer func() {
		teardownErr := m.teardownPubsub(m.Manifest.Manifest())
		err = multierr.Append(err, teardownErr)
	}()

	select {
	case <-ctx.Done():
		if ctx.Err() != nil {
			return ctx.Err()
		}
	case err = <-runnerErrCh:
		return err
	case err = <-manifestErrCh:
		return err
	}

	return nil
}

func (m *F3) handleIncomingMessages(ctx context.Context, queue chan gpbft.ValidatedMessage) {
	ctx, m.client.incomingCancel = context.WithCancel(ctx)
loop:
	for {
		var msg *pubsub.Message
		msg, err := m.msgSub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				err = nil
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
	manifestUpdate := make(chan struct{}, 3)
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
		// Update the mmanifest in the client to update power table
		// and network name (without this signatures will fail)
		m.client.manifest = m.Manifest

		if m.Manifest.Manifest().ReBootstrap {
			// kill runner and teardown pubsub. This will also
			// teardown the pubsub topic
			m.runner.Stop()

			// TODO: Clean the datastore when we rebootstrap to avoid
			// persisting data between runs

			// when we rebootstrap we need to start from instance 0, because
			// we may not have anything in the certstore to fetch the
			// right chain through host.GetProposalForInstance

			m.setupGpbftRunner(ctx, 0, errCh)
		} else {
			// immediately stop listening to network messages
			m.client.incomingCancel()
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

			messageQueue := make(chan gpbft.ValidatedMessage, 20)
			m.client.messageQueue = messageQueue
			// notify update to host to pick up the new message queue
			manifestUpdate <- struct{}{}
			m.handleIncomingMessages(ctx, messageQueue)
		}
	}
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

// Methods exposed for testing purposes
func (m *F3) CurrentGpbftInstace() uint64 {
	return m.runner.participant.UnsafeCurrentInstance()
}

func (m *F3) IsRunning() bool {
	return m.runner != nil
}

func (m *F3) GetPowerTable(ctx context.Context, ts gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return m.client.GetPowerTable(ctx, ts)
}
