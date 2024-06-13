package f3

import (
	"bytes"
	"context"
	"errors"

	"github.com/filecoin-project/go-f3/certstore"
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

	ds     datastore.Datastore
	host   host.Host
	pubsub *pubsub.PubSub
	runner *gpbftRunner
	ec     ECBackend
	log    Logger

	client *client
}

type client struct {
	certstore *certstore.Store
	id        gpbft.ActorID
	nn        gpbft.NetworkName

	gpbft.Verifier
	gpbft.SignerWithMarshaler
	logger         Logger
	loggerWithSkip Logger

	// Populated after Run is called
	messageQueue <-chan gpbft.ValidatedMessage
	msgTopic     *pubsub.Topic
}

func (mc *client) BroadcastMessage(ctx context.Context, mb *gpbft.MessageBuilder) error {
	msg, err := mb.Build(mc.nn, mc.SignerWithMarshaler, mc.id)
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
	err = mc.msgTopic.Publish(ctx, bw.Bytes())
	if err != nil {
		return xerrors.Errorf("publishing on topic: %w", err)
	}
	return nil
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
	ps *pubsub.PubSub, sigs gpbft.SignerWithMarshaler, verif gpbft.Verifier, ec ECBackend, log Logger) (*F3, error) {
	ds = namespace.Wrap(ds, manifest.DatastorePrefix())
	cs, err := certstore.OpenOrCreateStore(ctx, ds, 0, manifest.InitialPowerTable())
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
		ds:        ds,
		host:      h,
		pubsub:    ps,
		ec:        ec,
		log:       log,

		client: &client{
			certstore:           cs,
			nn:                  manifest.NetworkName(),
			id:                  id,
			Verifier:            verif,
			SignerWithMarshaler: sigs,
			logger:              log,
			loggerWithSkip:      loggerWithSkip,
		},
	}

	return &m, nil
}

func (m *F3) setupMsgPubsub(runner *gpbftRunner) error {
	pubsubTopicName := m.Manifest.MsgPubSubTopic()

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
	m.client.msgTopic = topic
	return nil
}

func (m *F3) teardownMsgPubsub() error {
	return m.teardownPubsub(m.client.msgTopic, m.Manifest.MsgPubSubTopic())
}

func (m *F3) teardownPubsub(topic *pubsub.Topic, topicName string) error {
	return multierr.Combine(
		m.pubsub.UnregisterTopicValidator(topicName),
		topic.Close(),
	)
}

// Sets up the gpbft runner, this is triggered at initialization or when a new config manifest is received.
// If the rebootstrap flag is enabled, it starts a new gpbftRunner for a specific manifest configuration
// If not, it just starts the message processing loop for the new pubsub topic for the manifest version.
// This function is responsible for setting up the pubsub topic for the
// network, starting the runner, and starting the message processing loop
func (m *F3) setupGpbftRunner(ctx context.Context, initialInstance uint64, rebootstrap bool, errCh chan error) {
	if err := m.setupMsgPubsub(m.runner); err != nil {
		errCh <- xerrors.Errorf("setting up pubsub: %w", err)
		return
	}

	msgSub, err := m.client.msgTopic.Subscribe()
	if err != nil {
		errCh <- xerrors.Errorf("subscribing to topic: %w", err)
		return
	}

	messageQueue := make(chan gpbft.ValidatedMessage, 20)
	m.client.messageQueue = messageQueue

	if rebootstrap {
		m.runner, err = newRunner(m.client.id, m.Manifest, m.client)
		if err != nil {
			errCh <- xerrors.Errorf("creating gpbft host: %w", err)
			return
		}

		go func() {
			err := m.runner.Run(initialInstance, ctx)
			m.log.Errorf("running host: %+v", err)
			errCh <- err
		}()
	}

	m.handleIncomingMessages(ctx, msgSub, messageQueue)
}

// Run start the module. It will exit when context is cancelled.
func (m *F3) Run(initialInstance uint64, ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	runnerErrCh := make(chan error, 1)
	manifestErrCh := make(chan error, 1)

	// bootstrap runner for the initial manifest
	go m.setupGpbftRunner(ctx, initialInstance, true, runnerErrCh)

	// run manifest provider. This runs a background goroutine that will
	// handle dynamic manifest updates if this is a dynamic manifest provider.
	// If it is a static manifest it does nothing.
	go m.Manifest.Run(ctx, manifestErrCh)

	var err error
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

func (m *F3) handleIncomingMessages(ctx context.Context, sub *pubsub.Subscription, queue chan gpbft.ValidatedMessage) {
loop:
	for {
		var msg *pubsub.Message
		msg, err := sub.Next(ctx)
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

	sub.Cancel()
}

// Callback to be triggered when there is a dynamic manifest change
func ManifestChangeCallback(m *F3) manifest.OnManifestChange {
	return func(ctx context.Context, initialInstance uint64, rebootstrap bool, errCh chan error) {
		if err := m.teardownMsgPubsub(); err != nil {
			// for now we just log the error and continue.
			// This is not critical, but alternative approaches welcome.
			m.log.Errorf("error stopping gpbft runner: %+v", err)
		}
		if rebootstrap {
			m.runner.Stop()
		}
		m.setupGpbftRunner(ctx, initialInstance, rebootstrap, errCh)
	}
}

type ECBackend interface {
	ChainHead(context.Context) (chan gpbft.TipSet, error)
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
