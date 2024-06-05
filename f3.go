package f3

import (
	"bytes"
	"context"

	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

type Module struct {
	Manifest  Manifest
	CertStore *certstore.Store

	ds     datastore.Datastore
	host   host.Host
	pubsub *pubsub.PubSub
	ec     ECBackend
	log    Logger

	client moduleClient
}

type moduleClient struct {
	id gpbft.ActorID
	nn gpbft.NetworkName
	gpbft.Verifier
	gpbft.SignerWithMarshaler
	logger         Logger
	loggerWithSkip Logger

	// Populated after Run is called
	messageQueue <-chan *gpbft.GMessage
	topic        *pubsub.Topic
}

func (mc moduleClient) BroadcastMessage(ctx context.Context, mb *gpbft.MessageBuilder) error {
	msg, err := mb.Build(mc.nn, mc.SignerWithMarshaler, mc.id)
	if err != nil {
		mc.Log("building message for: %d: %+v", mc.id, err)
		return err
	}
	var bw bytes.Buffer
	err = msg.MarshalCBOR(&bw)
	if err != nil {
		mc.Log("marshalling GMessage: %+v", err)
	}
	return mc.topic.Publish(ctx, bw.Bytes())
}

func (mc moduleClient) IncommingMessages() <-chan *gpbft.GMessage {
	return mc.messageQueue
}

// Log fulfills the gpbft.Tracer interface
func (mc moduleClient) Log(fmt string, args ...any) {
	mc.loggerWithSkip.Debugf(fmt, args...)
}

func (mc moduleClient) Logger() Logger {
	return mc.logger
}

// New creates and setups f3 with libp2p
// The context is used for initialization not runtime.
func New(ctx context.Context, id gpbft.ActorID, manifest Manifest, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, sigs gpbft.SignerWithMarshaler, verif gpbft.Verifier, ec ECBackend, log Logger) (*Module, error) {
	ds = namespace.Wrap(ds, manifest.NetworkName.DatastorePrefix())
	cs, err := certstore.NewStore(ctx, ds)
	if err != nil {
		return nil, xerrors.Errorf("creating CertStore: %w", err)
	}
	loggerWithSkip := log
	if zapLogger, ok := log.(*logging.ZapEventLogger); ok {
		loggerWithSkip = logging.WithSkip(zapLogger, 1)
	}

	m := Module{
		Manifest:  manifest,
		CertStore: cs,

		ds:     ds,
		host:   h,
		pubsub: ps,
		ec:     ec,
		log:    log,

		client: moduleClient{
			nn:                  manifest.NetworkName,
			id:                  id,
			Verifier:            verif,
			SignerWithMarshaler: sigs,
			logger:              log,
			loggerWithSkip:      loggerWithSkip,
		},
	}

	return &m, nil
}
func (m *Module) setupPubsub() error {
	pubsubTopicName := m.Manifest.NetworkName.PubSubTopic()
	err := m.pubsub.RegisterTopicValidator(pubsubTopicName, m.pubsubTopicValidator)
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

func (m *Module) teardownPubsub() error {
	return multierr.Combine(
		m.pubsub.UnregisterTopicValidator(m.Manifest.NetworkName.PubSubTopic()),
		m.client.topic.Close(),
	)
}

// Run start the module. It will exit when context is cancelled.
func (m *Module) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := m.setupPubsub(); err != nil {
		return xerrors.Errorf("setting up pubsub: %w", err)
	}

	sub, err := m.client.topic.Subscribe()
	if err != nil {
		return xerrors.Errorf("subscribing to topic: %w", err)
	}

	messageQueue := make(chan *gpbft.GMessage, 20)
	m.client.messageQueue = messageQueue

	r, err := newRunner(m.client.id, m.Manifest, m.client)
	if err != nil {
		return xerrors.Errorf("creating gpbft host: %w", err)
	}

	runnerErrCh := make(chan error, 1)

	go func() {
		err := r.Run(ctx)
		m.log.Errorf("running host: %+v", err)
		runnerErrCh <- err
	}()

loop:
	for {
		var msg *pubsub.Message
		msg, err = sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				err = nil
				break
			}
			m.log.Errorf("pubsub subscription.Next() returned an error: %+v", err)
			break
		}
		gmsg, ok := msg.ValidatorData.(*gpbft.GMessage)
		if !ok {
			m.log.Errorf("invalid ValidatorData: %+v", msg.ValidatorData)
			continue
		}

		select {
		case messageQueue <- gmsg:
		case <-ctx.Done():
			break loop
		case err = <-runnerErrCh:
			break loop
		}
	}

	sub.Cancel()
	if err2 := m.teardownPubsub(); err2 != nil {
		err = multierr.Append(err, xerrors.Errorf("shutting down pubsub: %w", err2))
	}
	return multierr.Append(err, ctx.Err())
}

var _ pubsub.ValidatorEx = (*Module)(nil).pubsubTopicValidator

// validator for the pubsub
func (m *Module) pubsubTopicValidator(ctx context.Context, pID peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	var gmsg gpbft.GMessage
	err := gmsg.UnmarshalCBOR(bytes.NewReader(msg.Data))
	if err != nil {
		return pubsub.ValidationReject
	}

	// TODO more validation
	msg.ValidatorData = &gmsg
	return pubsub.ValidationAccept
}

type ECBackend interface{}

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
