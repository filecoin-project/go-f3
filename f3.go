package f3

import (
	"bytes"
	"context"
	"errors"
	"time"

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

type F3 struct {
	Manifest  Manifest
	CertStore *certstore.Store

	ds     datastore.Datastore
	host   host.Host
	pubsub *pubsub.PubSub
	ec     ECBackend
	log    Logger

	client *client
}

type client struct {
	certstore *certstore.Store
	id        gpbft.ActorID
	nn        gpbft.NetworkName
	ec        ECBackend

	gpbft.Verifier
	gpbft.SignerWithMarshaler
	logger         Logger
	loggerWithSkip Logger

	// Populated after Run is called
	messageQueue <-chan gpbft.ValidatedMessage
	topic        *pubsub.Topic
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
	err = mc.topic.Publish(ctx, bw.Bytes())
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
func New(ctx context.Context, id gpbft.ActorID, manifest Manifest, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, sigs gpbft.SignerWithMarshaler, verif gpbft.Verifier, ec ECBackend, log Logger) (*F3, error) {
	ds = namespace.Wrap(ds, manifest.NetworkName.DatastorePrefix())
	loggerWithSkip := log
	if zapLogger, ok := log.(*logging.ZapEventLogger); ok {
		loggerWithSkip = logging.WithSkip(zapLogger, 1)
	}

	m := F3{
		Manifest: manifest,

		ds:     ds,
		host:   h,
		pubsub: ps,
		ec:     ec,
		log:    log,

		client: &client{
			ec:                  ec,
			nn:                  manifest.NetworkName,
			id:                  id,
			Verifier:            verif,
			SignerWithMarshaler: sigs,
			logger:              log,
			loggerWithSkip:      loggerWithSkip,
		},
	}

	cs, err := certstore.OpenStore(ctx, ds)
	if err != nil {
		log.Warnf("opening CertStore, it might not have been created: %+v", err)
	} else {
		m.setCertStore(cs)
	}

	return &m, nil
}

func (m *F3) setCertStore(cs *certstore.Store) {
	m.CertStore = cs
	m.client.certstore = cs
}

func (m *F3) setupPubsub(runner *gpbftRunner) error {
	pubsubTopicName := m.Manifest.NetworkName.PubSubTopic()

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

func (m *F3) teardownPubsub() error {
	return multierr.Combine(
		m.pubsub.UnregisterTopicValidator(m.Manifest.NetworkName.PubSubTopic()),
		m.client.topic.Close(),
	)
}

func (m *F3) boostrap(ctx context.Context, initialInstance uint64) error {
	head, err := m.ec.GetHead(ctx)
	if err != nil {
		return xerrors.Errorf("failed to get the head: %w", err)
	}

	if head.Epoch() < m.Manifest.BootstrapEpoch {
		// wait for bootstrap epoch
		for {
			head, err := m.ec.GetHead(ctx)
			if err != nil {
				return xerrors.Errorf("getting head: %w", err)
			}
			if head.Epoch() >= m.Manifest.BootstrapEpoch {
				break
			}

			m.log.Infof("wating for bootstrap epoch (%d): currently at epoch %d", m.Manifest.BootstrapEpoch, head.Epoch())
			aim := time.Until(head.Timestamp().Add(m.Manifest.ECPeriod))
			// correct for null epochs
			for aim < 0 {
				aim += m.Manifest.ECPeriod
			}

			select {
			case <-time.After(aim):
			case <-ctx.Done():
				return nil
			}
		}
	}

	ts, err := m.ec.GetTipsetByEpoch(ctx, m.Manifest.BootstrapEpoch-m.Manifest.ECFinality)
	if err != nil {
		return xerrors.Errorf("getting initial power tipset: %w", err)
	}

	initialPowerTable, err := m.ec.GetPowerTable(ctx, ts.Key())
	if err != nil {
		return xerrors.Errorf("getting initial power table: %w", err)
	}

	cs, err := certstore.CreateStore(ctx, m.ds, initialInstance, initialPowerTable)
	if err != nil {
		return xerrors.Errorf("creating certstore: %w", err)
	}
	m.setCertStore(cs)
	return nil
}

// Run start the module. It will exit when context is cancelled.
func (m *F3) Run(initialInstance uint64, ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if m.CertStore == nil {
		err := m.boostrap(ctx, initialInstance)
		if err != nil {
			return xerrors.Errorf("failed to boostrap: %w", err)
		}

	}

	runner, err := newRunner(m.client.id, m.Manifest, m.client)
	if err != nil {
		return xerrors.Errorf("creating gpbft host: %w", err)
	}

	if err := m.setupPubsub(runner); err != nil {
		return xerrors.Errorf("setting up pubsub: %w", err)
	}

	sub, err := m.client.topic.Subscribe()
	if err != nil {
		return xerrors.Errorf("subscribing to topic: %w", err)
	}

	messageQueue := make(chan gpbft.ValidatedMessage, 20)
	m.client.messageQueue = messageQueue

	runnerErrCh := make(chan error, 1)

	go func() {
		err := runner.Run(initialInstance, ctx)
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
		gmsg, ok := msg.ValidatorData.(gpbft.ValidatedMessage)
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

type ECBackend interface {
	// GetTipsetByEpoch should return a tipset before the one requested if the requested
	// tipset does not exist due to null epochs
	GetTipsetByEpoch(ctx context.Context, epoch int64) (TipSet, error)
	GetTipset(context.Context, gpbft.TipSetKey) (TipSet, error)
	GetHead(context.Context) (TipSet, error)
	GetParent(context.Context, TipSet) (TipSet, error)

	GetPowerTable(context.Context, gpbft.TipSetKey) (gpbft.PowerEntries, error)
}

type TipSet interface {
	Key() gpbft.TipSetKey
	Beacon() []byte
	Epoch() int64
	Timestamp() time.Time
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
