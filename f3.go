package f3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Kubuxu/go-broadcast"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"go.uber.org/multierr"
)

type F3 struct {
	Manifest Manifest
	// certStore is nil until Run is called on the F3
	certStore *certstore.Store

	ds     datastore.Datastore
	host   host.Host
	pubsub *pubsub.PubSub
	ec     ECBackend
	log    Logger

	client *client
}

type client struct {
	// certStore is nil until Run is called on the F3
	certStore   *certstore.Store
	networkName gpbft.NetworkName
	ec          ECBackend

	signingMarshaller gpbft.SigningMarshaler

	busBroadcast broadcast.Channel[*gpbft.MessageBuilder]

	gpbft.Verifier
	logger         Logger
	loggerWithSkip Logger

	// Populated after Run is called
	messageQueue <-chan gpbft.ValidatedMessage
	topic        *pubsub.Topic
}

func (mc *client) BroadcastMessage(ctx context.Context, mb *gpbft.MessageBuilder) error {
	mb.SetNetworkName(mc.networkName)
	mb.SetSigningMarshaler(mc.signingMarshaller)
	mc.busBroadcast.Publish(mb)
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
// signingMarshaller can be nil for default SigningMarshaler
func New(ctx context.Context, manifest Manifest, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, verif gpbft.Verifier, ec ECBackend, log Logger, signingMarshaller gpbft.SigningMarshaler) (*F3, error) {
	ds = namespace.Wrap(ds, manifest.NetworkName.DatastorePrefix())
	loggerWithSkip := log
	if zapLogger, ok := log.(*logging.ZapEventLogger); ok {
		loggerWithSkip = logging.WithSkip(zapLogger, 1)
	}
	if signingMarshaller == nil {
		signingMarshaller = gpbft.DefaultSigningMarshaller
	}

	m := F3{
		Manifest: manifest,

		ds:     ds,
		host:   h,
		pubsub: ps,
		ec:     ec,
		log:    log,

		client: &client{
			ec:                ec,
			networkName:       manifest.NetworkName,
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

func (m *F3) Broadcast(ctx context.Context, signatureBuilder *gpbft.SignatureBuilder, msgSig []byte, vrf []byte) {
	msg := signatureBuilder.Build(msgSig, vrf)

	var bw bytes.Buffer
	err := msg.MarshalCBOR(&bw)
	if err != nil {
		m.log.Errorf("marshalling GMessage: %+v", err)
		return
	}
	err = m.client.topic.Publish(ctx, bw.Bytes())
	if err != nil {
		m.log.Errorf("publishing on topic: %w", err)
	}
}

func (m *F3) setCertStore(cs *certstore.Store) {
	m.certStore = cs
	m.client.certStore = cs
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
		return fmt.Errorf("registering topic validator: %w", err)
	}

	topic, err := m.pubsub.Join(pubsubTopicName)
	if err != nil {
		return fmt.Errorf("could not join on pubsub topic: %s: %w", pubsubTopicName, err)
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

func (m *F3) boostrap(ctx context.Context) error {
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
				return ctx.Err()
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

	cs, err := certstore.CreateStore(ctx, m.ds, m.Manifest.InitialInstance, initialPowerTable)
	if err != nil {
		return xerrors.Errorf("creating certstore: %w", err)
	}
	m.setCertStore(cs)
	return nil
}

func (m *F3) GetLatestCert(ctx context.Context) (*certs.FinalityCertificate, error) {
	if m.certStore == nil {
		return nil, xerrors.Errorf("F3 is not running")
	}
	return m.certStore.Latest(), nil
}
func (m *F3) GetCert(ctx context.Context, instance uint64) (*certs.FinalityCertificate, error) {
	if m.certStore == nil {
		return nil, xerrors.Errorf("F3 is not running")
	}
	return m.certStore.Get(ctx, instance)
}

// Run start the module. It will exit when context is cancelled.
func (m *F3) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	runner, err := newRunner(m.Manifest, m.client)
	if err != nil {
		return fmt.Errorf("creating gpbft host: %w", err)
	}

	if err := m.setupPubsub(runner); err != nil {
		return fmt.Errorf("setting up pubsub: %w", err)
	}

	sub, err := m.client.topic.Subscribe()
	if err != nil {
		return fmt.Errorf("subscribing to topic: %w", err)
	}

	messageQueue := make(chan gpbft.ValidatedMessage, 20)
	m.client.messageQueue = messageQueue

	runnerErrCh := make(chan error, 1)

	go func() {
		latest := m.certStore.Latest()
		startInstance := uint64(m.Manifest.InitialInstance)
		if latest != nil {
			startInstance = latest.GPBFTInstance + 1
		}
		err := runner.Run(startInstance, ctx)
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
		err = multierr.Append(err, fmt.Errorf("shutting down pubsub: %w", err2))
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
