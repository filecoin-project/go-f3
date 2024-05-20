package f3

import (
	"bytes"
	"context"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

type Module struct {
	Manifest  Manifest
	CertStore *certs.Store

	id     gpbft.ActorID
	ds     datastore.Datastore
	host   host.Host
	pubsub *pubsub.PubSub
	verif  gpbft.Verifier
	sigs   gpbft.Signer
	ec     ECBackend
	log    Logger

	// topic is populated after Run is called
	topic *pubsub.Topic
}

// NewModule creates and setups new libp2p f3 module
// The context is used for initialization not runtime.
func NewModule(ctx context.Context, id gpbft.ActorID, manifest Manifest, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, sigs gpbft.Signer, verif gpbft.Verifier, ec ECBackend, log Logger) (*Module, error) {
	ds = namespace.Wrap(ds, manifest.NetworkName.DatastorePrefix())
	cs, err := certs.NewStore(ctx, ds)
	if err != nil {
		return nil, xerrors.Errorf("creating CertStore: %w", err)
	}

	m := Module{
		Manifest:  manifest,
		CertStore: cs,

		id:     id,
		ds:     ds,
		host:   h,
		pubsub: ps,
		verif:  verif,
		sigs:   sigs,
		ec:     ec,
		log:    log,
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
	m.topic = topic
	return nil
}

func (m *Module) teardownPubsub() error {
	return multierr.Combine(
		m.pubsub.UnregisterTopicValidator(m.Manifest.NetworkName.PubSubTopic()),
		m.topic.Close(),
	)
}

// Run start the module. It will exit when context is cancelled.
func (m *Module) Run(ctx context.Context) error {
	if err := m.setupPubsub(); err != nil {
		return xerrors.Errorf("setting up pubsub: %w", err)
	}

	sub, err := m.topic.Subscribe()
	if err != nil {
		return xerrors.Errorf("subscribing to topic: %w", err)
	}

	h, err := newHost(m.id, m.Manifest, func(ctx context.Context, b []byte) error {
		return m.topic.Publish(ctx, b)
	}, m.sigs, m.verif, m.log)
	if err != nil {
		return xerrors.Errorf("creating gpbft host: %w", err)
	}

	go func() {
		err := h.Run(ctx)
		m.log.Errorf("starting host: %+v", err)
	}()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			m.log.Errorf("pubsub subscription.Next() returned an error: %+v", err)
			break
		}
		var gmsg gpbft.GMessage
		err = gmsg.UnmarshalCBOR(bytes.NewReader(msg.Data))
		if err != nil {
			m.log.Info("bad pubsub message: %w", err)
			continue
		}
		h.MessageQueue <- &gmsg
	}

	sub.Cancel()
	if err := m.teardownPubsub(); err != nil {
		return xerrors.Errorf("shutting down pubsub: %w", err)
	}
	return ctx.Err()
}

var _ pubsub.ValidatorEx = (*Module)(nil).pubsubTopicValidator

// validator for the pubsub
func (m *Module) pubsubTopicValidator(ctx context.Context, pID peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	return pubsub.ValidationAccept
}

type ECBackend interface{}

type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Panic(args ...interface{})
	Panicf(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
}
