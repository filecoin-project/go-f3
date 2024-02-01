package f3

import (
	"context"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

type Module struct {
	NetworkName NetworkName
	CertStore   *CertStore

	ds     datastore.Datastore
	host   host.Host
	pubsub *pubsub.PubSub
	verif  Verifier
	sigs   Signer
	ec     ECBackend
	log    Logger

	topic *pubsub.Topic
}

// NewModule creates and setups new libp2p f3 module
// The context is used for initialization not runtime.
func NewModule(ctx context.Context, nn NetworkName, ds datastore.Datastore, h host.Host,
	ps *pubsub.PubSub, sigs Signer, verif Verifier, ec ECBackend, log Logger) (*Module, error) {
	ds = namespace.Wrap(ds, nn.DatastorePrefix())
	cs, err := NewCertStore(ctx, ds)
	if err != nil {
		return nil, xerrors.Errorf("creating CertStore: %w", err)
	}

	m := Module{
		NetworkName: nn,
		CertStore:   cs,

		ds:     ds,
		host:   h,
		pubsub: ps,
		verif:  verif,
		sigs:   sigs,
		ec:     ec,
		log:    log,
	}

	if err := m.setupPubsub(); err != nil {
		return nil, xerrors.Errorf("setting up pubsub on %s: %w", nn.PubSubTopic(), err)
	}

	return &m, nil
}
func (m *Module) setupPubsub() error {
	// pubsub will probably move to separate file/struct
	pubsubTopicName := m.NetworkName.PubSubTopic()
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
		m.pubsub.UnregisterTopicValidator(m.NetworkName.PubSubTopic()),
		m.topic.Close(),
	)
}

// Run start the module. It will exit when context is cancelled.
func (m *Module) Run(ctx context.Context) error {
	sub, err := m.topic.Subscribe()
	if err != nil {
		return xerrors.Errorf("subscribing to topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			m.log.Errorf("pubsub subscription.Next() returned an error: %+v", err)
			break
		}
		_ = msg
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

type TODOVerifier struct{}

// Verifies a signature for the given sender ID.
func (v *TODOVerifier) Verify(pubKey PubKey, msg []byte, sig []byte) bool {
	panic("not implemented")
}

// Aggregates signatures from a participant to an existing signature.
func (v *TODOVerifier) Aggregate(sig [][]byte, aggSignature []byte) []byte {
	panic("not implemented")
}

// VerifyAggregate verifies an aggregate signature.
func (v *TODOVerifier) VerifyAggregate(payload []byte, aggSig []byte, signers []PubKey) bool {
	panic("not implemented")
}

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
