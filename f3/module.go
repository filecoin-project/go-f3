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
	networkName NetworkName
	CertStore   *CertStore

	ds     datastore.Datastore
	host   host.Host
	pubsub pubsub.PubSub
	sigs   SigningBackend

	topic *pubsub.Topic
}

// NewModule creates and setups new libp2p f3 module
// TODO notification about new EC chains
// TODO FCEX
func NewModule(nn NetworkName, ds datastore.Datastore, h host.Host, ps pubsub.PubSub, sigs SigningBackend) (*Module, error) {
	ds = namespace.Wrap(ds, nn.DatastorePrefix())
	cs, err := NewCertStore(ds)
	if err != nil {
		return nil, xerrors.Errorf("creating CertStore: %w", err)
	}

	m := Module{
		networkName: nn,
		CertStore:   cs,

		ds:     ds,
		host:   h,
		pubsub: ps,
		sigs:   sigs,
	}

	if err := m.setupPubsub(); err != nil {
		return nil, xerrors.Errorf("setting up pubsub on %s: %w", nn.PubSubTopic(), err)
	}

	return &m, nil
}
func (m *Module) setupPubsub() error {
	// pubsub will probably move to separate file/struct
	pubsubTopicName := m.networkName.PubSubTopic()
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
		m.pubsub.UnregisterTopicValidator(m.networkName.PubSubTopic()),
		m.topic.Close(),
	)
}

func (m *Module) Run(ctx context.Context) error {
	sub, err := m.topic.Subscribe()
	if err != nil {
		return xerrors.Errorf("subscribing to topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			//TODO log error
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

// SigningBackend contains methods f3 will use for signature
type SigningBackend interface {
}
