package f3

import (
	"bytes"
	"context"
	"errors"

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
	runner *gpbftRunner
	ec     ECBackend
	log    Logger

	client *clientImpl

	manifestServerID peer.ID
	nextManifest     *Manifest
}

type clientImpl struct {
	id gpbft.ActorID
	nn gpbft.NetworkName
	gpbft.Verifier
	gpbft.SignerWithMarshaler
	logger         Logger
	loggerWithSkip Logger

	// Populated after Run is called
	manifestQueue <-chan *Manifest
	messageQueue  <-chan gpbft.ValidatedMessage
	msgTopic      *pubsub.Topic
	manifestTopic *pubsub.Topic
}

func (mc *clientImpl) BroadcastMessage(ctx context.Context, mb *gpbft.MessageBuilder) error {
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
	return mc.msgTopic.Publish(ctx, bw.Bytes())
}

func (mc *clientImpl) IncomingMessages() <-chan gpbft.ValidatedMessage {
	return mc.messageQueue
}

func (mc clientImpl) IncomingManifest() <-chan *Manifest {
	return mc.manifestQueue
}

var _ gpbft.Tracer = (*clientImpl)(nil)

// Log fulfills the gpbft.Tracer interface
func (mc *clientImpl) Log(fmt string, args ...any) {
	mc.loggerWithSkip.Debugf(fmt, args...)
}

func (mc *clientImpl) Logger() Logger {
	return mc.logger
}

// New creates and setups f3 with libp2p
// The context is used for initialization not runtime.
func New(ctx context.Context, id gpbft.ActorID, manifest Manifest, ds datastore.Datastore, h host.Host, manifestServer peer.ID,
	ps *pubsub.PubSub, sigs gpbft.SignerWithMarshaler, verif gpbft.Verifier, ec ECBackend, log Logger) (*F3, error) {
	ds = namespace.Wrap(ds, manifest.NetworkName.DatastorePrefix())
	cs, err := certstore.OpenOrCreateStore(ctx, ds, 0, manifest.InitialPowerTable)
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

		client: &clientImpl{
			nn:                  manifest.NetworkName,
			id:                  id,
			Verifier:            verif,
			SignerWithMarshaler: sigs,
			logger:              log,
			loggerWithSkip:      loggerWithSkip,
		},

		manifestServerID: manifestServer,
	}

	return &m, nil
}
func (m *F3) setupMsgPubsub(runner *gpbftRunner) (err error) {
	pubsubTopicName := m.Manifest.PubSubTopic()

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
			return pubsub.ValidationReject
		}
		if err != nil {
			m.log.Warnf("unknown error during validation: %+v", err)
			return pubsub.ValidationIgnore
		}
		msg.ValidatorData = validatedMessage
		return pubsub.ValidationAccept
	}

	m.client.msgTopic, err = m.setupPubsub(pubsubTopicName, validator)
	return
}

func (m *F3) teardownMsgPubsub() error {
	return m.teardownPubsub(m.client.msgTopic, m.Manifest.PubSubTopic())
}

func (m *F3) setupPubsub(topicName string, validator any) (*pubsub.Topic, error) {
	err := m.pubsub.RegisterTopicValidator(topicName, validator)
	if err != nil {
		return nil, xerrors.Errorf("registering topic validator: %w", err)
	}

	topic, err := m.pubsub.Join(topicName)
	if err != nil {
		return nil, xerrors.Errorf("could not join on pubsub topic: %s: %w", topicName, err)
	}
	return topic, nil
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

// Logic triggered when a new manifest is received
func (m *F3) onManifestChange(ctx context.Context, initialInstance uint64, rebootstrap bool, errCh chan error) {
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

// Run start the module. It will exit when context is cancelled.
func (m *F3) Run(initialInstance uint64, ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	runnerErrCh := make(chan error, 1)
	manifestErrCh := make(chan error, 1)

	// bootstrap runner for the initial manifest
	go m.setupGpbftRunner(ctx, initialInstance, true, runnerErrCh)

	// only start manifest service if the diagnostics server id is set
	if m.manifestServerID != "" {
		manifestQueue := make(chan *Manifest, 5)
		m.client.manifestQueue = manifestQueue
		go m.handleIncomingManifests(ctx, manifestQueue, manifestErrCh)
	}

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

// Checks if we should accept the manifest that we received through pubsub
func (m *F3) acceptNextManifest(manifest *Manifest) bool {

	// if the manifest is older, skip it
	if manifest.Sequence <= m.Manifest.Sequence ||
		manifest.UpgradeEpoch < m.Manifest.UpgradeEpoch {
		return false
	}

	return true
}

func (m *F3) handleIncomingManifests(ctx context.Context, manifestQueue chan *Manifest, errCh chan error) {
	if err := m.setupManifestPubsub(); err != nil {
		errCh <- xerrors.Errorf("setting up pubsub: %w", err)
		return
	}

	manifestSub, err := m.client.manifestTopic.Subscribe()
	if err != nil {
		errCh <- xerrors.Errorf("subscribing to topic: %w", err)
		return
	}

	// FIXME; This is a stub and should be replaced with whatever
	// function allow us to subscribe to new epochs coming from EC.
	ecSub, err := m.ec.ChainHead(ctx)
	if err != nil {
		errCh <- xerrors.Errorf("subscribing to chain events: %w", err)
		return
	}

loop:
	for {
		select {
		// Check first if there is a new configuration manifest that needs to be applied.
		case ts := <-ecSub:
			if m.nextManifest != nil {
				// if the upgrade epoch is reached or already passed.
				if ts.Epoch >= m.nextManifest.UpgradeEpoch {
					// update the current manifest
					m.Manifest = *m.nextManifest
					m.nextManifest = nil
					// stop existing pubsub and subscribe to the new one
					// if the re-bootstrap flag is enabled, it will setup a new runner with the new config.
					go m.onManifestChange(ctx, uint64(m.Manifest.UpgradeEpoch), m.nextManifest.ReBootstrap, errCh)
					if !m.nextManifest.ReBootstrap {
						// TODO: If the manifest doesn't have the re-bootstrap flagged
						// enabled, no new runner is setup, we reuse the existing one.
						// We need to pass the manifest to the runner to notify
						// that there is a new configuration to be applied without
						// restarting the runner.
						// Some of the configurations that we expect to run in this way are
						// - Updates to the power table
						// - ECStabilisationDelay
						// - more?
						manifestQueue <- &m.Manifest
					}
					continue
				}
			}

		default:
			var msg *pubsub.Message
			msg, err = manifestSub.Next(ctx)
			if err != nil {
				if ctx.Err() != nil {
					err = nil
					break
				}
				m.log.Errorf("manifestPubsub subscription.Next() returned an error: %+v", err)
				break
			}
			manifest, ok := msg.ValidatorData.(*Manifest)
			if !ok {
				m.log.Errorf("invalid manifestValidatorData: %+v", msg.ValidatorData)
				continue
			}

			if !m.acceptNextManifest(manifest) {
				continue
			}

			m.nextManifest = manifest

			select {
			case <-ctx.Done():
				break loop
			default:
			}
		}
	}

	manifestSub.Cancel()
	if err := m.teardownManifestPubsub(); err != nil {
		errCh <- xerrors.Errorf("shutting down manifest pubsub: %w", err)
	}
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

func (m *F3) setupManifestPubsub() (err error) {
	// using the same validator approach used for the message pubsub
	// to be homogeneous.
	var validator pubsub.ValidatorEx = func(ctx context.Context, pID peer.ID,
		msg *pubsub.Message) pubsub.ValidationResult {
		var manifest Manifest
		err := manifest.Unmarshal(bytes.NewReader(msg.Data))
		if err != nil {
			return pubsub.ValidationReject
		}

		// manifest should come from the expected diagnostics server
		if pID != m.manifestServerID {
			return pubsub.ValidationReject
		}

		// TODO: Any additional validation?
		// Expect a sequence number that is over our current sequence number.
		// Expect an upgradeEpoch over the upgradeEpoch of the current manifests?
		// These should probably not be ValidationRejects to avoid banning in gossipsub
		// the centralized server in case of misconfigurations or bugs.
		msg.ValidatorData = &manifest
		return pubsub.ValidationAccept
	}
	m.client.manifestTopic, err = m.setupPubsub(ManifestPubSubTopicName, validator)
	return
}

func (m *F3) teardownManifestPubsub() error {
	return m.teardownPubsub(m.client.manifestTopic, ManifestPubSubTopicName)
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
