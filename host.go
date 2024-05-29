package f3

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"golang.org/x/xerrors"
)

type Client interface {
	gpbft.Signer
	gpbft.Verifier
	gpbft.Tracer

	BroadcastMessage(context.Context, *gpbft.MessageBuilder) error
	IncommingMessages() <-chan *gpbft.GMessage
	Logger() Logger
}

type gpbftRunner struct {
	client      Client
	participant *gpbft.Participant
	manifest    Manifest

	selfMessageQueue chan *gpbft.GMessage //for the future when self messages are async

	alertTimer *time.Timer

	runningCtx context.Context
	log        Logger

	host *gpbftHost
}

type gpbftHost struct {
	Client
	runner *gpbftRunner
}

func newRunner(id gpbft.ActorID, m Manifest, client Client) (*gpbftRunner, error) {
	runner := &gpbftRunner{
		client:           client,
		manifest:         m,
		selfMessageQueue: make(chan *gpbft.GMessage, 20),
		log:              client.Logger(),
	}
	host := &gpbftHost{
		Client: client,
		runner: runner,
	}
	runner.host = host

	// create a stopped timer to facilitate alerts requested from gpbft
	runner.alertTimer = time.NewTimer(100 * time.Hour)
	if !runner.alertTimer.Stop() {
		<-runner.alertTimer.C
	}

	runner.log.Infof("starting host for P%d", id)
	p, err := gpbft.NewParticipant(host, gpbft.WithTracer(client))
	if err != nil {
		return nil, xerrors.Errorf("creating participant: %w", err)
	}
	runner.participant = p
	return runner, nil
}

func (h *gpbftRunner) Run(ctx context.Context) error {
	var cancel func()
	h.runningCtx, cancel = context.WithCancel(ctx)
	defer cancel()

	// TODO(Kubuxu): temporary hack until re-broadcast and/or booststrap synchronisation are implemented
	time.Sleep(2 * time.Second)

	err := h.participant.Start()
	if err != nil {
		return xerrors.Errorf("starting a participant: %w", err)
	}

	messageQueue := h.client.IncommingMessages()
loop:
	for {
		select {
		case msg := <-h.selfMessageQueue:
			err = h.deliverMessage(msg)
		default:
		}
		if err != nil {
			break loop
		}

		select {
		case <-h.alertTimer.C:
			h.log.Infof("alarm fired")
			err = h.participant.ReceiveAlarm()
		case msg := <-h.selfMessageQueue:
			err = h.deliverMessage(msg)
		case msg, ok := <-messageQueue:
			if !ok {
				err = xerrors.Errorf("incoming messsage queue closed")
				break loop
			}
			err = h.deliverMessage(msg)
		case <-ctx.Done():
			return nil
		}
		if err != nil {
			break loop
		}
	}
	h.log.Errorf("gpbfthost exiting: %+v", err)
	return err
}

func (h *gpbftRunner) deliverMessage(msg *gpbft.GMessage) error {
	valid, err := h.participant.ValidateMessage(msg)
	if err != nil {
		return err
	}
	return h.participant.ReceiveMessage(valid)
}

// Returns inputs to the next GPBFT instance.
// These are:
// - the EC chain to propose,
// - the power table specifying the participants,
// - the beacon value for generating tickets.
// These will be used as input to a subsequent instance of the protocol.
// The chain should be a suffix of the last chain notified to the host via
// ReceiveDecision (or known to be final via some other channel).
func (h *gpbftHost) GetChainForInstance(instance uint64) (gpbft.ECChain, error) {
	// TODO: this is just a complete fake
	ts := sim.NewTipSetGenerator(1)
	chain, err := gpbft.NewChain(gpbft.TipSet{Epoch: 0, Key: ts.Sample()}, gpbft.TipSet{Epoch: 1, Key: ts.Sample()})
	if err != nil {
		return nil, err
	}

	return chain, nil
}

func (h *gpbftHost) GetCommitteeForInstance(instance uint64) (*gpbft.PowerTable, []byte, error) {
	table := gpbft.NewPowerTable()
	err := table.Add(h.runner.manifest.InitialPowerTable...)
	if err != nil {
		return nil, nil, err
	}
	return table, []byte{'A'}, nil
}

// Returns the network's name (for signature separation)
func (h *gpbftHost) NetworkName() gpbft.NetworkName {
	return h.runner.manifest.NetworkName
}

// Sends a message to all other participants.
// The message's sender must be one that the network interface can sign on behalf of.
func (h *gpbftHost) RequestBroadcast(mb *gpbft.MessageBuilder) error {
	err := h.Client.BroadcastMessage(h.runner.runningCtx, mb)
	if err != nil {
		h.runner.log.Errorf("broadcasting GMessage: %+v", err)
		return err
	}
	return nil
}

// Returns the current network time.
func (h *gpbftHost) Time() time.Time {
	return time.Now()
}

// Sets an alarm to fire after the given timestamp.
// At most one alarm can be set at a time.
// Setting an alarm replaces any previous alarm that has not yet fired.
// The timestamp may be in the past, in which case the alarm will fire as soon as possible
// (but not synchronously).
func (h *gpbftHost) SetAlarm(at time.Time) {
	h.runner.log.Infof("set alarm for %v", at)
	// we cannot reuse the timer because we don't know if it was read or not
	h.runner.alertTimer.Stop()
	h.runner.alertTimer = time.NewTimer(time.Until(at))
}

// Receives a finality decision from the instance, with signatures from a strong quorum
// of participants justifying it.
// The decision payload always has round = 0 and step = DECIDE.
// The notification must return the timestamp at which the next instance should begin,
// based on the decision received (which may be in the past).
// E.g. this might be: finalised tipset timestamp + epoch duration + stabilisation delay.
func (h *gpbftHost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	h.runner.log.Infof("got decision: %+v", decision)
	//TODO propagate and save this for use in GetCanonicalChain
	return time.Now().Add(2 * time.Second)
}

// MarshalPayloadForSigning marshals the given payload into the bytes that should be signed.
// This should usually call `Payload.MarshalForSigning(NetworkName)` except when testing as
// that method is slow (computes a merkle tree that's necessary for testing).
func (h *gpbftHost) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	return p.MarshalForSigning(nn)
}
