package f3

import (
	"bytes"
	"context"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"golang.org/x/xerrors"
)

type gpbfthost struct {
	participant *gpbft.Participant
	manifest    Manifest
	gpbft.Signer
	gpbft.Verifier
	broadcast func(context.Context, []byte) error

	MessageQueue     chan *gpbft.GMessage
	SelfMessageQueue chan *gpbft.GMessage //for the future when self messages are async

	log Logger

	timer      *time.Timer
	runningCtx context.Context
}

func newHost(id gpbft.ActorID, m Manifest, broadcast func(context.Context, []byte) error,
	s gpbft.Signer, v gpbft.Verifier, log Logger) (*gpbfthost, error) {
	h := &gpbfthost{
		manifest:         m,
		Signer:           s,
		Verifier:         v,
		MessageQueue:     make(chan *gpbft.GMessage, 20),
		SelfMessageQueue: make(chan *gpbft.GMessage, 20),
		broadcast:        broadcast,

		log: log,
	}

	// create a stopped timer
	h.timer = time.NewTimer(100 * time.Hour)
	if !h.timer.Stop() {
		<-h.timer.C
	}

	log.Infof("starting host for P%d", id)
	p, err := gpbft.NewParticipant(id, h, gpbft.WithTracer(tracer{log}))
	if err != nil {
		return nil, xerrors.Errorf("creating participant: %w", err)
	}
	h.participant = p
	return h, nil
}

func (h *gpbfthost) Run(ctx context.Context) error {
	h.runningCtx = ctx
	time.Sleep(2 * time.Second)
	err := h.participant.Start()
	if err != nil {
		return xerrors.Errorf("starting a participant: %w", err)
	}

loop:
	for {
		select {
		case msg := <-h.SelfMessageQueue:
			_, err = h.participant.ReceiveMessage(msg, true)
		default:
		}
		if err != nil {
			break loop
		}

		select {
		case <-h.timer.C:
			h.log.Infof("alarm fired")
			err = h.participant.ReceiveAlarm()
		case msg := <-h.SelfMessageQueue:
			_, err = h.participant.ReceiveMessage(msg, true)
		case msg := <-h.MessageQueue:
			_, err = h.participant.ReceiveMessage(msg, false)
		case <-ctx.Done():
			err = ctx.Err()
		}
		if err != nil {
			break loop
		}
	}
	h.log.Errorf("gpbfthost exiting: %+v", err)
	return err
}

//
// internal APIs after
//

// Returns inputs to the next GPBFT instance.
// These are:
// - the EC chain to propose,
// - the power table specifying the participants,
// - the beacon value for generating tickets.
// These will be used as input to a subsequent instance of the protocol.
// The chain should be a suffix of the last chain notified to the host via
// ReceiveDecision (or known to be final via some other channel).
func (h *gpbfthost) GetCanonicalChain() (chain gpbft.ECChain, power gpbft.PowerTable, beacon []byte) {
	// TODO: this is just a complete fake
	ts := sim.NewTipSetGenerator(1)
	chain, err := gpbft.NewChain(gpbft.TipSet{Epoch: 0, Key: ts.Sample()}, gpbft.TipSet{Epoch: 1, Key: ts.Sample()})
	if err != nil {
		h.log.Errorf("creating chain: %+v", err)
		return nil, *gpbft.NewPowerTable(), nil
	}
	table := gpbft.NewPowerTable()
	err = table.Add(h.manifest.InitialPowerTable...)
	if err != nil {
		h.log.Errorf("creating powertable: %+v", err)
		return nil, *gpbft.NewPowerTable(), nil
	}

	return chain, *table, []byte{'A'}
}

// Returns the network's name (for signature separation)
func (h *gpbfthost) NetworkName() gpbft.NetworkName {
	return h.manifest.NetworkName
}

// Sends a message to all other participants.
// The message's sender must be one that the network interface can sign on behalf of.
func (h *gpbfthost) Broadcast(msg *gpbft.GMessage) {
	h.log.Info("broadcast")

	var bw bytes.Buffer
	err := msg.MarshalCBOR(&bw)
	if err != nil {
		h.log.Errorf("marshalling GMessage: %+v", err)
	}
	err = h.broadcast(h.runningCtx, bw.Bytes())
	if err != nil {
		h.log.Errorf("broadcasting GMessage: %+v", err)
	}
}

// Returns the current network time.
func (h *gpbfthost) Time() time.Time {
	return time.Now()
}

// Sets an alarm to fire after the given timestamp.
// At most one alarm can be set at a time.
// Setting an alarm replaces any previous alarm that has not yet fired.
// The timestamp may be in the past, in which case the alarm will fire as soon as possible
// (but not synchronously).
func (h *gpbfthost) SetAlarm(at time.Time) {
	h.log.Infof("set alarm for %v", at)
	// we cannot reuse the timer because we don't know if it was read or not
	h.timer.Stop()
	h.timer = time.NewTimer(time.Until(at))
}

// Receives a finality decision from the instance, with signatures from a strong quorum
// of participants justifying it.
// The decision payload always has round = 0 and step = DECIDE.
// The notification must return the timestamp at which the next instance should begin,
// based on the decision received (which may be in the past).
// E.g. this might be: finalised tipset timestamp + epoch duration + stabilisation delay.
func (h *gpbfthost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	h.log.Infof("got decision: %+v", decision)
	//TODO propagate and save this for use in GetCanonicalChain
	return time.Now().Add(2 * time.Second)
}

// MarshalPayloadForSigning marshals the given payload into the bytes that should be signed.
// This should usually call `Payload.MarshalForSigning(NetworkName)` except when testing as
// that method is slow (computes a merkle tree that's necessary for testing).
func (h *gpbfthost) MarshalPayloadForSigning(p *gpbft.Payload) []byte {
	return p.MarshalForSigning(h.manifest.NetworkName)
}

type tracer struct {
	Logger
}

func (t tracer) Log(fmt string, args ...any) {
	t.Debugf(fmt, args...)
}
