package f3

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/sim"
	"golang.org/x/xerrors"
)

// gpbftRunner is responsible for running gpbft.Participant, taking in all concurrent events and
// passing them to gpbft in a single thread.
type gpbftRunner struct {
	client      *client
	participant *gpbft.Participant
	manifest    manifest.ManifestProvider

	alertTimer *time.Timer

	runningCtx context.Context
	ctxCancel  func()
	log        Logger
}

// gpbftHost is a newtype of gpbftRunner exposing APIs required by the gpbft.Participant
type gpbftHost gpbftRunner

func newRunner(id gpbft.ActorID, m manifest.ManifestProvider, client *client) (*gpbftRunner, error) {
	runner := &gpbftRunner{
		client:   client,
		manifest: m,
		log:      client.Logger(),
	}

	// create a stopped timer to facilitate alerts requested from gpbft
	runner.alertTimer = time.NewTimer(100 * time.Hour)
	if !runner.alertTimer.Stop() {
		<-runner.alertTimer.C
	}

	runner.log.Infof("starting host for P%d", id)
	// configure participants according to the config from the manifest
	opts := append(m.GpbftOptions(), gpbft.WithTracer(client))
	p, err := gpbft.NewParticipant((*gpbftHost)(runner), opts...)
	if err != nil {
		return nil, xerrors.Errorf("creating participant: %w", err)
	}
	runner.participant = p
	return runner, nil
}

func (h *gpbftRunner) Run(instance uint64, ctx context.Context) error {
	h.runningCtx, h.ctxCancel = context.WithCancel(ctx)
	defer h.ctxCancel()

	// TODO(Kubuxu): temporary hack until re-broadcast and/or booststrap synchronisation are implemented
	time.Sleep(2 * time.Second)

	err := h.participant.StartInstance(instance)
	if err != nil {
		return xerrors.Errorf("starting a participant: %w", err)
	}

	manifestUpdates := h.manifest.Subscribe()
loop:
	for {

		// we need to retrieve the queue again in every
		// iteration in case there has been a manifest change
		// and we are subscribed to a new topic
		messageQueue := h.client.IncomingMessages()

		// if there is a manifest update handle it immediately
		select {
		case <-manifestUpdates:
			// TODO: Perform configuration changes to the runner host
			// - Power table updates
			// - MaxLookBack and ECStabilizationDelay
			// i.e. anything that doesn't require a re-bootstrap.
			continue
		default:
		}

		// prioritise alarm delivery
		// although there is no guarantee that alarm won't fire between
		// the two select statements
		select {
		case <-h.alertTimer.C:
			err = h.participant.ReceiveAlarm()
		default:
		}
		if err != nil {
			break loop
		}

		select {
		case <-h.alertTimer.C:
			err = h.participant.ReceiveAlarm()
		case msg, ok := <-messageQueue:
			if !ok {
				err = xerrors.Errorf("incoming messsage queue closed")
				break loop
			}
			err = h.participant.ReceiveMessage(msg)
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

func (h *gpbftRunner) Stop() {
	h.ctxCancel()
}

func (h *gpbftRunner) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return h.participant.ValidateMessage(msg)
}

// Returns inputs to the next GPBFT instance.
// These are:
// - the supplemental data.
// - the EC chain to propose.
// These will be used as input to a subsequent instance of the protocol.
// The chain should be a suffix of the last chain notified to the host via
// ReceiveDecision (or known to be final via some other channel).
func (h *gpbftHost) GetProposalForInstance(instance uint64) (*gpbft.SupplementalData, gpbft.ECChain, error) {
	// TODO: this is just a complete fake

	pt, _, err := h.GetCommitteeForInstance(0)
	if err != nil {
		return nil, nil, xerrors.Errorf("getting power table: %w", err)
	}
	ptCid, err := certs.MakePowerTableCID(pt.Entries)
	if err != nil {
		return nil, nil, xerrors.Errorf("computing power table CID: %w", err)
	}

	ts := sim.NewTipSetGenerator(1)
	chain, err := gpbft.NewChain(
		gpbft.TipSet{Epoch: 0, Key: ts.Sample(), PowerTable: ptCid},
		gpbft.TipSet{Epoch: 1, Key: ts.Sample(), PowerTable: ptCid},
	)
	if err != nil {
		return nil, nil, xerrors.Errorf("geenrating chain: %w", err)
	}
	sd := &gpbft.SupplementalData{
		PowerTable: ptCid,
	}

	// TODO: use lookback to return the correct next power table commitment and commitments hash.
	return sd, chain, nil
}

func (h *gpbftHost) GetCommitteeForInstance(instance uint64) (*gpbft.PowerTable, []byte, error) {
	// TODO: Add any additional power table entries from the manifest
	table := gpbft.NewPowerTable()
	err := table.Add(h.manifest.InitialPowerTable()...)
	if err != nil {
		return nil, nil, err
	}
	return table, []byte{'A'}, nil
}

// Returns the network's name (for signature separation)
func (h *gpbftHost) NetworkName() gpbft.NetworkName {
	return h.manifest.NetworkName()
}

// Sends a message to all other participants.
// The message's sender must be one that the network interface can sign on behalf of.
func (h *gpbftHost) RequestBroadcast(mb *gpbft.MessageBuilder) error {
	err := h.client.BroadcastMessage(h.runningCtx, mb)
	if err != nil {
		h.log.Errorf("broadcasting GMessage: %+v", err)
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
	h.log.Infof("set alarm for %v", at)
	// we cannot reuse the timer because we don't know if it was read or not
	h.alertTimer.Stop()
	h.alertTimer = time.NewTimer(time.Until(at))
}

// Receives a finality decision from the instance, with signatures from a strong quorum
// of participants justifying it.
// The decision payload always has round = 0 and step = DECIDE.
// The notification must return the timestamp at which the next instance should begin,
// based on the decision received (which may be in the past).
// E.g. this might be: finalised tipset timestamp + epoch duration + stabilisation delay.
func (h *gpbftHost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	h.log.Infof("got decision: %+v", decision)
	err := h.saveDecision(decision)
	if err != nil {
		h.log.Errorf("error while saving decision: %+v", err)
	}

	return time.Now().Add(2 * time.Second)
}

func (h *gpbftHost) saveDecision(decision *gpbft.Justification) error {
	instance := decision.Vote.Instance
	current, _, err := h.GetCommitteeForInstance(instance)
	if err != nil {
		return xerrors.Errorf("getting commitee for current instance %d: %w", instance, err)
	}

	next, _, err := h.GetCommitteeForInstance(instance + 1)
	if err != nil {
		return xerrors.Errorf("getting commitee for next instance %d: %w", instance+1, err)
	}
	powerDiff := certs.MakePowerTableDiff(current.Entries, next.Entries)

	cert, err := certs.NewFinalityCertificate(powerDiff, decision)
	if err != nil {
		return xerrors.Errorf("forming certificate out of decision: %w", err)
	}
	_, _, _, err = certs.ValidateFinalityCertificates(h, h.NetworkName(), current.Entries, decision.Vote.Instance, cert.ECChain.Base())
	if err != nil {
		return xerrors.Errorf("certificate is invalid: %w", err)
	}

	err = h.client.certstore.Put(h.runningCtx, cert)
	if err != nil {
		return xerrors.Errorf("saving ceritifcate in a store: %w", err)
	}
	return nil
}

// MarshalPayloadForSigning marshals the given payload into the bytes that should be signed.
// This should usually call `Payload.MarshalForSigning(NetworkName)` except when testing as
// that method is slow (computes a merkle tree that's necessary for testing).
func (h *gpbftHost) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	return h.client.MarshalPayloadForSigning(nn, p)
}

// Verifies a signature for the given public key.
// Implementations must be safe for concurrent use.
func (h *gpbftHost) Verify(pubKey gpbft.PubKey, msg []byte, sig []byte) error {
	return h.client.Verify(pubKey, msg, sig)
}

// Aggregates signatures from a participants.
func (h *gpbftHost) Aggregate(pubKeys []gpbft.PubKey, sigs [][]byte) ([]byte, error) {
	return h.client.Aggregate(pubKeys, sigs)
}

// VerifyAggregate verifies an aggregate signature.
// Implementations must be safe for concurrent use.
func (h *gpbftHost) VerifyAggregate(payload []byte, aggSig []byte, signers []gpbft.PubKey) error {
	return h.client.VerifyAggregate(payload, aggSig, signers)
}

// Signs a message with the secret key corresponding to a public key.
func (h *gpbftHost) Sign(sender gpbft.PubKey, msg []byte) ([]byte, error) {
	return h.client.Sign(sender, msg)
}
