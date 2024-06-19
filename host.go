package f3

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"golang.org/x/xerrors"
)

// gpbftRunner is responsible for running gpbft.Participant, taking in all concurrent events and
// passing them to gpbft in a single thread.
type gpbftRunner struct {
	client      *client
	participant *gpbft.Participant
	manifest    Manifest

	alertTimer *time.Timer

	runningCtx context.Context
	log        Logger
}

// gpbftHost is a newtype of gpbftRunner exposing APIs required by the gpbft.Participant
type gpbftHost gpbftRunner

func newRunner(id gpbft.ActorID, m Manifest, client *client) (*gpbftRunner, error) {
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

	runner.log.Infof("starting runner for P%d", id)
	p, err := gpbft.NewParticipant((*gpbftHost)(runner), gpbft.WithTracer(client))
	if err != nil {
		return nil, xerrors.Errorf("creating participant: %w", err)
	}
	runner.participant = p
	return runner, nil
}

func (h *gpbftRunner) Run(instance uint64, ctx context.Context) error {
	var cancel func()
	h.runningCtx, cancel = context.WithCancel(ctx)
	defer cancel()

	// TODO(Kubuxu): temporary hack until re-broadcast and/or booststrap synchronisation are implemented
	time.Sleep(2 * time.Second)

	err := h.participant.StartInstance(instance)
	if err != nil {
		return xerrors.Errorf("starting a participant: %w", err)
	}

	messageQueue := h.client.IncomingMessages()
loop:
	for {
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

func (h *gpbftRunner) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return h.participant.ValidateMessage(msg)
}

func (h *gpbftHost) collectChain(base TipSet, head TipSet) ([]TipSet, error) {
	// TODO: optimize when head is way beyond base
	res := make([]TipSet, 0, 2*gpbft.CHAIN_MAX_LEN)
	res = append(res, head)
	for !bytes.Equal(head.Key(), base.Key()) {
		var err error
		head, err = h.client.ec.GetParent(h.runningCtx, head)
		if err != nil {
			return nil, xerrors.Errorf("walking back the chain: %w", err)
		}
		res = append(res, head)
	}
	slices.Reverse(res)
	return res[1:], nil
}

// Returns inputs to the next GPBFT instance.
// These are:
// - the supplemental data.
// - the EC chain to propose.
// These will be used as input to a subsequent instance of the protocol.
// The chain should be a suffix of the last chain notified to the host via
// ReceiveDecision (or known to be final via some other channel).
func (h *gpbftHost) GetProposalForInstance(instance uint64) (*gpbft.SupplementalData, gpbft.ECChain, error) {
	var baseTsk gpbft.TipSetKey
	if instance == 0 {
		ts, err := h.client.ec.GetTipsetByEpoch(h.runningCtx,
			h.manifest.BootstrapEpoch-h.manifest.ECFinality)
		if err != nil {
			return nil, nil, xerrors.Errorf("getting boostrap base: %w", err)
		}
		baseTsk = ts.Key()
	} else {
		cert, err := h.client.certstore.Get(h.runningCtx, instance-1)
		if err != nil {
			return nil, nil, xerrors.Errorf("getting cert for previous instance(%d): %w", instance-1, err)
		}
		baseTsk = cert.ECChain.Head().Key
	}

	baseTs, err := h.client.ec.GetTipset(h.runningCtx, baseTsk)
	if err != nil {
		return nil, nil, xerrors.Errorf("getting base TS: %w", err)
	}
	headTs, err := h.client.ec.GetHead(h.runningCtx)
	if err != nil {
		return nil, nil, xerrors.Errorf("getting head TS: %w", err)
	}

	collectedChain, err := h.collectChain(baseTs, headTs)
	if err != nil {
		return nil, nil, xerrors.Errorf("collecting chain: %w", err)
	}

	base := gpbft.TipSet{
		Epoch: baseTs.Epoch(),
		Key:   baseTs.Key(),
	}
	pte, err := h.client.ec.GetPowerTable(h.runningCtx, baseTs.Key())
	if err != nil {
		return nil, nil, xerrors.Errorf("getting power table for base: %w", err)
	}
	base.PowerTable, err = certs.MakePowerTableCID(pte)
	if err != nil {
		return nil, nil, xerrors.Errorf("computing powertable CID for base: %w", err)
	}

	suffix := make([]gpbft.TipSet, min(gpbft.CHAIN_MAX_LEN-1, len(collectedChain))) // -1 because of base
	for i := range suffix {
		suffix[i].Key = collectedChain[i].Key()
		suffix[i].Epoch = collectedChain[i].Epoch()

		pte, err = h.client.ec.GetPowerTable(h.runningCtx, suffix[i].Key)
		if err != nil {
			return nil, nil, xerrors.Errorf("getting power table for suffix %d: %w", i, err)
		}
		suffix[i].PowerTable, err = certs.MakePowerTableCID(pte)
		if err != nil {
			return nil, nil, xerrors.Errorf("computing powertable CID for base: %w", err)
		}
	}
	chain, err := gpbft.NewChain(base, suffix...)
	if err != nil {
		return nil, nil, xerrors.Errorf("making new chain: %w", err)
	}

	var supplData gpbft.SupplementalData
	pt, _, err := h.GetCommitteeForInstance(instance + 1)
	if err != nil {
		return nil, nil, xerrors.Errorf("getting commite for %d: %w", instance+1, err)
	}

	supplData.PowerTable, err = certs.MakePowerTableCID(pt.Entries)
	if err != nil {
		return nil, nil, xerrors.Errorf("making power table cid for supplemental data: %w", err)
	}

	return &supplData, chain, nil
}

func (h *gpbftHost) GetCommitteeForInstance(instance uint64) (*gpbft.PowerTable, []byte, error) {
	var powerTsk gpbft.TipSetKey
	var powerEntries gpbft.PowerEntries
	var err error

	if instance < h.manifest.CommiteeLookback {
		//boostrap phase
		ts, err := h.client.ec.GetTipsetByEpoch(h.runningCtx, h.manifest.BootstrapEpoch-h.manifest.ECFinality)
		if err != nil {
			return nil, nil, xerrors.Errorf("getting tipset for boostrap epoch with lookback: %w", err)
		}
		powerTsk = ts.Key()
		powerEntries, err = h.client.ec.GetPowerTable(h.runningCtx, powerTsk)
		if err != nil {
			return nil, nil, xerrors.Errorf("getting power table: %w", err)
		}
	} else {
		cert, err := h.client.certstore.Get(h.runningCtx, instance-h.manifest.CommiteeLookback)
		if err != nil {
			return nil, nil, xerrors.Errorf("getting finality certificate: %w", err)
		}
		powerTsk = cert.ECChain.Head().Key

		powerEntries, err = h.client.certstore.GetPowerTable(h.runningCtx, instance)
		if err != nil {
			// this fires every round, is this correct?
			h.log.Infof("failed getting power table from certstore: %v, falling back to EC", err)

			powerEntries, err = h.client.ec.GetPowerTable(h.runningCtx, powerTsk)
			if err != nil {
				return nil, nil, xerrors.Errorf("getting power table: %w", err)
			}
		}
	}

	ts, err := h.client.ec.GetTipset(h.runningCtx, powerTsk)
	if err != nil {
		return nil, nil, xerrors.Errorf("getting tipset: %w", err)
	}

	table := gpbft.NewPowerTable()
	err = table.Add(powerEntries...)
	if err != nil {
		return nil, nil, xerrors.Errorf("adding entries to power table: %w", err)
	}

	return table, ts.Beacon(), nil
}

// Returns the network's name (for signature separation)
func (h *gpbftHost) NetworkName() gpbft.NetworkName {
	return h.manifest.NetworkName
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
	h.log.Infof("got decision, finalized head at epoch: %d", decision.Vote.Value.Head().Epoch)
	err := h.saveDecision(decision)
	if err != nil {
		h.log.Errorf("error while saving decision: %+v", err)
	}
	ts, err := h.client.ec.GetTipset(h.runningCtx, decision.Vote.Value.Head().Key)
	if err != nil {
		h.log.Errorf("could not get timestamp of just finalized tipset: %+v", err)
		return time.Now().Add(h.manifest.ECDelay)
	}

	return ts.Timestamp().Add(h.manifest.ECDelay)
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
