package f3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/internal/writeaheadlog"
	"github.com/filecoin-project/go-f3/manifest"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

type BroadcastMessage func(*gpbft.MessageBuilder)

// gpbftRunner is responsible for running gpbft.Participant, taking in all concurrent events and
// passing them to gpbft in a single thread.
type gpbftRunner struct {
	certStore   *certstore.Store
	manifest    *manifest.Manifest
	ec          ec.Backend
	pubsub      *pubsub.PubSub
	clock       clock.Clock
	verifier    gpbft.Verifier
	wal         *writeaheadlog.WriteAheadLog[walEntry, *walEntry]
	outMessages chan<- *gpbft.MessageBuilder
	equivFilter equivocationFilter

	participant *gpbft.Participant
	topic       *pubsub.Topic

	alertTimer *clock.Timer

	runningCtx context.Context
	errgrp     *errgroup.Group
	ctxCancel  context.CancelFunc

	// msgsMutex guards access to selfMessages
	msgsMutex    sync.Mutex
	selfMessages map[uint64]map[roundPhase][]*gpbft.GMessage
}

type roundPhase struct {
	round uint64
	phase gpbft.Phase
}

func newRunner(
	ctx context.Context,
	cs *certstore.Store,
	ec ec.Backend,
	ps *pubsub.PubSub,
	verifier gpbft.Verifier,
	out chan<- *gpbft.MessageBuilder,
	m *manifest.Manifest,
	wal *writeaheadlog.WriteAheadLog[walEntry, *walEntry],
	pID peer.ID,
) (*gpbftRunner, error) {
	runningCtx, ctxCancel := context.WithCancel(context.WithoutCancel(ctx))
	errgrp, runningCtx := errgroup.WithContext(runningCtx)

	runner := &gpbftRunner{
		certStore:    cs,
		manifest:     m,
		ec:           ec,
		pubsub:       ps,
		clock:        clock.GetClock(runningCtx),
		verifier:     verifier,
		wal:          wal,
		outMessages:  out,
		runningCtx:   runningCtx,
		errgrp:       errgrp,
		ctxCancel:    ctxCancel,
		equivFilter:  newEquivocationFilter(pID),
		selfMessages: make(map[uint64]map[roundPhase][]*gpbft.GMessage),
	}

	// create a stopped timer to facilitate alerts requested from gpbft
	runner.alertTimer = runner.clock.Timer(0)
	if !runner.alertTimer.Stop() {
		<-runner.alertTimer.C
	}

	walEntries, err := wal.All()
	if err != nil {
		return nil, fmt.Errorf("reading WAL: %w", err)
	}

	var maxInstance uint64
	for _, v := range walEntries {
		runner.equivFilter.ProcessBroadcast(v.Message)
		instance := v.Message.Vote.Instance
		if runner.selfMessages[instance] == nil {
			runner.selfMessages[instance] = make(map[roundPhase][]*gpbft.GMessage)
		}
		// WAL is dumb. To avoid relying on it returning sorted entries or making it
		// searchable add all messages, then trim down to the last instance.
		key := roundPhase{
			round: v.Message.Vote.Round,
			phase: v.Message.Vote.Phase,
		}
		runner.selfMessages[instance][key] = append(runner.selfMessages[instance][key], v.Message)
		maxInstance = max(maxInstance, instance)
	}

	// Trim down to the largest instance.
	for instance := range runner.selfMessages {
		if instance < maxInstance {
			delete(runner.selfMessages, instance)
		}
	}

	log.Infof("Starting gpbft runner")
	opts := append(m.GpbftOptions(), gpbft.WithTracer(tracer))
	p, err := gpbft.NewParticipant((*gpbftHost)(runner), opts...)
	if err != nil {
		return nil, fmt.Errorf("creating participant: %w", err)
	}
	runner.participant = p
	return runner, nil
}

func (h *gpbftRunner) Start(ctx context.Context) (_err error) {
	defer func() {
		if _err != nil {
			_err = multierr.Append(_err, h.Stop(ctx))
		}
	}()

	messageQueue, err := h.startPubsub()
	if err != nil {
		return err
	}

	finalityCertificates, unsubCerts := h.certStore.Subscribe()
	select {
	case c := <-finalityCertificates:
		if err := h.receiveCertificate(c); err != nil {
			log.Errorf("error when receiving certificate: %+v", err)
		}
	default:
		if err := h.startInstanceAt(h.manifest.InitialInstance, h.clock.Now()); err != nil {
			log.Errorf("error when starting instance %d: %+v", h.manifest.InitialInstance, err)
		}
	}

	h.errgrp.Go(func() (_err error) {
		defer func() {
			unsubCerts()
			if _err != nil && h.runningCtx.Err() == nil {
				log.Errorf("exited GPBFT runner early: %+v", _err)
			}
		}()
		for h.runningCtx.Err() == nil {
			// prioritise finality certificates and alarm delivery
			select {
			case c := <-finalityCertificates:
				if err := h.receiveCertificate(c); err != nil {
					log.Errorf("error when recieving certificate: %+v", err)
				}
				continue
			case <-h.alertTimer.C:
				if err := h.participant.ReceiveAlarm(); err != nil {
					// TODO: Probably want to just abort the instance and wait
					// for a finality certificate at this point?
					log.Errorf("error when receiving alarm: %+v", err)
				}
				continue
			default:
			}

			// Handle messages, finality certificates, and alarms
			select {
			case c := <-finalityCertificates:
				if err := h.receiveCertificate(c); err != nil {
					log.Errorf("error when recieving certificate: %+v", err)
				}
			case <-h.alertTimer.C:
				if err := h.participant.ReceiveAlarm(); err != nil {
					// TODO: Probably want to just abort the instance and wait
					// for a finality certificate at this point?
					log.Errorf("error when receiving alarm: %+v", err)
				}
			case msg, ok := <-messageQueue:
				if !ok {
					return fmt.Errorf("incoming message queue closed")
				}
				if err := h.participant.ReceiveMessage(msg); err != nil {
					// We silently drop failed messages because GPBFT will
					// return errors for, e.g., messages from old instances.
					// Given the async nature of our pubsub message handling, we
					// could easily receive these.
					// TODO: we need to distinguish between "fatal" and
					// "non-fatal" errors here. Ideally only returning "real"
					// errors.
					log.Errorf("error when processing message: %+v", err)
				}
			case <-h.runningCtx.Done():
				return nil
			}

		}
		return nil
	})

	// Asynchronously checkpoint the decided tipset keys by explicitly making a
	// separate subscription to the cert store. This may cause a sync in a case where
	// the finalized tipset is not already stored by the chain store, which is a
	// blocking operation. Hence, the asynchronous checkpointing.
	//
	// Note, there is no guarantee that every finalized tipset will be checkpointed.
	// Because:
	//  1. the subscription only returns the latest certificate, i.e. may
	//     miss intermediate certificates, and
	//  2. errors that may occur during checkpointing are silently logged
	//     to allow checkpointing of future finality certificates.
	//
	// Triggering the checkpointing here means that certstore remains the sole source
	// of truth in terms of tipsets that have been finalised.
	finalize, unsubFinalize := h.certStore.Subscribe()
	h.errgrp.Go(func() error {
		defer unsubFinalize()
		for h.runningCtx.Err() == nil {
			select {
			case <-h.runningCtx.Done():
				return nil
			case cert, ok := <-finalize:
				if !ok {
					// This should never happen according to certstore subscribe semantic. If it
					// does, error loudly since the chances are the cause is a programmer error.
					return errors.New("cert store subscription to finalize tipsets was closed unexpectedly")
				}
				if h.manifest.EC.Finalize {
					key := cert.ECChain.Head().Key
					if err := h.ec.Finalize(h.runningCtx, key); err != nil {
						// There is not much we can do here other than logging. The next instance start
						// will effectively retry checkpointing the latest finalized tipset. This error
						// will not impact the selection of next instance chain.
						log.Errorf("error while finalizing decision at EC: %+v", err)
					}
				} else {
					ts := cert.ECChain.Head()
					log.Infow("skipping finalization of a new head because the current manifest specifies that tipsets should not be finalized",
						"tsk", ts.Key,
						"epoch", ts.Epoch,
					)
				}
				const keepInstancesInWAL = 5
				if cert.GPBFTInstance > keepInstancesInWAL {
					err := h.wal.Purge(cert.GPBFTInstance - keepInstancesInWAL)
					if err != nil {
						log.Errorw("failed to purge messages from WAL", "error", err)
					}
				}
				h.msgsMutex.Lock()
				for instance := range h.selfMessages {
					if instance < cert.GPBFTInstance {
						delete(h.selfMessages, instance)
					}
				}
				h.msgsMutex.Unlock()
			}
		}
		return nil
	})
	return nil
}

func (h *gpbftRunner) receiveCertificate(c *certs.FinalityCertificate) error {
	nextInstance := c.GPBFTInstance + 1
	currentInstance := h.participant.Progress().ID
	if currentInstance >= nextInstance {
		return nil
	}

	log.Debugw("skipping forwards based on cert", "from", currentInstance, "to", nextInstance)

	nextInstanceStart := h.computeNextInstanceStart(c)
	return h.startInstanceAt(nextInstance, nextInstanceStart)
}

func (h *gpbftRunner) startInstanceAt(instance uint64, at time.Time) error {
	// Look for any existing messages in WAL for the next instance, and if there is
	// any replay them to self to aid the participant resume progress when possible.
	//
	// Collect the replay messages first then replay them to avid holding the lock
	// for too long if for whatever reason validate/receive is slow.
	var replay []*gpbft.GMessage
	h.msgsMutex.Lock()
	if messages, found := h.selfMessages[instance]; found {
		replay = make([]*gpbft.GMessage, 0, len(messages))
		for _, message := range messages {
			replay = append(replay, message...)
		}
	}
	h.msgsMutex.Unlock()

	// Order of messages does not matter to GPBFT. But sort them in ascending order
	// of instance, round, phase, sender for a more optimal resumption.
	slices.SortFunc(replay, func(one *gpbft.GMessage, other *gpbft.GMessage) int {
		switch {
		case one.Vote.Instance < other.Vote.Instance:
			return -1
		case one.Vote.Instance > other.Vote.Instance:
			return 1
		case one.Vote.Round < other.Vote.Round:
			return -1
		case one.Vote.Round > other.Vote.Round:
			return 1
		case one.Vote.Phase < other.Vote.Phase:
			return -1
		case one.Vote.Phase > other.Vote.Phase:
			return 1
		case one.Sender < other.Sender:
			return -1
		case one.Sender > other.Sender:
			return 1
		default:
			return 0
		}
	})

	for _, message := range replay {
		if validated, err := h.participant.ValidateMessage(message); err != nil {
			log.Warnw("invalid self message", "message", message, "err", err)
		} else if err := h.participant.ReceiveMessage(validated); err != nil {
			log.Warnw("failed to send resumption message", "message", message, "err", err)
		}
	}
	return h.participant.StartInstanceAt(instance, at)
}

func (h *gpbftRunner) computeNextInstanceStart(cert *certs.FinalityCertificate) (_nextStart time.Time) {
	ecDelay := time.Duration(h.manifest.EC.DelayMultiplier * float64(h.manifest.EC.Period))

	head, err := h.ec.GetHead(h.runningCtx)
	if err != nil {
		// this should not happen
		log.Errorf("ec.GetHead returned error: %+v", err)
		return h.clock.Now().Add(ecDelay)
	}

	// the head of the cert becomes the new base
	baseTipSet := cert.ECChain.Head()
	// we are not trying to fetch the new base tipset from EC as it might not be available
	// instead we compute the relative time from the EC.Head
	baseTimestamp := computeTipsetTimestampAtEpoch(head, baseTipSet.Epoch, h.manifest.EC.Period)

	// Try to align instances while catching up, if configured.
	if h.manifest.CatchUpAlignment > 0 {
		defer func() {
			now := h.clock.Now()

			// If we were supposed to start this instance more than one GPBFT round ago, assume
			// we're behind and try to align our start times. This helps keep nodes
			// in-sync when bootstrapping and catching up.
			if _nextStart.Before(now.Add(-h.manifest.CatchUpAlignment)) {
				delay := now.Sub(baseTimestamp)
				if offset := delay % h.manifest.CatchUpAlignment; offset > 0 {
					delay += h.manifest.CatchUpAlignment - offset
				}
				_nextStart = baseTimestamp.Add(delay)
			}
		}()
	}

	lookbackDelay := h.manifest.EC.Period * time.Duration(h.manifest.EC.HeadLookback)

	if cert.ECChain.HasSuffix() {
		// we decided on something new, the tipset that got finalized can at minimum be 30-60s old.
		return baseTimestamp.Add(ecDelay).Add(lookbackDelay)
	}
	if cert.GPBFTInstance == h.manifest.InitialInstance {
		// if we are at initial instance, there is no history to look at
		return baseTimestamp.Add(ecDelay).Add(lookbackDelay)
	}
	backoffTable := h.manifest.EC.BaseDecisionBackoffTable

	attempts := 0
	backoffMultipler := 2.0 // baseline 1 + 1 to account for the one ECDelay after which we got the base decistion
	for instance := cert.GPBFTInstance - 1; instance > h.manifest.InitialInstance; instance-- {
		cert, err := h.certStore.Get(h.runningCtx, instance)
		if err != nil {
			log.Errorf("error while getting instance %d from certstore: %+v", instance, err)
			break
		}
		if cert.ECChain.HasSuffix() {
			break
		}

		attempts += 1
		if attempts < len(backoffTable) {
			backoffMultipler += backoffTable[attempts]
		} else {
			// if we are beyond backoffTable, reuse the last element
			backoffMultipler += backoffTable[len(backoffTable)-1]
		}
	}

	backoff := time.Duration(float64(ecDelay) * backoffMultipler)
	log.Infof("backing off for: %v", backoff)

	return baseTimestamp.Add(backoff).Add(lookbackDelay)
}

// Sends a message to all other participants.
// The message's sender must be one that the network interface can sign on behalf of.
func (h *gpbftRunner) BroadcastMessage(ctx context.Context, msg *gpbft.GMessage) error {
	if !h.equivFilter.ProcessBroadcast(msg) {
		// equivocation filter does its own logging and this error just gets logged
		return nil
	}
	err := h.wal.Append(walEntry{msg})
	if err != nil {
		log.Errorw("appending to WAL", "error", err)
	}

	h.msgsMutex.Lock()
	if h.selfMessages[msg.Vote.Instance] == nil {
		h.selfMessages[msg.Vote.Instance] = make(map[roundPhase][]*gpbft.GMessage)
	}
	key := roundPhase{
		round: msg.Vote.Round,
		phase: msg.Vote.Phase,
	}
	h.selfMessages[msg.Vote.Instance][key] = append(h.selfMessages[msg.Vote.Instance][key], msg)
	h.msgsMutex.Unlock()

	if h.topic == nil {
		return pubsub.ErrTopicClosed
	}
	var bw bytes.Buffer
	err = msg.MarshalCBOR(&bw)
	if err != nil {
		return fmt.Errorf("marshalling GMessage for broadcast: %w", err)
	}

	err = h.topic.Publish(ctx, bw.Bytes())
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}
	return nil
}

func (h *gpbftRunner) rebroadcastMessage(msg *gpbft.GMessage) error {
	if !h.equivFilter.ProcessBroadcast(msg) {
		// equivocation filter does its own logging and this error just gets logged
		return nil
	}
	if h.topic == nil {
		return pubsub.ErrTopicClosed
	}
	var bw bytes.Buffer
	if err := msg.MarshalCBOR(&bw); err != nil {
		return fmt.Errorf("marshalling GMessage for broadcast: %w", err)
	}
	if err := h.topic.Publish(h.runningCtx, bw.Bytes()); err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}
	return nil
}

var _ pubsub.ValidatorEx = (*gpbftRunner)(nil).validatePubsubMessage

func (h *gpbftRunner) validatePubsubMessage(ctx context.Context, _ peer.ID, msg *pubsub.Message) (_result pubsub.ValidationResult) {
	defer func(start time.Time) {
		recordValidationTime(ctx, start, _result)
	}(time.Now())

	var gmsg gpbft.GMessage
	if err := gmsg.UnmarshalCBOR(bytes.NewReader(msg.Data)); err != nil {
		return pubsub.ValidationReject
	}

	switch validatedMessage, err := h.participant.ValidateMessage(&gmsg); {
	case errors.Is(err, gpbft.ErrValidationInvalid):
		log.Debugf("validation error during validation: %+v", err)
		return pubsub.ValidationReject
	case errors.Is(err, gpbft.ErrValidationTooOld):
		// we got the message too late
		return pubsub.ValidationIgnore
	case errors.Is(err, gpbft.ErrValidationNotRelevant):
		// The message is valid but will not effectively aid progress of GPBFT. Ignore it
		// to stop its further propagation across the network.
		return pubsub.ValidationIgnore
	case errors.Is(err, gpbft.ErrValidationNoCommittee):
		log.Debugf("commitee error during validation: %+v", err)
		return pubsub.ValidationIgnore
	case err != nil:
		log.Infof("unknown error during validation: %+v", err)
		return pubsub.ValidationIgnore
	default:
		recordValidatedMessage(ctx, validatedMessage)
		msg.ValidatorData = validatedMessage
		return pubsub.ValidationAccept
	}
}

func (h *gpbftRunner) setupPubsub() error {
	pubsubTopicName := h.manifest.PubSubTopic()
	err := h.pubsub.RegisterTopicValidator(pubsubTopicName, h.validatePubsubMessage)
	if err != nil {
		return fmt.Errorf("registering topic validator: %w", err)
	}

	// Force the default (sender + seqno) message de-duplication mechanism instead of hashing
	// the message (as lotus does) as we need to be able to re-broadcast duplicate messages with
	// the same content.
	topic, err := h.pubsub.Join(pubsubTopicName, pubsub.WithTopicMessageIdFn(psutil.GPBFTMessageIdFn))
	if err != nil {
		return fmt.Errorf("could not join on pubsub topic: %s: %w", pubsubTopicName, err)
	}

	if err := topic.SetScoreParams(psutil.PubsubTopicScoreParams); err != nil {
		log.Infow("failed to set topic score params", "error", err)
	}

	h.topic = topic

	return nil
}

func (h *gpbftRunner) teardownPubsub() error {
	var err error
	if h.topic != nil {
		err = multierr.Combine(
			h.topic.Close(),
			h.pubsub.UnregisterTopicValidator(h.topic.String()),
		)

		if errors.Is(err, context.Canceled) {
			err = nil
		}
	}
	return err
}

func (h *gpbftRunner) startPubsub() (<-chan gpbft.ValidatedMessage, error) {
	if err := h.setupPubsub(); err != nil {
		return nil, err
	}

	sub, err := h.topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("could not subscribe to pubsub topic: %s: %w", sub.Topic(), err)
	}

	messageQueue := make(chan gpbft.ValidatedMessage, 20)
	h.errgrp.Go(func() error {
		defer func() {
			sub.Cancel()
			close(messageQueue)
		}()

		for h.runningCtx.Err() == nil {
			var msg *pubsub.Message
			msg, err := sub.Next(h.runningCtx)
			if err != nil {
				if h.runningCtx.Err() != nil {
					return nil
				}
				return fmt.Errorf("pubsub message subscription returned an error: %w", err)
			}
			gmsg, ok := msg.ValidatorData.(gpbft.ValidatedMessage)
			if !ok {
				log.Errorf("invalid msgValidatorData: %+v", msg.ValidatorData)
				continue
			}
			select {
			case messageQueue <- gmsg:
			case <-h.runningCtx.Done():
				return nil
			}
		}
		return nil
	})
	return messageQueue, nil
}

var (
	_ gpbft.Host     = (*gpbftHost)(nil)
	_ gpbft.Progress = (*gpbftRunner)(nil).Progress
)

// gpbftHost is a newtype of gpbftRunner exposing APIs required by the gpbft.Participant
type gpbftHost gpbftRunner

func (h *gpbftHost) RequestRebroadcast(instant gpbft.Instant) error {
	var rebroadcasts []*gpbft.GMessage
	h.msgsMutex.Lock()
	if roundPhaseMessages, found := h.selfMessages[instant.ID]; found {
		if messages, found := roundPhaseMessages[roundPhase{round: instant.Round, phase: instant.Phase}]; found {
			rebroadcasts = slices.Clone(messages)
		}
	}
	h.msgsMutex.Unlock()
	var err error
	if len(rebroadcasts) > 0 {
		obfuscatedHost := (*gpbftRunner)(h)
		for _, message := range rebroadcasts {
			err = multierr.Append(err, obfuscatedHost.rebroadcastMessage(message))
		}
	}
	return err
}

func (h *gpbftHost) collectChain(base ec.TipSet, head ec.TipSet) ([]ec.TipSet, error) {
	// TODO: optimize when head is way beyond base
	res := make([]ec.TipSet, 0, 2*gpbft.ChainMaxLen)
	res = append(res, head)

	current := head
	for !bytes.Equal(current.Key(), base.Key()) {
		if current.Epoch() < base.Epoch() {
			metrics.headDiverged.Add(h.runningCtx, 1)
			log.Infow("reorg-ed away from base, proposing just base",
				"head", head.String(), "base", base.String())
			return nil, nil
		}
		var err error
		current, err = h.ec.GetParent(h.runningCtx, current)
		if err != nil {
			return nil, fmt.Errorf("walking back the chain: %w", err)
		}
		res = append(res, current)
	}
	slices.Reverse(res)
	return res[1:], nil
}

func (h *gpbftRunner) Stop(context.Context) error {
	h.ctxCancel()
	return multierr.Combine(
		h.wal.Close(),
		h.errgrp.Wait(),
		h.teardownPubsub(),
	)
}

// Progress returns the latest progress of GPBFT consensus in terms of instance
// ID, round and phase.
//
// This API is safe for concurrent use.
func (h *gpbftRunner) Progress() gpbft.Instant {
	return h.participant.Progress()
}

// Returns inputs to the next GPBFT instance.
// These are:
// - the supplemental data.
// - the EC chain to propose.
// These will be used as input to a subsequent instance of the protocol.
// The chain should be a suffix of the last chain notified to the host via
// ReceiveDecision (or known to be final via some other channel).
func (h *gpbftHost) GetProposal(instance uint64) (_ *gpbft.SupplementalData, _ gpbft.ECChain, _err error) {
	defer func(start time.Time) {
		metrics.proposalFetchTime.Record(context.TODO(), time.Since(start).Seconds(), metric.WithAttributes(attrStatusFromErr(_err)))
	}(time.Now())

	var baseTsk gpbft.TipSetKey
	if instance == h.manifest.InitialInstance {
		ts, err := h.ec.GetTipsetByEpoch(h.runningCtx,
			h.manifest.BootstrapEpoch-h.manifest.EC.Finality)
		if err != nil {
			return nil, nil, fmt.Errorf("getting boostrap base: %w", err)
		}
		baseTsk = ts.Key()
	} else {
		cert, err := h.certStore.Get(h.runningCtx, instance-1)
		if err != nil {
			return nil, nil, fmt.Errorf("getting cert for previous instance(%d): %w", instance-1, err)
		}
		baseTsk = cert.ECChain.Head().Key
	}

	baseTs, err := h.ec.GetTipset(h.runningCtx, baseTsk)
	if err != nil {
		return nil, nil, fmt.Errorf("getting base TS: %w", err)
	}
	headTs, err := h.ec.GetHead(h.runningCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("getting head TS: %w", err)
	}

	collectedChain, err := h.collectChain(baseTs, headTs)
	if err != nil {
		return nil, nil, fmt.Errorf("collecting chain: %w", err)
	}

	// If we have an explicit head-lookback, trim the chain.
	if h.manifest.EC.HeadLookback > 0 {
		collectedChain = collectedChain[:max(0, len(collectedChain)-h.manifest.EC.HeadLookback)]
	}

	// less than ECPeriod since production of the head agreement is unlikely, trim the chain.
	if len(collectedChain) > 0 && h.clock.Since(collectedChain[len(collectedChain)-1].Timestamp()) < h.manifest.EC.Period {
		collectedChain = collectedChain[:len(collectedChain)-1]
	}

	base := gpbft.TipSet{
		Epoch: baseTs.Epoch(),
		Key:   baseTs.Key(),
	}
	pte, err := h.ec.GetPowerTable(h.runningCtx, baseTs.Key())
	if err != nil {
		return nil, nil, fmt.Errorf("getting power table for base: %w", err)
	}
	base.PowerTable, err = certs.MakePowerTableCID(pte)
	if err != nil {
		return nil, nil, fmt.Errorf("computing powertable CID for base: %w", err)
	}

	suffix := make([]gpbft.TipSet, min(gpbft.ChainMaxLen-1, len(collectedChain))) // -1 because of base
	for i := range suffix {
		suffix[i].Key = collectedChain[i].Key()
		suffix[i].Epoch = collectedChain[i].Epoch()

		pte, err = h.ec.GetPowerTable(h.runningCtx, suffix[i].Key)
		if err != nil {
			return nil, nil, fmt.Errorf("getting power table for suffix %d: %w", i, err)
		}
		suffix[i].PowerTable, err = certs.MakePowerTableCID(pte)
		if err != nil {
			return nil, nil, fmt.Errorf("computing powertable CID for base: %w", err)
		}
	}
	chain, err := gpbft.NewChain(base, suffix...)
	if err != nil {
		return nil, nil, fmt.Errorf("making new chain: %w", err)
	}

	var supplData gpbft.SupplementalData
	committee, err := h.GetCommittee(instance + 1)
	if err != nil {
		return nil, nil, fmt.Errorf("getting commite for %d: %w", instance+1, err)
	}

	supplData.PowerTable, err = certs.MakePowerTableCID(committee.PowerTable.Entries)
	if err != nil {
		return nil, nil, fmt.Errorf("making power table cid for supplemental data: %w", err)
	}

	return &supplData, chain, nil
}

func (h *gpbftHost) GetCommittee(instance uint64) (_ *gpbft.Committee, _err error) {
	defer func(start time.Time) {
		metrics.committeeFetchTime.Record(context.TODO(), time.Since(start).Seconds(), metric.WithAttributes(attrStatusFromErr(_err)))
	}(time.Now())

	var powerTsk gpbft.TipSetKey
	var powerEntries gpbft.PowerEntries
	var err error

	if instance < h.manifest.InitialInstance+h.manifest.CommitteeLookback {
		//boostrap phase
		powerEntries, err = h.certStore.GetPowerTable(h.runningCtx, h.manifest.InitialInstance)
		if err != nil {
			return nil, fmt.Errorf("getting power table: %w", err)
		}
		if h.certStore.Latest() == nil {
			ts, err := h.ec.GetTipsetByEpoch(h.runningCtx, h.manifest.BootstrapEpoch-h.manifest.EC.Finality)
			if err != nil {
				return nil, fmt.Errorf("getting tipset for boostrap epoch with lookback: %w", err)
			}
			powerTsk = ts.Key()
		} else {
			cert, err := h.certStore.Get(h.runningCtx, h.manifest.InitialInstance)
			if err != nil {
				return nil, fmt.Errorf("getting finality certificate: %w", err)
			}
			powerTsk = cert.ECChain.Base().Key
		}
	} else {
		cert, err := h.certStore.Get(h.runningCtx, instance-h.manifest.CommitteeLookback)
		if err != nil {
			return nil, fmt.Errorf("getting finality certificate: %w", err)
		}
		powerTsk = cert.ECChain.Head().Key

		powerEntries, err = h.certStore.GetPowerTable(h.runningCtx, instance)
		if err != nil {
			log.Debugf("failed getting power table from certstore: %v, falling back to EC", err)

			powerEntries, err = h.ec.GetPowerTable(h.runningCtx, powerTsk)
			if err != nil {
				return nil, fmt.Errorf("getting power table: %w", err)
			}
		}
	}

	ts, err := h.ec.GetTipset(h.runningCtx, powerTsk)
	if err != nil {
		return nil, fmt.Errorf("getting tipset: %w", err)
	}

	table := gpbft.NewPowerTable()
	if err := table.Add(powerEntries...); err != nil {
		return nil, fmt.Errorf("adding entries to power table: %w", err)
	}
	if err := table.Validate(); err != nil {
		return nil, fmt.Errorf("invalid power table for instance %d: %w", instance, err)
	}

	// NOTE: we're intentionally keeping participants here even if they have no
	// effective power (after rounding power) to simplify things. The runtime cost is
	// minimal and it means that the keys can be aggregated before any rounding is done.
	// TODO: this is slow and under a lock, but we only want to do it once per
	// instance... ideally we'd have a per-instance lock/once, but that probably isn't
	// worth it.
	agg, err := h.Aggregate(table.Entries.PublicKeys())
	if err != nil {
		return nil, fmt.Errorf("failed to pre-compute aggregate mask for instance %d: %w", instance, err)
	}

	return &gpbft.Committee{
		PowerTable:        table,
		Beacon:            ts.Beacon(),
		AggregateVerifier: agg,
	}, nil
}

// Returns the network's name (for signature separation)
func (h *gpbftHost) NetworkName() gpbft.NetworkName {
	return h.manifest.NetworkName
}

// Sends a message to all other participants.
func (h *gpbftHost) RequestBroadcast(mb *gpbft.MessageBuilder) error {
	select {
	case h.outMessages <- mb:
		return nil
	case <-h.runningCtx.Done():
		return h.runningCtx.Err()
	}
}

// Returns the current network time.
func (h *gpbftHost) Time() time.Time {
	return h.clock.Now()
}

// Sets an alarm to fire after the given timestamp.
// At most one alarm can be set at a time.
// Setting an alarm replaces any previous alarm that has not yet fired.
// The timestamp may be in the past, in which case the alarm will fire as soon as possible
// (but not synchronously).
func (h *gpbftHost) SetAlarm(at time.Time) {
	log.Debugf("set alarm for %v", at)
	// we cannot reuse the timer because we don't know if it was read or not
	h.alertTimer.Stop()
	if at.IsZero() {
		// If "at" is zero, we cancel the timer entirely. Unfortunately, we still have to
		// replace it for the reason stated above.
		h.alertTimer = h.clock.Timer(0)
		if !h.alertTimer.Stop() {
			<-h.alertTimer.C
		}
	} else {
		h.alertTimer = h.clock.Timer(max(0, h.clock.Until(at)))
	}
}

// Receives a finality decision from the instance, with signatures from a strong quorum
// of participants justifying it.
// The decision payload always has round = 0 and phase = DECIDE.
// The notification must return the timestamp at which the next instance should begin,
// based on the decision received (which may be in the past).
// E.g. this might be: finalised tipset timestamp + epoch duration + stabilisation delay.
func (h *gpbftHost) ReceiveDecision(decision *gpbft.Justification) (time.Time, error) {
	log.Infow("reached a decision", "instance", decision.Vote.Instance,
		"ecHeadEpoch", decision.Vote.Value.Head().Epoch)
	cert, err := h.saveDecision(decision)
	if err != nil {
		err := fmt.Errorf("error while saving decision: %+v", err)
		log.Error(err)
		return time.Time{}, err
	}
	return (*gpbftRunner)(h).computeNextInstanceStart(cert), nil
}

func (h *gpbftHost) saveDecision(decision *gpbft.Justification) (*certs.FinalityCertificate, error) {
	instance := decision.Vote.Instance
	current, err := h.GetCommittee(instance)
	if err != nil {
		return nil, fmt.Errorf("getting commitee for current instance %d: %w", instance, err)
	}

	next, err := h.GetCommittee(instance + 1)
	if err != nil {
		return nil, fmt.Errorf("getting commitee for next instance %d: %w", instance+1, err)
	}
	powerDiff := certs.MakePowerTableDiff(current.PowerTable.Entries, next.PowerTable.Entries)

	cert, err := certs.NewFinalityCertificate(powerDiff, decision)
	if err != nil {
		return nil, fmt.Errorf("forming certificate out of decision: %w", err)
	}
	_, _, _, err = certs.ValidateFinalityCertificates(h, h.NetworkName(), current.PowerTable.Entries, decision.Vote.Instance, nil, cert)
	if err != nil {
		return nil, fmt.Errorf("certificate is invalid: %w", err)
	}

	err = h.certStore.Put(h.runningCtx, cert)
	if err != nil {
		return nil, fmt.Errorf("saving ceritifcate in a store: %w", err)
	}

	return cert, nil
}

// MarshalPayloadForSigning marshals the given payload into the bytes that should be signed.
// This should usually call `Payload.MarshalForSigning(NetworkName)` except when testing as
// that method is slow (computes a merkle tree that's necessary for testing).
func (h *gpbftHost) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	if m, ok := h.verifier.(gpbft.SigningMarshaler); ok {
		return m.MarshalPayloadForSigning(nn, p)
	} else {
		return p.MarshalForSigning(nn)
	}
}

// Verifies a signature for the given public key.
// Implementations must be safe for concurrent use.
func (h *gpbftHost) Verify(pubKey gpbft.PubKey, msg []byte, sig []byte) error {
	return h.verifier.Verify(pubKey, msg, sig)
}

func (h *gpbftHost) Aggregate(pubKeys []gpbft.PubKey) (gpbft.Aggregate, error) {
	return h.verifier.Aggregate(pubKeys)
}
