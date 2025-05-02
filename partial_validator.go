package f3

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/caching"
	"go.opentelemetry.io/otel/metric"
)

var (
	_                           PartialMessageValidator = (*cachingPartialValidator)(nil)
	_                           gpbft.ValidatedMessage  = (*fullyValidatedMessage)(nil)
	messageCacheNamespace                               = []byte("partial_message")
	justificationCacheNamespace                         = []byte("partial_justification")
)

type PartialMessageValidator interface {
	PartiallyValidateMessage(ctx context.Context, msg *PartialGMessage) (*PartiallyValidatedMessage, error)
	ValidateMessage(ctx context.Context, msg *PartiallyValidatedMessage) (gpbft.ValidatedMessage, error)
}

type PartiallyValidatedMessage struct {
	*PartialGMessage
}
type fullyValidatedMessage struct{ *gpbft.GMessage }

func (v fullyValidatedMessage) Message() *gpbft.GMessage { return v.GMessage }

// TODO: Reduce duplicate code between this and the full validator. Doing this
//       would need a deep refactor of the full validator, the chain key and the way
//       messages are keyed for caching in the validator. At this time we are opting
//       for taking a debt having a two flavour validator to be paid later.
//       See: https://github.com/filecoin-project/go-f3/issues/826

type cachingPartialValidator struct {
	cache             *caching.GroupedSet
	committeeLookback uint64
	committeeProvider gpbft.CommitteeProvider
	networkName       gpbft.NetworkName
	signing           gpbft.Verifier
	progress          gpbft.Progress
}

func newCachingPartialValidator(host gpbft.Host, progress gpbft.Progress, cache *caching.GroupedSet, committeeLookback uint64) *cachingPartialValidator {
	return &cachingPartialValidator{
		cache:             cache,
		committeeProvider: host,
		committeeLookback: committeeLookback,
		networkName:       host.NetworkName(),
		signing:           host,
		progress:          progress,
	}
}

func (v *cachingPartialValidator) PartiallyValidateMessage(ctx context.Context, msg *PartialGMessage) (*PartiallyValidatedMessage, error) {
	if msg == nil {
		return nil, gpbft.ErrValidationInvalid
	}

	// Check relative to current progress, identical to the full validator.
	switch current := v.progress(); {
	case msg.Vote.Instance >= current.ID+v.committeeLookback:
		// Message is beyond current + committee lookback.
		return nil, gpbft.ErrValidationNoCommittee
	case msg.Vote.Instance > current.ID,
		msg.Vote.Instance+1 == current.ID && msg.Vote.Phase == gpbft.DECIDE_PHASE:
		// Only proceed to validate the message if it:
		//  * belongs to an instance within the range of current to current + committee lookback, or
		//  * is a DECIDE message belonging to previous instance.
	case msg.Vote.Instance == current.ID:
		// Message belongs to current instance. Only validate messages that are relevant,
		// i.e.:
		//   * When current instance is at DECIDE phase only validate DECIDE messages.
		//   * Otherwise, only validate messages that would be rebroadcasted, i.e. QUALITY,
		//     DECIDE, messages from previous round, and messages from current round.
		// Anything else is not relevant.
		switch {
		case current.Phase == gpbft.DECIDE_PHASE && msg.Vote.Phase != gpbft.DECIDE_PHASE:
			return nil, gpbft.ErrValidationNotRelevant
		case msg.Vote.Phase == gpbft.QUALITY_PHASE,
			msg.Vote.Phase == gpbft.DECIDE_PHASE,
			// Check if message round is larger than or equal to current round.
			msg.Vote.Round >= current.Round,
			// Check if message round is equal to previous round. Note that we increment the
			// message round to check this in order to avoid unit64 wrapping.
			msg.Vote.Round+1 == current.Round:
			// Message is relevant. Progress to further validation.
		default:
			return nil, gpbft.ErrValidationNotRelevant
		}
	default:
		// Message belongs to an instance older than the previous instance.
		return nil, gpbft.ErrValidationTooOld
	}

	// Pre-caching setup, identical to full validator.
	var buf bytes.Buffer
	var cacheMessage bool
	if err := msg.MarshalCBOR(&buf); err != nil {
		log.Errorw("failed to marshal message for caching", "err", err)
	} else if alreadyValidated, err := v.cache.Contains(msg.Vote.Instance, messageCacheNamespace, buf.Bytes()); err != nil {
		log.Errorw("failed to check already validated messages", "err", err)
	} else if alreadyValidated {
		metrics.partialValidationCache.Add(ctx, 1, metric.WithAttributes(attrCacheHit, attrCacheKindMessage))
		return &PartiallyValidatedMessage{PartialGMessage: msg}, nil
	} else {
		cacheMessage = true
		metrics.partialValidationCache.Add(ctx, 1, metric.WithAttributes(attrCacheMiss, attrCacheKindMessage))
	}

	comt, err := v.committeeProvider.GetCommittee(ctx, msg.Vote.Instance)
	if err != nil {
		return nil, fmt.Errorf("failed to get committee for instance %d: %s: %w", msg.Vote.Instance, err, gpbft.ErrValidationNoCommittee)
	}
	// Check sender is eligible, identical to full validator
	senderPower, senderPubKey := comt.PowerTable.Get(msg.Sender)
	if senderPower == 0 {
		return nil, fmt.Errorf("sender %d with zero power or not in power table: %w", msg.Sender, gpbft.ErrValidationInvalid)
	}

	// Postpone the validity check for the Vote Value ECChain itself, but proceed
	// with the phase-specific checks. Because, the phase-specific checks only need
	// to know if the ECChain is zero or not. We can weakly assume it is not as long
	// as the VoteValueKey isn't zero
	switch msg.Vote.Phase {
	case gpbft.QUALITY_PHASE:
		if msg.Vote.Round != 0 {
			return nil, fmt.Errorf("unexpected round %d for quality phase: %w", msg.Vote.Round, gpbft.ErrValidationInvalid)
		}
		if msg.VoteValueKey.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for quality phase: %w", gpbft.ErrValidationInvalid)
		}
	case gpbft.CONVERGE_PHASE:
		if msg.Vote.Round == 0 {
			return nil, fmt.Errorf("unexpected round 0 for converge phase: %w", gpbft.ErrValidationInvalid)
		}
		if msg.VoteValueKey.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for converge phase: %w", gpbft.ErrValidationInvalid)
		}
		if !gpbft.VerifyTicket(v.networkName, comt.Beacon, msg.Vote.Instance, msg.Vote.Round, senderPubKey, v.signing, msg.Ticket) {
			return nil, fmt.Errorf("failed to verify ticket from %v: %w", msg.Sender, gpbft.ErrValidationInvalid)
		}
	case gpbft.DECIDE_PHASE:
		if msg.Vote.Round != 0 {
			return nil, fmt.Errorf("unexpected non-zero round %d for decide phase: %w", msg.Vote.Round, gpbft.ErrValidationInvalid)
		}
		if msg.VoteValueKey.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for decide phase: %w", gpbft.ErrValidationInvalid)
		}
	case gpbft.PREPARE_PHASE, gpbft.COMMIT_PHASE:
		// No additional checks needed for these phases.
	default:
		return nil, fmt.Errorf("invalid vote phase: %d: %w", msg.Vote.Phase, gpbft.ErrValidationInvalid)
	}

	// Check vote signature by marshaling the payload with the pre-computed vote value key.
	sigPayload := msg.Vote.MarshalForSigningWithValueKey(v.networkName, msg.VoteValueKey)
	if err := v.signing.Verify(senderPubKey, sigPayload, msg.Signature); err != nil {
		return nil, fmt.Errorf("invalid signature on %v, %v: %w", msg, err, gpbft.ErrValidationInvalid)
	}

	// Check if justification is required, similar to full validator but checking if
	// VoteValueKey is zero instead of Vote.Value.
	needsJustification := !(msg.Vote.Phase == gpbft.QUALITY_PHASE ||
		(msg.Vote.Phase == gpbft.PREPARE_PHASE && msg.Vote.Round == 0) ||
		(msg.Vote.Phase == gpbft.COMMIT_PHASE && msg.VoteValueKey.IsZero()))

	if needsJustification {
		if err := v.validateJustification(ctx, msg, comt); err != nil {
			return nil, fmt.Errorf("%v: %w", err, gpbft.ErrValidationInvalid)
		}
	} else if msg.Justification != nil {
		return nil, fmt.Errorf("message %v has unexpected justification: %w", msg, gpbft.ErrValidationInvalid)
	}

	if cacheMessage {
		if _, err := v.cache.Add(msg.Vote.Instance, messageCacheNamespace, buf.Bytes()); err != nil {
			log.Warnw("failed to cache to already validated message", "err", err)
		}
	}
	return &PartiallyValidatedMessage{PartialGMessage: msg}, nil
}

func (v *cachingPartialValidator) validateJustification(ctx context.Context, msg *PartialGMessage, comt *gpbft.Committee) error {
	if msg.Justification == nil {
		return fmt.Errorf("message for phase %v round %v has no justification", msg.Vote.Phase, msg.Vote.Round)
	}

	// Only cache the justification if:
	//  * marshaling it was successful, and
	//  * it isn't yet present in the cache.
	//
	// Identify the full validator.
	var cacheJustification bool
	var buf bytes.Buffer
	if err := msg.Justification.MarshalCBOR(&buf); err != nil {
		log.Errorw("failed to marshal justification for caching", "err", err)
	} else if alreadyValidated, err := v.cache.Contains(msg.Vote.Instance, justificationCacheNamespace, buf.Bytes()); err != nil {
		log.Warnw("failed to check if justification is already cached", "err", err)
	} else if alreadyValidated {
		metrics.partialValidationCache.Add(ctx, 1, metric.WithAttributes(attrCacheHit, attrCacheKindJustification))
		return nil
	} else {
		cacheJustification = true
		metrics.partialValidationCache.Add(ctx, 1, metric.WithAttributes(attrCacheMiss, attrCacheKindJustification))
	}

	// Check that the justification is for the same instance, identical to the full
	// validator.
	if msg.Vote.Instance != msg.Justification.Vote.Instance {
		return fmt.Errorf("message with instanceID %v has evidence from instanceID: %v", msg.Vote.Instance, msg.Justification.Vote.Instance)
	}
	if !msg.Vote.SupplementalData.Eq(&msg.Justification.Vote.SupplementalData) {
		return fmt.Errorf("message and justification have inconsistent supplemental data: %v != %v", msg.Vote.SupplementalData, msg.Justification.Vote.SupplementalData)
	}

	// Check every remaining field of the justification, according to the phase
	// requirements. But using the vote value key from the message instead of the
	// vote value itself. Here's how:
	//  1. Use the expectation map to check round / phase of justification.
	//  2. Instead of validating the justification vote value, use the map to set the
	//     expectation on what it should be if it was valid.
	//  3. Compute the signing payload based on that expectation, and if it was
	//     valid, then signature validation should pass.
	//
	// This way both the justification vote value and the justification signature are validated
	// without having to know the vote value explicitly.
	expectations := map[gpbft.Phase]map[gpbft.Phase]struct {
		Round uint64
		Value gpbft.ECChainKey
	}{
		// CONVERGE is justified by a strong quorum of COMMIT for bottom,
		// or a strong quorum of PREPARE for the same value, from the previous round.
		gpbft.CONVERGE_PHASE: {
			gpbft.COMMIT_PHASE:  {msg.Vote.Round - 1, gpbft.ECChainKey{}},
			gpbft.PREPARE_PHASE: {msg.Vote.Round - 1, msg.VoteValueKey},
		},
		// PREPARE is justified by the same rules as CONVERGE (in rounds > 0).
		gpbft.PREPARE_PHASE: {
			gpbft.COMMIT_PHASE:  {msg.Vote.Round - 1, gpbft.ECChainKey{}},
			gpbft.PREPARE_PHASE: {msg.Vote.Round - 1, msg.VoteValueKey},
		},
		// COMMIT is justified by strong quorum of PREPARE from the same round with the same value.
		gpbft.COMMIT_PHASE: {
			gpbft.PREPARE_PHASE: {msg.Vote.Round, msg.VoteValueKey},
		},
		// DECIDE is justified by strong quorum of COMMIT with the same value.
		// The DECIDE message doesn't specify a round.
		gpbft.DECIDE_PHASE: {
			gpbft.COMMIT_PHASE: {math.MaxUint64, msg.VoteValueKey},
		},
	}
	var expectedJustificationVoteValueKey gpbft.ECChainKey
	if expectedPhases, ok := expectations[msg.Vote.Phase]; ok {
		if expected, ok := expectedPhases[msg.Justification.Vote.Phase]; ok {
			if msg.Justification.Vote.Round != expected.Round && expected.Round != math.MaxUint64 {
				return fmt.Errorf("message %v has justification from wrong round %d", msg, msg.Justification.Vote.Round)
			}
			expectedJustificationVoteValueKey = expected.Value
		} else {
			return fmt.Errorf("message %v has justification with unexpected phase: %v", msg, msg.Justification.Vote.Phase)
		}
	} else {
		return fmt.Errorf("message %v has unexpected phase for justification", msg)
	}

	// Check justification power and signature, identical to full validator.
	var justificationPower int64
	signers := make([]int, 0)
	if err := msg.Justification.Signers.ForEach(func(bit uint64) error {
		if int(bit) >= len(comt.PowerTable.Entries) {
			return fmt.Errorf("invalid signer index: %d", bit)
		}
		power := comt.PowerTable.ScaledPower[bit]
		if power == 0 {
			return fmt.Errorf("signer with ID %d has no power", comt.PowerTable.Entries[bit].ID)
		}
		justificationPower += power
		signers = append(signers, int(bit))
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over signers: %w", err)
	}
	if !gpbft.IsStrongQuorum(justificationPower, comt.PowerTable.ScaledTotal) {
		return fmt.Errorf("message %v has justification with insufficient power: %v", msg, justificationPower)
	}

	// Check justification signature by computing the signing payload using what a
	// valid justification vote value should be.
	payload := msg.Justification.Vote.MarshalForSigningWithValueKey(v.networkName, expectedJustificationVoteValueKey)
	if err := comt.AggregateVerifier.VerifyAggregate(signers, payload, msg.Justification.Signature); err != nil {
		return fmt.Errorf("verification of the aggregate failed: %+v: %w", msg.Justification, err)
	}

	if cacheJustification {
		if _, err := v.cache.Add(msg.Vote.Instance, justificationCacheNamespace, buf.Bytes()); err != nil {
			log.Warnw("failed to cache to already validated justification", "err", err)
		}
	}
	return nil
}

func (v *cachingPartialValidator) ValidateMessage(ctx context.Context, pmsg *PartiallyValidatedMessage) (gpbft.ValidatedMessage, error) {
	// A partially validated message validates everything apart from validation rules
	// that require knowing what the vote value is directly. These rules are:
	//  - Consistency of the chain key with the chain itself.
	//  - ECChain validity.
	//  - Zero-valued ECChain relative to phase in both Vote and justification.
	//
	// Re chain key consistency check, this has been done once already within the
	// chainexchange subsystem. But we repeat it anyway here to ensure that the chain
	// key is consistent with the vote value.

	if pmsg == nil || pmsg.PartialGMessage == nil {
		return nil, gpbft.ErrValidationInvalid
	}
	if err := pmsg.Vote.Value.Validate(); err != nil {
		return nil, fmt.Errorf("invalid vote value: %v: %w", err, gpbft.ErrValidationInvalid)
	}

	// Check the consistency chain key with the vote value.
	if pmsg.VoteValueKey != pmsg.Vote.Value.Key() {
		return nil, fmt.Errorf("vote value key does not match vote value: %w", gpbft.ErrValidationInvalid)
	}

	// If the key is zero, then the vote value must be zero, along with justification
	// vote value if justification is present.
	justified := pmsg.Justification != nil
	if pmsg.VoteValueKey.IsZero() {
		if !pmsg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected non-zero value for zero vote value key: %w", gpbft.ErrValidationInvalid)
		}
		if justified && !pmsg.Justification.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected non-zero justification value for zero vote value key: %w", gpbft.ErrValidationInvalid)
		}
	}
	if justified {
		// Abbreviated version of the expectation map from the full validator.
		expectations := map[gpbft.Phase]map[gpbft.Phase]*gpbft.ECChain{
			gpbft.CONVERGE_PHASE: {
				gpbft.COMMIT_PHASE:  &gpbft.ECChain{},
				gpbft.PREPARE_PHASE: pmsg.Vote.Value,
			},
			gpbft.PREPARE_PHASE: {
				gpbft.COMMIT_PHASE:  &gpbft.ECChain{},
				gpbft.PREPARE_PHASE: pmsg.Vote.Value,
			},
			gpbft.COMMIT_PHASE: {
				gpbft.PREPARE_PHASE: pmsg.Vote.Value,
			},
			gpbft.DECIDE_PHASE: {
				gpbft.COMMIT_PHASE: pmsg.Vote.Value,
			},
		}
		if expectedPhases, ok := expectations[pmsg.Vote.Phase]; ok {
			if expectedValue, ok := expectedPhases[pmsg.Justification.Vote.Phase]; ok {
				if !pmsg.Justification.Vote.Value.Eq(expectedValue) {
					return nil, fmt.Errorf("message %v has justification for a different value: %v: %w", pmsg, pmsg.Justification.Vote.Value, gpbft.ErrValidationInvalid)
				}
			} else {
				return nil, fmt.Errorf("message %v has justification with unexpected phase: %v: %w", pmsg, pmsg.Justification.Vote.Phase, gpbft.ErrValidationInvalid)
			}
		} else {
			return nil, fmt.Errorf("message %v has unexpected phase for justification: %w", pmsg, gpbft.ErrValidationInvalid)
		}
	}
	return &fullyValidatedMessage{GMessage: pmsg.GMessage}, nil
}
