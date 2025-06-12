package gpbft

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/filecoin-project/go-f3/internal/caching"
	"github.com/filecoin-project/go-state-types/cbor"
	"go.opentelemetry.io/otel/metric"
)

var (
	_ MessageValidator        = (*cachingValidator)(nil)
	_ PartialMessageValidator = (*cachingValidator)(nil)

	validationNamespaces = struct {
		message       func(partial bool) validatorNamespace
		justification func(partial bool) validatorNamespace
	}{
		message: func(partial bool) validatorNamespace {
			if partial {
				return validatorNamespace("partial_message")
			}
			return validatorNamespace("message")
		},
		justification: func(partial bool) validatorNamespace {
			if partial {
				return validatorNamespace("partial_justification")
			}
			return validatorNamespace("justification")
		},
	}
)

type validatedMessage struct{ msg *GMessage }

func (v *validatedMessage) Message() *GMessage { return v.msg }

type partiallyValidatedMessage struct{ msg *PartialGMessage }

func (v *partiallyValidatedMessage) PartialMessage() *PartialGMessage { return v.msg }

type validatorNamespace = []byte

type cachingValidator struct {
	// cache is a bounded cache that stores identifiers of the messages or
	// justifications validated by this validator, grouped by their respective
	// instance identifiers. During validation, if a message or justification is
	// already present in the cache, it will be skipped to avoid redundant
	// validations. Otherwise, once validated, the cache is updated to include it.
	cache             *caching.GroupedSet
	committeeLookback uint64
	committeeProvider CommitteeProvider
	networkName       NetworkName
	verifier          Verifier
	progress          Progress
}

func newValidator(nn NetworkName, verifier Verifier, cp CommitteeProvider, progress Progress, cache *caching.GroupedSet, committeeLookback uint64) *cachingValidator {
	return &cachingValidator{
		cache:             cache,
		committeeProvider: cp,
		committeeLookback: committeeLookback,
		networkName:       nn,
		verifier:          verifier,
		progress:          progress,
	}
}

// ValidateMessage checks if the given message is valid. If invalid, an error is
// returned. ErrValidationInvalid indicates that the message will never be valid
// invalid and may be safely dropped.
func (v *cachingValidator) ValidateMessage(ctx context.Context, msg *GMessage) (valid ValidatedMessage, err error) {
	if msg == nil {
		return nil, ErrValidationInvalid
	}

	if err := v.validateByProgress(msg); err != nil {
		return nil, err
	}
	cacheKey, err := v.getCacheKey(msg)
	if err != nil {
		log.Warnw("failed to get cache key for message", "err", err)
		cacheKey = nil
	}
	if err = v.validateMessageWithVoteValueKey(ctx, cacheKey, nil, msg); err != nil {
		return nil, err
	}
	return &validatedMessage{msg: msg}, nil
}

func (v *cachingValidator) PartiallyValidateMessage(ctx context.Context, msg *PartialGMessage) (PartiallyValidatedMessage, error) {
	if msg == nil || msg.GMessage == nil {
		return nil, ErrValidationInvalid
	}

	if err := v.validateByProgress(msg.GMessage); err != nil {
		return nil, err
	}

	cacheKey, err := v.getCacheKey(msg)
	if err != nil {
		log.Warnw("failed to get cache key for partial message", "err", err)
		cacheKey = nil
	}

	if err := v.validateMessageWithVoteValueKey(ctx, cacheKey, &msg.VoteValueKey, msg.GMessage); err != nil {
		return nil, err
	}
	return &partiallyValidatedMessage{
		msg: msg,
	}, nil
}

func (v *cachingValidator) FullyValidateMessage(_ context.Context, pvmsg PartiallyValidatedMessage) (ValidatedMessage, error) {
	// A partially validated message validates everything apart from validation rules
	// that require knowing what the vote value is directly. These rules are:
	//  - Consistency of the chain key with the chain itself.
	//  - ECChain validity.
	//  - Zero-valued ECChain relative to phase in both Vote and justification.
	//
	// Re chain key consistency check, this has been done once already within the
	// chainexchange subsystem. But we repeat it anyway here to ensure that the chain
	// key is consistent with the vote value.

	if pvmsg == nil {
		return nil, ErrValidationInvalid
	}
	pmsg := pvmsg.PartialMessage()
	if pmsg == nil || pmsg.GMessage == nil {
		return nil, ErrValidationInvalid
	}
	if err := pmsg.Vote.Value.Validate(); err != nil {
		return nil, fmt.Errorf("invalid vote value: %v: %w", err, ErrValidationInvalid)
	}
	// Check the consistency chain key with the vote value.
	if pmsg.VoteValueKey != pmsg.Vote.Value.Key() {
		return nil, fmt.Errorf("vote value key does not match vote value: %w", ErrValidationInvalid)
	}

	// Check the message again relatively to the current progress in case it's become
	// irrelevant. This check is inexpensive, so we do it again even though it was already
	// done in partial validation.
	if err := v.validateByProgress(pmsg.GMessage); err != nil {
		return nil, err
	}

	// If the key is zero, then the vote value must be zero, along with justification
	// vote value if justification is present.
	justified := pmsg.Justification != nil
	if pmsg.VoteValueKey.IsZero() {
		if !pmsg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected non-zero value for zero vote value key: %w", ErrValidationInvalid)
		}
		if justified && !pmsg.Justification.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected non-zero justification value for zero vote value key: %w", ErrValidationInvalid)
		}
	}
	if justified {
		// Abbreviated version of the expectation map from the full validator.
		expectations := map[Phase]map[Phase]*ECChain{
			CONVERGE_PHASE: {
				COMMIT_PHASE:  &ECChain{},
				PREPARE_PHASE: pmsg.Vote.Value,
			},
			PREPARE_PHASE: {
				COMMIT_PHASE:  &ECChain{},
				PREPARE_PHASE: pmsg.Vote.Value,
			},
			COMMIT_PHASE: {
				PREPARE_PHASE: pmsg.Vote.Value,
			},
			DECIDE_PHASE: {
				COMMIT_PHASE: pmsg.Vote.Value,
			},
		}
		if expectedPhases, ok := expectations[pmsg.Vote.Phase]; ok {
			if expectedValue, ok := expectedPhases[pmsg.Justification.Vote.Phase]; ok {
				if !pmsg.Justification.Vote.Value.Eq(expectedValue) {
					return nil, fmt.Errorf("message %v has justification for a different value: %v: %w", pvmsg, pmsg.Justification.Vote.Value, ErrValidationInvalid)
				}
			} else {
				return nil, fmt.Errorf("message %v has justification with unexpected phase: %v: %w", pvmsg, pmsg.Justification.Vote.Phase, ErrValidationInvalid)
			}
		} else {
			return nil, fmt.Errorf("message %v has unexpected phase for justification: %w", pvmsg, ErrValidationInvalid)
		}
	}
	return &validatedMessage{msg: pmsg.GMessage}, nil
}

func (v *cachingValidator) validateMessageWithVoteValueKey(ctx context.Context, cacheKey []byte, valueKey *ECChainKey, msg *GMessage) (_err error) {
	partial := valueKey != nil
	cacheNamespace := validationNamespaces.message(partial)
	if len(cacheKey) > 0 {
		if alreadyValidated, err := v.isAlreadyValidated(msg.Vote.Instance, cacheNamespace, cacheKey); err != nil {
			log.Errorw("failed to check if message is already validated", "partial", partial, "err", err)
		} else if alreadyValidated {
			metrics.validationCache.Add(ctx, 1, metric.WithAttributes(attrCacheHit, attrCacheKindMessage, attrPartial(partial)))
			return nil
		} else {
			metrics.validationCache.Add(ctx, 1, metric.WithAttributes(attrCacheMiss, attrCacheKindMessage, attrPartial(partial)))
		}
	}

	comt, err := v.committeeProvider.GetCommittee(ctx, msg.Vote.Instance)
	if err != nil {
		return fmt.Errorf("failed to get committee for instance %d: %s: %w", msg.Vote.Instance, err, ErrValidationNoCommittee)
	}
	// Check sender is eligible.
	senderPower, senderPubKey := comt.PowerTable.Get(msg.Sender)
	if senderPower == 0 {
		return fmt.Errorf("sender %d with zero power or not in power table: %w", msg.Sender, ErrValidationInvalid)
	}

	// Check that message value is a valid chain.
	if err := msg.Vote.Value.Validate(); err != nil {
		return fmt.Errorf("invalid message vote value chain: %w: %w", err, ErrValidationInvalid)
	}

	// The message is a vote for bottom if both the vote value and the value key are zero.
	// This means the message is not partial and explicitly caries a zero vote value.
	voteForBottom := (msg.Vote.Value.IsZero() && !partial) || (partial && valueKey.IsZero())

	// Check phase-specific constraints.
	switch msg.Vote.Phase {
	case QUALITY_PHASE:
		if msg.Vote.Round != 0 {
			return fmt.Errorf("unexpected round %d for quality phase: %w", msg.Vote.Round, ErrValidationInvalid)
		}
		if voteForBottom {
			return fmt.Errorf("unexpected zero value for quality phase: %w", ErrValidationInvalid)
		}
	case CONVERGE_PHASE:
		if msg.Vote.Round == 0 {
			return fmt.Errorf("unexpected round 0 for converge phase: %w", ErrValidationInvalid)
		}
		if voteForBottom {
			return fmt.Errorf("unexpected zero value for converge phase: %w", ErrValidationInvalid)
		}
		if !VerifyTicket(v.networkName, comt.Beacon, msg.Vote.Instance, msg.Vote.Round, senderPubKey, v.verifier, msg.Ticket) {
			return fmt.Errorf("failed to verify ticket from %v: %w", msg.Sender, ErrValidationInvalid)
		}
	case DECIDE_PHASE:
		if msg.Vote.Round != 0 {
			return fmt.Errorf("unexpected non-zero round %d for decide phase: %w", msg.Vote.Round, ErrValidationInvalid)
		}
		if voteForBottom {
			return fmt.Errorf("unexpected zero value for decide phase: %w", ErrValidationInvalid)
		}
	case PREPARE_PHASE, COMMIT_PHASE:
		// No additional checks for PREPARE and COMMIT.
	default:
		return fmt.Errorf("invalid vote phase: %d: %w", msg.Vote.Phase, ErrValidationInvalid)
	}

	// Check vote signature.
	var sigPayload []byte
	if partial {
		sigPayload = msg.Vote.MarshalForSigningWithValueKey(v.networkName, *valueKey)
	} else {
		sigPayload = msg.Vote.MarshalForSigning(v.networkName)
	}
	if err := v.verifier.Verify(senderPubKey, sigPayload, msg.Signature); err != nil {
		return fmt.Errorf("invalid signature on %v, %v: %w", msg, err, ErrValidationInvalid)
	}

	// Check justification.
	needsJustification := !(msg.Vote.Phase == QUALITY_PHASE ||
		(msg.Vote.Phase == PREPARE_PHASE && msg.Vote.Round == 0) ||
		(msg.Vote.Phase == COMMIT_PHASE && voteForBottom))

	if needsJustification {
		if err := v.validateJustification(ctx, valueKey, msg, comt); err != nil {
			return fmt.Errorf("%v: %w", err, ErrValidationInvalid)
		}
	} else if msg.Justification != nil {
		return fmt.Errorf("message %v has unexpected justification: %w", msg, ErrValidationInvalid)
	}

	if len(cacheKey) > 0 {
		// A non-empty cache key indicates that the cache key for the message was successfully
		// computed, so we can cache the message.
		if _, err := v.cache.Add(msg.Vote.Instance, cacheNamespace, cacheKey); err != nil {
			log.Warnw("failed to cache to already validated message", "err", err)
		}
	}

	return nil
}

func (v *cachingValidator) validateByProgress(msg *GMessage) error {
	// Infer whether to proceed to validate the message relative to the current instance.
	switch current := v.progress(); {
	case msg.Vote.Instance >= current.ID+v.committeeLookback:
		// The Message is beyond current + committee lookback.
		return ErrValidationNoCommittee
	case msg.Vote.Instance > current.ID,
		msg.Vote.Instance+1 == current.ID && msg.Vote.Phase == DECIDE_PHASE:
		// Only proceed to validate the message if it:
		//  * belongs to an instance within the range of current to current + committee lookback, or
		//  * is a DECIDE message belonging to previous instance.
	case msg.Vote.Instance == current.ID:
		// Message belongs to current instance. Only validate messages that are relevant,
		// i.e.:
		//   * When the current instance is at DECIDE phase only validate DECIDE messages.
		//   * Otherwise, only validate messages that would be rebroadcasted, i.e. QUALITY,
		//     DECIDE, messages from the previous round, and messages from the current round.
		// Anything else isn't relevant.
		switch {
		case current.Phase == DECIDE_PHASE && msg.Vote.Phase != DECIDE_PHASE:
			return ErrValidationNotRelevant
		case msg.Vote.Phase == QUALITY_PHASE,
			msg.Vote.Phase == DECIDE_PHASE,
			// Check if the message round is larger than or equal to the current round.
			msg.Vote.Round >= current.Round,
			// Check if the message round is equal to the previous round. Note that we
			// increment the message round to check this to avoid unit64 wrapping.
			msg.Vote.Round+1 == current.Round:
			// The Message is relevant. Progress to further validation.
		default:
			return ErrValidationNotRelevant
		}
	default:
		// The Message belongs to an instance older than the previous instance.
		return ErrValidationTooOld
	}
	return nil
}

func (v *cachingValidator) validateJustification(ctx context.Context, valueKey *ECChainKey, msg *GMessage, comt *Committee) error {
	if msg.Justification == nil {
		return fmt.Errorf("message for phase %v round %v has no justification", msg.Vote.Phase, msg.Vote.Round)
	}

	partial := valueKey != nil
	cacheNamespace := validationNamespaces.justification(partial)

	// It doesn't matter whether the justification is partial or not. Because, namespace
	// separates the two.
	cacheKey, err := v.getCacheKey(msg.Justification)
	var alreadyValidated bool
	if err != nil {
		log.Warnw("failed to get cache key for justification", "partial", partial, "err", err)
		// If we can't compute the cache key, we can't cache the justification. But we
		// can still validate it.
		cacheKey = nil
	} else {
		// Only cache the justification if:
		//  * marshalling it was successful, and
		//  * it is not yet present in the cache.
		if alreadyValidated, err = v.isAlreadyValidated(msg.Vote.Instance, cacheNamespace, cacheKey); err != nil {
			log.Warnw("failed to check if justification is already cached", "partial", partial, "err", err)
		} else if alreadyValidated {
			metrics.validationCache.Add(ctx, 1, metric.WithAttributes(attrCacheHit, attrCacheKindJustification, attrPartial(partial)))
			return nil
		} else {
			metrics.validationCache.Add(ctx, 1, metric.WithAttributes(attrCacheMiss, attrCacheKindJustification, attrPartial(partial)))
		}
	}

	// Check that the justification is for the same instance.
	if msg.Vote.Instance != msg.Justification.Vote.Instance {
		return fmt.Errorf("message with instanceID %v has evidence from instanceID: %v", msg.Vote.Instance, msg.Justification.Vote.Instance)
	}
	if !msg.Vote.SupplementalData.Eq(&msg.Justification.Vote.SupplementalData) {
		return fmt.Errorf("message and justification have inconsistent supplemental data: %v != %v", msg.Vote.SupplementalData, msg.Justification.Vote.SupplementalData)
	}
	// Check that justification vote value is a valid chain.
	if err := msg.Justification.Vote.Value.Validate(); err != nil {
		return fmt.Errorf("invalid justification vote value chain: %w", err)
	}

	// Check every remaining field of the justification, according to the phase requirements.
	// This map goes from the message phase to the expected justification phase(s),
	// to the required vote values for justification by that phase.
	// Anything else is disallowed.
	expectations := map[Phase]map[Phase]struct {
		Round uint64
		Value *ECChain
	}{
		// CONVERGE is justified by a strong quorum of COMMIT for bottom,
		// or a strong quorum of PREPARE for the same value, from the previous round.
		CONVERGE_PHASE: {
			COMMIT_PHASE:  {msg.Vote.Round - 1, &ECChain{}},
			PREPARE_PHASE: {msg.Vote.Round - 1, msg.Vote.Value},
		},
		// PREPARE is justified by the same rules as CONVERGE (in rounds > 0).
		PREPARE_PHASE: {
			COMMIT_PHASE:  {msg.Vote.Round - 1, &ECChain{}},
			PREPARE_PHASE: {msg.Vote.Round - 1, msg.Vote.Value},
		},
		// COMMIT is justified by a strong quorum of PREPARE from the same round with the same value.
		COMMIT_PHASE: {
			PREPARE_PHASE: {msg.Vote.Round, msg.Vote.Value},
		},
		// DECIDE is justified by a strong quorum of COMMIT with the same value.
		// The DECIDE message doesn't specify a round.
		DECIDE_PHASE: {
			COMMIT_PHASE: {math.MaxUint64, msg.Vote.Value},
		},
	}

	var expectedVoteValueKey ECChainKey
	if expectedPhases, ok := expectations[msg.Vote.Phase]; ok {
		if expected, ok := expectedPhases[msg.Justification.Vote.Phase]; ok {
			if msg.Justification.Vote.Round != expected.Round && expected.Round != math.MaxUint64 {
				return fmt.Errorf("message %v has justification from wrong round %d", msg, msg.Justification.Vote.Round)
			}

			// There are 4 possible cases:
			// 1. The justification is from a complete message with a non-zero value
			// 2. The justification is from a complete message with a zero value
			// 3. The justification is from a partial message with non-zero value key
			// 4. The justification is from a partial message with zero value key
			//
			// In cases 1 and 2, the justification vote value must match the expected value
			// exactly.
			//
			// Whereas in cases 3 and 4, the justification vote can't directly be checked and
			// instead we rely on asserting the value via signature verification. Because the
			// signing payload uses the value key only.
			if partial {
				expectedVoteValueKey = *valueKey
			} else {
				if !msg.Justification.Vote.Value.Eq(expected.Value) {
					return fmt.Errorf("message %v has justification for a different value: %v", msg, msg.Justification.Vote.Value)
				}
				expectedVoteValueKey = expected.Value.Key()
			}
		} else {
			return fmt.Errorf("message %v has justification with unexpected phase: %v", msg, msg.Justification.Vote.Phase)
		}
	} else {
		return fmt.Errorf("message %v has unexpected phase for justification", msg)
	}

	// Check justification power and signature.
	justificationPower, signers, err := msg.Justification.GetSigners(comt.PowerTable)
	if err != nil {
		return fmt.Errorf("failed to get justification signers: %w", err)
	}
	if !IsStrongQuorum(justificationPower, comt.PowerTable.ScaledTotal) {
		return fmt.Errorf("message %v has justification with insufficient power: %v :%w", msg, justificationPower, ErrValidationInvalid)
	}

	payload := msg.Justification.Vote.MarshalForSigningWithValueKey(v.networkName, expectedVoteValueKey)
	if err := comt.AggregateVerifier.VerifyAggregate(signers, payload, msg.Justification.Signature); err != nil {
		return fmt.Errorf("verification of the aggregate failed: %+v: %w", msg.Justification, err)
	}

	if len(cacheKey) > 0 {
		// A non-empty cache key indicates that the cache key for the message was successfully
		// computed, so we can cache the justification.
		if _, err := v.cache.Add(msg.Vote.Instance, cacheNamespace, cacheKey); err != nil {
			log.Warnw("failed to cache to already validated justification", "partial", partial, "err", err)
		}
	}
	return nil
}

func (v *cachingValidator) isAlreadyValidated(group uint64, namespace validatorNamespace, cacheKey []byte) (bool, error) {
	alreadyValidated, err := v.cache.Contains(group, namespace, cacheKey)
	if err != nil {
		return false, fmt.Errorf("failed to check if already validated: %w", err)
	}
	return alreadyValidated, nil
}

func (v *cachingValidator) getCacheKey(msg cbor.Marshaler) ([]byte, error) {
	var buf bytes.Buffer
	if err := msg.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("failed to get cache key: %w", err)
	}
	return buf.Bytes(), nil
}
