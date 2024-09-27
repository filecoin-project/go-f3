package gpbft

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/filecoin-project/go-f3/internal/caching"
	"go.opentelemetry.io/otel/metric"
)

var (
	_ MessageValidator = (*cachingValidator)(nil)
)

type cachingValidator struct {
	// cache is a bounded cache that stores identifiers of the messages or
	// justifications validated by this validator, grouped by their respective
	// instance identifiers. During validation, if a message or justification is
	// already present in the cache, it will be skipped to avoid redundant
	// validations. Otherwise, once validated the cache is updated to include it.
	cache             *caching.GroupedSet
	committeeLookback uint64
	committeeProvider CommitteeProvider
	networkName       NetworkName
	signing           Signatures
	progress          Progress
}

func newValidator(host Host, cp CommitteeProvider, progress Progress, cache *caching.GroupedSet, committeeLookback uint64) *cachingValidator {
	return &cachingValidator{
		cache:             cache,
		committeeProvider: cp,
		committeeLookback: committeeLookback,
		networkName:       host.NetworkName(),
		signing:           host,
		progress:          progress,
	}
}

// ValidateMessage checks if the given message is valid. If invalid, an error is
// returned. ErrValidationInvalid indicates that the message will never be valid
// invalid and may be safely dropped.
func (v *cachingValidator) ValidateMessage(msg *GMessage) (valid ValidatedMessage, err error) {
	if msg == nil {
		return nil, ErrValidationInvalid
	}

	// Infer whether to proceed validating the message relative to the current instance.
	switch currentInstance, currentRound, currentPhase := v.progress(); {
	case msg.Vote.Instance >= currentInstance+v.committeeLookback:
		// Message is beyond current + committee lookback.
		return nil, ErrValidationNoCommittee
	case msg.Vote.Instance > currentInstance,
		msg.Vote.Instance+1 == currentInstance && msg.Vote.Phase == DECIDE_PHASE:
		// Only proceed to validate the message if it:
		//  * belongs to an instance within the range of current to current + committee lookback, or
		//  * is a DECIDE message belonging to previous instance.
	case msg.Vote.Instance == currentInstance:
		// Message belongs to current instance. Only validate messages that are relevant,
		// i.e.:
		//   * When current instance is at DECIDE phase only validate DECIDE messages.
		//   * Otherwise, only validate messages that would be rebroadcasted, i.e. QUALITY,
		//     DECIDE, messages from previous round, and messages from current round.
		// Anything else is not relevant.
		switch {
		case currentPhase == DECIDE_PHASE && msg.Vote.Phase != DECIDE_PHASE:
			return nil, ErrValidationNotRelevant
		case msg.Vote.Phase == QUALITY_PHASE,
			msg.Vote.Phase == DECIDE_PHASE,
			// Check if message round is larger than or equal to current round.
			msg.Vote.Round >= currentRound,
			// Check if message round is equal to previous round. Note that we increment the
			// message round to check this in order to avoid unit64 wrapping.
			msg.Vote.Round+1 == currentRound:
			// Message is relevant. Progress to further validation.
		default:
			return nil, ErrValidationNotRelevant
		}
	default:
		// Message belongs to an instance older than the previous instance.
		return nil, ErrValidationTooOld
	}

	var buf bytes.Buffer
	var cacheMessage bool
	if err := msg.MarshalCBOR(&buf); err != nil {
		log.Errorw("failed to marshal message for caching", "err", err)
	} else if alreadyValidated, err := v.cache.Contains(msg.Vote.Instance, messageCacheNamespace, buf.Bytes()); err != nil {
		log.Errorw("failed to check already validated messages", "err", err)
	} else if alreadyValidated {
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheHit, attrCacheKindMessage))
		return &validatedMessage{msg: msg}, nil
	} else {
		cacheMessage = true
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheMiss, attrCacheKindMessage))
	}

	comt, err := v.committeeProvider.GetCommittee(msg.Vote.Instance)
	if err != nil {
		return nil, ErrValidationNoCommittee
	}
	// Check sender is eligible.
	senderPower, senderPubKey := comt.PowerTable.Get(msg.Sender)
	if senderPower == 0 {
		return nil, fmt.Errorf("sender %d with zero power or not in power table: %w", msg.Sender, ErrValidationInvalid)
	}

	// Check that message value is a valid chain.
	if err := msg.Vote.Value.Validate(); err != nil {
		return nil, fmt.Errorf("invalid message vote value chain: %w: %w", err, ErrValidationInvalid)
	}

	// Check phase-specific constraints.
	switch msg.Vote.Phase {
	case QUALITY_PHASE:
		if msg.Vote.Round != 0 {
			return nil, fmt.Errorf("unexpected round %d for quality phase: %w", msg.Vote.Round, ErrValidationInvalid)
		}
		if msg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for quality phase: %w", ErrValidationInvalid)
		}
	case CONVERGE_PHASE:
		if msg.Vote.Round == 0 {
			return nil, fmt.Errorf("unexpected round 0 for converge phase: %w", ErrValidationInvalid)
		}
		if msg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for converge phase: %w", ErrValidationInvalid)
		}
		if !VerifyTicket(v.networkName, comt.Beacon, msg.Vote.Instance, msg.Vote.Round, senderPubKey, v.signing, msg.Ticket) {
			return nil, fmt.Errorf("failed to verify ticket from %v: %w", msg.Sender, ErrValidationInvalid)
		}
	case DECIDE_PHASE:
		if msg.Vote.Round != 0 {
			return nil, fmt.Errorf("unexpected non-zero round %d for decide phase: %w", msg.Vote.Round, ErrValidationInvalid)
		}
		if msg.Vote.Value.IsZero() {
			return nil, fmt.Errorf("unexpected zero value for decide phase: %w", ErrValidationInvalid)
		}
	case PREPARE_PHASE, COMMIT_PHASE:
		// No additional checks for PREPARE and COMMIT.
	default:
		return nil, fmt.Errorf("invalid vote phase: %d: %w", msg.Vote.Phase, ErrValidationInvalid)
	}

	// Check vote signature.
	sigPayload := v.signing.MarshalPayloadForSigning(v.networkName, &msg.Vote)
	if err := v.signing.Verify(senderPubKey, sigPayload, msg.Signature); err != nil {
		return nil, fmt.Errorf("invalid signature on %v, %v: %w", msg, err, ErrValidationInvalid)
	}

	// Check justification.
	needsJustification := !(msg.Vote.Phase == QUALITY_PHASE ||
		(msg.Vote.Phase == PREPARE_PHASE && msg.Vote.Round == 0) ||
		(msg.Vote.Phase == COMMIT_PHASE && msg.Vote.Value.IsZero()))

	if needsJustification {
		if err := v.validateJustification(msg, comt); err != nil {
			return nil, fmt.Errorf("%v: %w", err, ErrValidationInvalid)
		}
	} else if msg.Justification != nil {
		return nil, fmt.Errorf("message %v has unexpected justification: %w", msg, ErrValidationInvalid)
	}

	if cacheMessage {
		if _, err := v.cache.Add(msg.Vote.Instance, messageCacheNamespace, buf.Bytes()); err != nil {
			log.Warnw("failed to cache to already validated message", "err", err)
		}
	}
	return &validatedMessage{msg: msg}, nil
}

func (v *cachingValidator) validateJustification(msg *GMessage, comt *Committee) error {
	if msg.Justification == nil {
		return fmt.Errorf("message for phase %v round %v has no justification", msg.Vote.Phase, msg.Vote.Round)
	}

	// Only cache the justification if:
	//  * marshalling it was successful, and
	//  * it is not already present in the cache.
	var cacheJustification bool
	var buf bytes.Buffer
	if err := msg.Justification.MarshalCBOR(&buf); err != nil {
		log.Errorw("failed to marshal justification for caching", "err", err)
	} else if alreadyValidated, err := v.cache.Contains(msg.Vote.Instance, justificationCacheNamespace, buf.Bytes()); err != nil {
		log.Warnw("failed to check if justification is already cached", "err", err)
	} else if alreadyValidated {
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheHit, attrCacheKindJustification))
		return nil
	} else {
		cacheJustification = true
		metrics.validationCache.Add(context.TODO(), 1, metric.WithAttributes(attrCacheMiss, attrCacheKindJustification))
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
		Value ECChain
	}{
		// CONVERGE is justified by a strong quorum of COMMIT for bottom,
		// or a strong quorum of PREPARE for the same value, from the previous round.
		CONVERGE_PHASE: {
			COMMIT_PHASE:  {msg.Vote.Round - 1, ECChain{}},
			PREPARE_PHASE: {msg.Vote.Round - 1, msg.Vote.Value},
		},
		// PREPARE is justified by the same rules as CONVERGE (in rounds > 0).
		PREPARE_PHASE: {
			COMMIT_PHASE:  {msg.Vote.Round - 1, ECChain{}},
			PREPARE_PHASE: {msg.Vote.Round - 1, msg.Vote.Value},
		},
		// COMMIT is justified by strong quorum of PREPARE from the same round with the same value.
		COMMIT_PHASE: {
			PREPARE_PHASE: {msg.Vote.Round, msg.Vote.Value},
		},
		// DECIDE is justified by strong quorum of COMMIT with the same value.
		// The DECIDE message doesn't specify a round.
		DECIDE_PHASE: {
			COMMIT_PHASE: {math.MaxUint64, msg.Vote.Value},
		},
	}

	if expectedPhases, ok := expectations[msg.Vote.Phase]; ok {
		if expected, ok := expectedPhases[msg.Justification.Vote.Phase]; ok {
			if msg.Justification.Vote.Round != expected.Round && expected.Round != math.MaxUint64 {
				return fmt.Errorf("message %v has justification from wrong round %d", msg, msg.Justification.Vote.Round)
			}
			if !msg.Justification.Vote.Value.Eq(expected.Value) {
				return fmt.Errorf("message %v has justification for a different value: %v", msg, msg.Justification.Vote.Value)
			}
		} else {
			return fmt.Errorf("message %v has justification with unexpected phase: %v", msg, msg.Justification.Vote.Phase)
		}
	} else {
		return fmt.Errorf("message %v has unexpected phase for justification", msg)
	}

	// Check justification power and signature.
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

	if !IsStrongQuorum(justificationPower, comt.PowerTable.ScaledTotal) {
		return fmt.Errorf("message %v has justification with insufficient power: %v", msg, justificationPower)
	}

	payload := v.signing.MarshalPayloadForSigning(v.networkName, &msg.Justification.Vote)
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
