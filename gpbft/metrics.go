package gpbft

import (
	"errors"

	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	attrKeyPhase = "phase"
	attrKeyErr   = "err"
)

var (
	meter = otel.Meter("f3/gpbft")

	attrInitialPhase    = attribute.String(attrKeyPhase, INITIAL_PHASE.String())
	attrQualityPhase    = attribute.String(attrKeyPhase, QUALITY_PHASE.String())
	attrConvergePhase   = attribute.String(attrKeyPhase, CONVERGE_PHASE.String())
	attrPreparePhase    = attribute.String(attrKeyPhase, PREPARE_PHASE.String())
	attrCommitPhase     = attribute.String(attrKeyPhase, COMMIT_PHASE.String())
	attrDecidePhase     = attribute.String(attrKeyPhase, DECIDE_PHASE.String())
	attrTerminatedPhase = attribute.String(attrKeyPhase, TERMINATED_PHASE.String())
	attrPhase           = map[Phase]attribute.KeyValue{
		INITIAL_PHASE:    attrInitialPhase,
		QUALITY_PHASE:    attrQualityPhase,
		CONVERGE_PHASE:   attrConvergePhase,
		PREPARE_PHASE:    attrPreparePhase,
		COMMIT_PHASE:     attrCommitPhase,
		DECIDE_PHASE:     attrDecidePhase,
		TERMINATED_PHASE: attrTerminatedPhase,
	}
	attrSkipToRound  = attribute.String("to", "round")
	attrSkipToDecide = attribute.String("to", "decide")

	attrCacheHit               = attribute.String("cache", "hit")
	attrCacheMiss              = attribute.String("cache", "miss")
	attrCacheKindMessage       = attribute.String("kind", "message")
	attrCacheKindJustification = attribute.String("kind", "justification")

	metrics = struct {
		phaseCounter       metric.Int64Counter
		roundHistogram     metric.Int64Histogram
		broadcastCounter   metric.Int64Counter
		reBroadcastCounter metric.Int64Counter
		errorCounter       metric.Int64Counter
		currentInstance    metric.Int64Gauge
		currentRound       metric.Int64Gauge
		currentPhase       metric.Int64Gauge
		skipCounter        metric.Int64Counter
		validationCache    metric.Int64Counter
	}{
		phaseCounter: measurements.Must(meter.Int64Counter("f3_gpbft_phase_counter", metric.WithDescription("Number of times phases change"))),
		roundHistogram: measurements.Must(meter.Int64Histogram("f3_gpbft_round_histogram",
			metric.WithDescription("Histogram of rounds per instance"),
			metric.WithExplicitBucketBoundaries(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 20.0, 50.0, 100.0, 1000.0),
		)),
		broadcastCounter:   measurements.Must(meter.Int64Counter("f3_gpbft_broadcast_counter", metric.WithDescription("Number of broadcasted messages"))),
		reBroadcastCounter: measurements.Must(meter.Int64Counter("f3_gpbft_rebroadcast_counter", metric.WithDescription("Number of rebroadcasted messages"))),
		errorCounter:       measurements.Must(meter.Int64Counter("f3_gpbft_error_counter", metric.WithDescription("Number of errors"))),
		currentInstance:    measurements.Must(meter.Int64Gauge("f3_gpbft_current_instance", metric.WithDescription("The ID of the current instance"))),
		currentRound:       measurements.Must(meter.Int64Gauge("f3_gpbft_current_round", metric.WithDescription("The current round number"))),
		currentPhase: measurements.Must(meter.Int64Gauge("f3_gpbft_current_phase",
			metric.WithDescription("The current phase represented as numeric value of gpbft.Phase: "+
				"0=INITIAL, 1=QUALITY, 2=CONVERGE, 3=PREPARE, 4=COMMIT, 5=DECIDE, and 6=TERMINATED"))),
		skipCounter: measurements.Must(meter.Int64Counter("f3_gpbft_skip_counter",
			metric.WithDescription("The number of times GPBFT skip either round or phase"))),
		validationCache: measurements.Must(meter.Int64Counter("f3_gpbft_validation_cache",
			metric.WithDescription("The number of times GPBFT validation cache resulted in hit or miss."))),
	}
)

func metricAttributeFromError(err error) attribute.KeyValue {
	var v string
	switch {
	case errors.Is(err, ErrValidationTooOld):
		v = "invalid_too_old"
	case errors.Is(err, ErrValidationNoCommittee):
		v = "invalid_no_committee"
	case errors.Is(err, ErrValidationInvalid):
		v = "invalid_msg"
	case errors.Is(err, ErrValidationWrongBase):
		v = "invalid_wrong_base"
	case errors.Is(err, ErrValidationWrongSupplement):
		v = "invalid_wrong_supp"
	case errors.As(err, &ValidationError{}):
		v = "type_invalid"
	case errors.Is(err, ErrReceivedWrongInstance):
		v = "wrong_instance"
	case errors.Is(err, ErrReceivedAfterTermination):
		v = "after_termination"
	case errors.Is(err, ErrReceivedInternalError):
		v = "internal"
	case errors.Is(err, &PanicError{}):
		// Any unknown error that ended up getting wrapped with PanicError.
		v = "recovered_panic"
	default:
		v = "unknown"
	}
	return attribute.KeyValue{Key: attrKeyErr, Value: attribute.StringValue(v)}
}
