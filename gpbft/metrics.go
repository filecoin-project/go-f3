package gpbft

import (
	"errors"

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

	metrics struct {
		phaseCounter              metric.Int64Counter
		roundHistogram            metric.Int64Histogram
		broadcastCounter          metric.Int64Counter
		reBroadcastCounter        metric.Int64Counter
		reBroadcastAttemptCounter metric.Int64Counter
		errorCounter              metric.Int64Counter
	}
)

func init() {
	metrics.phaseCounter = must(meter.Int64Counter("f3_gpbft_phase_counter", metric.WithDescription("Number of times phases change")))
	metrics.roundHistogram = must(meter.Int64Histogram("f3_gpbft_round_histogram",
		metric.WithDescription("Histogram of rounds per instance"),
		metric.WithExplicitBucketBoundaries(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 20.0, 50.0, 100.0, 1000.0),
	))
	metrics.broadcastCounter = must(meter.Int64Counter("f3_gpbft_broadcast_counter", metric.WithDescription("Number of broadcasted messages")))
	metrics.reBroadcastCounter = must(meter.Int64Counter("f3_gpbft_rebroadcast_counter", metric.WithDescription("Number of rebroadcasted messages")))
	metrics.reBroadcastAttemptCounter = must(meter.Int64Counter("f3_gpbft_rebroadcast_attempt_counter", metric.WithDescription("Number of rebroadcast attempts")))
	metrics.errorCounter = must(meter.Int64Counter("f3_gpbft_error_counter", metric.WithDescription("Number of errors")))
}

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

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
