package gpbft

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const attrKeyPhase = "phase"

var (
	meter = otel.Meter("f3/gpbft")

	attrInitialPhase    = attribute.String(attrKeyPhase, INITIAL_PHASE.String())
	attrQualityPhase    = attribute.String(attrKeyPhase, QUALITY_PHASE.String())
	attrConvergePhase   = attribute.String(attrKeyPhase, CONVERGE_PHASE.String())
	attrPreparePhase    = attribute.String(attrKeyPhase, PREPARE_PHASE.String())
	attrCommitPhase     = attribute.String(attrKeyPhase, COMMIT_PHASE.String())
	attrDecidePhase     = attribute.String(attrKeyPhase, DECIDE_PHASE.String())
	attrTerminatedPhase = attribute.String(attrKeyPhase, TERMINATED_PHASE.String())

	metrics struct {
		phaseCounter              metric.Int64Counter
		roundHistogram            metric.Int64Histogram
		broadcastCounter          metric.Int64Counter
		reBroadcastCounter        metric.Int64Counter
		reBroadcastAttemptCounter metric.Int64Counter
	}
)

func init() {
	metrics.phaseCounter = must(meter.Int64Counter("phase_counter", metric.WithDescription("Number of times phases change")))
	metrics.roundHistogram = must(meter.Int64Histogram("round_histogram",
		metric.WithDescription("Histogram of rounds per instance"),
		metric.WithExplicitBucketBoundaries(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 20.0, 50.0, 100.0, 1000.0),
	))
	metrics.broadcastCounter = must(meter.Int64Counter("broadcast_counter", metric.WithDescription("Number of broadcasted messages")))
	metrics.reBroadcastCounter = must(meter.Int64Counter("rebroadcast_counter", metric.WithDescription("Number of rebroadcasted messages")))
	metrics.reBroadcastAttemptCounter = must(
		meter.Int64Counter(
			"rebroadcast_attempt_counter", metric.WithDescription("Number of rebroadcast attempts")))
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}