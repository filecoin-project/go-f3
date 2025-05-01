package f3

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/measurements"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var attrCacheHit = attribute.String("cache", "hit")
var attrCacheMiss = attribute.String("cache", "miss")
var attrCacheKindMessage = attribute.String("kind", "message")
var attrCacheKindJustification = attribute.String("kind", "justification")

var meter = otel.Meter("f3")
var samples = struct {
	messages       *measurements.SampleSet
	justifications *measurements.SampleSet
}{
	// The the max size of 25K is based on network size of ~3K and 5 phases of GPBFT,
	// where the vast majority of instances should complete in one round. That makes
	// up ~15K messages per instance. We may observe messages from previous or future
	// rounds, hence an additional capacity of 10K, making a total of 25K messages.
	//
	// Although, the number of observable messages with justification are expected to
	// be less than the total number of observed messages we use the same 25K bounded
	// size for justifications. The memory footprint of the additionally stored
	// samples is negligible in return for a larger sample size and ultimately a more
	// accurate measurement considering justification signature validation is more
	// expensive than message signature validation.
	//
	// Since sampleSet caps the max size of cache keys to 96 bytes (the length
	// of BLS signatures) the total memory footprint for 50K samples should be be
	// below 10MB (5MB * 2 due to bookkeeping overhead).
	messages:       measurements.NewSampleSet(25_000),
	justifications: measurements.NewSampleSet(25_000),
}

var metrics = struct {
	headDiverged             metric.Int64Counter
	reconfigured             metric.Int64Counter
	manifestsReceived        metric.Int64Counter
	validationTime           metric.Float64Histogram
	proposalFetchTime        metric.Float64Histogram
	committeeFetchTime       metric.Float64Histogram
	validatedMessages        metric.Int64Counter
	partialMessages          metric.Int64UpDownCounter
	partialMessageDuplicates metric.Int64Counter
	partialMessagesDropped   metric.Int64Counter
	partialMessageInstances  metric.Int64UpDownCounter
	partialValidationCache   metric.Int64Counter
	ecFinalizeTime           metric.Float64Histogram
}{
	headDiverged:      measurements.Must(meter.Int64Counter("f3_head_diverged", metric.WithDescription("Number of times we encountered the head has diverged from base scenario."))),
	reconfigured:      measurements.Must(meter.Int64Counter("f3_reconfigured", metric.WithDescription("Number of times we reconfigured due to new manifest being delivered."))),
	manifestsReceived: measurements.Must(meter.Int64Counter("f3_manifests_received", metric.WithDescription("Number of manifests we have received"))),
	validationTime: measurements.Must(meter.Float64Histogram("f3_validation_time",
		metric.WithDescription("Histogram of time spent validating broadcasted in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1.0, 10.0),
		metric.WithUnit("s"),
	)),
	proposalFetchTime: measurements.Must(meter.Float64Histogram("f3_proposal_fetch_time",
		metric.WithDescription("Histogram of time spent fetching proposal per instance in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 10.0, 100.0),
		metric.WithUnit("s"),
	)),
	committeeFetchTime: measurements.Must(meter.Float64Histogram("f3_committee_fetch_time",
		metric.WithDescription("Histogram of time spent fetching committees per instance in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 10.0, 100.0),
		metric.WithUnit("s"),
	)),
	validatedMessages: measurements.Must(meter.Int64Counter("f3_validated_messages",
		metric.WithDescription("Number of validated GPBFT messages."))),
	partialMessages: measurements.Must(meter.Int64UpDownCounter("f3_partial_messages",
		metric.WithDescription("Number of partial GPBFT messages pending fulfilment."))),
	partialMessageDuplicates: measurements.Must(meter.Int64Counter("f3_partial_message_duplicates",
		metric.WithDescription("Number of partial GPBFT messages received that already have an unfulfilled message for the same instance, sender, round and phase."))),
	partialMessagesDropped: measurements.Must(meter.Int64Counter("f3_partial_messages_dropped",
		metric.WithDescription("Number of partial GPBFT messages or chain broadcasts were dropped due to consumers being too slow."))),
	partialMessageInstances: measurements.Must(meter.Int64UpDownCounter("f3_partial_message_instances",
		metric.WithDescription("Number of instances with partial GPBFT messages pending fulfilment."))),
	partialValidationCache: measurements.Must(meter.Int64Counter("f3_partial_validation_cache",
		metric.WithDescription("The number of times partial validation cache resulted in hit or miss."))),
	ecFinalizeTime: measurements.Must(meter.Float64Histogram("f3_ec_finalize_time",
		metric.WithDescription("Histogram of the number of seconds spent checkpointing an F3 decision in EC tagged by status"),
		metric.WithExplicitBucketBoundaries(0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1.0, 10.0),
		metric.WithUnit("s"),
	)),
}

func recordValidatedMessage(ctx context.Context, msg gpbft.ValidatedMessage) {
	// The given msg and its validated value should never be nil; but defensively
	// check anyway.
	if msg == nil || msg.Message() == nil {
		return
	}

	vmsg := msg.Message()
	attrs := make([]attribute.KeyValue, 0, 3)
	if samples.messages.Contains(vmsg.Signature) {
		attrs = append(attrs, attribute.Bool("duplicate_message", true))
	}
	if vmsg.Justification != nil {
		attrs = append(attrs, attribute.Bool("justified", true))
		if samples.justifications.Contains(vmsg.Justification.Signature) {
			attrs = append(attrs, attribute.Bool("duplicate_justification", true))
		}
	}
	metrics.validatedMessages.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func recordValidationTime(ctx context.Context, start time.Time, result pubsub.ValidationResult, partiallyValidated bool) {
	metrics.validationTime.Record(
		ctx,
		time.Since(start).Seconds(),
		metric.WithAttributes(
			measurements.AttrFromPubSubValidationResult(result),
			attribute.Bool("partially_validated", partiallyValidated)))
}

// attrStatusFromErr returns an attribute with key "status" and value set to "success" if
// err is nil, and "failure" otherwise.
func attrStatusFromErr(err error) attribute.KeyValue {
	var v string
	switch err {
	case nil:
		v = "success"
	default:
		v = "failure"
	}
	return attribute.String("status", v)
}
