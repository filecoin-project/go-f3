package f3

import (
	"context"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3")
var metrics = struct {
	headDiverged       metric.Int64Counter
	reconfigured       metric.Int64Counter
	manifestsReceived  metric.Int64Counter
	validationTime     metric.Float64Histogram
	proposalFetchTime  metric.Float64Histogram
	committeeFetchTime metric.Float64Histogram
}{
	headDiverged:      must(meter.Int64Counter("f3_head_diverged", metric.WithDescription("Number of times we encountered the head has diverged from base scenario."))),
	reconfigured:      must(meter.Int64Counter("f3_reconfigured", metric.WithDescription("Number of times we reconfigured due to new manifest being delivered."))),
	manifestsReceived: must(meter.Int64Counter("f3_manifests_received", metric.WithDescription("Number of manifests we have received"))),
	validationTime: must(meter.Float64Histogram("f3_validation_time",
		metric.WithDescription("Histogram of time spent validating broadcasted in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1.0),
		metric.WithUnit("s"),
	)),
	proposalFetchTime: must(meter.Float64Histogram("f3_proposal_fetch_time",
		metric.WithDescription("Histogram of time spent fetching proposal per instance in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0),
		metric.WithUnit("s"),
	)),
	committeeFetchTime: must(meter.Float64Histogram("f3_committee_fetch_time",
		metric.WithDescription("Histogram of time spent fetching committees per instance in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0),
		metric.WithUnit("s"),
	)),
}

func recordValidationTime(ctx context.Context, start time.Time, result pubsub.ValidationResult) {
	var v string
	switch result {
	case pubsub.ValidationAccept:
		v = "accepted"
	case pubsub.ValidationReject:
		v = "rejected"
	case pubsub.ValidationIgnore:
		v = "ignored"
	default:
		v = "unknown"
	}
	metrics.validationTime.Record(
		ctx,
		time.Since(start).Seconds(),
		metric.WithAttributes(attribute.KeyValue{Key: "result", Value: attribute.StringValue(v)}))
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

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
