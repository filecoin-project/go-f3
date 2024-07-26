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
	headDiverged      metric.Int64Counter
	reconfigured      metric.Int64Counter
	manifestsReceived metric.Int64Counter
	validationTime    metric.Int64Histogram
}{
	headDiverged:      must(meter.Int64Counter("f3_head_diverged", metric.WithDescription("Number of times we encountered the head has diverged from base scenario."))),
	reconfigured:      must(meter.Int64Counter("f3_reconfigured", metric.WithDescription("Number of times we reconfigured due to new manifest being delivered."))),
	manifestsReceived: must(meter.Int64Counter("f3_manifests_received", metric.WithDescription("Number of manifests we have received"))),
	validationTime: must(meter.Int64Histogram("f3_validation_time_ms",
		metric.WithDescription("Histogram of time spent validating broadcasted in milliseconds"),
		metric.WithExplicitBucketBoundaries(1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 1000.0),
		metric.WithUnit("ms"),
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
		time.Since(start).Milliseconds(),
		metric.WithAttributes(attribute.KeyValue{Key: "result", Value: attribute.StringValue(v)}))
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
