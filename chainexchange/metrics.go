package chainexchange

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("f3/chainexchange")

	attrKindWanted     = attribute.String("kind", "wanted")
	attrKindDiscovered = attribute.String("kind", "discovered")

	metrics = struct {
		chains            metric.Int64Counter
		broadcasts        metric.Int64Counter
		broadcastChainLen metric.Int64Gauge
		notifications     metric.Int64Counter
		instances         metric.Int64UpDownCounter
		validatedMessages metric.Int64Counter
		validationTime    metric.Float64Histogram
	}{
		chains:            measurements.Must(meter.Int64Counter("f3_chainexchange_chains", metric.WithDescription("Number of chains engaged in chainexhange by status."))),
		broadcasts:        measurements.Must(meter.Int64Counter("f3_chainexchange_broadcasts", metric.WithDescription("Number of chains broadcasts made by chainexchange."))),
		broadcastChainLen: measurements.Must(meter.Int64Gauge("f3_chainexchange_broadcast_chain_length", metric.WithDescription("The latest length of broadcasted chain."))),
		notifications:     measurements.Must(meter.Int64Counter("f3_chainexchange_notifications", metric.WithDescription("Number of chain discovery notified by chainexchange."))),
		instances:         measurements.Must(meter.Int64UpDownCounter("f3_chainexchange_instances", metric.WithDescription("Number of instances engaged in chainexchage."))),
		validatedMessages: measurements.Must(meter.Int64Counter("f3_chainexchange_validated_messages", metric.WithDescription("Number of pubsub messages validated tagged by result."))),
		validationTime: measurements.Must(meter.Float64Histogram("f3_chainexchange_validation_time",
			metric.WithDescription("Histogram of time spent validating chainexchange messages in seconds."),
			metric.WithExplicitBucketBoundaries(0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1.0, 10.0),
			metric.WithUnit("s"),
		)),
	}
)

func attrFromWantedDiscovered(wanted, discovered bool) attribute.Set {
	return attribute.NewSet(
		attribute.Bool("wanted", wanted),
		attribute.Bool("discovered", discovered),
	)
}
