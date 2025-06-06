package pmsg

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3")

var metrics = struct {
	partialMessages          metric.Int64UpDownCounter
	partialMessageDuplicates metric.Int64Counter
	partialMessagesDropped   metric.Int64Counter
	partialMessageInstances  metric.Int64UpDownCounter
}{
	partialMessages: measurements.Must(meter.Int64UpDownCounter("f3_partial_messages",
		metric.WithDescription("Number of partial GPBFT messages pending fulfilment."))),
	partialMessageDuplicates: measurements.Must(meter.Int64Counter("f3_partial_message_duplicates",
		metric.WithDescription("Number of partial GPBFT messages received that already have an unfulfilled message for the same instance, sender, round and phase."))),
	partialMessagesDropped: measurements.Must(meter.Int64Counter("f3_partial_messages_dropped",
		metric.WithDescription("Number of partial GPBFT messages or chain broadcasts were dropped due to consumers being too slow."))),
	partialMessageInstances: measurements.Must(meter.Int64UpDownCounter("f3_partial_message_instances",
		metric.WithDescription("Number of instances with partial GPBFT messages pending fulfilment."))),
}
