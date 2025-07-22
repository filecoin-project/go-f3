package observer

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3/observer")
var attrErrorType = attribute.Key("error.type")

var metrics = struct {
	rotations    metric.Int64Counter
	msgsReceived metric.Int64Counter
}{
	rotations: measurements.Must(meter.Int64Counter(
		"f3_observer_rotations",
		metric.WithDescription("The number of rotations performed."),
	)),
	msgsReceived: measurements.Must(meter.Int64Counter(
		"f3_observer_msgs_received",
		metric.WithDescription("The number of messages received."),
	)),
}
