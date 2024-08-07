package certstore

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3/certstore")
var metrics = struct {
	latestInstance metric.Int64Gauge
}{
	latestInstance: measurements.Must(meter.Int64Gauge("f3_certstore_latest_instance", metric.WithDescription("The latest instance available in certstore."))),
}
