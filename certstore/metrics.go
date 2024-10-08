package certstore

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3/certstore")
var metrics = struct {
	latestInstance       metric.Int64Gauge
	latestFinalizedEpoch metric.Int64Gauge
	tipsetsPerInstance   metric.Int64Gauge
}{
	latestInstance: measurements.Must(meter.Int64Gauge("f3_certstore_latest_instance",
		metric.WithDescription("The latest instance available in certstore."),
		metric.WithUnit("{instance}"),
	)),
	latestFinalizedEpoch: measurements.Must(meter.Int64Gauge("f3_certstore_latest_finalized_epoch",
		metric.WithDescription("The latest finalized epoch."),
		metric.WithUnit("{epoch}"),
	)),
	tipsetsPerInstance: measurements.Must(meter.Int64Gauge("f3_certstore_tipsets_per_instance",
		metric.WithDescription("The number of new tipsets finalized per instance."),
		metric.WithUnit("{tipset}"),
	)),
}
