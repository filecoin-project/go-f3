package polling

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3/certexchange/polling")
var metrics = struct {
	activePeers              metric.Int64Gauge
	backoffPeers             metric.Int64Gauge
	predictedPollingInterval metric.Float64Gauge
	pollDuration             metric.Float64Histogram
	peersPolled              metric.Int64Histogram
	peersRequiredPerPoll     metric.Int64Histogram
	pollEfficiency           metric.Float64Histogram
}{
	activePeers: measurements.Must(meter.Int64Gauge(
		"f3_certexchange_polling_active_peers",
		metric.WithDescription("The number of active certificate exchange peers."),
		metric.WithUnit("{peer}"),
	)),
	backoffPeers: measurements.Must(meter.Int64Gauge(
		"f3_certexchange_polling_backoff_peers",
		metric.WithDescription("The number of active certificate exchange peers on backoff."),
		metric.WithUnit("{peer}"),
	)),
	predictedPollingInterval: measurements.Must(meter.Float64Gauge(
		"f3_certexchange_polling_predicted_interval",
		metric.WithDescription("The predicted certificate exchange polling interval."),
		metric.WithUnit("s"),
	)),
	pollDuration: measurements.Must(meter.Float64Histogram(
		"f3_certexchange_polling_poll_duration",
		metric.WithDescription("The certificate exchange total poll duration."),
		metric.WithUnit("s"),
	)),
	peersPolled: measurements.Must(meter.Int64Histogram(
		"f3_certexchange_polling_peers_polled",
		metric.WithDescription("The number of peers polled per certificate exchange poll."),
		metric.WithUnit("{peer}"),
	)),
	peersRequiredPerPoll: measurements.Must(meter.Int64Histogram(
		"f3_certexchange_polling_peers_required_per_poll",
		metric.WithDescription("The number of peers we should be selecting per poll (optimally)."),
		metric.WithUnit("{peer}"),
	)),
	pollEfficiency: measurements.Must(meter.Float64Histogram(
		"f3_certexchange_polling_poll_efficiency",
		metric.WithDescription("The fraction of requests necessary to make progress."),
	)),
}

var attrMadeProgress = attribute.Key("made-progress")
