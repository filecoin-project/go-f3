package certexchange

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3/certexchange")
var attrWithPowerTable = attribute.Key("with-power-table")

var metrics = struct {
	requestLatency     metric.Float64Histogram
	totalResponseTime  metric.Float64Histogram
	serveTime          metric.Float64Histogram
	certificatesServed metric.Int64Histogram
}{
	requestLatency: measurements.Must(meter.Float64Histogram(
		"f3_certexchange_request_latency",
		metric.WithDescription("The outbound request latency."),
		metric.WithUnit("s"),
	)),
	totalResponseTime: measurements.Must(meter.Float64Histogram(
		"f3_certexchange_total_response_time",
		metric.WithDescription("The total time for outbound requests."),
		metric.WithUnit("s"),
	)),
	serveTime: measurements.Must(meter.Float64Histogram(
		"f3_certexchange_serve_time",
		metric.WithDescription("The time spent serving requests."),
		metric.WithUnit("s"),
	)),
	certificatesServed: measurements.Must(meter.Int64Histogram(
		"f3_certexchange_certificates_served",
		metric.WithDescription("The number of certificates served (per request)."),
		metric.WithUnit("{certificate}"),
	)),
}
