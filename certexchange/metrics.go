package certexchange

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3/certexchange")

var clientMetrics = struct {
	requests        metric.Int64Counter
	dialFailures    metric.Int64Counter
	requestFailures metric.Int64Counter
}{
	requests: must(meter.Int64Counter(
		"f3_certexchange_client_requests",
		metric.WithDescription("The total number of requests made."),
	)),
	dialFailures: must(meter.Int64Counter(
		"f3_certexchange_client_failed_dials",
		metric.WithDescription("The number of failed certexchange dials."),
	)),
	requestFailures: must(meter.Int64Counter(
		"f3_certexchange_client_failed_requests",
		metric.WithDescription("The number of failed certexchange requests (total)."),
	)),
}

var serverMetrics = struct {
	requests                      metric.Int64Counter
	requestsFailed                metric.Int64Counter
	certificatesServed            metric.Int64Counter
	powerTablesServed             metric.Int64Counter
	certificatesServedPerResponse metric.Int64Histogram
	responseTimeMS                metric.Int64Histogram
}{
	requests: must(meter.Int64Counter(
		"f3_certexchange_server_requests",
		metric.WithDescription("The total number of requests received."),
	)),
	requestsFailed: must(meter.Int64Counter(
		"f3_certexchange_server_requests_failed",
		metric.WithDescription("The total number of requests failed."),
	)),
	powerTablesServed: must(meter.Int64Counter(
		"f3_certexchange_server_power_tables_served",
		metric.WithDescription("The number of power tables served."),
	)),
	certificatesServed: must(meter.Int64Counter(
		"f3_certexchange_server_certificates_served",
		metric.WithDescription("The number of certificates served."),
	)),
	certificatesServedPerResponse: must(meter.Int64Histogram(
		"f3_certexchange_server_certificates_served_per_response",
		metric.WithDescription("The number of certificates served per response."),
	)),
	responseTimeMS: must(meter.Int64Histogram(
		"f3_certexchange_server_response_time_ms",
		metric.WithDescription("The time it takes to respond to requests, excluding failures (milliseconds)."),
	)),
}
