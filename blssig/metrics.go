package blssig

import (
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/filecoin-project/go-f3/internal/measurements"
)

var meter = otel.Meter("f3/blssig")
var attrCached = attribute.Key("cached")

var log = logging.Logger("f3/blssig")

var metrics = struct {
	decompressPoint metric.Int64Counter
	verify          metric.Int64Counter
	verifyAggregate metric.Int64Histogram
	aggregate       metric.Int64Histogram
}{
	decompressPoint: measurements.Must(meter.Int64Counter(
		"f3_blssig_decompress_point",
		metric.WithDescription("Number of times we decompress points."),
	)),
	verify: measurements.Must(meter.Int64Counter(
		"f3_blssig_verify",
		metric.WithDescription("Number of signatures verified."),
	)),
	verifyAggregate: measurements.Must(meter.Int64Histogram(
		"f3_blssig_verify_aggregate",
		metric.WithDescription("Aggregate signatures verified."),
	)),
	aggregate: measurements.Must(meter.Int64Histogram(
		"f3_blssig_aggregate",
		metric.WithDescription("Signatures aggregated."),
	)),
}
