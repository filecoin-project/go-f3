package certstore

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3/certstore")
var metrics = struct {
	latestInstance metric.Int64Gauge
}{
	latestInstance: must(meter.Int64Gauge("f3_certstore_latest_instance", metric.WithDescription("The latest instance available in certstore."))),
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
