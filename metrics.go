package f3

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3")
var metrics = struct {
	headDiverged       metric.Int64Counter
	reconfigured       metric.Int64Counter
	manifests_received metric.Int64Counter
}{
	headDiverged:       must(meter.Int64Counter("f3_head_diverged", metric.WithDescription("Number of times we encountered the head has diverged from base scenario."))),
	reconfigured:       must(meter.Int64Counter("f3_reconfigured", metric.WithDescription("Number of times we reconfigured due to new manifest being delivered."))),
	manifests_received: must(meter.Int64Counter("f3_manifests_received", metric.WithDescription("Number of manifests we have received"))),
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
