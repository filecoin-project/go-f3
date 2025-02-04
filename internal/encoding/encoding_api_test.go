package encoding

import "go.opentelemetry.io/otel/attribute"

// GetMetricAttribute returns the attribute for metric collection, exported for
// testing purposes.
func (c *ZSTD[T]) GetMetricAttribute() attribute.KeyValue { return c.getMetricAttribute() }
