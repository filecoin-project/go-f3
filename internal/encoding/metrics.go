package encoding

import (
	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	attrCodecCbor    = attribute.String("codec", "cbor")
	attrCodecZstd    = attribute.String("codec", "zstd")
	attrActionEncode = attribute.String("action", "encode")
	attrActionDecode = attribute.String("action", "decode")

	meter = otel.Meter("f3/internal/encoding")

	metrics = struct {
		encodingTime         metric.Float64Histogram
		zstdCompressionRatio metric.Float64Histogram
	}{
		encodingTime: measurements.Must(meter.Float64Histogram(
			"f3_internal_encoding_time",
			metric.WithDescription("The time spent on encoding/decoding in seconds."),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 10.0),
		)),
		zstdCompressionRatio: measurements.Must(meter.Float64Histogram(
			"f3_internal_encoding_zstd_compression_ratio",
			metric.WithDescription("The ratio of compressed to uncompressed data size for zstd encoding."),
			metric.WithExplicitBucketBoundaries(0.0, 0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0),
		)),
	}
)

func attrSuccessFromErr(err error) attribute.KeyValue {
	return attribute.Bool("success", err == nil)
}
