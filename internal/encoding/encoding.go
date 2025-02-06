package encoding

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// maxDecompressedSize is the default maximum amount of memory allocated by the
// zstd decoder. The limit of 1MiB is chosen based on the default maximum message
// size in GossipSub.
const maxDecompressedSize = 1 << 20

var bufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, maxDecompressedSize)
		return &buf
	},
}

type CBORMarshalUnmarshaler interface {
	cbg.CBORMarshaler
	cbg.CBORUnmarshaler
}

type EncodeDecoder[T CBORMarshalUnmarshaler] interface {
	Encode(v T) ([]byte, error)
	Decode([]byte, T) error
}

type CBOR[T CBORMarshalUnmarshaler] struct{}

func NewCBOR[T CBORMarshalUnmarshaler]() *CBOR[T] {
	return &CBOR[T]{}
}

func (c *CBOR[T]) Encode(m T) (_ []byte, _err error) {
	defer func(start time.Time) {
		metrics.encodingTime.Record(context.Background(),
			time.Since(start).Seconds(),
			metric.WithAttributes(attrCodecCbor, attrActionEncode, attrSuccessFromErr(_err)))
	}(time.Now())
	var out bytes.Buffer
	if err := m.MarshalCBOR(&out); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func (c *CBOR[T]) Decode(v []byte, t T) (_err error) {
	defer func(start time.Time) {
		metrics.encodingTime.Record(context.Background(),
			time.Since(start).Seconds(),
			metric.WithAttributes(attrCodecCbor, attrActionDecode, attrSuccessFromErr(_err)))
	}(time.Now())
	r := bytes.NewReader(v)
	return t.UnmarshalCBOR(r)
}

type ZSTD[T CBORMarshalUnmarshaler] struct {
	cborEncoding *CBOR[T]
	compressor   *zstd.Encoder
	decompressor *zstd.Decoder

	metricAttr       attribute.KeyValue
	metricAttrLoader sync.Once
}

func NewZSTD[T CBORMarshalUnmarshaler]() (*ZSTD[T], error) {
	writer, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	reader, err := zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(maxDecompressedSize),
		zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, err
	}
	return &ZSTD[T]{
		cborEncoding: &CBOR[T]{},
		compressor:   writer,
		decompressor: reader,
	}, nil
}

func (c *ZSTD[T]) Encode(t T) (_ []byte, _err error) {
	defer func(start time.Time) {
		metrics.encodingTime.Record(context.Background(),
			time.Since(start).Seconds(),
			metric.WithAttributes(attrCodecZstd, attrActionEncode, attrSuccessFromErr(_err)))
	}(time.Now())
	decompressed, err := c.cborEncoding.Encode(t)
	if len(decompressed) > maxDecompressedSize {
		// Error out early if the encoded value is too large to be decompressed.
		return nil, fmt.Errorf("encoded value cannot exceed maximum size: %d > %d", len(decompressed), maxDecompressedSize)
	}
	if err != nil {
		return nil, err
	}
	maxCompressedSize := c.compressor.MaxEncodedSize(len(decompressed))
	compressed := c.compressor.EncodeAll(decompressed, make([]byte, 0, maxCompressedSize))
	c.meterCompressionRatio(len(decompressed), len(compressed))
	return compressed, nil
}

func (c *ZSTD[T]) Decode(compressed []byte, t T) (_err error) {
	defer func(start time.Time) {
		metrics.encodingTime.Record(context.Background(),
			time.Since(start).Seconds(),
			metric.WithAttributes(attrCodecZstd, attrActionDecode, attrSuccessFromErr(_err)))
	}(time.Now())
	buf := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(buf)
	decompressed, err := c.decompressor.DecodeAll(compressed, (*buf)[:0])
	if err != nil {
		return err
	}
	c.meterCompressionRatio(len(decompressed), len(compressed))
	return c.cborEncoding.Decode(decompressed, t)
}

func (c *ZSTD[T]) meterCompressionRatio(decompressedSize, compressedSize int) {
	compressionRatio := float64(decompressedSize) / float64(compressedSize)
	metrics.zstdCompressionRatio.Record(context.Background(), compressionRatio, metric.WithAttributes(c.getMetricAttribute()))
}

func (c *ZSTD[T]) getMetricAttribute() attribute.KeyValue {
	c.metricAttrLoader.Do(func() {
		const key = "type"
		switch target := reflect.TypeFor[T](); {
		case target.Kind() == reflect.Ptr:
			c.metricAttr = attribute.String(key, target.Elem().Name())
		default:
			c.metricAttr = attribute.String(key, target.Name())
		}
	})
	return c.metricAttr
}
