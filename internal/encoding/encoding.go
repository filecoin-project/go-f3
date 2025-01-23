package encoding

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// maxDecompressedSize is the default maximum amount of memory allocated by the
// zstd decoder. The limit of 1MiB is chosen based on the default maximum message
// size in GossipSub.
const maxDecompressedSize = 1 << 20

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

func (c *CBOR[T]) Encode(m T) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.MarshalCBOR(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *CBOR[T]) Decode(v []byte, t T) error {
	r := bytes.NewReader(v)
	return t.UnmarshalCBOR(r)
}

type ZSTD[T CBORMarshalUnmarshaler] struct {
	cborEncoding *CBOR[T]
	compressor   *zstd.Encoder
	decompressor *zstd.Decoder
}

func NewZSTD[T CBORMarshalUnmarshaler]() (*ZSTD[T], error) {
	writer, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	reader, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxDecompressedSize))
	if err != nil {
		return nil, err
	}
	return &ZSTD[T]{
		cborEncoding: &CBOR[T]{},
		compressor:   writer,
		decompressor: reader,
	}, nil
}

func (c *ZSTD[T]) Encode(m T) ([]byte, error) {
	cborEncoded, err := c.cborEncoding.Encode(m)
	if len(cborEncoded) > maxDecompressedSize {
		// Error out early if the encoded value is too large to be decompressed.
		return nil, fmt.Errorf("encoded value cannot exceed maximum size: %d > %d", len(cborEncoded), maxDecompressedSize)
	}
	if err != nil {
		return nil, err
	}
	compressed := c.compressor.EncodeAll(cborEncoded, make([]byte, 0, len(cborEncoded)))
	return compressed, nil
}

func (c *ZSTD[T]) Decode(v []byte, t T) error {
	cborEncoded, err := c.decompressor.DecodeAll(v, make([]byte, 0, len(v)))
	if err != nil {
		return err
	}
	return c.cborEncoding.Decode(cborEncoded, t)
}
