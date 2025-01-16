package f3

import (
	"bytes"

	"github.com/klauspost/compress/zstd"
)

var (
	_ gMessageEncoding = (*cborGMessageEncoding)(nil)
	_ gMessageEncoding = (*zstdGMessageEncoding)(nil)
)

type gMessageEncoding interface {
	Encode(message *PartialGMessage) ([]byte, error)
	Decode([]byte) (*PartialGMessage, error)
}

type cborGMessageEncoding struct{}

func (c *cborGMessageEncoding) Encode(m *PartialGMessage) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.MarshalCBOR(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *cborGMessageEncoding) Decode(v []byte) (*PartialGMessage, error) {
	r := bytes.NewReader(v)
	var msg PartialGMessage
	if err := msg.UnmarshalCBOR(r); err != nil {
		return nil, err
	}
	return &msg, nil
}

type zstdGMessageEncoding struct {
	cborEncoding cborGMessageEncoding
	compressor   *zstd.Encoder
	decompressor *zstd.Decoder
}

func newZstdGMessageEncoding() (*zstdGMessageEncoding, error) {
	writer, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	reader, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &zstdGMessageEncoding{
		compressor:   writer,
		decompressor: reader,
	}, nil
}

func (c *zstdGMessageEncoding) Encode(m *PartialGMessage) ([]byte, error) {
	cborEncoded, err := c.cborEncoding.Encode(m)
	if err != nil {
		return nil, err
	}
	compressed := c.compressor.EncodeAll(cborEncoded, make([]byte, 0, len(cborEncoded)))
	return compressed, err
}

func (c *zstdGMessageEncoding) Decode(v []byte) (*PartialGMessage, error) {
	cborEncoded, err := c.decompressor.DecodeAll(v, make([]byte, 0, len(v)))
	if err != nil {
		return nil, err
	}
	return c.cborEncoding.Decode(cborEncoded)
}
