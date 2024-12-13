package f3

import (
	"bytes"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/klauspost/compress/zstd"
)

var (
	_ gMessageEncoding = (*cborGMessageEncoding)(nil)
	_ gMessageEncoding = (*zstdGMessageEncoding)(nil)
)

type gMessageEncoding interface {
	Encode(*gpbft.GMessage) ([]byte, error)
	Decode([]byte) (*gpbft.GMessage, error)
}

type cborGMessageEncoding struct{}

func (c *cborGMessageEncoding) Encode(m *gpbft.GMessage) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.MarshalCBOR(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *cborGMessageEncoding) Decode(v []byte) (*gpbft.GMessage, error) {
	r := bytes.NewReader(v)
	var msg gpbft.GMessage
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

func (c *zstdGMessageEncoding) Encode(m *gpbft.GMessage) ([]byte, error) {
	cborEncoded, err := c.cborEncoding.Encode(m)
	if err != nil {
		return nil, err
	}
	compressed := c.compressor.EncodeAll(cborEncoded, make([]byte, 0, len(cborEncoded)))
	return compressed, err
}

func (c *zstdGMessageEncoding) Decode(v []byte) (*gpbft.GMessage, error) {
	cborEncoded, err := c.decompressor.DecodeAll(v, make([]byte, 0, len(v)))
	if err != nil {
		return nil, err
	}
	return c.cborEncoding.Decode(cborEncoded)
}
