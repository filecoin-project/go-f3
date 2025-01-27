package encoding_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/filecoin-project/go-f3/internal/encoding"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var (
	_ cbg.CBORMarshaler   = (*testValue)(nil)
	_ cbg.CBORUnmarshaler = (*testValue)(nil)
)

type testValue struct {
	Value string
}

func (m *testValue) MarshalCBOR(w io.Writer) error {
	return cbg.WriteByteArray(w, []byte(m.Value))
}

func (m *testValue) UnmarshalCBOR(r io.Reader) error {
	data, err := cbg.ReadByteArray(r, cbg.MaxLength)
	if err != nil {
		return err
	}
	m.Value = string(data)
	return err
}

func TestCBOR(t *testing.T) {
	subject := encoding.NewCBOR[*testValue]()
	data := &testValue{Value: "fish"}
	encoded, err := subject.Encode(data)
	require.NoError(t, err)
	decoded := &testValue{}
	err = subject.Decode(encoded, decoded)
	require.NoError(t, err)
	require.Equal(t, data.Value, decoded.Value)
}

func TestZSTD(t *testing.T) {
	encoder, err := encoding.NewZSTD[*testValue]()
	require.NoError(t, err)
	data := &testValue{Value: "lobster"}
	encoded, err := encoder.Encode(data)
	require.NoError(t, err)
	decoded := &testValue{}
	err = encoder.Decode(encoded, decoded)
	require.NoError(t, err)
	require.Equal(t, data.Value, decoded.Value)
}

func TestZSTDLimits(t *testing.T) {
	subject, err := encoding.NewZSTD[*testValue]()
	require.NoError(t, err)

	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	var v testValue
	v.Value = string(make([]byte, cbg.ByteArrayMaxLen*2))

	var buf bytes.Buffer
	require.NoError(t, v.MarshalCBOR(&buf))

	tooLargeACompression := writer.EncodeAll(buf.Bytes(), nil)
	// Assert the compressed size is less than 1MiB, in other words, transportable by
	// the default GossipSub message size limit.
	require.Less(t, len(tooLargeACompression), 1<<20)

	var dest testValue
	require.ErrorContains(t, subject.Decode(tooLargeACompression, &dest), "decompressed size exceeds configured limit")
}
