package gpbft_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func TestPayloadMarshalForSigning(t *testing.T) {
	nn := gpbft.NetworkName("filecoin")
	powerTableCid := gpbft.MakeCid([]byte("foo"))
	encoded := (&gpbft.Payload{
		Instance: 1,
		Round:    2,
		Step:     3,
		SupplementalData: gpbft.SupplementalData{
			Commitments: [32]byte{0x42},
			PowerTable:  powerTableCid,
		},
		Value: nil, // not valid in f3, but an empty merkle-tree is defined to hash to all zeros.
	}).MarshalForSigning(nn)
	require.Len(t, encoded, 96+len(powerTableCid))
	assert.Equal(t, encoded[:15], []byte("GPBFT:filecoin:"))            // separators
	assert.Equal(t, encoded[15], uint8(3))                              // step
	assert.Equal(t, binary.BigEndian.Uint64(encoded[16:24]), uint64(2)) // round
	assert.Equal(t, binary.BigEndian.Uint64(encoded[24:32]), uint64(1)) // instance (32-byte right aligned)
	assert.EqualValues(t, encoded[32:64], [32]byte{0x42})               // commitments root
	assert.EqualValues(t, encoded[64:96], [32]byte{})                   // tipsets
	assert.EqualValues(t, encoded[96:], powerTableCid)                  // next power table

	// Simulate a signed decide message, the one we'll need to verify as a part of finality certificates.
	encoded = (&gpbft.Payload{
		Instance: 29,
		Round:    0,
		Step:     gpbft.DECIDE_PHASE,
		SupplementalData: gpbft.SupplementalData{
			Commitments: [32]byte{},
			PowerTable:  powerTableCid,
		},
		Value: nil, // not valid in f3, but an empty merkle-tree is defined to hash to all zeros.
	}).MarshalForSigning(nn)
	expected := make([]byte, 96)

	// We expect it to be prefixed with "GPBFT:filecoin:\x05
	copy(expected, []byte("GPBFT:filecoin:\x05"))

	// We expect the instance to be encoded in the last 8 bytes, 32-byte right-aligned.
	expected[31] = 29
	expected = append(expected, powerTableCid...)

	assert.Equal(t, expected, encoded)
}

func BenchmarkPayloadMarshalForSigning(b *testing.B) {
	nn := gpbft.NetworkName("filecoin")
	maxChain := make([]gpbft.TipSet, gpbft.ChainMaxLen)
	for i := range maxChain {
		ts := make([]byte, 38*5)
		binary.BigEndian.PutUint64(ts, uint64(i))
		maxChain[i] = gpbft.TipSet{
			Epoch:      int64(i),
			Key:        ts,
			PowerTable: make([]byte, 38),
		}
	}
	payload := &gpbft.Payload{
		Instance: 1,
		Round:    2,
		Step:     3,
		Value:    maxChain,
	}
	for i := 0; i < b.N; i++ {
		payload.MarshalForSigning(nn)
	}
}

func TestTipSetMarshalForSigning(t *testing.T) {
	const expectedLen = 8 + // epoch
		32 + // commitments
		38 + // tispset cid
		38 // power table cid

	tsk := make([]byte, 38*5)
	tsk[0] = 110
	pt := make([]byte, 38)
	pt[0] = 123
	comm := [32]byte{0x42}
	ts := gpbft.TipSet{
		Epoch:       1,
		Key:         tsk,
		PowerTable:  pt,
		Commitments: comm,
	}

	var buf bytes.Buffer
	require.NoError(t, cbg.WriteByteArray(&buf, ts.Key))
	tsCid := gpbft.MakeCid(buf.Bytes())

	encoded := ts.MarshalForSigning()
	require.Len(t, encoded, expectedLen)
	assert.Equal(t, binary.BigEndian.Uint64(encoded[:8]), uint64(ts.Epoch))
	assert.Equal(t, encoded[8:40], ts.Commitments[:])
	assert.Equal(t, encoded[40:78], tsCid)
	assert.Equal(t, encoded[78:], ts.PowerTable)
}
