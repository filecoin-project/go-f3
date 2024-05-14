package gpbft_test

import (
	"encoding/binary"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPayloadMarshalForSigning(t *testing.T) {
	nn := gpbft.NetworkName("filecoin")
	encoded := (&gpbft.Payload{
		Instance: 1,
		Round:    2,
		Step:     3,
		Value:    nil, // not valid in f3, but an empty merkle-tree is defined to hash to all zeros.
	}).MarshalForSigning(nn)
	require.Len(t, encoded, 64)
	assert.Equal(t, encoded[:15], []byte("GPBFT:filecoin:"))            // separators
	assert.Equal(t, encoded[15], uint8(3))                              // step
	assert.Equal(t, binary.BigEndian.Uint64(encoded[16:24]), uint64(2)) // round
	assert.Equal(t, binary.BigEndian.Uint64(encoded[24:32]), uint64(1)) // instance (32-byte right aligned)
	assert.Equal(t, encoded[32:64], make([]byte, 32))                   // 32-byte aligned merkle-tree root

	// Simulate a signe decide message, the one we'll need to verify as a part of finality certificates.
	encoded = (&gpbft.Payload{
		Instance: 29,
		Round:    0,
		Step:     gpbft.DECIDE_PHASE,
		Value:    nil, // not valid in f3, but an empty merkle-tree is defined to hash to all zeros.
	}).MarshalForSigning(nn)
	expected := make([]byte, 64)

	// We expect it to be prefixed with "GPBFT:filecoin:\x05
	copy(expected, []byte("GPBFT:filecoin:\x05"))

	// We expect the instance to be encoded in the last 8 bytes, 32-byte right-aligned.
	expected[31] = 29

	assert.Equal(t, expected, encoded)
}

func BenchmarkPayloadMarshalForSigning(b *testing.B) {
	nn := gpbft.NetworkName("filecoin")
	maxChain := make([]gpbft.TipSet, gpbft.CHAIN_MAX_LEN)
	for i := range maxChain {
		ts := make([]byte, 38*5)
		binary.BigEndian.PutUint64(ts, uint64(i))
		maxChain[i] = gpbft.TipSet{
			Epoch:      int64(i),
			TipSet:     ts,
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
