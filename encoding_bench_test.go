package f3

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/encoding"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const seed = 1413

func BenchmarkCborEncoding(b *testing.B) {
	rng := rand.New(rand.NewSource(seed))
	encoder := encoding.NewCBOR[*gpbft.PartialGMessage]()
	msg := generateRandomPartialGMessage(b, rng)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			got, err := encoder.Encode(msg)
			require.NoError(b, err)
			require.NotEmpty(b, got)
		}
	})
}

func BenchmarkCborDecoding(b *testing.B) {
	rng := rand.New(rand.NewSource(seed))
	encoder := encoding.NewCBOR[*gpbft.PartialGMessage]()
	msg := generateRandomPartialGMessage(b, rng)
	data, err := encoder.Encode(msg)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var got gpbft.PartialGMessage
			require.NoError(b, encoder.Decode(data, &got))
			requireEqualPartialMessages(b, msg, &got)
		}
	})
}

func BenchmarkZstdEncoding(b *testing.B) {
	rng := rand.New(rand.NewSource(seed))
	encoder, err := encoding.NewZSTD[*gpbft.PartialGMessage]()
	require.NoError(b, err)
	msg := generateRandomPartialGMessage(b, rng)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			got, err := encoder.Encode(msg)
			require.NoError(b, err)
			require.NotEmpty(b, got)
		}
	})
}

func BenchmarkZstdDecoding(b *testing.B) {
	rng := rand.New(rand.NewSource(seed))
	encoder, err := encoding.NewZSTD[*gpbft.PartialGMessage]()
	require.NoError(b, err)
	msg := generateRandomPartialGMessage(b, rng)
	data, err := encoder.Encode(msg)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var got gpbft.PartialGMessage
			require.NoError(b, encoder.Decode(data, &got))
			requireEqualPartialMessages(b, msg, &got)
		}
	})
}

func requireEqualPartialMessages(b *testing.B, expected, actual *gpbft.PartialGMessage) {
	// Because empty ECChain gets marshaled as null, we need to use ECChain.Eq for
	// checking equality. Hence, the custom equality check.
	require.Equal(b, expected.Sender, actual.Sender)
	require.Equal(b, expected.Signature, actual.Signature)
	require.Equal(b, expected.VoteValueKey, actual.VoteValueKey)
	require.Equal(b, expected.Ticket, actual.Ticket)
	require.True(b, expected.Vote.Eq(&actual.Vote))
	if expected.Justification == nil {
		require.Nil(b, actual.Justification)
	} else {
		require.NotNil(b, actual.Justification)
		require.Equal(b, expected.Justification.Signature, actual.Justification.Signature)
		require.Equal(b, expected.Justification.Signers, actual.Justification.Signers)
		require.True(b, expected.Justification.Vote.Eq(&actual.Justification.Vote))
	}
}

func generateRandomPartialGMessage(b *testing.B, rng *rand.Rand) *gpbft.PartialGMessage {
	var pgmsg gpbft.PartialGMessage
	pgmsg.GMessage = generateRandomGMessage(b, rng)
	pgmsg.GMessage.Vote.Value = nil
	if pgmsg.Justification != nil {
		pgmsg.GMessage.Justification.Vote.Value = nil
	}
	pgmsg.VoteValueKey = gpbft.ECChainKey(generateRandomBytes(b, rng, 32))
	return &pgmsg
}

func generateRandomGMessage(b *testing.B, rng *rand.Rand) *gpbft.GMessage {
	var maybeTicket []byte
	if rng.Float64() < 0.5 {
		maybeTicket = generateRandomBytes(b, rng, 96)
	}
	return &gpbft.GMessage{
		Sender:        gpbft.ActorID(rng.Uint64()),
		Vote:          generateRandomPayload(b, rng),
		Signature:     generateRandomBytes(b, rng, 96),
		Ticket:        maybeTicket,
		Justification: generateRandomJustification(b, rng),
	}
}

func generateRandomJustification(b *testing.B, rng *rand.Rand) *gpbft.Justification {
	return &gpbft.Justification{
		Vote:      generateRandomPayload(b, rng),
		Signers:   generateRandomBitfield(b, rng),
		Signature: generateRandomBytes(b, rng, 96),
	}
}

func generateRandomBytes(b *testing.B, rng *rand.Rand, n int) []byte {
	buf := make([]byte, n)
	_, err := rng.Read(buf)
	require.NoError(b, err)
	return buf
}

func generateRandomPayload(b *testing.B, rng *rand.Rand) gpbft.Payload {
	return gpbft.Payload{
		Instance: rng.Uint64(),
		Round:    rng.Uint64(),
		Phase:    gpbft.Phase(rng.Intn(int(gpbft.COMMIT_PHASE)) + 1),
		Value:    generateRandomECChain(b, rng, rng.Intn(gpbft.ChainMaxLen)+1),
		SupplementalData: gpbft.SupplementalData{
			PowerTable: generateRandomCID(b, rng),
		},
	}
}

func generateRandomBitfield(b *testing.B, rng *rand.Rand) bitfield.BitField {
	ids := make([]uint64, rng.Intn(2_000)+1)
	for i := range ids {
		ids[i] = rng.Uint64()
	}
	// Copy the bitfield once to force initialization of internal bit field state.
	// This is to work around the equality assertions in tests, where under the hood
	// reflection is used to check for equality. This way we can avoid writing custom
	// equality checking for bitfields.
	bitField, err := bitfield.NewFromSet(ids).Copy()
	require.NoError(b, err)
	return bitField
}

func generateRandomECChain(b *testing.B, rng *rand.Rand, length int) *gpbft.ECChain {
	chain := &gpbft.ECChain{
		TipSets: make([]*gpbft.TipSet, length),
	}
	epoch := int64(rng.Uint64())
	for i := range length {
		chain.TipSets[i] = generateRandomTipSet(b, rng, epoch+int64(i))
	}
	return chain
}

func generateRandomTipSet(b *testing.B, rng *rand.Rand, epoch int64) *gpbft.TipSet {
	return &gpbft.TipSet{
		Epoch:      epoch,
		Key:        generateRandomTipSetKey(b, rng),
		PowerTable: generateRandomCID(b, rng),
	}
}

func generateRandomTipSetKey(b *testing.B, rng *rand.Rand) gpbft.TipSetKey {
	key := make([]byte, rng.Intn(gpbft.TipsetKeyMaxLen)+1)
	_, err := rng.Read(key)
	require.NoError(b, err)
	return key
}

func generateRandomCID(b *testing.B, rng *rand.Rand) cid.Cid {
	sum, err := multihash.Sum(generateRandomBytes(b, rng, 32), multihash.SHA2_256, -1)
	require.NoError(b, err)
	return cid.NewCidV1(cid.Raw, sum)
}
