package blssignatures

import (
	"testing"

	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/sign"
	"github.com/drand/kyber/sign/bdn"
	"github.com/drand/kyber/util/random"
	"github.com/filecoin-project/go-f3/f3"
	"github.com/stretchr/testify/require"
)

var dummyMessage = []byte{0, 1, 2, 3, 4, 5, 6, 7}
var differentDummyMessage = []byte{7, 6, 5, 4, 3, 2, 1, 0}

func TestBDN(t *testing.T) {
	suite := bls12381.NewBLS12381Suite()
	n := 10

	// Generate key pairs.
	privKeys := make([]kyber.Scalar, n)
	pubKeys := make([]kyber.Point, n)
	for i := 0; i < n; i++ {
		privKeys[i], pubKeys[i] = bdn.NewKeyPair(suite, random.New())
	}

	// Generate signatures.
	sigs := make([][]byte, n)
	for i := 0; i < n; i++ {
		var err error
		sigs[i], err = bdn.Sign(suite, privKeys[i], dummyMessage)
		require.NoError(t, err)
	}

	// Verify signatures.
	for i := 0; i < n; i++ {
		require.NoError(t, bdn.Verify(suite, pubKeys[i], dummyMessage, sigs[i]))
	}

	// Aggregate all signatures.
	mask, err := sign.NewMask(suite, pubKeys, nil)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, mask.SetBit(i, true))
	}
	aggKey, err := bdn.AggregatePublicKeys(suite, mask)
	require.NoError(t, err)
	aggSig, err := bdn.AggregateSignatures(suite, sigs, mask)
	require.NoError(t, err)
	aggSigBytes, err := aggSig.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, bdn.Verify(suite, aggKey, dummyMessage, aggSigBytes))

	// Aggregate half of the signatures.
	mask, err = sign.NewMask(suite, pubKeys, nil)
	require.NoError(t, err)
	for i := 0; i < n/2; i++ {
		require.NoError(t, mask.SetBit(i, true))
	}
	aggKey, err = bdn.AggregatePublicKeys(suite, mask)
	require.NoError(t, err)
	aggSig, err = bdn.AggregateSignatures(suite, sigs[:n/2], mask)
	require.NoError(t, err)
	aggSigBytes, err = aggSig.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, bdn.Verify(suite, aggKey, dummyMessage, aggSigBytes))
}

func TestBLSSigner_Sign_Verify(t *testing.T) {
	signers, schemes := dummySystem(t, 2)
	require.Equal(t, 2, len(schemes))

	sig0, err := signers[0].Sign(dummyMessage)
	require.NoError(t, err)
	sig1, err := signers[1].Sign(dummyMessage)
	require.NoError(t, err)

	require.NoError(t, signers[0].Verify(0, dummyMessage, sig0))
	require.NoError(t, signers[0].Verify(1, dummyMessage, sig1))
	require.NoError(t, signers[1].Verify(0, dummyMessage, sig0))
	require.NoError(t, signers[1].Verify(1, dummyMessage, sig1))

	require.NotNil(t, signers[0].Verify(0, dummyMessage, sig1))
	require.NotNil(t, signers[0].Verify(1, differentDummyMessage, sig1))
}

func TestBLSScheme_Aggregate(t *testing.T) {
	signers, schemes := dummySystem(t, 4)

	sigs := make([][]byte, 4)
	for i, signer := range signers {
		var err error
		sigs[i], err = signer.Sign(dummyMessage)
		require.NoError(t, err)
	}

	// Valid signature, aggregate all, in order.
	aggValidAllOrdered, err := schemes[0].NewAggregator()
	require.NoError(t, err)
	require.NoError(t, aggValidAllOrdered.Add(0, sigs[0]))
	require.NoError(t, aggValidAllOrdered.Add(1, sigs[1]))
	require.NoError(t, aggValidAllOrdered.Add(2, sigs[2]))
	require.NoError(t, aggValidAllOrdered.Add(3, sigs[3]))
	validSigAllOrdered, err := aggValidAllOrdered.Aggregate()
	require.NoError(t, err)
	require.NoError(t, schemes[0].VerifyAggSig(dummyMessage, validSigAllOrdered))
	require.NotNil(t, schemes[0].VerifyAggSig(differentDummyMessage, validSigAllOrdered))
	require.NoError(t, schemes[1].VerifyAggSig(dummyMessage, validSigAllOrdered))
	require.NotNil(t, schemes[1].VerifyAggSig(differentDummyMessage, validSigAllOrdered))

	// Valid signature, aggregate all, out of order.
	aggValidAll, err := schemes[0].NewAggregator()
	require.NoError(t, err)
	require.NoError(t, aggValidAll.Add(2, sigs[2]))
	require.NoError(t, aggValidAll.Add(0, sigs[0]))
	require.NoError(t, aggValidAll.Add(3, sigs[3]))
	require.NoError(t, aggValidAll.Add(1, sigs[1]))
	validSigAll, err := aggValidAll.Aggregate()
	require.NoError(t, err)
	require.NoError(t, schemes[0].VerifyAggSig(dummyMessage, validSigAll))
	require.NotNil(t, schemes[0].VerifyAggSig(differentDummyMessage, validSigAll))
	require.NoError(t, schemes[1].VerifyAggSig(dummyMessage, validSigAll))
	require.NotNil(t, schemes[1].VerifyAggSig(differentDummyMessage, validSigAll))

	// Valid signature, aggregate only some.
	aggValidSome, err := schemes[0].NewAggregator()
	require.NoError(t, err)
	require.NoError(t, aggValidSome.Add(2, sigs[2]))
	require.NoError(t, aggValidSome.Add(0, sigs[0]))
	validSigSome, err := aggValidSome.Aggregate()
	require.NoError(t, err)
	require.NoError(t, schemes[0].VerifyAggSig(dummyMessage, validSigSome))
	require.NotNil(t, schemes[0].VerifyAggSig(differentDummyMessage, validSigSome))
	require.NoError(t, schemes[1].VerifyAggSig(dummyMessage, validSigSome))
	require.NotNil(t, schemes[1].VerifyAggSig(differentDummyMessage, validSigSome))

	// Valid signature, aggregate only some.
	aggValidSingle, err := schemes[0].NewAggregator()
	require.NoError(t, err)
	require.NoError(t, aggValidSingle.Add(2, sigs[2]))
	validSigSingle, err := aggValidSingle.Aggregate()
	require.NoError(t, err)
	require.NoError(t, schemes[0].VerifyAggSig(dummyMessage, validSigSingle))
	require.NotNil(t, schemes[0].VerifyAggSig(differentDummyMessage, validSigSingle))
	require.NoError(t, schemes[1].VerifyAggSig(dummyMessage, validSigSingle))
	require.NotNil(t, schemes[1].VerifyAggSig(differentDummyMessage, validSigSingle))

	// Empty signature.
	aggEmpty, err := schemes[0].NewAggregator()
	require.NoError(t, err)
	emptySig, err := aggEmpty.Aggregate()
	require.NoError(t, err)
	require.NoError(t, schemes[0].VerifyAggSig(dummyMessage, emptySig))
	require.NoError(t, schemes[0].VerifyAggSig(differentDummyMessage, emptySig))
	require.NoError(t, schemes[1].VerifyAggSig(dummyMessage, emptySig))
	require.NoError(t, schemes[1].VerifyAggSig(differentDummyMessage, emptySig))

	// Invalid signature.
	aggInvalid, err := schemes[0].NewAggregator()
	require.NoError(t, err)
	require.NoError(t, aggInvalid.Add(0, sigs[3]))
	require.NoError(t, aggInvalid.Add(1, sigs[1]))
	require.NoError(t, aggInvalid.Add(2, sigs[2]))
	invalidSig, err := aggInvalid.Aggregate()
	require.NoError(t, err)
	require.NotNil(t, schemes[0].VerifyAggSig(dummyMessage, invalidSig))
	require.NotNil(t, schemes[1].VerifyAggSig(differentDummyMessage, invalidSig))
}

func dummySystem(t *testing.T, numParticipants int) ([]*BLSSigner, []*BLSAggScheme) {
	t.Helper()

	// Create power table and private keys.
	powerTable := f3.NewPowerTable(make([]f3.PowerEntry, 0))
	privKeys := make([][]byte, numParticipants)
	for i := 0; i < numParticipants; i++ {

		// Generate key pair.
		privKey, pubKey := bdn.NewKeyPair(bls12381.NewBLS12381Suite(), random.New())

		pubKeyData, err := pubKey.MarshalBinary()
		require.NoError(t, err)
		privKeys[i], err = privKey.MarshalBinary()
		require.NoError(t, err)

		err = powerTable.Add(f3.ActorID(i), f3.NewStoragePower(1), pubKeyData)
		require.NoError(t, err)
	}

	// Create one signature scheme object per private key.
	schemes := make([]*BLSAggScheme, numParticipants)
	signers := make([]*BLSSigner, numParticipants)
	for i, privKey := range privKeys {
		var err error
		signers[i], err = NewBLSSigner(privKey, powerTable)
		require.NoError(t, err)
		schemes[i], err = NewBLSAggScheme(powerTable)
		require.NoError(t, err)
	}

	return signers, schemes
}
