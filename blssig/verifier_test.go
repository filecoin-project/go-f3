package blssig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	bls12381 "go.dedis.ch/kyber/v4/pairing/bls12381/kilic"
	"go.dedis.ch/kyber/v4/sign/bdn"
)

func BenchmarkBLSSigning(b *testing.B) {
	var (
		blsSuit   = bls12381.NewBLS12381Suite()
		blsSchema = bdn.NewSchemeOnG2(blsSuit)
	)
	privKey, pubKey := blsSchema.NewKeyPair(blsSuit.RandomStream())
	pubKeyB, err := pubKey.MarshalBinary()
	require.NoError(b, err)
	signer := SignerWithKeyOnG1(pubKeyB, privKey)
	verifier := VerifierWithKeyOnG1()
	ctx := context.Background()

	sig, err := signer.Sign(ctx, pubKeyB, pubKeyB)
	require.NoError(b, err)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := verifier.Verify(pubKeyB, pubKeyB, sig)
		require.NoError(b, err)
	}
}
