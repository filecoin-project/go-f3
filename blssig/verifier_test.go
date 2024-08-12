package blssig

import (
	"context"
	"testing"

	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func BenchmarkBLSSigning(b *testing.B) {
	var (
		blsSuit   = bls12381.NewBLS12381Suite()
		blsSchema = bdn.NewSchemeOnG2(blsSuit)
	)
	privKey, pubKey := blsSchema.NewKeyPair(blsSuit.RandomStream())
	pubKeyB, err := pubKey.MarshalBinary()
	var pubK gpbft.PubKey
	copy(pubK[:], pubKeyB)
	require.NoError(b, err)
	signer := SignerWithKeyOnG1(pubK, privKey)
	verifier := VerifierWithKeyOnG1()
	ctx := context.Background()

	sig, err := signer.Sign(ctx, pubK, pubKeyB)
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := verifier.Verify(pubK, pubKeyB, sig)
			require.NoError(b, err)
		}
	})
	//for i := 0; i < b.N; i++ {
	//	err := verifier.Verify(pubKeyB, pubKeyB, sig)
	//	require.NoError(b, err)
	//}
}
