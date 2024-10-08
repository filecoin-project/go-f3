package signing_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.dedis.ch/kyber/v4/sign/bdn"

	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/gpbft"
	bls12381 "github.com/filecoin-project/go-f3/internal/gnark"
	"github.com/filecoin-project/go-f3/internal/signing"
)

type (
	SigningTestSuite struct {
		suite.Suite
		signerTestSubject SignerTestSubject
		verifier          gpbft.Verifier
	}
	SignerTestSubject func(*testing.T) (gpbft.PubKey, gpbft.Signer)
)

func TestBLSSigning(t *testing.T) {
	var (
		blsSuit   = bls12381.NewSuiteBLS12381()
		blsSchema = bdn.NewSchemeOnG2(blsSuit)
	)
	t.Parallel()
	suite.Run(t, NewSigningSuite(func(t *testing.T) (gpbft.PubKey, gpbft.Signer) {
		privKey, pubKey := blsSchema.NewKeyPair(blsSuit.RandomStream())
		pubKeyB, err := pubKey.MarshalBinary()
		require.NoError(t, err)
		return pubKeyB, blssig.SignerWithKeyOnG1(pubKeyB, privKey)
	}, blssig.VerifierWithKeyOnG1()))
}

func TestFakeSigning(t *testing.T) {
	t.Parallel()
	var fakeSigning = signing.NewFakeBackend()
	suite.Run(t, NewSigningSuite(func(t *testing.T) (gpbft.PubKey, gpbft.Signer) {
		pubKey, _ := fakeSigning.GenerateKey()
		return pubKey, fakeSigning
	}, fakeSigning))
}

func NewSigningSuite(signer SignerTestSubject, verifier gpbft.Verifier) *SigningTestSuite {
	return &SigningTestSuite{
		signerTestSubject: signer,
		verifier:          verifier,
	}
}

func (s *SigningTestSuite) TestSignAndVerify() {
	ctx := context.Background()
	t := s.Suite.T()
	pubKey, signer := s.signerTestSubject(s.T())
	msg := []byte("test message")
	sig, err := signer.Sign(ctx, pubKey, msg)
	require.NoError(t, err)

	err = s.verifier.Verify(pubKey, msg, sig)
	require.NoError(t, err)

	pubKey2, signer2 := s.signerTestSubject(s.T())
	err = s.verifier.Verify(pubKey2, msg, sig)
	require.Error(t, err)

	err = s.verifier.Verify(pubKey, msg, nil)
	require.Error(t, err)

	err = s.verifier.Verify(pubKey, msg, []byte("short sig"))
	require.Error(t, err)

	sig2, err := signer2.Sign(ctx, pubKey2, msg)
	require.NoError(t, err)

	err = s.verifier.Verify(pubKey, msg, sig2)
	require.Error(t, err)
}

func (s *SigningTestSuite) TestAggregateAndVerify() {
	ctx := context.Background()
	t := s.Suite.T()
	msg := []byte("test message")
	pubKey1, signer1 := s.signerTestSubject(s.T())
	pubKey2, signer2 := s.signerTestSubject(s.T())
	pubKeys := []gpbft.PubKey{pubKey1, pubKey2}

	aggregator, err := s.verifier.Aggregate(pubKeys)
	require.NoError(s.T(), err)

	mask := []int{0, 1}
	sigs := make([][]byte, len(pubKeys))
	sigs[0], err = signer1.Sign(ctx, pubKey1, msg)
	require.NoError(s.T(), err)
	sigs[1], err = signer2.Sign(ctx, pubKey2, msg)
	require.NoError(s.T(), err)

	aggSig, err := aggregator.Aggregate(mask, sigs)
	require.NoError(t, err)

	err = aggregator.VerifyAggregate(mask, msg, aggSig)
	require.NoError(t, err)

	aggSig, err = aggregator.Aggregate(mask[0:1], sigs[0:1])
	require.NoError(t, err)

	err = aggregator.VerifyAggregate(mask, msg, aggSig)
	require.Error(t, err)

	aggSig, err = aggregator.Aggregate(mask, [][]byte{sigs[0], sigs[0]})
	require.NoError(t, err)

	err = aggregator.VerifyAggregate(mask, msg, aggSig)
	require.Error(t, err)

	err = aggregator.VerifyAggregate(mask, msg, []byte("bad sig"))
	require.Error(t, err)

	_, err = aggregator.Aggregate(mask, [][]byte{sigs[0]})
	require.Error(t, err, "mismatched pubkeys and sigs lengths should fail")

	{
		pubKeys2 := slices.Clone(pubKeys)
		pubKey3, _ := s.signerTestSubject(s.T())
		pubKeys2[0] = pubKey3
		wrongKeyAggregator, err := s.verifier.Aggregate(pubKeys2)
		require.NoError(t, err)

		require.Error(t, wrongKeyAggregator.VerifyAggregate(mask, msg, aggSig), "wrong pubkey should error")
	}

	t.Run("mask out of range", func(t *testing.T) {
		_, err = aggregator.Aggregate([]int{0, 3}, [][]byte{sigs[0]})
		require.Error(t, err, "mask out of range")
	})

	t.Run("empty signature is always valid", func(t *testing.T) {
		sig, err := aggregator.Aggregate([]int{}, [][]byte{})
		require.NoError(t, err)

		err = aggregator.VerifyAggregate([]int{}, []byte("anything"), sig)
		require.NoError(t, err)
	})
}
