package test

import (
	"context"
	"testing"

	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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
		blsSuit   = bls12381.NewBLS12381Suite()
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

	sigs := make([][]byte, len(pubKeys))
	var err error
	sigs[0], err = signer1.Sign(ctx, pubKey1, msg)
	require.NoError(s.T(), err)
	sigs[1], err = signer2.Sign(ctx, pubKey2, msg)
	require.NoError(s.T(), err)

	aggSig, err := s.verifier.Aggregate(pubKeys, sigs)
	require.NoError(t, err)

	err = s.verifier.VerifyAggregate(msg, aggSig, pubKeys)
	require.NoError(t, err)

	aggSig, err = s.verifier.Aggregate(pubKeys[0:1], sigs[0:1])
	require.NoError(t, err)

	err = s.verifier.VerifyAggregate(msg, aggSig, pubKeys)
	require.Error(t, err)

	aggSig, err = s.verifier.Aggregate(pubKeys, [][]byte{sigs[0], sigs[0]})
	require.NoError(t, err)

	err = s.verifier.VerifyAggregate(msg, aggSig, pubKeys)
	require.Error(t, err)

	err = s.verifier.VerifyAggregate(msg, []byte("bad sig"), pubKeys)
	require.Error(t, err)
}
