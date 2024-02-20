package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

type SigningSuite struct {
	signer   gpbft.Signer
	verifier gpbft.Verifier
}

func NewSigningSuite(signer gpbft.Signer, verifier gpbft.Verifier) *SigningSuite {
	return &SigningSuite{
		signer:   signer,
		verifier: verifier,
	}
}

func (s *SigningSuite) TestSignAndVerify(t *testing.T) {
	pubKey := s.signer.GenerateKey()
	msg := []byte("test message")
	sig, err := s.signer.Sign(pubKey, msg)
	require.NoError(t, err)

	err = s.verifier.Verify(pubKey, msg, sig)
	require.NoError(t, err)

	pubKey2 := s.signer.GenerateKey()
	err = s.verifier.Verify(pubKey2, msg, sig)
	require.Error(t, err)

	err = s.verifier.Verify(pubKey, msg, nil)
	require.Error(t, err)

	err = s.verifier.Verify(pubKey, msg, []byte("short sig"))
	require.Error(t, err)

	sig2, err := s.signer.Sign(pubKey2, msg)
	require.NoError(t, err)

	err = s.verifier.Verify(pubKey, msg, sig2)
	require.Error(t, err)
}

func (s *SigningSuite) TestAggregateAndVerify(t *testing.T) {
	msg := []byte("test message")
	pubKeys := []gpbft.PubKey{s.signer.GenerateKey(), s.signer.GenerateKey()}
	sigs := make([][]byte, len(pubKeys))
	for i, pubKey := range pubKeys {
		sig, err := s.signer.Sign(pubKey, msg)
		require.NoError(t, err)
		sigs[i] = sig
	}

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

func (s *SigningSuite) Run(t *testing.T) {
	t.Run("TestSignAndVerify", s.TestSignAndVerify)
	t.Run("TestAggregateAndVerify", s.TestAggregateAndVerify)
}
