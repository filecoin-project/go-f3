package signing_test

import (
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/signing"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SigningTestSuite struct {
	suite.Suite
	subject signing.Backend
}

func NewSigningTestSuite(signer signing.Backend) *SigningTestSuite {
	return &SigningTestSuite{
		subject: signer,
	}
}

func (s *SigningTestSuite) TestSignAndVerify() {
	t := s.Suite.T()
	pubKey, _ := s.subject.GenerateKey()
	msg := []byte("test message")
	sig, err := s.subject.Sign(pubKey, msg)
	require.NoError(t, err)

	err = s.subject.Verify(pubKey, msg, sig)
	require.NoError(t, err)

	pubKey2, _ := s.subject.GenerateKey()
	err = s.subject.Verify(pubKey2, msg, sig)
	require.Error(t, err)

	err = s.subject.Verify(pubKey, msg, nil)
	require.Error(t, err)

	err = s.subject.Verify(pubKey, msg, []byte("short sig"))
	require.Error(t, err)

	sig2, err := s.subject.Sign(pubKey2, msg)
	require.NoError(t, err)

	err = s.subject.Verify(pubKey, msg, sig2)
	require.Error(t, err)
}

func (s *SigningTestSuite) TestAggregateAndVerify() {
	t := s.Suite.T()
	msg := []byte("test message")
	pubKey1, _ := s.subject.GenerateKey()
	pubKey2, _ := s.subject.GenerateKey()
	pubKeys := []gpbft.PubKey{pubKey1, pubKey2}

	sigs := make([][]byte, len(pubKeys))
	var err error
	sigs[0], err = s.subject.Sign(pubKey1, msg)
	require.NoError(s.T(), err)
	sigs[1], err = s.subject.Sign(pubKey2, msg)
	require.NoError(s.T(), err)

	aggSig, err := s.subject.Aggregate(pubKeys, sigs)
	require.NoError(t, err)

	err = s.subject.VerifyAggregate(msg, aggSig, pubKeys)
	require.NoError(t, err)

	aggSig, err = s.subject.Aggregate(pubKeys[0:1], sigs[0:1])
	require.NoError(t, err)

	err = s.subject.VerifyAggregate(msg, aggSig, pubKeys)
	require.Error(t, err)

	aggSig, err = s.subject.Aggregate(pubKeys, [][]byte{sigs[0], sigs[0]})
	require.NoError(t, err)

	err = s.subject.VerifyAggregate(msg, aggSig, pubKeys)
	require.Error(t, err)

	err = s.subject.VerifyAggregate(msg, []byte("bad sig"), pubKeys)
	require.Error(t, err)
}
