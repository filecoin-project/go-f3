package blssig

import (
	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign"

	// we use it only for signing, verification is rogue key safe
	"github.com/drand/kyber/sign/bls" //nolint:staticcheck
	"github.com/filecoin-project/go-f3/gpbft"
	"golang.org/x/xerrors"
)

type Signer struct {
	suite    pairing.Suite
	scheme   sign.AggregatableScheme
	keyGroup kyber.Group

	// Maps public keys to private keys.
	keys map[string]kyber.Scalar
}

func SignerWithKeyOnG2() *Signer {
	suite := bls12381.NewBLS12381Suite()
	return &Signer{
		suite:    suite,
		scheme:   bls.NewSchemeOnG1(suite),
		keyGroup: suite.G2(),
		keys:     make(map[string]kyber.Scalar),
	}
}

func (s *Signer) GenerateKey() gpbft.PubKey {
	priv, pub := s.scheme.NewKeyPair(s.suite.RandomStream())
	pubKeyB, err := pub.MarshalBinary()
	if err != nil {
		panic(err)
	}
	s.keys[string(pubKeyB)] = priv

	return gpbft.PubKey(pubKeyB)
}

var _ gpbft.Signer = (*Signer)(nil)

func (s *Signer) Sign(sender gpbft.PubKey, msg []byte) ([]byte, error) {
	priv, ok := s.keys[string(sender)]
	if !ok {
		return nil, xerrors.Errorf("could not find key for %X", string(sender))
	}
	return s.scheme.Sign(priv, msg)

}
