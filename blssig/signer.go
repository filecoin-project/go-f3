package blssig

import (
	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign"

	// we use it only for signing, verification is rogue key safe
	"github.com/drand/kyber/sign/bls" //nolint:staticcheck
	"github.com/filecoin-project/go-f3/f3"
	"golang.org/x/xerrors"
)

type Signer struct {
	suite    pairing.Suite
	scheme   sign.AggregatableScheme
	keyGroup kyber.Group

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

func (s *Signer) GenerateKey() f3.PubKey {
	priv, pub := s.scheme.NewKeyPair(s.suite.RandomStream())
	pubKeyB, err := pub.MarshalBinary()
	if err != nil {
		panic(err)
	}
	s.keys[string(pubKeyB)] = priv

	return f3.PubKey(pubKeyB)
}

var _ f3.Signer = (*Signer)(nil)

func (s *Signer) Sign(sender f3.PubKey, msg []byte) ([]byte, error) {
	priv, ok := s.keys[string(sender)]
	if !ok {
		return nil, xerrors.Errorf("could not find key for %X", string(sender))
	}
	return s.scheme.Sign(priv, msg)

}
