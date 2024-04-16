package blssig

import (
	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/gpbft"
	"golang.org/x/xerrors"
)

type Verifier struct {
	suite    pairing.Suite
	scheme   *bdn.Scheme
	keyGroup kyber.Group
}

func VerifierWithKeyOnG1() Verifier {
	suite := bls12381.NewBLS12381Suite()
	return Verifier{
		suite:    suite,
		scheme:   bdn.NewSchemeOnG2(suite),
		keyGroup: suite.G1(),
	}
}

func (v Verifier) Verify(pubKey gpbft.PubKey, msg, sig []byte) error {
	pubKeyPoint := v.keyGroup.Point()
	err := pubKeyPoint.UnmarshalBinary(pubKey)
	if err != nil {
		return xerrors.Errorf("unarshalling public key: %w", err)
	}
	if pubKeyPoint.Equal(v.keyGroup.Point().Null()) {
		return xerrors.Errorf("the public key is a null point")
	}

	return v.scheme.Verify(pubKeyPoint, msg, sig)
}
