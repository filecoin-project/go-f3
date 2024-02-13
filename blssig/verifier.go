package blssig

import (
	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/f3"
	"golang.org/x/xerrors"
)

type Verifier struct {
	suite    pairing.Suite
	keyGroup kyber.Group
}

func VerifierWithKeyOnG2() Verifier {
	suite := bls12381.NewBLS12381Suite()
	return Verifier{
		suite:    suite,
		keyGroup: suite.G2(),
	}
}

func (v Verifier) Verify(pubKey f3.PubKey, msg, sig []byte) error {
	// rejecting zero point pubkeys?
	pubKeyPoint := v.keyGroup.Point()
	err := pubKeyPoint.UnmarshalBinary(pubKey)
	if err != nil {
		return xerrors.Errorf("unarshalling public key: %w", err)
	}
	if pubKeyPoint.Equal(v.keyGroup.Point().Null()) {
		return xerrors.Errorf("the public key is a null point")
	}

	return bdn.Verify(v.suite, pubKeyPoint, msg, sig)
}
