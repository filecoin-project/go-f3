package blssig

import (
	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
)

type Signer struct {
	suite    pairing.Suite
	keyGroup kyber.Group
}

func SignerWithKeyOnG2() Verifier {
	suite := bls12381.NewBLS12381Suite()
	return Verifier{
		suite:    suite,
		keyGroup: suite.G2(),
	}
}
