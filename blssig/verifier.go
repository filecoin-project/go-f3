package blssig

import (
	"fmt"
	"sync"

	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/gpbft"
)

type Verifier struct {
	suite    pairing.Suite
	scheme   *bdn.Scheme
	keyGroup kyber.Group

	mu         sync.RWMutex
	pointCache map[string]kyber.Point
}

func VerifierWithKeyOnG1() *Verifier {
	suite := bls12381.NewBLS12381Suite()
	return &Verifier{
		suite:    suite,
		scheme:   bdn.NewSchemeOnG2(suite),
		keyGroup: suite.G1(),
	}
}

func (v *Verifier) Verify(pubKey gpbft.PubKey, msg, sig []byte) error {
	point, err := v.pubkeyToPoint(pubKey)
	if err != nil {
		return fmt.Errorf("unarshalling public key: %w", err)
	}

	return v.scheme.Verify(point, msg, sig)
}
