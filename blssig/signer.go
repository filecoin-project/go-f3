package blssig

import (
	"bytes"
	"errors"

	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/sign"

	// we use it only for signing, verification is rogue key safe
	"github.com/drand/kyber/sign/bls" //nolint:staticcheck
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ gpbft.Signer = (*Signer)(nil)

type Signer struct {
	scheme  sign.AggregatableScheme
	pubKey  gpbft.PubKey
	privKey kyber.Scalar
}

func SignerWithKeyOnG2(pub gpbft.PubKey, privKey kyber.Scalar) *Signer {
	return &Signer{
		scheme:  bls.NewSchemeOnG1(bls12381.NewBLS12381Suite()),
		pubKey:  pub,
		privKey: privKey,
	}
}

func (s *Signer) Sign(sender gpbft.PubKey, msg []byte) ([]byte, error) {
	if !bytes.Equal(sender, s.pubKey) {
		return nil, errors.New("cannot sign: unknown sender")
	}
	return s.scheme.Sign(s.privKey, msg)
}
