package blssig

import (
	"bytes"
	"context"
	"errors"

	"go.dedis.ch/kyber/v4"
	"go.dedis.ch/kyber/v4/sign/bdn"

	"github.com/filecoin-project/go-f3/gpbft"
	bls12381 "github.com/filecoin-project/go-f3/internal/gnark"
)

var _ gpbft.Signer = (*Signer)(nil)

type Signer struct {
	scheme  *bdn.Scheme
	pubKey  gpbft.PubKey
	privKey kyber.Scalar
}

func SignerWithKeyOnG1(pub gpbft.PubKey, privKey kyber.Scalar) *Signer {
	return &Signer{
		scheme:  bdn.NewSchemeOnG2(bls12381.NewSuiteBLS12381()),
		pubKey:  pub,
		privKey: privKey,
	}
}

func (s *Signer) Sign(_ context.Context, sender gpbft.PubKey, msg []byte) ([]byte, error) {
	if !bytes.Equal(sender, s.pubKey) {
		return nil, errors.New("cannot sign: unknown sender")
	}
	return s.scheme.Sign(s.privKey, msg)
}
