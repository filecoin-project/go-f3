package sim

import (
	"errors"

	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ SigningBacked = (*BLSSigningBackend)(nil)

type BLSSigningBackend struct {
	gpbft.Verifier
	signersByPubKey map[string]*blssig.Signer
	suite           pairing.Suite
	scheme          *bdn.Scheme
}

func (b *BLSSigningBackend) Sign(sender gpbft.PubKey, msg []byte) ([]byte, error) {
	signer, known := b.signersByPubKey[string(sender)]
	if !known {
		return nil, errors.New("cannot sign: unknown sender")
	}
	return signer.Sign(sender, msg)
}

func NewBLSSigningBackend() *BLSSigningBackend {
	suite := bls12381.NewBLS12381Suite()
	return &BLSSigningBackend{
		Verifier:        blssig.VerifierWithKeyOnG2(),
		signersByPubKey: make(map[string]*blssig.Signer),
		suite:           suite,
		scheme:          bdn.NewSchemeOnG1(suite),
	}
}

func (b *BLSSigningBackend) GenerateKey() (gpbft.PubKey, any) {
	priv, pub := b.scheme.NewKeyPair(b.suite.RandomStream())
	pubKeyB, err := pub.MarshalBinary()
	if err != nil {
		panic(err)
	}
	b.signersByPubKey[string(pubKeyB)] = blssig.SignerWithKeyOnG2(pubKeyB, priv)
	return pubKeyB, priv
}
