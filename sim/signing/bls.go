package signing

import (
	"context"
	"errors"
	"sync"

	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Backend = (*BLSBackend)(nil)

type BLSBackend struct {
	gpbft.Verifier
	suite  pairing.Suite
	scheme *bdn.Scheme

	// signersMutex guards access to signersByPubKey.
	signersMutex    sync.RWMutex
	signersByPubKey map[gpbft.PubKey]*blssig.Signer
}

func (b *BLSBackend) Sign(ctx context.Context, sender gpbft.PubKey, msg []byte) ([]byte, error) {
	b.signersMutex.RLock()
	signer, known := b.signersByPubKey[sender]
	b.signersMutex.RUnlock()

	if !known {
		return nil, errors.New("cannot sign: unknown sender")
	}
	return signer.Sign(ctx, sender, msg)
}

func NewBLSBackend() *BLSBackend {
	suite := bls12381.NewBLS12381Suite()
	return &BLSBackend{
		Verifier:        blssig.VerifierWithKeyOnG1(),
		signersByPubKey: make(map[gpbft.PubKey]*blssig.Signer),
		suite:           suite,
		scheme:          bdn.NewSchemeOnG2(suite),
	}
}

func (b *BLSBackend) GenerateKey() (gpbft.PubKey, any) {

	priv, pub := b.scheme.NewKeyPair(b.suite.RandomStream())
	pubKeyB, err := pub.MarshalBinary()
	if err != nil {
		panic(err)
	}
	var pubKey gpbft.PubKey
	copy(pubKey[:], pubKeyB)
	b.signersMutex.Lock()
	defer b.signersMutex.Unlock()
	b.signersByPubKey[pubKey] = blssig.SignerWithKeyOnG1(pubKey, priv)
	return pubKey, priv
}

func (b *BLSBackend) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	return p.MarshalForSigning(nn)
}
