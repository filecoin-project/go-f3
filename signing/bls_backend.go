package signing

import (
	"errors"
	"sync"

	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Backend = (*BLSBackend)(nil)

type BLSBackend struct {
	gpbft.Verifier
	suite  pairing.Suite
	scheme *bdn.Scheme

	// signersMutex guards access to signersByPubKey.
	signersMutex    sync.RWMutex
	signersByPubKey map[string]*Signer
}

func (b *BLSBackend) Sign(sender gpbft.PubKey, msg []byte) ([]byte, error) {
	b.signersMutex.RLock()
	signer, known := b.signersByPubKey[string(sender)]
	b.signersMutex.RUnlock()

	if !known {
		return nil, errors.New("cannot sign: unknown sender")
	}
	return signer.Sign(sender, msg)
}

func NewBLSBackend() *BLSBackend {
	suite := bls12381.NewBLS12381Suite()
	return &BLSBackend{
		Verifier:        VerifierWithKeyOnG1(),
		signersByPubKey: make(map[string]*Signer),
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

	b.signersMutex.Lock()
	defer b.signersMutex.Unlock()
	b.signersByPubKey[string(pubKeyB)] = SignerWithKeyOnG1(pubKeyB, priv)
	return pubKeyB, priv
}

func (b *BLSBackend) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	return p.MarshalForSigning(nn)
}
