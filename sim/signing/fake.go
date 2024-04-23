package signing

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Backend = (*FakeBackend)(nil)

type FakeBackend struct {
	i        int
	keyPairs map[string]string
}

func NewFakeBackend() *FakeBackend {
	return &FakeBackend{
		keyPairs: make(map[string]string),
	}
}

func (s *FakeBackend) GenerateKey() (gpbft.PubKey, any) {
	pubKey := gpbft.PubKey(fmt.Sprintf("pubkey:%08x", s.i))
	privKey := fmt.Sprintf("privkey:%08x", s.i)
	s.keyPairs[string(pubKey)] = privKey
	s.i++
	return pubKey, privKey
}

func (s *FakeBackend) Sign(signer gpbft.PubKey, msg []byte) ([]byte, error) {
	return s.generateSignature(signer, msg)
}

func (s *FakeBackend) generateSignature(signer gpbft.PubKey, msg []byte) ([]byte, error) {
	priv, known := s.keyPairs[string(signer)]
	if !known {
		return nil, errors.New("unknown signer")
	}
	hasher := sha256.New()
	hasher.Write(signer)
	hasher.Write([]byte(priv))
	hasher.Write(msg)
	return hasher.Sum(nil), nil
}

func (s *FakeBackend) Verify(signer gpbft.PubKey, msg, sig []byte) error {
	switch wantSig, err := s.generateSignature(signer, msg); {
	case err != nil:
		return fmt.Errorf("cannot verify: %w", err)
	case !bytes.Equal(wantSig, sig):
		return errors.New("signature is not valid")
	default:
		return nil
	}
}

func (_ *FakeBackend) Aggregate(signers []gpbft.PubKey, sigs [][]byte) ([]byte, error) {
	if len(signers) != len(sigs) {
		return nil, errors.New("public keys and signatures length mismatch")
	}
	hasher := sha256.New()
	for i, signer := range signers {
		hasher.Write(signer)
		hasher.Write(sigs[i])
	}
	return hasher.Sum(nil), nil
}

func (s *FakeBackend) VerifyAggregate(payload, aggSig []byte, signers []gpbft.PubKey) error {
	hasher := sha256.New()
	for _, signer := range signers {
		sig, err := s.Sign(signer, payload)
		if err != nil {
			return err
		}
		hasher.Write(signer)
		hasher.Write(sig)
	}
	if !bytes.Equal(aggSig, hasher.Sum(nil)) {
		return errors.New("signature is not valid")
	}
	return nil
}
