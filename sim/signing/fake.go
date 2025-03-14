package signing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Backend = (*FakeBackend)(nil)

type FakeBackend struct {
	// mu Guards both i and allowed map access.
	mu      sync.RWMutex
	i       int
	allowed map[string]struct{}
}

func NewFakeBackend() *FakeBackend {
	return &FakeBackend{allowed: make(map[string]struct{})}
}

func (s *FakeBackend) GenerateKey() (gpbft.PubKey, any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pubKey := gpbft.PubKey(fmt.Sprintf("pubkey::%08x", s.i))
	privKey := fmt.Sprintf("privkey:%08x", s.i)
	s.allowed[string(pubKey)] = struct{}{}
	s.i++
	return pubKey, privKey
}

func (s *FakeBackend) Allow(i int) gpbft.PubKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	pubKey := gpbft.PubKey(fmt.Sprintf("pubkey::%08x", i))
	s.allowed[string(pubKey)] = struct{}{}
	return pubKey
}

func (s *FakeBackend) Sign(_ context.Context, signer gpbft.PubKey, msg []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.allowed[string(signer)]; !ok {
		return nil, fmt.Errorf("cannot sign: unknown sender")
	}
	return s.generateSignature(signer, msg)
}

func (s *FakeBackend) generateSignature(signer gpbft.PubKey, msg []byte) ([]byte, error) {
	if len(signer) != 16 {
		return nil, fmt.Errorf("wrong signer pubkey length: %d != 16", len(signer))
	}
	priv := "privkey" + string(signer[7:])

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

func (s *FakeBackend) Aggregate(keys []gpbft.PubKey) (gpbft.Aggregate, error) {
	for i, signer := range keys {
		if len(signer) != 16 {
			return nil, fmt.Errorf("wrong signer %d pubkey length: %d != 16", i, len(signer))
		}
	}

	return &fakeAggregate{
		keys:    keys,
		backend: s,
	}, nil

}

func (v *FakeBackend) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	//TODO: remove this whole facility
	return p.MarshalForSigning(nn)
}

type fakeAggregate struct {
	keys    []gpbft.PubKey
	backend *FakeBackend
}

// Aggregate implements gpbft.Aggregate.
func (f *fakeAggregate) Aggregate(signerMask []int, sigs [][]byte) ([]byte, error) {
	if len(signerMask) != len(sigs) {
		return nil, errors.New("public keys and signatures length mismatch")
	}
	hasher := sha256.New()
	for i, bit := range signerMask {
		if bit >= len(f.keys) {
			return nil, fmt.Errorf("signer %d out of range", bit)
		}
		_ = binary.Write(hasher, binary.BigEndian, int64(bit))
		hasher.Write(f.keys[bit])
		hasher.Write(sigs[i])
	}
	return hasher.Sum(nil), nil
}

// VerifyAggregate implements gpbft.Aggregate.
func (f *fakeAggregate) VerifyAggregate(signerMask []int, payload []byte, aggSig []byte) error {
	hasher := sha256.New()
	for _, bit := range signerMask {
		signer := f.keys[bit]
		sig, err := f.backend.generateSignature(signer, payload)
		if err != nil {
			return err
		}
		_ = binary.Write(hasher, binary.BigEndian, int64(bit))
		hasher.Write(signer)
		hasher.Write(sig)
	}
	if !bytes.Equal(aggSig, hasher.Sum(nil)) {
		return errors.New("signature is not valid")
	}
	return nil
}
