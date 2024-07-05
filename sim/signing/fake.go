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
	"golang.org/x/xerrors"
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
	pubKey := gpbft.PubKey(fmt.Sprintf("pubkey::%08x", i))
	s.allowed[string(pubKey)] = struct{}{}
	return pubKey
}

func (s *FakeBackend) Sign(_ context.Context, signer gpbft.PubKey, msg []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.allowed[string(signer)]; !ok {
		return nil, xerrors.Errorf("cannot sign: unknown sender")
	}
	return s.generateSignature(signer, msg)
}

func (s *FakeBackend) generateSignature(signer gpbft.PubKey, msg []byte) ([]byte, error) {
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

func (*FakeBackend) Aggregate(signers []gpbft.PubKey, sigs [][]byte) ([]byte, error) {
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
		sig, err := s.generateSignature(signer, payload)
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

func (v *FakeBackend) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	length := len(gpbft.DOMAIN_SEPARATION_TAG) + 2 + len(nn)
	length += 1 + 8 + 8 // step + round + instance
	length += 4         // len(p.Value)
	for i := range p.Value {
		ts := &p.Value[i]
		length += 8 // epoch
		length += len(ts.Key)
		length += len(ts.Commitments)
		length += len(ts.PowerTable)
	}

	var buf bytes.Buffer
	buf.Grow(length)
	buf.WriteString(gpbft.DOMAIN_SEPARATION_TAG)
	buf.WriteString(":")
	buf.WriteString(string(nn))
	buf.WriteString(":")

	_ = binary.Write(&buf, binary.BigEndian, p.Step)
	_ = binary.Write(&buf, binary.BigEndian, p.Round)
	_ = binary.Write(&buf, binary.BigEndian, p.Instance)
	_, _ = buf.Write(p.SupplementalData.Commitments[:])
	_, _ = buf.Write(p.SupplementalData.PowerTable)
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(p.Value)))
	for i := range p.Value {
		ts := &p.Value[i]

		_ = binary.Write(&buf, binary.BigEndian, ts.Epoch)
		_, _ = buf.Write(ts.Commitments[:])
		_, _ = buf.Write(ts.PowerTable)
		_, _ = buf.Write(ts.Key)
	}
	return buf.Bytes()
}
