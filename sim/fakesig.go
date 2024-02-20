package sim

import (
	"bytes"
	"fmt"

	"github.com/filecoin-project/go-f3/gpbft"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/xerrors"
)

type FakeSigner struct {
	i int
}

var _ gpbft.Signer = (*FakeSigner)(nil)
var _ gpbft.Verifier = (*FakeSigner)(nil)

func (s *FakeSigner) GenerateKey() gpbft.PubKey {
	pubKey := gpbft.PubKey(fmt.Sprintf("pubkey:%08x", s.i))
	s.i++
	return pubKey
}

func (_ *FakeSigner) Sign(signer gpbft.PubKey, msg []byte) ([]byte, error) {
	hash, _ := blake2b.New256(nil)
	hash.Write(signer)
	hash.Write(msg)

	return hash.Sum(nil), nil
}

func (_ *FakeSigner) Verify(signer gpbft.PubKey, msg, sig []byte) error {
	hash, _ := blake2b.New256(nil)
	hash.Write(signer)
	hash.Write(msg)

	if !bytes.Equal(hash.Sum(nil), sig) {
		return xerrors.Errorf("signature miss-match: %x != %x", hash.Sum(nil), sig)
	}
	return nil
}

func (_ *FakeSigner) Aggregate(pubKeys []gpbft.PubKey, sigs [][]byte) ([]byte, error) {

	// Fake implementation.
	hash, _ := blake2b.New256(nil)
	for i, s := range sigs {
		hash.Write(pubKeys[i])
		hash.Write(s)
	}

	return hash.Sum(nil), nil
}

func (_ *FakeSigner) VerifyAggregate(payload, aggSig []byte, signers []gpbft.PubKey) error {
	aggHash, _ := blake2b.New256(nil)
	sigHash, _ := blake2b.New256(nil)
	sumBuf := make([]byte, 0, blake2b.Size256)
	for _, signer := range signers {
		sigHash.Reset()
		sumBuf := sumBuf[:0]

		sigHash.Write(signer)
		sigHash.Write(payload)
		sig := sigHash.Sum(sumBuf)

		aggHash.Write(signer)
		aggHash.Write(sig)
	}
	if !bytes.Equal(aggSig, aggHash.Sum(nil)) {
		return xerrors.Errorf("aggregate signature miss-match")
	}

	return nil
}
