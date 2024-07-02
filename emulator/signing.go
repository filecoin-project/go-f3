package emulator

import (
	"bytes"
	"errors"
	"hash/crc32"

	"github.com/filecoin-project/go-f3/gpbft"
)

var (
	_ gpbft.Verifier         = (*adhocSigning)(nil)
	_ gpbft.Signer           = (*adhocSigning)(nil)
	_ gpbft.SigningMarshaler = (*adhocSigning)(nil)

	signing adhocSigning
)

// adhocSigning marshals, signs and verifies messages on behalf of any given
// public key but uniquely and deterministically so using crc32 hash function for
// performance. This implementation is not secure nor collision resistant. A
// typical Instance power table is small enough to make the risk of collisions
// negligible.
type adhocSigning struct{}

func (s adhocSigning) Sign(sender gpbft.PubKey, msg []byte) ([]byte, error) {
	hasher := crc32.NewIEEE()
	if _, err := hasher.Write(sender); err != nil {
		return nil, err
	}
	if _, err := hasher.Write(msg); err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func (s adhocSigning) Verify(sender gpbft.PubKey, msg, got []byte) error {
	switch want, err := s.Sign(sender, msg); {
	case err != nil:
		return err
	case !bytes.Equal(want, got):
		return errors.New("invalid signature")
	default:
		return nil
	}
}

func (s adhocSigning) Aggregate(signers []gpbft.PubKey, sigs [][]byte) ([]byte, error) {
	if len(signers) != len(sigs) {
		return nil, errors.New("public keys and signatures length mismatch")
	}
	hasher := crc32.NewIEEE()
	for i, signer := range signers {
		if _, err := hasher.Write(signer); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(sigs[i]); err != nil {
			return nil, err
		}
	}
	return hasher.Sum(nil), nil
}

func (s adhocSigning) VerifyAggregate(payload, got []byte, signers []gpbft.PubKey) error {
	signatures := make([][]byte, len(signers))
	var err error
	for i, signer := range signers {
		signatures[i], err = s.Sign(signer, payload)
		if err != nil {
			return err
		}
	}
	want, err := s.Aggregate(signers, signatures)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return errors.New("invalid aggregate")
	}
	return nil
}

func (s adhocSigning) MarshalPayloadForSigning(name gpbft.NetworkName, payload *gpbft.Payload) []byte {
	return payload.MarshalForSigning(name)
}
