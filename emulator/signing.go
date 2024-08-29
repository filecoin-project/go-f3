package emulator

import (
	"bytes"
	"context"
	"encoding/binary"
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

func (s adhocSigning) Sign(_ context.Context, sender gpbft.PubKey, msg []byte) ([]byte, error) {
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
	switch want, err := s.Sign(context.Background(), sender, msg); {
	case err != nil:
		return err
	case !bytes.Equal(want, got):
		return errors.New("invalid signature")
	default:
		return nil
	}
}

type aggregate struct {
	keys    []gpbft.PubKey
	signing adhocSigning
}

// Aggregate implements gpbft.Aggregate.
func (a *aggregate) Aggregate(signerMask []int, sigs [][]byte) ([]byte, error) {
	if len(signerMask) != len(sigs) {
		return nil, errors.New("public keys and signatures length mismatch")
	}
	hasher := crc32.NewIEEE()
	for i, bit := range signerMask {
		if err := binary.Write(hasher, binary.BigEndian, uint64(bit)); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(a.keys[bit]); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(sigs[i]); err != nil {
			return nil, err
		}
	}
	return hasher.Sum(nil), nil
}

// VerifyAggregate implements gpbft.Aggregate.
func (a *aggregate) VerifyAggregate(signerMask []int, payload []byte, got []byte) error {
	signatures := make([][]byte, len(signerMask))
	var err error
	for i, bit := range signerMask {
		signatures[i], err = a.signing.Sign(context.Background(), a.keys[bit], payload)
		if err != nil {
			return err
		}
	}
	want, err := a.Aggregate(signerMask, signatures)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return errors.New("invalid aggregate")
	}
	return nil
}

func (s adhocSigning) Aggregate(keys []gpbft.PubKey) (gpbft.Aggregate, error) {
	return &aggregate{keys: keys,
		signing: s,
	}, nil
}

func (s adhocSigning) MarshalPayloadForSigning(name gpbft.NetworkName, payload *gpbft.Payload) []byte {
	return payload.MarshalForSigning(name)
}
