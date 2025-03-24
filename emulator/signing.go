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
	_ Signing = (*adhocSigning)(nil)
	_ Signing = (*erroneousSigning)(nil)
	_ Signing = (*panicSigning)(nil)
)

type Signing interface {
	gpbft.Verifier
	gpbft.Signer
}

// AdhocSigning marshals, signs and verifies messages on behalf of any given
// public key but uniquely and deterministically so using crc32 hash function for
// performance. This implementation is not secure nor collision resistant. A
// typical Instance power table is small enough to make the risk of collisions
// negligible.
func AdhocSigning() Signing { return adhocSigning{} }

// ErroneousSigning returns an error for every Signing API that can return an error.
func ErroneousSigning() Signing { return erroneousSigning{} }

// PanicSigning panics for every Signing API.
func PanicSigning() Signing { return panicSigning{} }

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

type erroneousSigning struct{}
type erroneousAggregate struct{}

func (p erroneousSigning) Verify(gpbft.PubKey, []byte, []byte) error {
	return errors.New("err Verify")
}

func (p erroneousAggregate) VerifyAggregate([]int, []byte, []byte) error {
	return errors.New("err VerifyAggregate")
}

func (p erroneousAggregate) Aggregate([]int, [][]byte) ([]byte, error) {
	return nil, errors.New("err Aggregate")
}

func (p erroneousSigning) Aggregate([]gpbft.PubKey) (gpbft.Aggregate, error) {
	return erroneousAggregate{}, nil
}
func (p erroneousSigning) Sign(context.Context, gpbft.PubKey, []byte) ([]byte, error) {
	return nil, errors.New("err Sign")
}

func (p erroneousSigning) MarshalPayloadForSigning(gpbft.NetworkName, *gpbft.Payload) []byte {
	return nil
}

type panicSigning struct{}
type panicAggregate struct{}

func (p panicSigning) Verify(gpbft.PubKey, []byte, []byte) error                         { panic("π") }
func (p panicSigning) VerifyAggregate([]byte, []byte, []gpbft.PubKey) error              { panic("π") }
func (p panicSigning) Sign(context.Context, gpbft.PubKey, []byte) ([]byte, error)        { panic("π") }
func (p panicSigning) MarshalPayloadForSigning(gpbft.NetworkName, *gpbft.Payload) []byte { panic("π") }

func (p panicSigning) Aggregate([]gpbft.PubKey) (gpbft.Aggregate, error) {
	return panicAggregate{}, nil
}

func (p panicAggregate) VerifyAggregate([]int, []byte, []byte) error { panic("π") }
func (p panicAggregate) Aggregate([]int, [][]byte) ([]byte, error)   { panic("π") }
