package blssig

import (
	"fmt"

	"github.com/filecoin-project/go-f3/f3"

	"github.com/drand/kyber"
	"github.com/drand/kyber/sign"
	"github.com/drand/kyber/sign/bdn"
	"golang.org/x/xerrors"
)

func (v Verifier) Aggregate(pubkeys []f3.PubKey, signatures [][]byte) ([]byte, error) {
	if len(pubkeys) != len(signatures) {
		return nil, xerrors.Errorf("lengths of pubkeys and sigs does not match %d != %d",
			len(pubkeys), len(signatures))
	}

	mask, err := v.pubkeysToMask(pubkeys)
	if err != nil {
		return nil, xerrors.Errorf("converting public keys to mask: %w", err)
	}

	aggSigPoint, err := bdn.AggregateSignatures(v.suite, signatures, mask)
	if err != nil {
		return nil, xerrors.Errorf("computing aggregate signature: %w", err)
	}
	aggSig, err := aggSigPoint.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshaling signature data: %w", err)
	}

	return aggSig, nil
}

func (v Verifier) VerifyAggregate(msg []byte, signature []byte, pubkeys []f3.PubKey) error {
	mask, err := v.pubkeysToMask(pubkeys)
	if err != nil {
		return xerrors.Errorf("converting public keys to mask: %w", err)
	}

	aggPubKey, err := bdn.AggregatePublicKeys(v.suite, mask)
	if err != nil {
		return xerrors.Errorf("aggregating public keys: %w", err)
	}

	return bdn.Verify(v.suite, aggPubKey, msg, signature)
}

func (v Verifier) pubkeysToMask(pubkeys []f3.PubKey) (*sign.Mask, error) {
	kPubkeys := make([]kyber.Point, 0, len(pubkeys))
	for i, p := range pubkeys {
		point := v.keyGroup.Point()
		err := point.UnmarshalBinary(p)
		if err != nil {
			return nil, xerrors.Errorf("unarshalling pubkey at index %d: %w", i, err)
		}
		if point.Equal(v.keyGroup.Point().Null()) {
			return nil, xerrors.Errorf("the public key at %d is a null point", i)
		}

		kPubkeys = append(kPubkeys, point)
	}

	mask, err := sign.NewMask(v.suite, kPubkeys, nil)
	if err != nil {
		return nil, xerrors.Errorf("creating key mask: %w", err)
	}
	for i := range kPubkeys {
		err := mask.SetBit(i, true)
		if err != nil {
			return nil, xerrors.Errorf("setting mask bit %d: %w", i, err)
		}
	}
	return mask, nil
}
