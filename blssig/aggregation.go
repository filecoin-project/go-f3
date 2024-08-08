package blssig

import (
	"fmt"

	"github.com/filecoin-project/go-f3/gpbft"

	"github.com/drand/kyber"
	"github.com/drand/kyber/sign"
)

// Max size of the point cache.
const maxPointCacheSize = 10_000

func (v *Verifier) Aggregate(pubkeys []gpbft.PubKey, signatures [][]byte) ([]byte, error) {
	if len(pubkeys) != len(signatures) {
		return nil, fmt.Errorf("lengths of pubkeys and sigs does not match %d != %d",
			len(pubkeys), len(signatures))
	}

	mask, err := v.pubkeysToMask(pubkeys)
	if err != nil {
		return nil, fmt.Errorf("converting public keys to mask: %w", err)
	}

	aggSigPoint, err := v.scheme.AggregateSignatures(signatures, mask)
	if err != nil {
		return nil, fmt.Errorf("computing aggregate signature: %w", err)
	}
	aggSig, err := aggSigPoint.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshaling signature data: %w", err)
	}

	return aggSig, nil
}

func (v *Verifier) VerifyAggregate(msg []byte, signature []byte, pubkeys []gpbft.PubKey) error {
	mask, err := v.pubkeysToMask(pubkeys)
	if err != nil {
		return fmt.Errorf("converting public keys to mask: %w", err)
	}

	aggPubKey, err := v.scheme.AggregatePublicKeys(mask)
	if err != nil {
		return fmt.Errorf("aggregating public keys: %w", err)
	}

	return v.scheme.Verify(aggPubKey, msg, signature)
}

func (v *Verifier) pubkeysToMask(pubkeys []gpbft.PubKey) (*sign.Mask, error) {
	kPubkeys := make([]kyber.Point, 0, len(pubkeys))
	for i, p := range pubkeys {
		point, err := v.pubkeyToPoint(p)
		if err != nil {
			return nil, fmt.Errorf("pubkey %d: %w", i, err)
		}
		kPubkeys = append(kPubkeys, point.Clone())
	}

	mask, err := sign.NewMask(v.suite, kPubkeys, nil)
	if err != nil {
		return nil, fmt.Errorf("creating key mask: %w", err)
	}
	for i := range kPubkeys {
		err := mask.SetBit(i, true)
		if err != nil {
			return nil, fmt.Errorf("setting mask bit %d: %w", i, err)
		}
	}
	return mask, nil
}
