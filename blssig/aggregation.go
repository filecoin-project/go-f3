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

func (v *Verifier) pubkeyToPoint(p gpbft.PubKey) (kyber.Point, error) {
	if len(p) > 96 {
		return nil, fmt.Errorf("public key is too large: %d > 96", len(p))
	}

	v.mu.RLock()
	point, ok := v.pointCache[string(p)]
	v.mu.RUnlock()
	if ok {
		return point.Clone(), nil
	}

	point = v.keyGroup.Point()
	err := point.UnmarshalBinary(p)
	if err != nil {
		return nil, fmt.Errorf("unarshalling pubkey: %w", err)
	}
	if point.Equal(v.keyGroup.Point().Null()) {
		return nil, fmt.Errorf("public key is a null point")
	}
	v.mu.Lock()

	// Initialize the cache, or re-initialize it if we've grown too big. We don't expect the
	// latter to happen in practice (would need over 10k participants), but better be safe than
	// sorry. We could, alternatively, use an LRU but... that's not worth the overhead for
	// somethign that shouldn't happen.
	if v.pointCache == nil || len(v.pointCache) >= maxPointCacheSize {
		v.pointCache = make(map[string]kyber.Point)
	}

	v.pointCache[string(p)] = point
	v.mu.Unlock()

	return point.Clone(), nil
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
