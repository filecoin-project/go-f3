package blssig

import (
	"fmt"
	"sync"

	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/gpbft"
)

type Verifier struct {
	suite    pairing.Suite
	scheme   *bdn.Scheme
	keyGroup kyber.Group

	mu         sync.RWMutex
	pointCache map[string]kyber.Point
}

func VerifierWithKeyOnG1() *Verifier {
	suite := bls12381.NewBLS12381Suite()
	return &Verifier{
		suite:    suite,
		scheme:   bdn.NewSchemeOnG2(suite),
		keyGroup: suite.G1(),
	}
}

func (v *Verifier) pubkeyToPoint(p gpbft.PubKey) (kyber.Point, error) {
	if len(p) != 48 {
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
	// something that shouldn't happen.
	if v.pointCache == nil || len(v.pointCache) >= maxPointCacheSize {
		v.pointCache = make(map[string]kyber.Point)
	}

	v.pointCache[string(p)] = point
	v.mu.Unlock()

	return point.Clone(), nil
}

func (v *Verifier) Verify(pubKey gpbft.PubKey, msg, sig []byte) error {
	point, err := v.pubkeyToPoint(pubKey)
	if err != nil {
		return fmt.Errorf("unarshalling public key: %w", err)
	}

	return v.scheme.Verify(point, msg, sig)
}
