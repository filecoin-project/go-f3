package blssig

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime/debug"
	"sync"

	"go.dedis.ch/kyber/v4"
	bls12381 "go.dedis.ch/kyber/v4/pairing/bls12381/kilic"
	"go.dedis.ch/kyber/v4/sign/bdn"
	"go.opentelemetry.io/otel/metric"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/measurements"
)

type Verifier struct {
	scheme   *bdn.Scheme
	keyGroup kyber.Group

	mu         sync.RWMutex
	pointCache map[string]kyber.Point
}

func VerifierWithKeyOnG1() *Verifier {
	suite := bls12381.NewBLS12381Suite()
	return &Verifier{
		scheme:   bdn.NewSchemeOnG2(suite),
		keyGroup: suite.G1(),
	}
}

func (v *Verifier) pubkeyToPoint(p gpbft.PubKey) (kyber.Point, error) {
	if len(p) != 48 {
		return nil, fmt.Errorf("public key is too large: %d > 96", len(p))
	}

	var point kyber.Point
	cached := true
	defer func() {
		metrics.decompressPoint.Add(context.TODO(), 1, metric.WithAttributes(attrCached.Bool(cached)))
	}()

	v.mu.RLock()
	point, cached = v.pointCache[string(p)]
	v.mu.RUnlock()
	if cached {
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

	_, cached = v.pointCache[string(p)] // for accurate metrics
	if !cached {
		v.pointCache[string(p)] = point
	}
	v.mu.Unlock()

	return point.Clone(), nil
}

func (v *Verifier) Verify(pubKey gpbft.PubKey, msg, sig []byte) (_err error) {
	defer func() {
		status := measurements.AttrStatusSuccess
		if _err != nil {
			status = measurements.AttrStatusError
		}
		if perr := recover(); perr != nil {
			msgStr := base64.StdEncoding.EncodeToString(msg)
			sigStr := base64.StdEncoding.EncodeToString(sig)
			pubKeyStr := base64.StdEncoding.EncodeToString(pubKey)
			_err = fmt.Errorf("panicked validating signature %q for message %q from %q: %v\n%s",
				sigStr, msgStr, pubKeyStr, perr, string(debug.Stack()))
			log.Error(_err)
			status = measurements.AttrStatusPanic
		}
		metrics.verify.Add(context.TODO(), 1, metric.WithAttributes(status))
	}()

	point, err := v.pubkeyToPoint(pubKey)
	if err != nil {
		return fmt.Errorf("unarshalling public key: %w", err)
	}

	return v.scheme.Verify(point, msg, sig)
}
