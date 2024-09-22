package blssig

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime/debug"

	"go.dedis.ch/kyber/v4"
	"go.opentelemetry.io/otel/metric"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/bls/bdn"
	"github.com/filecoin-project/go-f3/internal/measurements"
)

// Max size of the point cache.
const maxPointCacheSize = 10_000

func (v *Verifier) Aggregate(pubkeys []gpbft.PubKey, signatures [][]byte) (_agg []byte, _err error) {
	defer func() {
		status := measurements.AttrStatusSuccess
		if _err != nil {
			status = measurements.AttrStatusError
		}

		if perr := recover(); perr != nil {
			_err = fmt.Errorf("panicked aggregating public keys: %v\n%s",
				perr, string(debug.Stack()))
			log.Error(_err)
			status = measurements.AttrStatusPanic
		}

		metrics.aggregate.Record(
			context.TODO(), int64(len(pubkeys)),
			metric.WithAttributes(status),
		)
	}()

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

func (v *Verifier) VerifyAggregate(msg []byte, signature []byte, pubkeys []gpbft.PubKey) (_err error) {
	defer func() {
		status := measurements.AttrStatusSuccess
		if _err != nil {
			status = measurements.AttrStatusError
		}

		if perr := recover(); perr != nil {
			msgStr := base64.StdEncoding.EncodeToString(msg)
			_err = fmt.Errorf("panicked verifying aggregate signature of %q: %v\n%s",
				msgStr, perr, string(debug.Stack()))
			log.Error(_err)
			status = measurements.AttrStatusPanic
		}

		metrics.verifyAggregate.Record(
			context.TODO(), int64(len(pubkeys)),
			metric.WithAttributes(status),
		)
	}()

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

func (v *Verifier) pubkeysToMask(pubkeys []gpbft.PubKey) (*bdn.Mask, error) {
	kPubkeys := make([]kyber.Point, 0, len(pubkeys))
	for i, p := range pubkeys {
		point, err := v.pubkeyToPoint(p)
		if err != nil {
			return nil, fmt.Errorf("pubkey %d: %w", i, err)
		}
		kPubkeys = append(kPubkeys, point.Clone())
	}

	mask, err := bdn.NewMask(v.keyGroup, kPubkeys, nil)
	if err != nil {
		return nil, fmt.Errorf("creating bdn mask: %w", err)
	}
	for i := range kPubkeys {
		err := mask.SetBit(i, true)
		if err != nil {
			return nil, fmt.Errorf("setting mask bit %d: %w", i, err)
		}
	}
	return mask, nil
}
