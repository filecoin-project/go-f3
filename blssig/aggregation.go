package blssig

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime/debug"

	"go.dedis.ch/kyber/v4"
	"go.dedis.ch/kyber/v4/sign/bdn"
	"go.opentelemetry.io/otel/metric"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/measurements"
)

// Max size of the point cache.
const maxPointCacheSize = 10_000

type aggregation struct {
	mask   *bdn.Mask
	scheme *bdn.Scheme
}

func (a *aggregation) Aggregate(mask []int, signatures [][]byte) (_agg []byte, _err error) {
	defer func() {
		status := measurements.AttrStatusSuccess
		if _err != nil {
			status = measurements.AttrStatusError
		}

		if perr := recover(); perr != nil {
			_err = fmt.Errorf("panicked aggregating signatures: %v\n%s",
				perr, string(debug.Stack()))
			log.Error(_err)
			status = measurements.AttrStatusPanic
		}

		metrics.aggregate.Record(
			context.TODO(), int64(len(mask)),
			metric.WithAttributes(status),
		)
	}()

	if len(mask) != len(signatures) {
		return nil, fmt.Errorf("lengths of pubkeys and sigs does not match %d != %d",
			len(mask), len(signatures))
	}

	bdnMask := a.mask.Clone()
	for _, bit := range mask {
		if err := bdnMask.SetBit(bit, true); err != nil {
			return nil, err
		}
	}

	aggSigPoint, err := a.scheme.AggregateSignatures(signatures, bdnMask)
	if err != nil {
		return nil, fmt.Errorf("computing aggregate signature: %w", err)
	}
	aggSig, err := aggSigPoint.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshaling signature data: %w", err)
	}

	return aggSig, nil
}

func (a *aggregation) VerifyAggregate(mask []int, msg []byte, signature []byte) (_err error) {
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
			context.TODO(), int64(len(mask)),
			metric.WithAttributes(status),
		)
	}()

	bdnMask := a.mask.Clone()
	for _, bit := range mask {
		if err := bdnMask.SetBit(bit, true); err != nil {
			return err
		}
	}

	aggPubKey, err := a.scheme.AggregatePublicKeys(bdnMask)
	if err != nil {
		return fmt.Errorf("aggregating public keys: %w", err)
	}

	return a.scheme.Verify(aggPubKey, msg, signature)
}

func (v *Verifier) Aggregate(pubkeys []gpbft.PubKey) (_agg gpbft.Aggregate, _err error) {
	defer func() {
		if perr := recover(); perr != nil {
			_err = fmt.Errorf("panicked aggregating public keys: %v\n%s",
				perr, string(debug.Stack()))
			log.Error(_err)
		}
	}()

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
		return nil, fmt.Errorf("creating key mask: %w", err)
	}
	return &aggregation{
		mask:   mask,
		scheme: v.scheme,
	}, nil
}
