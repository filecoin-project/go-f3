package gpbft

import (
	"errors"
	"time"
)

var (
	defaultDelta                = 3 * time.Second
	defaultDeltaBackOffExponent = 2.0
)

// Option represents a configurable parameter.
type Option func(*options) error

type options struct {
	delta                time.Duration
	deltaBackOffExponent float64
	// tracer traces logic logs for debugging and simulation purposes.
	tracer          Tracer
	initialInstance uint64
}

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		delta:                defaultDelta,
		deltaBackOffExponent: defaultDeltaBackOffExponent,
	}
	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

// WithDelta sets the expected bound on message propagation latency. Defaults to
// 3 seconds if unspecified. Delta must be larger than zero.
//
// The default of 3 seconds for the value of Dela is based previous observations of
// the upper bound on the GossipSub network-wide propagation time in Filecoin network.
//
// See: https://github.com/filecoin-project/FIPs/blob/master/FIPS/fip-0086.md#synchronization-of-participants-in-the-current-instance
func WithDelta(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.New("delta duration cannot be less than zero")
		}
		o.delta = d
		return nil
	}
}

// WithDeltaBackOffExponent sets the delta back-off exponent for each round.
// Defaults to 1.3 if unspecified. It must be larger than zero.
//
// See: https://github.com/filecoin-project/FIPs/blob/master/FIPS/fip-0086.md#synchronization-of-participants-in-the-current-instance
func WithDeltaBackOffExponent(e float64) Option {
	return func(o *options) error {
		if e < 0 {
			return errors.New("delta backoff exponent cannot be less than zero")
		}
		o.deltaBackOffExponent = e
		return nil
	}
}

// WithInitialInstance sets the first instance number. Defaults to zero if
// unspecified.
func WithInitialInstance(i uint64) Option {
	return func(o *options) error {
		o.initialInstance = i
		return nil
	}
}

// WithTracer sets the Tracer for this gPBFT instance, which receives diagnostic
// logs about the state mutation. Defaults to no tracer if unspecified.
func WithTracer(t Tracer) Option {
	return func(o *options) error {
		o.tracer = t
		return nil
	}
}
