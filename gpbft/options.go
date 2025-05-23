package gpbft

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	defaultDelta                        = 3 * time.Second
	defaultDeltaBackOffExponent         = 2.0
	defaultMaxCachedInstances           = 10
	defaultMaxCachedMessagesPerInstance = 25_000
	defaultCommitteeLookback            = 10
)

// Option represents a configurable parameter.
type Option func(*options) error

type options struct {
	delta                time.Duration
	deltaBackOffExponent float64

	qualityDeltaMulti float64

	committeeLookback                uint64
	maxLookaheadRounds               uint64
	rebroadcastAfter                 func(int) time.Duration
	rebroadcastImmediatelyAfterRound uint64

	maxCachedInstances           int
	maxCachedMessagesPerInstance int

	// tracer traces logic logs for debugging and simulation purposes.
	tracer Tracer
}

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		delta:                            defaultDelta,
		deltaBackOffExponent:             defaultDeltaBackOffExponent,
		qualityDeltaMulti:                1.0,
		committeeLookback:                defaultCommitteeLookback,
		rebroadcastAfter:                 defaultRebroadcastAfter,
		rebroadcastImmediatelyAfterRound: 3,
		maxCachedInstances:               defaultMaxCachedInstances,
		maxCachedMessagesPerInstance:     defaultMaxCachedMessagesPerInstance,
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

func WithQualityDeltaMultiplier(m float64) Option {
	return func(o *options) error {
		if m < 0 {
			return errors.New("quality duration multiplier cannot be less than zero")
		}
		o.qualityDeltaMulti = m
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

// WithMaxLookaheadRounds sets the maximum number of rounds ahead of the current
// round for which messages without justification are buffered. Setting a max
// value of larger than zero would aid gPBFT to potentially reach consensus in
// fewer rounds during periods of asynchronous broadcast as well as re-broadcast.
// Defaults to zero if unset.
func WithMaxLookaheadRounds(r uint64) Option {
	return func(o *options) error {
		o.maxLookaheadRounds = r
		return nil
	}
}

// WithMaxCachedInstances sets the maximum number of instances for which
// validated messages are cached. Defaults to 10 if unset.
func WithMaxCachedInstances(v int) Option {
	return func(o *options) error {
		o.maxCachedInstances = v
		return nil
	}
}

// WithMaxCachedMessagesPerInstance sets the maximum number of validated messages
// that are cached per instance. Defaults to 25K if unset.
func WithMaxCachedMessagesPerInstance(v int) Option {
	return func(o *options) error {
		o.maxCachedMessagesPerInstance = v
		return nil
	}
}

// WithCommitteeLookback sets the number of instances in the past from which the
// committee for the latest instance is derived. Defaults to 10 if unset.
func WithCommitteeLookback(lookback uint64) Option {
	return func(o *options) error {
		o.committeeLookback = lookback
		return nil
	}
}

// WithRebroadcastImmediatelyAfterRound sets the round after which rebroadcast is
// scheduled without waiting for phase timeout to expire first.
//
// Defaults to 3 if unset.
func WithRebroadcastImmediatelyAfterRound(round uint64) Option {
	return func(o *options) error {
		o.rebroadcastImmediatelyAfterRound = round
		return nil
	}
}

var defaultRebroadcastAfter = exponentialBackoffer(1.3, 0.1, 3*time.Second, 30*time.Second)

// WithRebroadcastBackoff sets the duration after the gPBFT timeout has elapsed, at
// which all messages in the current round are rebroadcast if the round cannot be
// terminated, i.e. a strong quorum of senders has not yet been achieved.
//
// The backoff duration grows exponentially up to the configured max and is randomized by "jitter"
// (a number between 0 and 1). Defaults to exponent of 1.3 with 3s base backoff growing to a maximum
// of 30s, randomized by 10% (5% in either direction).
func WithRebroadcastBackoff(exponent, jitter float64, base, max time.Duration) Option {
	return func(o *options) error {
		if base <= 0 {
			return fmt.Errorf("rebroadcast backoff duration must be greater than zero; got: %s", base)
		}
		if max < base {
			return fmt.Errorf("rebroadcast backoff max duration must be greater than base; got: %s", max)
		}
		o.rebroadcastAfter = exponentialBackoffer(exponent, jitter, base, max)
		return nil
	}
}

func exponentialBackoffer(exponent, jitter float64, base, maxBackoff time.Duration) func(attempt int) time.Duration {
	return func(attempt int) time.Duration {
		shift := 1.0 + (rand.Float64()-0.5)*jitter
		nextBackoff := float64(base) * math.Pow(exponent, float64(attempt)) * shift
		if nextBackoff > float64(maxBackoff) {
			return maxBackoff
		}
		return time.Duration(nextBackoff)
	}
}
