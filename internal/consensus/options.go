package consensus

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
)

type fakeECOptions struct {
	clock                 clock.Clock
	seed                  int64
	initialPowerTable     gpbft.PowerEntries
	evolvePowerTable      PowerTableMutator
	bootstrapEpoch        int64
	ecPeriod              time.Duration
	ecMaxLookback         int64
	nullTipsetProbability float64
	forkAfterEpochs       int64
	forkSeed              int64
}

type FakeECOption func(*fakeECOptions)
type PowerTableMutator func(epoch int64, pt gpbft.PowerEntries) gpbft.PowerEntries

func newFakeECOptions(o ...FakeECOption) *fakeECOptions {
	opts := &fakeECOptions{
		clock:                 clock.RealClock,
		ecPeriod:              30 * time.Second,
		seed:                  time.Now().UnixNano(),
		forkSeed:              time.Now().UnixNano() / 2,
		nullTipsetProbability: 0.015,
	}
	for _, apply := range o {
		apply(opts)
	}
	return opts
}

func WithBootstrapEpoch(epoch int64) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.bootstrapEpoch = epoch
	}
}

func WithSeed(seed int64) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.seed = seed
	}
}

func WithInitialPowerTable(initialPowerTable gpbft.PowerEntries) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.initialPowerTable = initialPowerTable
	}
}

func WithECPeriod(ecPeriod time.Duration) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.ecPeriod = ecPeriod
	}
}

func WithMaxLookback(distance int64) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.ecMaxLookback = distance
	}
}

func WithEvolvingPowerTable(fn PowerTableMutator) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.evolvePowerTable = fn
	}
}

// WithClock sets the clock used to determine the current time. This is
// useful for testing purposes, as it allows you to control the time
// progression of the EC. The default clock is the system clock.
func WithClock(clock clock.Clock) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.clock = clock
	}
}

// WithForkSeed sets the seed used to generate fork chains. For this option to
// take effect, WithForkAfterEpochs must be set to a value greater than 0.
func WithForkSeed(e int64) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.forkSeed = e
	}
}

// WithForkAfterEpochs sets the minimum number of epochs from the latest
// finalized tipset key after which this EC may fork away.
func WithForkAfterEpochs(e int64) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.forkAfterEpochs = e
	}
}

func WithNullTipsetProbability(p float64) FakeECOption {
	return func(ec *fakeECOptions) {
		ec.nullTipsetProbability = p
	}
}
