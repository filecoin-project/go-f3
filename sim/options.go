package sim

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
	"github.com/filecoin-project/go-f3/sim/signing"
)

const (
	defaultSimNetworkName      = "sim"
	defaultTipSetGeneratorSeed = 0x264803e715714f95 // Seed from Drand.
	defaultHonestCount         = 3
)

var (
	defaultBaseChain     gpbft.ECChain
	defaultBeacon        = []byte("beacon")
	defaultGraniteConfig = gpbft.GraniteConfig{
		Delta:                2.0,
		DeltaBackOffExponent: 1.3,
	}
	defaultLatencyModel latency.Model
)

func init() {
	var err error
	defaultBaseChain, err = gpbft.NewChain([]byte(("genesis")))
	if err != nil {
		panic("failed to instantiate default simulation base chain")
	}
	defaultLatencyModel, err = latency.NewLogNormal(time.Now().UnixMilli(), time.Second*5)
	if err != nil {
		panic("failed to instantiate default simulation latency model")
	}
}

type Option func(*options) error

type options struct {
	// latencyModel models the cross participant communication latencyModel throughout a
	// simulation.
	latencyModel latency.Model
	// honestCount is the honest participant count. Honest participants have one unit
	// of power each.
	honestCount int
	// Duration of EC epochs.
	ecEpochDuration time.Duration
	// Time to wait after EC epoch before starting next instance.
	ecStabilisationDelay time.Duration
	// If nil then FakeSigningBackend is used unless overridden by F3_TEST_USE_BLS
	signingBacked      signing.Backend
	graniteConfig      *gpbft.GraniteConfig
	traceLevel         int
	networkName        gpbft.NetworkName
	tipSetGenerator    *TipSetGenerator
	initialInstance    uint64
	baseChain          *gpbft.ECChain
	beacon             []byte
	adversaryGenerator adversary.Generator
	adversaryCount     uint64
}

func newOptions(o ...Option) (*options, error) {
	var opts options
	for _, apply := range o {
		if err := apply(&opts); err != nil {
			return nil, err
		}
	}
	if opts.honestCount == 0 {
		opts.honestCount = defaultHonestCount
	}
	if opts.latencyModel == nil {
		opts.latencyModel = defaultLatencyModel
	}
	if opts.graniteConfig == nil {
		opts.graniteConfig = &defaultGraniteConfig
	}
	if opts.signingBacked == nil {
		opts.signingBacked = signing.NewFakeBackend()
	}
	if opts.networkName == "" {
		opts.networkName = defaultSimNetworkName
	}
	if opts.tipSetGenerator == nil {
		opts.tipSetGenerator = NewTipSetGenerator(defaultTipSetGeneratorSeed)
	}
	if opts.baseChain == nil {
		opts.baseChain = &defaultBaseChain
	}
	if opts.beacon == nil {
		opts.beacon = defaultBeacon
	}
	return &opts, nil
}

func WithHonestParticipantCount(i int) Option {
	return func(o *options) error {
		o.honestCount = i
		return nil
	}
}

// WithSigningBackend sets the signing backend to be used by all participants in
// the simulation. Defaults to signing.FakeBackend if unset.
//
// See signing.FakeBackend, signing.BLSBackend.
func WithSigningBackend(sb signing.Backend) Option {
	return func(o *options) error {
		o.signingBacked = sb
		return nil
	}
}

func WithLatencyModel(lm latency.Model) Option {
	return func(o *options) error {
		o.latencyModel = lm
		return nil
	}
}

func WithECEpochDuration(d time.Duration) Option {
	return func(o *options) error {
		o.ecEpochDuration = d
		return nil
	}
}

func WitECStabilisationDelay(d time.Duration) Option {
	return func(o *options) error {
		o.ecStabilisationDelay = d
		return nil
	}
}

func WithGraniteConfig(c *gpbft.GraniteConfig) Option {
	return func(o *options) error {
		o.graniteConfig = c
		return nil
	}
}

func WithAdversary(generator adversary.Generator) Option {
	return func(o *options) error {
		// TODO: parameterise number of adversary counts.

		// Hard-coded to 1 in order to reduce the LOC up for review. Future work will
		// parameterise this for multiple adversary instances in a simulation
		o.adversaryCount = 1
		o.adversaryGenerator = generator
		return nil
	}
}

func WithBaseChain(base *gpbft.ECChain) Option {
	return func(o *options) error {
		o.baseChain = base
		return nil
	}
}

func WithTipSetGenerator(tsg *TipSetGenerator) Option {
	return func(o *options) error {
		o.tipSetGenerator = tsg
		return nil
	}
}

func WithECStabilisationDelay(d time.Duration) Option {
	return func(o *options) error {
		o.ecStabilisationDelay = d
		return nil
	}
}

func WithTraceLevel(i int) Option {
	return func(o *options) error {
		o.traceLevel = i
		return nil
	}
}
