package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/signing"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
)

const (
	defaultSimNetworkName = "sim"
)

var (
	defaultBaseChain gpbft.ECChain
)

func init() {
	var err error
	defaultBaseChain, err = gpbft.NewChain(gpbft.TipSet{
		Epoch:       0,
		Key:         []byte("genesis"),
		PowerTable:  []byte("genesis-powertable"),
		Commitments: [32]byte{},
	})
	if err != nil {
		panic("failed to instantiate default simulation base chain")
	}
}

type Option func(*options) error

type options struct {
	// latencyModel models the cross participant communication latencyModel throughout a
	// simulation.
	latencyModel latency.Model
	// honestParticipantArchetypes is the honest participant count and ec chain
	// generator. Honest participants have one unit of power each.
	honestParticipantArchetypes []participantArchetype
	// Duration of simEC epochs.
	ecEpochDuration time.Duration
	// Time to wait after EC epoch before starting next instance.
	ecStabilisationDelay    time.Duration
	globalStabilizationTime time.Duration
	// If nil then FakeSigningBackend is used unless overridden by F3_TEST_USE_BLS
	signingBacked      signing.Backend
	gpbftOptions       []gpbft.Option
	traceLevel         int
	networkName        gpbft.NetworkName
	baseChain          *gpbft.ECChain
	adversaryGenerator adversary.Generator
	adversaryCount     uint64
	ignoreConsensusFor []gpbft.ActorID
}

type participantArchetype struct {
	count                 int
	ecChainGenerator      ECChainGenerator
	storagePowerGenerator StoragePowerGenerator
}

func newOptions(o ...Option) (*options, error) {
	var opts options
	for _, apply := range o {
		if err := apply(&opts); err != nil {
			return nil, err
		}
	}
	if len(opts.honestParticipantArchetypes) == 0 {
		return nil, errors.New("at least one honest participant must be added")
	}
	if opts.latencyModel == nil {
		opts.latencyModel = latency.None
	}
	if opts.signingBacked == nil {
		opts.signingBacked = signing.NewFakeBackend()
	}
	if opts.networkName == "" {
		opts.networkName = defaultSimNetworkName
	}
	if opts.baseChain == nil {
		opts.baseChain = &defaultBaseChain
	}
	return &opts, nil
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

func WithLatencyModeler(lm latency.Modeler) Option {
	return func(o *options) error {
		var err error
		o.latencyModel, err = lm()
		return err
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

func WithGpbftOptions(gOpts ...gpbft.Option) Option {
	return func(o *options) error {
		o.gpbftOptions = gOpts
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

func AddHonestParticipants(count int, ecg ECChainGenerator, spg StoragePowerGenerator) Option {
	return func(o *options) error {
		if count <= 0 {
			return fmt.Errorf("honest participant count must be larger than zero; got: %d", count)
		}
		o.honestParticipantArchetypes = append(o.honestParticipantArchetypes,
			participantArchetype{
				count:                 count,
				ecChainGenerator:      ecg,
				storagePowerGenerator: spg,
			})
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

func WithGlobalStabilizationTime(d time.Duration) Option {
	return func(o *options) error {
		o.globalStabilizationTime = d
		return nil
	}
}

// WithIgnoreConsensusFor sets the participant IDs for which the simulation will
// not error if they do not reach consensus at the end of each instance. Defaults
// to none.
func WithIgnoreConsensusFor(id ...gpbft.ActorID) Option {
	return func(o *options) error {
		o.ignoreConsensusFor = id
		return nil
	}
}
