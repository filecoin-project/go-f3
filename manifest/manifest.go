package manifest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-datastore"
	"golang.org/x/xerrors"
)

var (
	// Default configuration for the EC Backend
	DefaultEcConfig = &EcConfig{
		ECFinality:        900,
		CommiteeLookback:  10,
		ECPeriod:          30 * time.Second,
		ECDelayMultiplier: 2.,
		// MaxBackoff is 15min given default params
		BaseDecisionBackoffTable: []float64{1.3, 1.69, 2.2, 2.86, 3.71, 4.83, 6.27, 7.5},
	}

	DefaultGpbftConfig = &GpbftConfig{
		Delta:                3 * time.Second,
		DeltaBackOffExponent: 2.0,
		MaxLookaheadRounds:   5,
	}

	DefaultGpbftOptions = []gpbft.Option{
		gpbft.WithMaxLookaheadRounds(DefaultGpbftConfig.MaxLookaheadRounds),
		gpbft.WithDelta(DefaultGpbftConfig.Delta),
		gpbft.WithDeltaBackOffExponent(DefaultGpbftConfig.DeltaBackOffExponent),
	}
)

type OnManifestChange func(ctx context.Context, prevManifest Manifest, errCh chan error)

type ManifestProvider interface {
	// Run starts any background tasks required for the operation
	// of the manifest provider.
	Run(context.Context, chan error)
	// Returns the list of gpbft options to be used for gpbft configuration
	GpbftOptions() []gpbft.Option
	// Set callback to trigger to apply new manifests from F3.
	SetManifestChangeCallback(OnManifestChange)
	// Manifest accessor
	Manifest() Manifest
}

type Version string

type GpbftConfig struct {
	Delta                time.Duration
	DeltaBackOffExponent float64
	MaxLookaheadRounds   uint64
}

type EcConfig struct {
	// The delay between tipsets.
	ECPeriod time.Duration
	// Number of epochs required to reach EC defined fianlity
	ECFinality int64
	// The multiplier on top of the ECPeriod of the time we will wait before starting a new instance,
	// referencing the timestampt of the latest finalized tipset.
	ECDelayMultiplier float64
	// Table of incremental multipliers to backoff in units of ECDelay in case of base decisions
	BaseDecisionBackoffTable []float64
	CommiteeLookback         uint64
}

// Manifest identifies the specific configuration for
// the F3 instance currently running.
type Manifest struct {
	// Sequence number of the manifest.
	// This is used to identify if a new config needs to be applied
	Sequence uint64
	// Initial instance to used for the f3 instance
	InitialInstance uint64
	// BootstrapEpoch from which the manifest should be applied
	BootstrapEpoch int64
	// Flag to determine if the peer should rebootstrap in this configuration
	// change at BootstrapEpoch
	ReBootstrap bool
	// Specifies if the manifest should pause F3 until a new configuration arrives.
	Pause bool
	// Network name to apply for this manifest.
	NetworkName gpbft.NetworkName
	// Updates to perform over the power table retrieved by the host
	// starting from BootstrapEpoch.
	PowerUpdate []certs.PowerTableDelta
	// Config parameters for gpbft
	*GpbftConfig
	// EC-specific parameters
	*EcConfig
}

func LocalDevnetManifest() Manifest {
	rng := make([]byte, 4)
	_, _ = rand.Read(rng)
	m := Manifest{
		NetworkName:    gpbft.NetworkName(fmt.Sprintf("localnet-%X", rng)),
		BootstrapEpoch: 1000,
		EcConfig:       DefaultEcConfig,
	}
	return m
}

// Version that uniquely identifies the manifest.
func (m Manifest) Version() (Version, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", xerrors.Errorf("computing manifest version: %w", err)
	}
	return Version(hex.EncodeToString(gpbft.MakeCid(b))), nil
}

// Marshal the manifest into JSON
// We use JSON because we need to serialize a float and time.Duration
// and the cbor serializer we use do not support these types yet.
func (m Manifest) Marshal() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, xerrors.Errorf("marshaling JSON: %w", err)
	}
	return b, nil
}

func (m *Manifest) Unmarshal(r io.Reader) error {
	err := json.NewDecoder(r).Decode(&m)
	if err != nil {
		return xerrors.Errorf("decoding JSON: %w", err)
	}
	return nil
}

func (m Manifest) DatastorePrefix() datastore.Key {
	return datastore.NewKey("/f3/" + string(m.NetworkName))
}

func (m Manifest) PubSubTopic() string {
	return PubSubTopicFromNetworkName(m.NetworkName)
}

func PubSubTopicFromNetworkName(nn gpbft.NetworkName) string {
	return "/f3/granite/0.0.1/" + string(nn)
}

func (m Manifest) GpbftOptions() []gpbft.Option {
	var opts []gpbft.Option

	if m.GpbftConfig == nil {
		return DefaultGpbftOptions
	}

	if m.Delta != 0 {
		opts = append(opts, gpbft.WithDelta(m.Delta))
	}
	opts = append(opts, gpbft.WithDeltaBackOffExponent(m.DeltaBackOffExponent))
	opts = append(opts, gpbft.WithMaxLookaheadRounds(m.MaxLookaheadRounds))

	return opts
}
