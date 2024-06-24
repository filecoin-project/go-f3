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
		ECFinality:       900,
		CommiteeLookback: 5,
		ECDelay:          30 * time.Second,

		ECPeriod: 30 * time.Second,
	}

	DefaultGpbftConfig = &GpbftConfig{
		Delta:                3 * time.Second,
		DeltaBackOffExponent: 2.0,
		MaxLookaheadRounds:   5,
	}

	DefaultGpbftOptions = []gpbft.Option{
		gpbft.WithMaxLookaheadRounds(DefaultGpbftConfig.MaxLookaheadRounds),
		gpbft.WithDelta(time.Duration(DefaultGpbftConfig.Delta) * time.Second),
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
	ECFinality       int64
	ECDelay          time.Duration
	CommiteeLookback uint64

	//Temporary
	ECPeriod            time.Duration
	ECBoostrapTimestamp time.Time
}

// Manifest identifies the specific configuration for
// the F3 instance currently running.
type Manifest struct {
	// Sequence number of the manifest.
	// This is used to identify if a new config needs to be applied
	Sequence uint64
	// Initial instance to use for gpbft
	InitialInstance uint64
	// BootstrapEpoch from which the manifest should be applied
	BootstrapEpoch int64
	// Flag to determine if the peer should rebootstrap in this configuration
	// change at BootstrapEpoch
	ReBootstrap bool
	// Network name to apply for this manifest.
	NetworkName gpbft.NetworkName
	// Initial power table used when bootstrapping. This is used for test
	// networks or if we want to force a specific power table when
	// rebootstrapping. If nil, the power table is fetched from the host
	InitialPowerTable gpbft.PowerEntries
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
	m.ECBoostrapTimestamp = time.Now().Add(-time.Duration(m.BootstrapEpoch) * m.ECPeriod)
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

// TODO: Describe how we are using json because we need to serialize a float
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
	return "/f3/granite/0.0.1/" + string(m.NetworkName)
}

func (m Manifest) GpbftOptions() []gpbft.Option {
	var opts []gpbft.Option

	if m.GpbftConfig == nil {
		return DefaultGpbftOptions
	}

	if m.Delta != 0 {
		opts = append(opts, gpbft.WithDelta(m.Delta*time.Second))
	}
	opts = append(opts, gpbft.WithDeltaBackOffExponent(m.DeltaBackOffExponent))
	opts = append(opts, gpbft.WithMaxLookaheadRounds(m.MaxLookaheadRounds))

	return opts
}
