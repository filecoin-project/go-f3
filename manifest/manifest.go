package manifest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
)

const VersionCapability = 1

var (
	// Default configuration for the EC Backend
	DefaultEcConfig = EcConfig{
		ECFinality:        900,
		CommitteeLookback: 10,
		ECPeriod:          30 * time.Second,
		ECDelayMultiplier: 2.,
		// MaxBackoff is 15min given default params
		BaseDecisionBackoffTable: []float64{1.3, 1.69, 2.2, 2.86, 3.71, 4.83, 6.27, 7.5},
	}

	DefaultGpbftConfig = GpbftConfig{
		Delta:                3 * time.Second,
		DeltaBackOffExponent: 2.0,
		MaxLookaheadRounds:   5,
	}

	DefaultGpbftOptions = []gpbft.Option{
		gpbft.WithMaxLookaheadRounds(DefaultGpbftConfig.MaxLookaheadRounds),
		gpbft.WithDelta(DefaultGpbftConfig.Delta),
		gpbft.WithDeltaBackOffExponent(DefaultGpbftConfig.DeltaBackOffExponent),
	}

	DefaultCxConfig = CxConfig{
		ClientRequestTimeout: 10 * time.Second,
		ServerRequestTimeout: time.Minute,
		MinimumPollInterval:  DefaultEcConfig.ECPeriod,
		MaximumPollInterval:  4 * DefaultEcConfig.ECPeriod,
	}
)

type OnManifestChange func(ctx context.Context, prevManifest Manifest) error

type ManifestProvider interface {
	// Start any background tasks required for the operation of the manifest provider.
	Start(context.Context) error
	// Stop stops a running manifest provider.
	Stop(context.Context) error
	// The channel on which manifest updates are returned.
	ManifestUpdates() <-chan *Manifest
}

// Certificate Exchange config
type CxConfig struct {
	// Request timeout for the certificate exchange client.
	ClientRequestTimeout time.Duration
	// Request timeout for the certificate exchange server.
	ServerRequestTimeout time.Duration
	// Minimum CX polling interval.
	MinimumPollInterval time.Duration
	// Maximum CX polling interval.
	MaximumPollInterval time.Duration
}

type GpbftConfig struct {
	Delta                time.Duration
	DeltaBackOffExponent float64
	MaxLookaheadRounds   uint64
}

type EcConfig struct {
	// The delay between tipsets.
	ECPeriod time.Duration
	// Number of epochs required to reach EC defined finality
	ECFinality int64
	// The multiplier on top of the ECPeriod of the time we will wait before starting a new instance,
	// referencing the timestamp of the latest finalized tipset.
	ECDelayMultiplier float64
	// Table of incremental multipliers to backoff in units of ECDelay in case of base decisions
	BaseDecisionBackoffTable []float64
	CommitteeLookback        uint64
}

func (e *EcConfig) Equal(o *EcConfig) bool {
	return e.ECPeriod == o.ECPeriod &&
		e.ECFinality == o.ECFinality &&
		e.ECDelayMultiplier == o.ECDelayMultiplier &&
		e.CommitteeLookback == o.CommitteeLookback &&
		slices.Equal(e.BaseDecisionBackoffTable, o.BaseDecisionBackoffTable)
}

// Manifest identifies the specific configuration for the F3 instance currently running.
type Manifest struct {
	// ProtocolVersion specifies protocol version to be used
	ProtocolVersion uint64
	// Initial instance to used for the f3 instance
	InitialInstance uint64
	// BootstrapEpoch from which the manifest should be applied
	BootstrapEpoch int64
	// Network name to apply for this manifest.
	NetworkName gpbft.NetworkName
	// Updates to perform over the power table from EC (by replacement). Any entries with 0
	// power will disable the participant.
	ExplicitPower gpbft.PowerEntries
	// Ignore the power table from EC.
	IgnoreECPower bool
	// InitialPowerTable specifies the optional CID of the initial power table
	InitialPowerTable *cid.Cid
	// Config parameters for gpbft
	GpbftConfig
	// EC-specific parameters
	EcConfig
	// Certificate Exchange specific parameters
	CxConfig
}

func (m *Manifest) Equal(o *Manifest) bool {
	if m == nil || o == nil {
		return m == o
	}

	return m.NetworkName == o.NetworkName &&
		m.InitialInstance == o.InitialInstance &&
		m.BootstrapEpoch == o.BootstrapEpoch &&
		m.IgnoreECPower == o.IgnoreECPower &&
		m.ExplicitPower.Equal(o.ExplicitPower) &&
		m.GpbftConfig == o.GpbftConfig &&
		m.EcConfig.Equal(&o.EcConfig) &&
		m.CxConfig == o.CxConfig &&
		m.ProtocolVersion == o.ProtocolVersion

}

func LocalDevnetManifest() *Manifest {
	rng := make([]byte, 4)
	_, _ = rand.Read(rng)
	m := &Manifest{
		ProtocolVersion: 1,
		NetworkName:     gpbft.NetworkName(fmt.Sprintf("localnet-%X", rng)),
		BootstrapEpoch:  1000,
		EcConfig:        DefaultEcConfig,
		GpbftConfig:     DefaultGpbftConfig,
		CxConfig:        DefaultCxConfig,
	}
	return m
}

// Marshal the manifest into JSON
// We use JSON because we need to serialize a float and time.Duration
// and the cbor serializer we use do not support these types yet.
func (m *Manifest) Marshal() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshaling JSON: %w", err)
	}
	return b, nil
}

func Unmarshal(r io.Reader) (*Manifest, error) {
	var m *Manifest
	return m, json.NewDecoder(r).Decode(&m)
}

func (m *Manifest) DatastorePrefix() datastore.Key {
	return datastore.NewKey("/f3/" + string(m.NetworkName))
}

func (m *Manifest) PubSubTopic() string {
	return PubSubTopicFromNetworkName(m.NetworkName)
}

func PubSubTopicFromNetworkName(nn gpbft.NetworkName) string {
	return "/f3/granite/0.0.1/" + string(nn)
}

func (m *Manifest) GpbftOptions() []gpbft.Option {
	if m.GpbftConfig == (GpbftConfig{}) {
		return DefaultGpbftOptions
	}
	var opts []gpbft.Option
	if m.Delta != 0 {
		opts = append(opts, gpbft.WithDelta(m.Delta))
	}
	opts = append(opts, gpbft.WithDeltaBackOffExponent(m.DeltaBackOffExponent))
	opts = append(opts, gpbft.WithMaxLookaheadRounds(m.MaxLookaheadRounds))

	return opts
}
