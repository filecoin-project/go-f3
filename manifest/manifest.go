package manifest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multihash"
)

// ErrNoManifest is returned when no manifest is known.
var ErrNoManifest = errors.New("no known manifest")

const VersionCapability = 6

var (
	DefaultCommitteeLookback uint64 = 10

	// Default configuration for the EC Backend
	DefaultEcConfig = EcConfig{
		Finality:        900,
		Period:          30 * time.Second,
		DelayMultiplier: 2.,
		// MaxBackoff is 15min given default params
		BaseDecisionBackoffTable: []float64{1.3, 1.69, 2.2, 2.86, 3.71, 4.83, 6.27, 7.5},
		HeadLookback:             0,
		Finalize:                 true,
	}

	DefaultGpbftConfig = GpbftConfig{
		Delta:                      6 * time.Second,
		DeltaBackOffExponent:       2.0,
		QualityDeltaMultiplier:     1.0,
		MaxLookaheadRounds:         5,
		ChainProposedLength:        gpbft.ChainDefaultLen,
		RebroadcastBackoffBase:     6 * time.Second,
		RebroadcastBackoffSpread:   0.1,
		RebroadcastBackoffExponent: 1.3,
		RebroadcastBackoffMax:      60 * time.Second,
	}

	DefaultCxConfig = CxConfig{
		ClientRequestTimeout: 10 * time.Second,
		ServerRequestTimeout: time.Minute,
		MinimumPollInterval:  DefaultEcConfig.Period,
		MaximumPollInterval:  4 * DefaultEcConfig.Period,
	}

	DefaultPubSubConfig = PubSubConfig{
		CompressionEnabled:             false,
		ChainCompressionEnabled:        true,
		GMessageSubscriptionBufferSize: 128,
		ValidatedMessageBufferSize:     128,
	}

	DefaultChainExchangeConfig = ChainExchangeConfig{
		SubscriptionBufferSize:         32,
		MaxChainLength:                 gpbft.ChainDefaultLen,
		MaxInstanceLookahead:           DefaultCommitteeLookback,
		MaxDiscoveredChainsPerInstance: 1_000,
		MaxWantedChainsPerInstance:     1_000,
		RebroadcastInterval:            2 * time.Second,
		MaxTimestampAge:                8 * time.Second,
	}

	// Default instance alignment when catching up.
	DefaultCatchUpAlignment = DefaultEcConfig.Period / 2
)

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

func (c *CxConfig) Validate() error {
	switch {
	case c.ClientRequestTimeout < 0:
		return fmt.Errorf("client request timeout must be non-negative, was %s", c.ClientRequestTimeout)
	case c.ServerRequestTimeout < 0:
		return fmt.Errorf("server request timeout must be non-negative, was %s", c.ServerRequestTimeout)
	case c.MinimumPollInterval < time.Millisecond:
		return fmt.Errorf("minimum polling interval must be at least 1ms, was %s", c.MinimumPollInterval)

	case c.MinimumPollInterval > c.MaximumPollInterval:
		return fmt.Errorf("maximum polling interval (%s) must not be less than the minimum (%s)",
			c.MaximumPollInterval, c.MinimumPollInterval)
	}
	return nil
}

type GpbftConfig struct {
	Delta                  time.Duration
	DeltaBackOffExponent   float64
	QualityDeltaMultiplier float64
	MaxLookaheadRounds     uint64

	ChainProposedLength int

	RebroadcastBackoffBase     time.Duration
	RebroadcastBackoffExponent float64
	RebroadcastBackoffSpread   float64
	RebroadcastBackoffMax      time.Duration
}

func (g *GpbftConfig) Validate() error {
	if g.Delta <= 0 {
		return fmt.Errorf("gpbft delta must be positive, was %s", g.Delta)
	}
	if g.DeltaBackOffExponent < 1.0 {
		return fmt.Errorf("gpbft backoff exponent must be at least 1.0, was %f", g.DeltaBackOffExponent)
	}

	if g.QualityDeltaMultiplier < 0 {
		return fmt.Errorf("gpbft quality duration multiplier is negative: %f", g.QualityDeltaMultiplier)
	}

	if g.ChainProposedLength < 1 {
		return fmt.Errorf("gpbft proposed chain length cannot be less than 1")
	}
	// not checking against gpbft.ChainMaxLen, it is handled gracefully

	if g.RebroadcastBackoffBase <= 0 {
		return fmt.Errorf("gpbft rebroadcast backoff base must be greater than 0, was %s",
			g.RebroadcastBackoffBase)
	}
	if g.RebroadcastBackoffExponent < 1.0 {
		return fmt.Errorf("gpbft rebroadcast backoff exponent must be at least 1.0, was %f",
			g.RebroadcastBackoffExponent)
	}
	if g.RebroadcastBackoffMax < g.RebroadcastBackoffBase {
		return fmt.Errorf("gpbft rebroadcast backoff max (%s) must be at least the backoff base (%s)",
			g.RebroadcastBackoffMax, g.RebroadcastBackoffBase)
	}
	return nil
}

func (g *GpbftConfig) ToOptions() []gpbft.Option {
	return []gpbft.Option{
		gpbft.WithDelta(g.Delta),
		gpbft.WithDeltaBackOffExponent(g.DeltaBackOffExponent),
		gpbft.WithQualityDeltaMultiplier(g.QualityDeltaMultiplier),
		gpbft.WithMaxLookaheadRounds(g.MaxLookaheadRounds),
		gpbft.WithRebroadcastBackoff(
			DefaultGpbftConfig.RebroadcastBackoffExponent,
			DefaultGpbftConfig.RebroadcastBackoffSpread,
			DefaultGpbftConfig.RebroadcastBackoffBase,
			DefaultGpbftConfig.RebroadcastBackoffMax,
		),
	}
}

type EcConfig struct {
	// The delay between tipsets.
	Period time.Duration
	// Number of epochs required to reach EC defined finality
	Finality int64
	// The multiplier on top of the Period of the time we will wait before starting a new instance,
	// referencing the timestamp of the latest finalized tipset.
	DelayMultiplier float64
	// Table of incremental multipliers to backoff in units of Delay in case of base decisions
	BaseDecisionBackoffTable []float64
	// HeadLookback number of unfinalized tipsets to remove from the head
	HeadLookback int
	// Finalize indicates whether F3 should finalize tipsets as F3 agrees on them.
	Finalize bool
}

func (e *EcConfig) Equal(o *EcConfig) bool {
	return e.Period == o.Period &&
		e.Finality == o.Finality &&
		e.DelayMultiplier == o.DelayMultiplier &&
		e.HeadLookback == o.HeadLookback &&
		slices.Equal(e.BaseDecisionBackoffTable, o.BaseDecisionBackoffTable)
}

func (e *EcConfig) Validate() error {
	switch {
	case e.HeadLookback < 0:
		return fmt.Errorf("ec head lookback must be non-negative, was %d", e.HeadLookback)
	case e.Period <= 0:
		return fmt.Errorf("ec period must be positive, was %s", e.Period)
	case e.Finality < 0:
		return fmt.Errorf("ec finality must be non-negative, was %d", e.Finality)
	case e.DelayMultiplier <= 0.0:
		return fmt.Errorf("ec delay multiplier must positive, was %f", e.DelayMultiplier)
	case len(e.BaseDecisionBackoffTable) == 0:
		return fmt.Errorf("ec backoff table must have at least one element")
	}

	for i, b := range e.BaseDecisionBackoffTable {
		if b < 0.0 {
			return fmt.Errorf("ec backoff table element %d is negative (%f)", i, b)
		}
	}
	return nil
}

type PubSubConfig struct {
	CompressionEnabled      bool
	ChainCompressionEnabled bool

	GMessageSubscriptionBufferSize int
	ValidatedMessageBufferSize     int
}

func (p *PubSubConfig) Validate() error {
	switch {
	case p.GMessageSubscriptionBufferSize < 1:
		return fmt.Errorf("pubsub gmessage subscription buffer size must be at least 1, got: %d", p.GMessageSubscriptionBufferSize)
	case p.ValidatedMessageBufferSize < 1:
		return fmt.Errorf("pubsub validated gmessage buffer size must be at least 1, got: %d", p.ValidatedMessageBufferSize)
	default:
		return nil
	}
}

type ChainExchangeConfig struct {
	SubscriptionBufferSize         int
	MaxChainLength                 int
	MaxInstanceLookahead           uint64
	MaxDiscoveredChainsPerInstance int
	MaxWantedChainsPerInstance     int
	RebroadcastInterval            time.Duration
	MaxTimestampAge                time.Duration
}

func (cx *ChainExchangeConfig) Validate() error {
	// Note that MaxInstanceLookahead may be zero, which indicates that the chain
	// exchange will only accept discovery of chains from current instance.
	switch {
	case cx.SubscriptionBufferSize < 1:
		return fmt.Errorf("chain exchange subscription buffer size must be at least 1, got: %d", cx.SubscriptionBufferSize)
	case cx.MaxChainLength < 1:
		return fmt.Errorf("chain exchange max chain length must be at least 1, got: %d", cx.MaxChainLength)
	case cx.MaxDiscoveredChainsPerInstance < 1:
		return fmt.Errorf("chain exchange max discovered chains per instance must be at least 1, got: %d", cx.MaxDiscoveredChainsPerInstance)
	case cx.MaxWantedChainsPerInstance < 1:
		return fmt.Errorf("chain exchange max wanted chains per instance must be at least 1, got: %d", cx.MaxWantedChainsPerInstance)
	case cx.RebroadcastInterval < 1*time.Millisecond:
		// The timestamp precision is set to milliseconds. Therefore, the rebroadcast interval must not be less than a millisecond.
		return fmt.Errorf("chain exchange rebroadcast interval cannot be less than 1ms, got: %s", cx.RebroadcastInterval)
	case cx.MaxTimestampAge < 4*cx.RebroadcastInterval:
		// The 4X is made up, but the idea is that the max timestamp age should give enough time for message propagation across at least a pair of nodes.
		return fmt.Errorf("chain exchange max timestamp age must be at least 4x the rebroadcast interval (%s), got: %s", cx.RebroadcastInterval, cx.MaxTimestampAge)

	default:
		return nil
	}
}

// Manifest identifies the specific configuration for the F3 instance currently running.
type Manifest struct {
	// Pause stops the participation in F3.
	Pause bool
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
	InitialPowerTable cid.Cid // !Defined() if nil
	// We take the current power table from the head tipset this many instances ago.
	CommitteeLookback uint64
	// The alignment of instances while catching up. This should be slightly larger than the
	// expected time to complete an instance.
	//
	// A good default is `4 * Manifest.Gpbft.Delta` (the expected time for a single-round
	// instance).
	CatchUpAlignment time.Duration
	// Config parameters for gpbft
	Gpbft GpbftConfig
	// EC-specific parameters
	EC EcConfig
	// Certificate Exchange specific parameters
	CertificateExchange CxConfig
	// PubSubConfig specifies the pubsub related configuration.
	PubSub PubSubConfig
	// ChainExchange specifies the chain exchange configuration parameters.
	ChainExchange ChainExchangeConfig
}

func (m *Manifest) Equal(o *Manifest) bool {
	if m == nil || o == nil {
		return m == o
	}

	return m.NetworkName == o.NetworkName &&
		m.Pause == o.Pause &&
		m.InitialInstance == o.InitialInstance &&
		m.BootstrapEpoch == o.BootstrapEpoch &&
		m.IgnoreECPower == o.IgnoreECPower &&
		m.CommitteeLookback == o.CommitteeLookback &&
		// Don't include this in equality checks because it doesn't change the meaning of
		// the manifest (and we don't want to restart the network when we first publish
		// this).
		// m.InitialPowerTable.Equals(o.InitialPowerTable) &&
		m.ExplicitPower.Equal(o.ExplicitPower) &&
		m.Gpbft == o.Gpbft &&
		m.EC.Equal(&o.EC) &&
		m.CertificateExchange == o.CertificateExchange &&
		m.ProtocolVersion == o.ProtocolVersion

}

func (m *Manifest) Validate() error {
	switch {
	case m == nil:
		return fmt.Errorf("invalid manifest: manifest is nil")
	case m.NetworkName == "":
		return fmt.Errorf("invalid manifest: network name must not be empty")
	case m.BootstrapEpoch < m.EC.Finality:
		return fmt.Errorf("invalid manifest: bootstrap epoch %d before finality %d",
			m.BootstrapEpoch, m.EC.Finality)
	case m.IgnoreECPower && len(m.ExplicitPower) == 0:
		return fmt.Errorf("invalid manifest: ignoring ec power with no explicit power")
	}

	if len(m.ExplicitPower) > 0 {
		pt := gpbft.NewPowerTable()
		if err := pt.Add(m.ExplicitPower...); err != nil {
			return fmt.Errorf("invalid manifest: invalid power entries")
		}

		if err := pt.Validate(); err != nil {
			return fmt.Errorf("invalid manifest: %w", err)
		}

		if m.IgnoreECPower && pt.Total.Sign() <= 0 {
			return fmt.Errorf("invalid manifest: no power")
		}
	}

	if err := m.Gpbft.Validate(); err != nil {
		return fmt.Errorf("invalid manifest: invalid gpbft config: %w", err)
	}
	if err := m.EC.Validate(); err != nil {
		return fmt.Errorf("invalid manifest: invalid EC config: %w", err)
	}
	if err := m.CertificateExchange.Validate(); err != nil {
		return fmt.Errorf("invalid manifest: invalid certificate exchange config: %w", err)
	}
	if err := m.PubSub.Validate(); err != nil {
		return fmt.Errorf("invalid manifest: invalid pubsub config: %w", err)
	}
	if err := m.ChainExchange.Validate(); err != nil {
		return fmt.Errorf("invalid manifest: invalid chain exchange config: %w", err)
	}
	if m.ChainExchange.MaxChainLength > m.Gpbft.ChainProposedLength {
		return fmt.Errorf("invalid manifest: chain exchange max chain length %d exceeds gpbft proposed chain length %d", m.ChainExchange.MaxChainLength, m.Gpbft.ChainProposedLength)
	}
	if m.ChainExchange.MaxInstanceLookahead > m.CommitteeLookback {
		return fmt.Errorf("invalid manifest: chain exchange max instance lookahead %d exceeds committee lookback %d", m.ChainExchange.MaxInstanceLookahead, m.CommitteeLookback)
	}

	return nil
}

func LocalDevnetManifest() *Manifest {
	rng := make([]byte, 4)
	_, _ = rand.Read(rng)
	m := &Manifest{
		ProtocolVersion:     VersionCapability,
		NetworkName:         gpbft.NetworkName(fmt.Sprintf("localnet-%X", rng)),
		BootstrapEpoch:      1000,
		CommitteeLookback:   DefaultCommitteeLookback,
		EC:                  DefaultEcConfig,
		Gpbft:               DefaultGpbftConfig,
		CertificateExchange: DefaultCxConfig,
		CatchUpAlignment:    DefaultCatchUpAlignment,
		PubSub:              DefaultPubSubConfig,
		ChainExchange:       DefaultChainExchangeConfig,
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
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, err
	}
	return m, m.Validate()
}

func (m *Manifest) DatastorePrefix() datastore.Key {
	return datastore.NewKey("/f3/" + string(m.NetworkName))
}

func (m *Manifest) PubSubTopic() string {
	return PubSubTopicFromNetworkName(m.NetworkName)
}

var cidPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.DagJSON,
	MhType:   multihash.BLAKE2B_MIN + 31,
	MhLength: 32,
}

func (m *Manifest) Cid() (cid.Cid, error) {
	marshalled, err := m.Marshal()
	if err != nil {
		return cid.Cid{}, err
	}
	return cidPrefix.Sum(marshalled)
}

func PubSubTopicFromNetworkName(nn gpbft.NetworkName) string {
	return "/f3/granite/0.0.3/" + string(nn)
}

func ChainExchangeTopicFromNetworkName(nn gpbft.NetworkName) string {
	return "/f3/chainexchange/0.0.1/" + string(nn)
}

func (m *Manifest) GpbftOptions() []gpbft.Option {
	return m.Gpbft.ToOptions()
}
