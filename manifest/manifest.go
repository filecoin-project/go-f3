package manifest

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multihash"
)

// ErrNoManifest is returned when no manifest is known.
var ErrNoManifest = errors.New("no known manifest")

const VersionCapability = 7

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
		CompressionEnabled:             true,
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

	DefaultPartialMessageManagerConfig = PartialMessageManagerConfig{
		PendingDiscoveredChainsBufferSize:     100,
		PendingPartialMessagesBufferSize:      100,
		PendingChainBroadcastsBufferSize:      100,
		PendingInstanceRemovalBufferSize:      10, // to match the default committee lookback.
		CompletedMessagesBufferSize:           100,
		MaxBufferedMessagesPerInstance:        25_000, // Based on 5 phases, network size of 2K participants at a couple of rounds plus some headroom.
		MaxCachedValidatedMessagesPerInstance: 25_000, // Based on 5 phases, network size of 2K participants at a couple of rounds plus some headroom.
	}

	// Default instance alignment when catching up.
	DefaultCatchUpAlignment = DefaultEcConfig.Period / 2
)

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
	case cx.MaxTimestampAge < 1*time.Millisecond:
		// The 1 ms is arbitrarily chosen as a reasonable non-zero minimum.
		return fmt.Errorf("chain exchange max timestamp age must not  be less than 1ms, got: %s", cx.MaxTimestampAge)
	default:
		return nil
	}
}

type PartialMessageManagerConfig struct {
	PendingDiscoveredChainsBufferSize     int
	PendingPartialMessagesBufferSize      int
	PendingChainBroadcastsBufferSize      int
	PendingInstanceRemovalBufferSize      int
	CompletedMessagesBufferSize           int
	MaxBufferedMessagesPerInstance        int
	MaxCachedValidatedMessagesPerInstance int
}

func (pmm *PartialMessageManagerConfig) Validate() error {
	switch {
	case pmm.PendingDiscoveredChainsBufferSize < 1:
		return fmt.Errorf("pending discovered chains buffer size must be at least 1, got: %d", pmm.PendingDiscoveredChainsBufferSize)
	case pmm.PendingPartialMessagesBufferSize < 1:
		return fmt.Errorf("pending partial messages buffer size must be at least 1, got: %d", pmm.PendingPartialMessagesBufferSize)
	case pmm.PendingChainBroadcastsBufferSize < 1:
		return fmt.Errorf("pending chain broadcasts buffer size must be at least 1, got: %d", pmm.PendingChainBroadcastsBufferSize)
	case pmm.PendingInstanceRemovalBufferSize < 1:
		return fmt.Errorf("pending instance removal buffer size must be at least 1, got: %d", pmm.PendingInstanceRemovalBufferSize)
	case pmm.CompletedMessagesBufferSize < 1:
		return fmt.Errorf("completed messages buffer size must be at least 1, got: %d", pmm.CompletedMessagesBufferSize)
	case pmm.MaxBufferedMessagesPerInstance < 1:
		return fmt.Errorf("max buffered messages per instance must be at least 1, got: %d", pmm.MaxBufferedMessagesPerInstance)
	case pmm.MaxCachedValidatedMessagesPerInstance < 1:
		return fmt.Errorf("max cached validated messages per instance must be at least 1, got: %d", pmm.MaxCachedValidatedMessagesPerInstance)
	default:
		return nil
	}
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
	// PartialMessageManager specifies the configuration for the partial message manager.
	PartialMessageManager PartialMessageManagerConfig
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
	if err := m.PartialMessageManager.Validate(); err != nil {
		return fmt.Errorf("invalid manifest: invalid partial message manager config: %w", err)
	}
	if m.Gpbft.ChainProposedLength > m.ChainExchange.MaxChainLength {
		return fmt.Errorf("invalid manifest: chain proposal length %d is greater than chain exchange max chain length %d", m.Gpbft.ChainProposedLength, m.ChainExchange.MaxChainLength)
	}
	if m.ChainExchange.MaxInstanceLookahead > m.CommitteeLookback {
		return fmt.Errorf("invalid manifest: chain exchange max instance lookahead %d exceeds committee lookback %d", m.ChainExchange.MaxInstanceLookahead, m.CommitteeLookback)
	}

	return nil
}

func LocalDevnetManifest() Manifest {
	rng := make([]byte, 4)
	_, _ = rand.Read(rng)
	return Manifest{
		ProtocolVersion:       VersionCapability,
		NetworkName:           gpbft.NetworkName(fmt.Sprintf("localnet-%X", rng)),
		BootstrapEpoch:        1000,
		CommitteeLookback:     DefaultCommitteeLookback,
		EC:                    DefaultEcConfig,
		Gpbft:                 DefaultGpbftConfig,
		CertificateExchange:   DefaultCxConfig,
		CatchUpAlignment:      DefaultCatchUpAlignment,
		PubSub:                DefaultPubSubConfig,
		ChainExchange:         DefaultChainExchangeConfig,
		PartialMessageManager: DefaultPartialMessageManagerConfig,
	}
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
