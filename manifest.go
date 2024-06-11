package f3

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"golang.org/x/xerrors"
)

const (
	ManifestPubSubTopicName = "/f3/manifests/0.0.1"
)

type Version string

type GpbftConfig struct {
	Delta                time.Duration
	DeltaBackOffExponent float64
	MaxLookaheadRounds   uint64
}

// Manifest identifies the specific configuration for
// the F3 instance currently running.
type Manifest struct {
	// Sequence number of the manifest.
	// This is used to identify if a new config needs to be applied
	Sequence uint64
	// UpgradeEpoch from which the manifest should be applied
	UpgradeEpoch int64
	// Flag to determine if the peer should rebootstrap in this configuration
	// change at UpgradeEpoch
	ReBootstrap bool
	// Network name to apply for this manifest.
	NetworkName gpbft.NetworkName
	// Time to wait after EC epoch before starting next instance.
	EcStabilisationDelay time.Duration
	// Initial power table used when bootstrapping. This is used for test
	// networks or if we want to force a specific power table when
	// rebootstrapping. If nil, the power table is fetched from the host
	InitialPowerTable []gpbft.PowerEntry
	// Updates to perform over the power table retrieved by the host
	// starting from UpgradeEpoch.
	PowerUpdate []certs.PowerTableDelta
	// Config parameters for gpbft
	*GpbftConfig
}

// Version that uniquely identifies the manifest.
// This is added to GPBFT messages to identify the configuration that was
// being used when broadcasting that message.
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

func (m Manifest) PubSubTopic() string {
	v, _ := m.Version()
	return m.NetworkName.PubSubTopic() + string(v)
}

func (m Manifest) toGpbftOpts() []gpbft.Option {
	var opts []gpbft.Option

	opts = append(opts, gpbft.WithInitialInstance(uint64(m.UpgradeEpoch)))
	if m.Delta != 0 {
		opts = append(opts, gpbft.WithDelta(m.Delta))
	}
	if m.DeltaBackOffExponent != 0 {
		opts = append(opts, gpbft.WithDeltaBackOffExponent(m.DeltaBackOffExponent))
	}
	if m.MaxLookaheadRounds != 0 {
		opts = append(opts, gpbft.WithMaxLookaheadRounds(m.MaxLookaheadRounds))
	}

	return opts
}
