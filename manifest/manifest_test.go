package manifest_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/stretchr/testify/require"
)

var base = manifest.Manifest{
	BootstrapEpoch: 900,
	NetworkName:    "test",
	ExplicitPower: gpbft.PowerEntries{
		{
			ID:     2,
			Power:  gpbft.NewStoragePower(1),
			PubKey: gpbft.PubKey{0},
		},
		{
			ID:     3,
			Power:  gpbft.NewStoragePower(1),
			PubKey: gpbft.PubKey{1},
		},
	},
	CommitteeLookback: 10,
	Gpbft: manifest.GpbftConfig{
		Delta:                      10,
		DeltaBackOffExponent:       1.2,
		MaxLookaheadRounds:         5,
		RebroadcastBackoffBase:     10,
		RebroadcastBackoffExponent: 1.3,
		RebroadcastBackoffMax:      30,
	},
	EC: manifest.EcConfig{
		Finality:                 900,
		DelayMultiplier:          2.0,
		Period:                   30 * time.Second,
		BaseDecisionBackoffTable: []float64{1.3, 1.69, 2.2, 2.86, 3.71, 4.83, 6.27, 8.16, 10.6, 13.79, 15.},
		HeadLookback:             0,
	},
	CertificateExchange: manifest.CxConfig{
		ClientRequestTimeout: 10 * time.Second,
		ServerRequestTimeout: time.Minute,
		MinimumPollInterval:  30 * time.Second,
		MaximumPollInterval:  2 * time.Minute,
	},
	CatchUpAlignment: 0,
}

func TestManifest_Validation(t *testing.T) {
	require.NoError(t, base.Validate())
	require.NoError(t, manifest.LocalDevnetManifest().Validate())

	cpy := base
	cpy.BootstrapEpoch = 50
	require.Error(t, cpy.Validate())

	cpy = base
	cpy.CertificateExchange.MinimumPollInterval = time.Nanosecond
	require.Error(t, cpy.Validate())
}

func TestManifest_Serialization(t *testing.T) {
	baseMarshalled, err := base.Marshal()
	require.NoError(t, err)
	require.NoError(t, err)

	for _, test := range []struct {
		name  string
		given []byte
		want  *manifest.Manifest
	}{
		{
			name:  "base",
			given: baseMarshalled,
			want:  &base,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := manifest.Unmarshal(bytes.NewReader(test.given))
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestManifest_NetworkName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		subject gpbft.NetworkName
	}{
		{
			name: "zero",
		},
		{
			name:    "non-zero",
			subject: "fish",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := manifest.Manifest{NetworkName: test.subject}
			gotDsPrefix := m.DatastorePrefix().String()
			require.True(t, strings.HasPrefix(gotDsPrefix, "/f3"))
			require.True(t, strings.HasSuffix(gotDsPrefix, string(test.subject)))
			gotPubSubTopic := m.PubSubTopic()
			require.True(t, strings.HasPrefix(gotPubSubTopic, "/f3/granite/0.0.2/"))
			require.True(t, strings.HasSuffix(gotPubSubTopic, string(test.subject)))
		})
	}
}
