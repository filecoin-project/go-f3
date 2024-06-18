package manifest_test

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/stretchr/testify/require"
)

var baseManifest manifest.Manifest = manifest.Manifest{
	Sequence:       0,
	BootstrapEpoch: 10,
	ReBootstrap:    true,
	NetworkName:    gpbft.NetworkName("test"),
	InitialPowerTable: []gpbft.PowerEntry{
		{
			ID:     0,
			PubKey: gpbft.PubKey{0},
			Power:  big.NewInt(1),
		},
		{
			ID:     1,
			PubKey: gpbft.PubKey{1},
			Power:  big.NewInt(1),
		},
	},
	PowerUpdate: []certs.PowerTableDelta{
		{
			ParticipantID: 2,
			PowerDelta:    big.NewInt(1),
			SigningKey:    gpbft.PubKey{0},
		},
		{
			ParticipantID: 3,
			PowerDelta:    big.NewInt(1),
			SigningKey:    gpbft.PubKey{1},
		},
	},
	GpbftConfig: &manifest.GpbftConfig{
		Delta:                10,
		DeltaBackOffExponent: 0.2,
		MaxLookaheadRounds:   10,
	},
	EcConfig: &manifest.EcConfig{
		ECFinality:       900,
		CommiteeLookback: 5,
		ECDelay:          30 * time.Second,

		ECPeriod: 30 * time.Second,
	},
}

func TestManifestSerialiation(t *testing.T) {
	b, err := baseManifest.Marshal()
	require.NoError(t, err)

	var m2 manifest.Manifest
	fmt.Println(string(b))

	err = m2.Unmarshal(bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, baseManifest, m2)
}

func TestManifestVersion(t *testing.T) {
	m := baseManifest
	v1, err := m.Version()
	require.NoError(t, err)
	v2, err := baseManifest.Version()
	require.NoError(t, err)
	require.Equal(t, v1, v2)

	m.Delta = 1
	m.NetworkName = "test2"
	v1, err = m.Version()
	require.NoError(t, err)
	require.NotEqual(t, v1, v2)

}

func TestNetworkName(t *testing.T) {
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
		test := test
		t.Run(test.name, func(t *testing.T) {
			m := manifest.Manifest{NetworkName: test.subject}
			gotDsPrefix := m.DatastorePrefix().String()
			require.True(t, strings.HasPrefix(gotDsPrefix, "/f3"))
			require.True(t, strings.HasSuffix(gotDsPrefix, string(test.subject)))
			gotPubSubTopic := m.PubSubTopic()
			require.True(t, strings.HasPrefix(gotPubSubTopic, "/f3/granite/0.0.1/"))
			require.True(t, strings.HasSuffix(gotPubSubTopic, string(test.subject)))
		})
	}
}
