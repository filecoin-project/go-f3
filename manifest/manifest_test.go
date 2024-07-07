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

var base manifest.Manifest = manifest.Manifest{
	BootstrapEpoch: 10,
	NetworkName:    gpbft.NetworkName("test"),
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
		ECFinality:               900,
		CommiteeLookback:         5,
		ECDelayMultiplier:        2.0,
		ECPeriod:                 30 * time.Second,
		BaseDecisionBackoffTable: []float64{1.3, 1.69, 2.2, 2.86, 3.71, 4.83, 6.27, 8.16, 10.6, 13.79, 15.},
	},
}

func TestManifest_Serialization(t *testing.T) {
	b, err := base.Marshal()
	require.NoError(t, err)

	var m2 manifest.Manifest
	fmt.Println(string(b))

	err = m2.Unmarshal(bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, base, m2)
}

func TestManifest_Version(t *testing.T) {
	m := base
	v1, err := m.Version()
	require.NoError(t, err)
	v2, err := base.Version()
	require.NoError(t, err)
	require.Equal(t, v1, v2)

	m.Delta = 1
	m.NetworkName = "test2"
	v1, err = m.Version()
	require.NoError(t, err)
	require.NotEqual(t, v1, v2)

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
