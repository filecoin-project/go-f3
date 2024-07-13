package manifest_test

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/stretchr/testify/require"
)

var base manifest.Manifest = manifest.Manifest{
	BootstrapEpoch: 10,
	NetworkName:    gpbft.NetworkName("test"),
	ExplicitPower: gpbft.PowerEntries{
		{
			ID:     2,
			Power:  big.NewInt(1),
			PubKey: gpbft.PubKey{0},
		},
		{
			ID:     3,
			Power:  big.NewInt(1),
			PubKey: gpbft.PubKey{1},
		},
	},
	GpbftConfig: manifest.GpbftConfig{
		Delta:                10,
		DeltaBackOffExponent: 0.2,
		MaxLookaheadRounds:   10,
	},
	EcConfig: manifest.EcConfig{
		ECFinality:               900,
		CommitteeLookback:        5,
		ECDelayMultiplier:        2.0,
		ECPeriod:                 30 * time.Second,
		BaseDecisionBackoffTable: []float64{1.3, 1.69, 2.2, 2.86, 3.71, 4.83, 6.27, 8.16, 10.6, 13.79, 15.},
	},
	CxConfig: manifest.CxConfig{
		ClientRequestTimeout: 10 * time.Second,
		ServerRequestTimeout: time.Minute,
		MinimumPollInterval:  30 * time.Second,
		MaximumPollInterval:  2 * time.Minute,
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
