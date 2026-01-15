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
	BootstrapEpoch:    900,
	NetworkName:       "test",
	CommitteeLookback: 10,
	Gpbft: manifest.GpbftConfig{
		Delta:                      10 * time.Second,
		DeltaBackOffExponent:       1.2,
		DeltaBackOffMax:            1 * time.Hour,
		QualityDeltaMultiplier:     1.0,
		MaxLookaheadRounds:         5,
		ChainProposedLength:        gpbft.ChainDefaultLen,
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
	PubSub:                manifest.DefaultPubSubConfig,
	ChainExchange:         manifest.DefaultChainExchangeConfig,
	PartialMessageManager: manifest.DefaultPartialMessageManagerConfig,
	CatchUpAlignment:      0,
}

func TestManifest_Validation(t *testing.T) {
	require.NoError(t, base.Validate())
	localDevnetManifest := manifest.LocalDevnetManifest()
	require.NoError(t, localDevnetManifest.Validate())

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
		want  manifest.Manifest
	}{
		{
			name:  "base",
			given: baseMarshalled,
			want:  base,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := manifest.Unmarshal(bytes.NewReader(test.given))
			require.NoError(t, err)
			require.Equal(t, &test.want, got)
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
			require.True(t, strings.HasPrefix(gotPubSubTopic, "/f3/granite/0.0.3/"))
			require.True(t, strings.HasSuffix(gotPubSubTopic, string(test.subject)))
		})
	}
}

func TestManifest_CID(t *testing.T) {
	t.Parallel()

	const (
		wantLocalDevnetCid = "baguqfiheaiqgpujeb5upzhbblchkuc2sxeis2y5upbsefktyspqqmjrcr27fiua"
		wantAfterUpdateCid = "baguqfiheaiqaixtgfxvyaqdkdy6nsdybpvwjw7vgtw22enjg24kunho3b5nrduq"
	)
	subject := manifest.LocalDevnetManifest()
	// Use a fixed network name for deterministic CID calculation.
	subject.NetworkName = "fish"

	got, err := subject.Cid()
	require.NoError(t, err)
	require.Equal(t, wantLocalDevnetCid, got.String())

	changedSubject := subject
	changedSubject.CommitteeLookback = changedSubject.CommitteeLookback + 1
	gotAfterUpdate, err := changedSubject.Cid()
	require.NoError(t, err)
	require.NotEqual(t, subject, changedSubject)
	require.NotEqual(t, wantLocalDevnetCid, gotAfterUpdate.String())
	require.Equal(t, wantAfterUpdateCid, gotAfterUpdate.String())
}
