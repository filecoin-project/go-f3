package f3_test

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

var baseManifest f3.Manifest = f3.Manifest{
	Sequence:             0,
	UpgradeEpoch:         10,
	ReBootstrap:          true,
	NetworkName:          gpbft.NetworkName("test"),
	EcStabilisationDelay: 10,
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
	GpbftConfig: &f3.GpbftConfig{
		Delta:                10,
		DeltaBackOffExponent: 0.2,
		MaxLookaheadRounds:   10,
	},
}

func TestManifestSerialiation(t *testing.T) {
	b, err := baseManifest.Marshal()
	require.NoError(t, err)

	var m2 f3.Manifest
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
