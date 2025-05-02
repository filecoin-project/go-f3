package gpbft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVrfSerializeSigInput(t *testing.T) {
	for _, tc := range []struct {
		name        string
		beacon      []byte
		instance    uint64
		round       uint64
		networkName NetworkName
		want        []byte
	}{
		{
			name:        "empty",
			beacon:      []byte{},
			instance:    0,
			round:       0,
			networkName: "",
			want:        []byte("VRF:::\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
		},
		{
			name:        "basic",
			beacon:      []byte("🥓"),
			instance:    1,
			round:       2,
			networkName: "testnet",
			want:        []byte("VRF:testnet:🥓:\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02"),
		},
		{
			name:        "long values",
			beacon:      bytes.Repeat([]byte{0xff}, 20),
			instance:    1<<63 - 1,
			round:       1<<63 - 1,
			networkName: "longnetworkname",
			want:        append(append([]byte("VRF:longnetworkname:"), bytes.Repeat([]byte{0xff}, 20)...), ':', 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := vrfSerializeSigInput(tc.beacon, tc.instance, tc.round, tc.networkName)
			require.Equal(t, tc.want, actual)
		})
	}
}
