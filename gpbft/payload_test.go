package gpbft_test

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func TestPayload_Eq(t *testing.T) {

	someChain, err := gpbft.NewChain(tipset0, tipSet1)
	require.NotNil(t, err)

	tests := []struct {
		name      string
		one       gpbft.Payload
		other     *gpbft.Payload
		wantEqual bool
	}{
		{
			name:      "zero-valued eq to self",
			one:       gpbft.Payload{},
			other:     &gpbft.Payload{},
			wantEqual: true,
		},
		{
			name: "zero-valued not eq to nil",
			one:  gpbft.Payload{},
		},
		{
			name: "fully populated eq to self",
			one: gpbft.Payload{
				Instance: 1,
				Round:    2,
				Step:     gpbft.TERMINATED_PHASE,
				SupplementalData: gpbft.SupplementalData{
					Commitments: [32]byte{1, 2, 3, 4},
					PowerTable:  []byte("fish"),
				},
				Value: someChain,
			},
			other: &gpbft.Payload{
				Instance: 1,
				Round:    2,
				Step:     gpbft.TERMINATED_PHASE,
				SupplementalData: gpbft.SupplementalData{
					Commitments: [32]byte{1, 2, 3, 4},
					PowerTable:  []byte("fish"),
				},
				Value: someChain,
			},
			wantEqual: true,
		},
		{
			name: "partly populated  not eq to self",
			one: gpbft.Payload{
				Instance: 1,
				Step:     gpbft.TERMINATED_PHASE,
				SupplementalData: gpbft.SupplementalData{
					Commitments: [32]byte{1, 2, 3, 4},
					PowerTable:  []byte("fish"),
				},
			},
			other: &gpbft.Payload{
				Instance: 1,
				Round:    2,
				Step:     gpbft.TERMINATED_PHASE,
				SupplementalData: gpbft.SupplementalData{
					Commitments: [32]byte{1, 2, 3, 4},
					PowerTable:  []byte("fish"),
				},
				Value: someChain,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantEqual, test.one.Eq(test.other))
		})
	}
}

func TestPayload_MarshalForSigning(t *testing.T) {
	someChain, err := gpbft.NewChain(tipset0, tipSet1)
	require.NotNil(t, err)

	tests := []struct {
		name        string
		subject     gpbft.Payload
		networkName gpbft.NetworkName
		want        []byte
	}{
		{
			name:    "zero-valued with empty network name",
			subject: gpbft.Payload{},
			want: []byte{
				0x47, 0x50, 0x42, 0x46, 0x54, 0x3a, 0x3a, 0x00, // DOMAIN_SEPARATION_TAG ":" network name
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name: "",
			subject: gpbft.Payload{
				Instance: 1,
				Round:    2,
				Step:     gpbft.TERMINATED_PHASE,
				SupplementalData: gpbft.SupplementalData{
					Commitments: [32]byte([]byte("🐡 fish unda da sea 🪼  🌊")),
					PowerTable:  []byte("barreleye"),
				},
				Value: someChain,
			},
			networkName: "🐠",
			want: []byte{
				0x47, 0x50, 0x42, 0x46, 0x54, 0x3a, 0xf0, 0x9f,
				0x90, 0xa0, 0x3a, 0x6, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x1, 0xf0, 0x9f, 0x90, 0xa1,
				0x20, 0x66, 0x69, 0x73, 0x68, 0x20, 0x75, 0x6e,
				0x64, 0x61, 0x20, 0x64, 0x61, 0x20, 0x73, 0x65,
				0x61, 0x20, 0xf0, 0x9f, 0xaa, 0xbc, 0x20, 0x20,
				0xf0, 0x9f, 0x8c, 0x8a, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x62, 0x61, 0x72, 0x72,
				0x65, 0x6c, 0x65, 0x79, 0x65},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := test.subject.MarshalForSigning(test.networkName)
			require.NotEmpty(t, got)
			require.True(t, bytes.HasPrefix(got, []byte(gpbft.DOMAIN_SEPARATION_TAG+":"+test.networkName+":")))
			require.Equal(t, test.want, got)
		})
	}
}
