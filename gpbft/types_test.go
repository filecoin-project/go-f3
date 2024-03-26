package gpbft_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func TestNewTipSetID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		subject  gpbft.TipSetID
		wantZero bool
	}{
		{
			name:     "zero-value struct is zero",
			wantZero: true,
		},
		{
			name:     "ZeroTipSetID is zero",
			subject:  gpbft.ZeroTipSetID(),
			wantZero: true,
		},
		{
			name:     "NewTipSet with zero values is zero",
			subject:  gpbft.NewTipSetID(nil),
			wantZero: true,
		},
		{
			name:    "Non-zero is not zero",
			subject: gpbft.NewTipSetID([]byte("fish")),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantZero, test.subject.IsZero())
			gotBytes := test.subject.Bytes()
			if test.wantZero {
				require.Empty(t, gotBytes)
			} else {
				require.NotEmpty(t, gotBytes)
			}
			require.Zero(t, test.subject.Compare(test.subject))
		})
	}
	t.Run("compare", func(t *testing.T) {
		wantFirst := gpbft.NewTipSetID([]byte("1 fish"))
		wantSecond := gpbft.NewTipSetID([]byte("2 lobster"))
		wantThird := gpbft.NewTipSetID([]byte("3 barreleye"))
		subject := []gpbft.TipSetID{wantSecond, wantFirst, wantThird}

		sort.Slice(subject, func(one, other int) bool {
			return subject[one].Compare(subject[other]) < 0
		})
		require.Equal(t, wantFirst, subject[0])
		require.Equal(t, wantSecond, subject[1])
		require.Equal(t, wantThird, subject[2])
	})
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
			gotDsPrefix := test.subject.DatastorePrefix().String()
			require.True(t, strings.HasPrefix(gotDsPrefix, "/f3"))
			require.True(t, strings.HasSuffix(gotDsPrefix, string(test.subject)))
			gotPubSubTopic := test.subject.PubSubTopic()
			require.True(t, strings.HasPrefix(gotPubSubTopic, "/f3/granite/0.0.1/"))
			require.True(t, strings.HasSuffix(gotPubSubTopic, string(test.subject)))
		})
	}
}
