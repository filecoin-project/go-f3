package gpbft_test

import (
	"strings"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

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
