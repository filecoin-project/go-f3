package observer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithBootstrapAddrsFromString_Resolution(t *testing.T) {
	for _, tt := range []struct {
		name  string
		addrs []string
	}{
		{
			name:  "dnsaddr",
			addrs: []string{"/dnsaddr/api.drand.sh"},
		},
		{
			name:  "dns",
			addrs: []string{"/dns/example.com/tcp/1347/p2p/12D3KooWGotz7nQavdkncoFb8QoX2YmfnjF1RuSz3PfuEhYUGrxr"},
		},
		{
			name:  "ip",
			addrs: []string{"/ip4/127.0.0.1/tcp/1347/p2p/12D3KooWGotz7nQavdkncoFb8QoX2YmfnjF1RuSz3PfuEhYUGrxr"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			apply := WithBootstrapAddrsFromString(tt.addrs...)
			require.NotNil(t, apply)
			got := &options{}
			require.NoError(t, apply(got))
			require.NotEmpty(t, tt.addrs, got.bootstrapAddrs)
		})
	}
}
