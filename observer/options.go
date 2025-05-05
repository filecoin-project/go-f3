package observer

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multiaddr-dns"
)

type Option func(*options) error

type options struct {
	host                       host.Host
	connectivityBootstrapPeers []peer.AddrInfo
	messageBufferSize          int
	subBufferSize              int
	networkName                gpbft.NetworkName

	connectivityCheckInterval          time.Duration
	connectivityConcurrency            int
	connectivityBootstrappersThreshold int
	connectivityDHTThreshold           int
	connectivityLotusPeersThreshold    int
	connectivityLotusAPIEndpoints      []string

	queryServerListenAddress string
	queryServerReadTimeout   time.Duration

	rotatePath     string
	rotateInterval time.Duration
	retention      time.Duration

	pubSub                  *pubsub.PubSub
	pubSubValidatorDisabled bool

	dataSourceName string
}

func newOptions(opts ...Option) (*options, error) {
	opt := options{
		messageBufferSize:         100,
		subBufferSize:             1024,
		connectivityCheckInterval: 10 * time.Second,
		connectivityConcurrency:   10,
		connectivityDHTThreshold:  5,
		queryServerReadTimeout:    5 * time.Second,
		rotatePath:                ".",
		rotateInterval:            10 * time.Minute,
		retention:                 -1,
	}
	for _, apply := range opts {
		if err := apply(&opt); err != nil {
			return nil, err
		}
	}

	var err error
	if opt.host == nil {
		opt.host, err = libp2p.New()
		if err != nil {
			return nil, err
		}
	}
	if opt.networkName == "" {
		return nil, fmt.Errorf("network name must be provided")
	}
	return &opt, nil
}

func WithHost(h host.Host) Option {
	return func(o *options) error {
		o.host = h
		return nil
	}
}

func WithPubSubValidatorDisabled(disable bool) Option {
	return func(o *options) error {
		o.pubSubValidatorDisabled = disable
		return nil
	}
}

func WithNetworkName(name gpbft.NetworkName) Option {
	return func(o *options) error {
		o.networkName = name
		return nil
	}
}

// WithBootstrapPeers sets the bootstrap peers for connectivity. The threshold
// is the minimum connectivity threshold below which the bootstrap peers are
// used to improve connectivity. Disabled if the threshold is set to 0 or no peers are provided.
//
// See: WithBootstrapPeersFromString.
func WithBootstrapPeers(threshold int, peers ...peer.AddrInfo) Option {
	return func(o *options) error {
		o.connectivityBootstrapPeers = peers
		o.connectivityBootstrappersThreshold = threshold
		return nil
	}
}

// WithBootstrapPeersFromString sets the bootstrap peers for connectivity. The
// threshold is the minimum connectivity threshold below which the bootstrap
// peers are used to improve connectivity. Disabled if the threshold is set to 0
// or no peers are provided.
//
// Any provided string addresses are resolved with a timeout of 10 seconds. An
// error is returned if any one of given addresses fail to resolve.
//
// See: WithBootstrapPeers.
func WithBootstrapPeersFromString(threshold int, peers ...string) Option {
	const maddrResolutionTimeout = 10 * time.Second
	return func(o *options) error {
		o.connectivityBootstrapPeers = make([]peer.AddrInfo, 0, len(peers))
		for _, v := range peers {
			maddr, err := multiaddr.NewMultiaddr(v)
			if err != nil {
				return fmt.Errorf("invalid multiaddr: %q: %w", v, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), maddrResolutionTimeout)
			defer cancel()
			resolved, err := madns.Resolve(ctx, maddr)
			if err != nil {
				return fmt.Errorf("failed to resolve multiaddr: %q: %w", v, err)
			}
			for _, maddr := range resolved {
				addr, err := peer.AddrInfoFromP2pAddr(maddr)
				if err != nil {
					return fmt.Errorf("invalid bootstrap address: %q: %w", v, err)
				}
				o.connectivityBootstrapPeers = append(o.connectivityBootstrapPeers, *addr)
			}
		}
		o.connectivityBootstrappersThreshold = threshold
		return nil
	}
}

// WithDHTPeerDiscovery sets the threshold for peer discovery via Filecoin DHT.
// Disabled if set to zero.
func WithDHTPeerDiscovery(threshold int) Option {
	return func(o *options) error {
		o.connectivityDHTThreshold = threshold
		return nil
	}
}

// WithLotusPeerDiscovery configures peer discovery via Filecoin.NetPeers API
// call through a list of lotus daemons. Disabled if threshold is set to 0 or no
// lotusDaemon endpoints are provided.
func WithLotusPeerDiscovery(threshold int, apiEndpoints ...string) Option {
	return func(o *options) error {
		o.connectivityLotusPeersThreshold = threshold
		o.connectivityLotusAPIEndpoints = apiEndpoints
		return nil
	}
}

func WithMessageBufferSize(messageBufferSize int) Option {
	return func(o *options) error {
		o.messageBufferSize = messageBufferSize
		return nil
	}
}

func WithSubscriptionBufferSize(subBufferSize int) Option {
	return func(o *options) error {
		o.subBufferSize = subBufferSize
		return nil
	}
}

func WithConnectivityCheckInterval(interval time.Duration) Option {
	return func(o *options) error {
		o.connectivityCheckInterval = interval
		return nil
	}
}

// WithMaxConcurrentConnectionAttempts sets the maximum number of concurrent
// connection attempts to make to peers. This is used to limit the number of
// concurrent connection attempts to make to peers when the connectivity
// threshold is below the specified threshold for any one of the configured
// connectivity repair mechanisms. The default is 50.
func WithMaxConcurrentConnectionAttempts(limit int) Option {
	return func(o *options) error {
		if limit < 1 {
			return fmt.Errorf("max concurrent connection attempts must be greater than 0")
		}
		o.connectivityConcurrency = limit
		return nil
	}
}

func WithConnectivityMinPeers(count int) Option {
	return func(o *options) error {
		o.connectivityBootstrappersThreshold = count
		return nil
	}
}

func WithQueryServerListenAddress(addr string) Option {
	return func(o *options) error {
		o.queryServerListenAddress = addr
		return nil
	}
}

func WithQueryServerReadTimeout(d time.Duration) Option {
	return func(o *options) error {
		o.queryServerReadTimeout = d
		return nil
	}
}

func WithRotatePath(path string) Option {
	return func(o *options) error {
		o.rotatePath = path
		return nil
	}
}

func WithRotateInterval(d time.Duration) Option {
	return func(o *options) error {
		o.rotateInterval = d
		return nil
	}
}

func WithRetention(retention time.Duration) Option {
	return func(o *options) error {
		o.retention = retention
		return nil
	}
}

func WithDataSourceName(dataSourceName string) Option {
	return func(o *options) error {
		o.dataSourceName = dataSourceName
		return nil
	}
}

func WithPubSub(ps *pubsub.PubSub) Option {
	return func(o *options) error {
		o.pubSub = ps
		return nil
	}
}
