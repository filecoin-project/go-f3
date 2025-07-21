package observer

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
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

	rotatePath       string
	rotateInterval   time.Duration
	retention        time.Duration
	maxRetentionSize int64

	pubSub                  *pubsub.PubSub
	pubSubValidatorDisabled bool

	dataSourceName string

	maxBatchSize  int
	maxBatchDelay time.Duration

	ecPeriod                          time.Duration
	initialPowerTableCID              cid.Cid
	finalityCertsClientRequestTimeout time.Duration
	finalityCertsStorePath            string
	finalityCertsVerifier             gpbft.Verifier
	finalityCertsInitialPollInterval  time.Duration
	finalityCertsMinPollInterval      time.Duration
	finalityCertsMaxPollInterval      time.Duration

	chainExchangeBufferSize    int
	chainExchangeMaxMessageAge time.Duration
}

func newOptions(opts ...Option) (*options, error) {
	opt := options{
		messageBufferSize:                 100,
		subBufferSize:                     1024,
		connectivityCheckInterval:         10 * time.Second,
		connectivityConcurrency:           10,
		connectivityDHTThreshold:          5,
		queryServerReadTimeout:            5 * time.Second,
		rotatePath:                        ".",
		rotateInterval:                    10 * time.Minute,
		retention:                         -1,
		maxBatchSize:                      1000,
		maxBatchDelay:                     time.Minute,
		ecPeriod:                          30 * time.Second,
		finalityCertsClientRequestTimeout: 10 * time.Second,
		finalityCertsVerifier:             blssig.VerifierWithKeyOnG1(),
		finalityCertsInitialPollInterval:  10 * time.Second,
		finalityCertsMinPollInterval:      30 * time.Second,
		finalityCertsMaxPollInterval:      2 * time.Minute,
		chainExchangeBufferSize:           1000,
		chainExchangeMaxMessageAge:        3 * time.Minute,
		maxRetentionSize:                  0,
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

// WithMaxRetentionSize sets the maximum size of the retention directory.
// This is weakly enforced, and the directory may grow larger than this
// size. If the directory grows larger than this size, the oldest files
// will be deleted until the directory size is below this size.
func WithMaxRetentionSize(size uint64) Option {
	return func(o *options) error {
		if size > math.MaxInt64 {
			return fmt.Errorf("max retention size must be less than or equal to %d", math.MaxInt64)
		}
		o.maxRetentionSize = int64(size)
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

func WithMaxBatchSize(size int) Option {
	return func(o *options) error {
		o.maxBatchSize = size
		return nil
	}
}

func WithMaxBatchDelay(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return fmt.Errorf("max batch delay must be greater than or equal to 0")
		}
		o.maxBatchDelay = d
		return nil
	}
}

func WithECPeriod(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("ec period must be greater than 0")
		}
		o.ecPeriod = d
		return nil
	}
}

func WithInitialPowerTableCID(c cid.Cid) Option {
	return func(o *options) error {
		if c == cid.Undef {
			return fmt.Errorf("initial power table CID must be defined")
		}
		o.initialPowerTableCID = c
		return nil
	}
}

func WithFinalityCertsClientRequestTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("finality certs client request timeout must be greater than 0")
		}
		o.finalityCertsClientRequestTimeout = d
		return nil
	}
}

func WithFinalityCertsStorePath(path string) Option {
	return func(o *options) error {
		o.finalityCertsStorePath = path
		return nil
	}
}

func WithFinalityCertsVerifier(v gpbft.Verifier) Option {
	return func(o *options) error {
		if v == nil {
			return fmt.Errorf("finality certs verifier cannot be nil")
		}
		o.finalityCertsVerifier = v
		return nil
	}
}

func WithFinalityCertsInitialPollInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("finality certs initial poll interval must be greater than 0")
		}
		o.finalityCertsInitialPollInterval = d
		return nil
	}
}

func WithFinalityCertsMinPollInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("finality certs minimum poll interval must be greater than 0")
		}
		o.finalityCertsMinPollInterval = d
		return nil
	}
}

func WithFinalityCertsMaxPollInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("finality certs maximum poll interval must be greater than 0")
		}
		o.finalityCertsMaxPollInterval = d
		return nil
	}
}

func WithChainExchangeBufferSize(size int) Option {
	return func(o *options) error {
		if size < 1 {
			return fmt.Errorf("chain exchange buffer size must be at least 1")
		}
		o.chainExchangeBufferSize = size
		return nil
	}
}

func WithChainExchangeMaxMessageAge(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("chain exchange max message age must be greater than 0")
		}
		o.chainExchangeMaxMessageAge = d
		return nil
	}
}
