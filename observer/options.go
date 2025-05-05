package observer

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
)

type Option func(*options) error

// NetworkNameChangeListener returns a channel that is notified whenever the
// network name changes. The channel should never be closed.
type NetworkNameChangeListener func(context.Context, *pubsub.PubSub) (<-chan gpbft.NetworkName, error)

type options struct {
	host                      host.Host
	bootstrapAddrs            []peer.AddrInfo
	messageBufferSize         int
	networkNameChangeListener NetworkNameChangeListener

	connectivityCheckInterval time.Duration
	connectivityMinPeers      int

	queryServerListenAddress string
	queryServerReadTimeout   time.Duration

	rotatePath     string
	rotateInterval time.Duration
	retention      time.Duration

	pubSub                *pubsub.PubSub
	dontRegisterValidator bool

	dataSourceName string
}

func newOptions(opts ...Option) (*options, error) {
	opt := options{
		messageBufferSize:         100,
		connectivityMinPeers:      5,
		connectivityCheckInterval: 10 * time.Second,
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
	if opt.networkNameChangeListener == nil {
		return nil, fmt.Errorf("network name change listener must be provided")
	}
	return &opt, nil
}

func WithHost(h host.Host) Option {
	return func(o *options) error {
		o.host = h
		return nil
	}
}

func WithNoValidator(dontRegister bool) Option {
	return func(o *options) error {
		o.dontRegisterValidator = dontRegister
		return nil
	}
}

func WithStaticNetworkName(name gpbft.NetworkName) Option {
	return WithNetworkNameChangeListener(
		func(ctx context.Context, _ *pubsub.PubSub) (<-chan gpbft.NetworkName, error) {
			networkChanged := make(chan gpbft.NetworkName, 1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case networkChanged <- name:
				return networkChanged, nil
			}
		},
	)
}

func WithDynamicNetworkNameFromManifestProvider(id peer.ID) Option {
	return WithNetworkNameChangeListener(
		func(ctx context.Context, pubSub *pubsub.PubSub) (<-chan gpbft.NetworkName, error) {
			provider, err := manifest.NewDynamicManifestProvider(pubSub, id)
			if err != nil {
				return nil, err
			}
			if err := provider.Start(ctx); err != nil {
				return nil, err
			}

			networkChanged := make(chan gpbft.NetworkName, 1)
			notifyLatestChange := func(change gpbft.NetworkName) {
				for ctx.Err() == nil {
					select {
					case <-ctx.Done():
						return
					case <-networkChanged:
						// Silently consume unseen network change.
					case networkChanged <- change:
						return
					}
				}
			}

			go func() {
				defer func() {
					if err := provider.Stop(context.Background()); err != nil {
						logger.Warnw("Failed to stop dynamic manifest provider gracefully", "err", err)
					}
				}()
				for ctx.Err() == nil {
					select {
					case <-ctx.Done():
						return
					case updated := <-provider.ManifestUpdates():
						notifyLatestChange(updated.NetworkName)
					}
				}
			}()
			return networkChanged, nil
		},
	)
}

func WithNetworkNameChangeListener(listener NetworkNameChangeListener) Option {
	return func(o *options) error {
		if listener == nil {
			return fmt.Errorf("listener must not be nil")
		}
		o.networkNameChangeListener = listener
		return nil
	}
}

func WithBootstrapAddrs(addrs ...peer.AddrInfo) Option {
	return func(o *options) error {
		o.bootstrapAddrs = addrs
		return nil
	}
}

func WithBootstrapAddrsFromString(addrs ...string) Option {
	const maddrResolutionTimeout = 10 * time.Second
	return func(o *options) error {
		o.bootstrapAddrs = make([]peer.AddrInfo, 0, len(addrs))
		for _, v := range addrs {
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
				o.bootstrapAddrs = append(o.bootstrapAddrs, *addr)
			}
		}
		return nil
	}
}

func WithMessageBufferSize(messageBufferSize int) Option {
	return func(o *options) error {
		o.messageBufferSize = messageBufferSize
		return nil
	}
}
func WithConnectivityCheckInterval(interval time.Duration) Option {
	return func(o *options) error {
		o.connectivityCheckInterval = interval
		return nil
	}
}

func WithConnectivityMinPeers(count int) Option {
	return func(o *options) error {
		o.connectivityMinPeers = count
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
