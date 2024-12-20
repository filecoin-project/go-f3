package chainexchange

import (
	"errors"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/manifest"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type Option func(*options) error

type options struct {
	topicName                      string
	topicScoreParams               *pubsub.TopicScoreParams
	subscriptionBufferSize         int
	pubsub                         *pubsub.PubSub
	progress                       gpbft.Progress
	maxChainLength                 int
	maxInstanceLookahead           uint64
	maxDiscoveredChainsPerInstance int
	maxWantedChainsPerInstance     int
	listener                       Listener
}

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		topicScoreParams:               psutil.PubsubTopicScoreParams,
		subscriptionBufferSize:         32,
		maxChainLength:                 gpbft.ChainMaxLen,
		maxInstanceLookahead:           manifest.DefaultCommitteeLookback,
		maxDiscoveredChainsPerInstance: 1000,
		maxWantedChainsPerInstance:     1000,
	}
	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	if opts.progress == nil {
		return nil, errors.New("gpbft progress must be set")
	}
	if opts.pubsub == nil {
		return nil, errors.New("pubsub must be set")
	}
	if opts.topicName == "" {
		return nil, errors.New("topic name must be set")
	}
	return opts, nil
}

func WithTopicName(name string) Option {
	return func(o *options) error {
		if name == "" {
			return errors.New("topic name cannot be empty")
		}
		o.topicName = name
		return nil
	}
}

func WithTopicScoreParams(params *pubsub.TopicScoreParams) Option {
	return func(o *options) error {
		o.topicScoreParams = params
		return nil
	}
}

func WithSubscriptionBufferSize(size int) Option {
	return func(o *options) error {
		if size < 1 {
			return errors.New("subscription buffer size must be at least 1")
		}
		o.subscriptionBufferSize = size
		return nil
	}
}

func WithPubSub(pubsub *pubsub.PubSub) Option {
	return func(o *options) error {
		if pubsub == nil {
			return errors.New("pubsub cannot be nil")
		}
		o.pubsub = pubsub
		return nil
	}
}

func WithProgress(progress gpbft.Progress) Option {
	return func(o *options) error {
		if progress == nil {
			return errors.New("progress cannot be nil")
		}
		o.progress = progress
		return nil
	}
}

func WithMaxChainLength(length int) Option {
	return func(o *options) error {
		if length < 1 {
			return errors.New("max chain length must be at least 1")
		}
		o.maxChainLength = length
		return nil
	}
}

func WithMaxInstanceLookahead(lookahead uint64) Option {
	return func(o *options) error {
		o.maxInstanceLookahead = lookahead
		return nil
	}
}

func WithMaxDiscoveredChainsPerInstance(max int) Option {
	return func(o *options) error {
		if max < 1 {
			return errors.New("max discovered chains per instance must be at least 1")
		}
		o.maxDiscoveredChainsPerInstance = max
		return nil
	}
}

func WithMaxWantedChainsPerInstance(max int) Option {
	return func(o *options) error {
		if max < 1 {
			return errors.New("max wanted chains per instance must be at least 1")
		}
		o.maxWantedChainsPerInstance = max
		return nil
	}
}

func WithListener(listener Listener) Option {
	return func(o *options) error {
		if listener == nil {
			return errors.New("listener cannot be nil")
		}
		o.listener = listener
		return nil
	}
}
