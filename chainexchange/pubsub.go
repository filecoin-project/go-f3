package chainexchange

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/merkle"
	lru "github.com/hashicorp/golang-lru/v2"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	log = logging.Logger("f3/chainexchange")

	_ ChainExchange      = (*PubSubChainExchange)(nil)
	_ pubsub.ValidatorEx = (*PubSubChainExchange)(nil).validatePubSubMessage

	chainPortionPlaceHolder = &chainPortion{}
)

type chainPortion struct {
	chain gpbft.ECChain
}

type PubSubChainExchange struct {
	*options

	// mu guards access to chains and API calls.
	mu                   sync.Mutex
	chainsWanted         map[uint64]*lru.Cache[string, *chainPortion]
	chainsDiscovered     map[uint64]*lru.Cache[string, *chainPortion]
	pendingCacheAsWanted chan Message
	topic                *pubsub.Topic
	stop                 func() error
}

func NewPubSubChainExchange(o ...Option) (*PubSubChainExchange, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &PubSubChainExchange{
		options:              opts,
		chainsWanted:         map[uint64]*lru.Cache[string, *chainPortion]{},
		chainsDiscovered:     map[uint64]*lru.Cache[string, *chainPortion]{},
		pendingCacheAsWanted: make(chan Message, 100), // TODO: parameterise.
	}, nil
}

func (p *PubSubChainExchange) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.pubsub.RegisterTopicValidator(p.topicName, p.validatePubSubMessage); err != nil {
		return fmt.Errorf("failed to register topic validator: %w", err)
	}
	var err error
	p.topic, err = p.pubsub.Join(p.topicName, pubsub.WithTopicMessageIdFn(psutil.ChainExchangeMessageIdFn))
	if err != nil {
		return fmt.Errorf("failed to join topic '%s': %w", p.topicName, err)
	}
	if p.topicScoreParams != nil {
		if err := p.topic.SetScoreParams(p.topicScoreParams); err != nil {
			// This can happen most likely due to router not supporting peer scoring. It's
			// non-critical. Hence, the warning log.
			log.Warnw("failed to set topic score params", "err", err)
		}
	}
	subscription, err := p.topic.Subscribe(pubsub.WithBufferSize(p.subscriptionBufferSize))
	if err != nil {
		_ = p.topic.Close()
		p.topic = nil
		return fmt.Errorf("failed to subscribe to topic '%s': %w", p.topicName, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for ctx.Err() == nil {
			msg, err := subscription.Next(ctx)
			if err != nil {
				log.Debugw("failed to read next message from subscription", "err", err)
				continue
			}
			cmsg := msg.ValidatorData.(Message)
			p.cacheAsDiscoveredChain(ctx, cmsg)
		}
		log.Debug("Stopped reading messages from chainexchange subscription.")
	}()
	go func() {
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case cmsg := <-p.pendingCacheAsWanted:
				p.cacheAsWantedChain(ctx, cmsg)
			}
		}
		log.Debug("Stopped caching chains as wanted.")
	}()
	p.stop = func() error {
		cancel()
		subscription.Cancel()
		_ = p.pubsub.UnregisterTopicValidator(p.topicName)
		_ = p.topic.Close()
		return nil
	}
	return nil
}

func (p *PubSubChainExchange) Key(chain gpbft.ECChain) Key {
	if chain.IsZero() {
		return nil
	}
	length := len(chain)
	values := make([][]byte, length)
	for i := range length {
		values[i] = chain[i].MarshalForSigning()
	}
	rootDigest := merkle.Tree(values)
	return rootDigest[:]
}

func (p *PubSubChainExchange) GetChainByInstance(ctx context.Context, instance uint64, key Key) (gpbft.ECChain, bool) {

	// We do not have to take instance as input, and instead we can just search
	// through all the instance as they are not expected to be more than 10. The
	// reason we do take it, however, is because:
	// * That information is readily available by the caller.
	// * It helps in optimising the search by limiting the search space to the
	//   instance, since PubSubChainExchange groups chains by instance for ease of
	//   removal.

	if key.IsZero() {
		return nil, false
	}

	cacheKey := string(key)

	// Check wanted keys first.

	wanted := p.getChainsWantedAt(instance)
	if portion, found := wanted.Get(cacheKey); found && !portion.IsPlaceholder() {
		return portion.chain, true
	}

	// Check if the chain for the key is discovered.
	discovered := p.getChainsDiscoveredAt(instance)
	if portion, found := discovered.Get(cacheKey); found {
		// Add it to the wanted cache and remove it from the discovered cache.
		wanted.Add(cacheKey, portion)
		discovered.Remove(cacheKey)

		chain := portion.chain
		if p.listener != nil {
			p.listener.NotifyChainDiscovered(ctx, key, instance, chain)
		}
		// TODO: Do we want to pull all the suffixes of the chain into wanted cache?
		return chain, true
	}

	// Otherwise, add a placeholder for the wanted key as a way to prioritise its
	// retention via LRU recent-ness.
	wanted.ContainsOrAdd(cacheKey, chainPortionPlaceHolder)
	return nil, false
}

func (p *PubSubChainExchange) getChainsWantedAt(instance uint64) *lru.Cache[string, *chainPortion] {
	p.mu.Lock()
	defer p.mu.Unlock()
	wanted, exists := p.chainsWanted[instance]
	if !exists {
		wanted = p.newChainPortionCache(p.maxWantedChainsPerInstance)
		p.chainsWanted[instance] = wanted
	}
	return wanted
}

func (p *PubSubChainExchange) getChainsDiscoveredAt(instance uint64) *lru.Cache[string, *chainPortion] {
	p.mu.Lock()
	defer p.mu.Unlock()
	discovered, exists := p.chainsDiscovered[instance]
	if !exists {
		discovered = p.newChainPortionCache(p.maxDiscoveredChainsPerInstance)
		p.chainsDiscovered[instance] = discovered
	}
	return discovered
}

func (p *PubSubChainExchange) newChainPortionCache(capacity int) *lru.Cache[string, *chainPortion] {
	cache, err := lru.New[string, *chainPortion](capacity)
	if err != nil {
		// This can only happen if the cache size is negative, which is validated via
		// options. Its occurrence for the purposes of chain exchange indicates a
		// programmer error.
		log.Fatalw("Failed to instantiate chain portion cache", "capacity", capacity, "err", err)
	}
	return cache
}

func (p *PubSubChainExchange) validatePubSubMessage(_ context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	var cmsg Message
	buf := bytes.NewBuffer(msg.Data)
	if err := cmsg.UnmarshalCBOR(buf); err != nil {
		log.Debugw("failed to decode message", "from", msg.GetFrom(), "err", err)
		return pubsub.ValidationReject
	}
	if cmsg.Chain.IsZero() {
		// No peer should broadcast a zero-length chain.
		return pubsub.ValidationReject
	}
	if err := cmsg.Chain.Validate(); err != nil {
		// Invalid chain.
		log.Debugw("Invalid chain", "from", msg.GetFrom(), "err", err)
		return pubsub.ValidationReject
	}
	switch current := p.progress(); {
	case
		cmsg.Instance < current.ID,
		cmsg.Instance > current.ID+p.maxInstanceLookahead:
		// Too far ahead or too far behind.
		return pubsub.ValidationIgnore
	}
	now := time.Now().Unix()
	lowerBound := now - int64(p.maxTimestampAge.Seconds())
	if lowerBound > cmsg.Timestamp || cmsg.Timestamp > now {
		// The timestamp is too old or too far ahead. Ignore the message to avoid
		// affecting peer scores.
		return pubsub.ValidationIgnore
	}
	// TODO: wire in the current base chain from an on-going instance to further
	//       tighten up validation.
	msg.ValidatorData = cmsg
	return pubsub.ValidationAccept
}

func (p *PubSubChainExchange) cacheAsDiscoveredChain(ctx context.Context, cmsg Message) {

	wanted := p.getChainsDiscoveredAt(cmsg.Instance)
	discovered := p.getChainsDiscoveredAt(cmsg.Instance)

	for offset := len(cmsg.Chain); offset >= 0 && ctx.Err() == nil; offset-- {
		// TODO: Expose internals of merkle.go so that keys can be generated
		//       cumulatively for a more efficient prefix chain key generation.
		prefix := cmsg.Chain.Prefix(offset)
		key := p.Key(prefix)
		cacheKey := string(key)
		if portion, found := wanted.Peek(cacheKey); !found {
			// Not a wanted key; add it to discovered chains if they are not there already,
			// i.e. without modifying the recent-ness of any of the discovered values.
			discovered.ContainsOrAdd(cacheKey, &chainPortion{
				chain: prefix,
			})
		} else if portion.IsPlaceholder() {
			// It is a wanted key with a placeholder; replace the placeholder with the actual
			// discovery.
			wanted.Add(cacheKey, &chainPortion{
				chain: prefix,
			})
		}
		// Nothing to do; the discovered value is already in the wanted chains with
		// discovered value.

		// Continue with the remaining prefix keys as we do not know if any of them have
		// been evicted from the cache or not. This should be cheap enough considering the
		// added complexity of tracking evictions relative to chain prefixes.
	}
}

func (p *PubSubChainExchange) Broadcast(ctx context.Context, msg Message) error {

	// Optimistically cache the broadcast chain and all of its prefixes as wanted.
	select {
	case p.pendingCacheAsWanted <- msg:
	case <-ctx.Done():
		return ctx.Err()
	default:
		log.Warnw("Dropping wanted cache entry. Chain exchange is too slow to process chains as wanted", "msg", msg)
	}

	// TODO: integrate zstd compression.
	var buf bytes.Buffer
	if err := msg.MarshalCBOR(&buf); err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	if err := p.topic.Publish(ctx, buf.Bytes()); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

type discovery struct {
	key      Key
	instance uint64
	chain    gpbft.ECChain
}

func (p *PubSubChainExchange) cacheAsWantedChain(ctx context.Context, cmsg Message) {
	var notifications []discovery
	wanted := p.getChainsWantedAt(cmsg.Instance)
	for offset := len(cmsg.Chain); offset >= 0 && ctx.Err() == nil; offset-- {
		// TODO: Expose internals of merkle.go so that keys can be generated
		//       cumulatively for a more efficient prefix chain key generation.
		prefix := cmsg.Chain.Prefix(offset)
		key := p.Key(prefix)
		cacheKey := string(key)
		if portion, found := wanted.Peek(cacheKey); !found || portion.IsPlaceholder() {
			wanted.Add(cacheKey, &chainPortion{
				chain: prefix,
			})
			if p.listener != nil {
				notifications = append(notifications, discovery{
					key:      key,
					instance: cmsg.Instance,
					chain:    prefix,
				})
			}
		}
		// Continue with the remaining prefix keys as we do not know if any of them have
		// been evicted from the cache or not. This should be cheap enough considering the
		// added complexity of tracking evictions relative to chain prefixes.
	}

	// Notify the listener outside the lock.
	if p.listener != nil {
		for _, notification := range notifications {
			p.listener.NotifyChainDiscovered(ctx, notification.key, notification.instance, notification.chain)
		}
	}
}

func (p *PubSubChainExchange) RemoveChainsByInstance(_ context.Context, instance uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.chainsWanted, instance)
	delete(p.chainsDiscovered, instance)
	return nil
}

func (p *PubSubChainExchange) Shutdown(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop != nil {
		return p.stop()
	}
	return nil
}

func (cp *chainPortion) IsPlaceholder() bool {
	return cp == chainPortionPlaceHolder
}
