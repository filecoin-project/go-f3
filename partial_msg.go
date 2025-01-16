package f3

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-f3/chainexchange"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	lru "github.com/hashicorp/golang-lru/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

var _ chainexchange.Listener = (*partialMessageManager)(nil)

type PartialGMessage struct {
	*gpbft.GMessage
	VoteValueKey chainexchange.Key `cborgen:"maxlen=32"`
}

type partialMessageKey struct {
	sender  gpbft.ActorID
	instant gpbft.Instant
}

type discoveredChain struct {
	key      chainexchange.Key
	instance uint64
	chain    gpbft.ECChain
}

type partialMessageManager struct {
	chainex *chainexchange.PubSubChainExchange

	// pmByInstance is a map of instance to a buffer of partial messages that are
	// keyed by sender+instance+round+phase.
	pmByInstance map[uint64]*lru.Cache[partialMessageKey, *PartiallyValidatedMessage]
	// pmkByInstanceByChainKey is used for an auxiliary lookup of all partial
	// messages for a given vote value at an instance.
	pmkByInstanceByChainKey map[uint64]map[string][]partialMessageKey
	// pendingPartialMessages is a channel of partial messages that are pending to be buffered.
	pendingPartialMessages chan *PartiallyValidatedMessage
	// pendingDiscoveredChains is a channel of chains discovered by chainexchange
	// that are pending to be processed.
	pendingDiscoveredChains chan *discoveredChain

	stop func()
}

func newPartialMessageManager(progress gpbft.Progress, ps *pubsub.PubSub, m *manifest.Manifest) (*partialMessageManager, error) {
	pmm := &partialMessageManager{
		pmByInstance:            make(map[uint64]*lru.Cache[partialMessageKey, *PartiallyValidatedMessage]),
		pmkByInstanceByChainKey: make(map[uint64]map[string][]partialMessageKey),
		pendingDiscoveredChains: make(chan *discoveredChain, 100),           // TODO: parameterize buffer size.
		pendingPartialMessages:  make(chan *PartiallyValidatedMessage, 100), // TODO: parameterize buffer size.
	}
	var err error
	pmm.chainex, err = chainexchange.NewPubSubChainExchange(
		chainexchange.WithListener(pmm),
		chainexchange.WithProgress(progress),
		chainexchange.WithPubSub(ps),
		chainexchange.WithMaxChainLength(m.ChainExchange.MaxChainLength),
		chainexchange.WithMaxDiscoveredChainsPerInstance(m.ChainExchange.MaxDiscoveredChainsPerInstance),
		chainexchange.WithMaxInstanceLookahead(m.ChainExchange.MaxInstanceLookahead),
		chainexchange.WithMaxWantedChainsPerInstance(m.ChainExchange.MaxWantedChainsPerInstance),
		chainexchange.WithSubscriptionBufferSize(m.ChainExchange.SubscriptionBufferSize),
		chainexchange.WithTopicName(manifest.ChainExchangeTopicFromNetworkName(m.NetworkName)),
	)
	if err != nil {
		return nil, err
	}
	return pmm, nil
}

func (pmm *partialMessageManager) Start(ctx context.Context) (<-chan *PartiallyValidatedMessage, error) {
	if err := pmm.chainex.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting chain exchange: %w", err)
	}

	completedMessages := make(chan *PartiallyValidatedMessage, 100) // TODO: parameterize buffer size.
	ctx, pmm.stop = context.WithCancel(context.Background())
	go func() {
		defer func() {
			close(completedMessages)
			log.Debugw("Partial message manager stopped.")
		}()

		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case discovered, ok := <-pmm.pendingDiscoveredChains:
				if !ok {
					return
				}
				partialMessageKeysAtInstance, found := pmm.pmkByInstanceByChainKey[discovered.instance]
				if !found {
					// There's no known instance with a partial message. Ignore the discovered chain.
					// There's also no need to optimistically store them here. Because, chainexchange
					// does this with safe caps on max future instances.
					continue
				}
				chainkey := string(discovered.key)
				partialMessageKeys, found := partialMessageKeysAtInstance[chainkey]
				if !found {
					// There's no known partial message at the instance for the discovered chain.
					// Ignore the discovery for the same reason as above.
					continue
				}
				buffer := pmm.getOrInitPartialMessageBuffer(discovered.instance)
				for _, messageKey := range partialMessageKeys {
					if pgmsg, found := buffer.Get(messageKey); found {
						pgmsg.Vote.Value = discovered.chain
						inferJustificationVoteValue(pgmsg.PartialGMessage)
						select {
						case <-ctx.Done():
							return
						case completedMessages <- pgmsg:
						default:
							log.Warnw("Dropped completed message as the gpbft runner is too slow to consume them.", "msg", pgmsg.GMessage)
						}
						buffer.Remove(messageKey)
					}
				}
				delete(partialMessageKeysAtInstance, chainkey)
			case pgmsg, ok := <-pmm.pendingPartialMessages:
				if !ok {
					return
				}
				key := partialMessageKey{
					sender: pgmsg.Sender,
					instant: gpbft.Instant{
						ID:    pgmsg.Vote.Instance,
						Round: pgmsg.Vote.Round,
						Phase: pgmsg.Vote.Phase,
					},
				}
				buffer := pmm.getOrInitPartialMessageBuffer(pgmsg.Vote.Instance)
				if found, _ := buffer.ContainsOrAdd(key, pgmsg); !found {
					pmkByChainKey := pmm.pmkByInstanceByChainKey[pgmsg.Vote.Instance]
					chainKey := string(pgmsg.VoteValueKey)
					pmkByChainKey[chainKey] = append(pmkByChainKey[chainKey], key)
				}
				// TODO: Add equivocation metrics: check if the message is different and if so
				//       increment the equivocations counter tagged by phase.
				//       See: https://github.com/filecoin-project/go-f3/issues/812
			}
		}
	}()
	return completedMessages, nil
}

func (pmm *partialMessageManager) BroadcastChain(ctx context.Context, instance uint64, chain gpbft.ECChain) error {
	if chain.IsZero() {
		return nil
	}

	// TODO: Implement an independent chain broadcast and rebroadcast heuristic.
	//       See: https://github.com/filecoin-project/go-f3/issues/814

	cmsg := chainexchange.Message{Instance: instance, Chain: chain}
	if err := pmm.chainex.Broadcast(ctx, cmsg); err != nil {
		return fmt.Errorf("broadcasting chain: %w", err)
	}
	log.Debugw("broadcasted chain", "instance", instance, "chain", chain)
	return nil
}

func (pmm *partialMessageManager) toPartialGMessage(msg *gpbft.GMessage) (*PartialGMessage, error) {
	msgCopy := *(msg)
	pmsg := &PartialGMessage{
		GMessage: &msgCopy,
	}
	if !pmsg.Vote.Value.IsZero() {
		pmsg.VoteValueKey = pmm.chainex.Key(pmsg.Vote.Value)
		pmsg.Vote.Value = gpbft.ECChain{}
	}
	if msg.Justification != nil && !pmsg.Justification.Vote.Value.IsZero() {
		justificationCopy := *(msg.Justification)
		pmsg.Justification = &justificationCopy
		// The justification vote value is either zero or the same as the vote value.
		// Anything else is considered to be an in invalid justification. In fact, for
		// any given phase and round we can always infer:
		//  1. whether a message should have a justification, and
		//  2. if so, for what chain at which round.
		//
		// Therefore, we can entirely ignore the justification vote value as far as
		// chainexchange is concerned and override it with a zero value. Upon arrival of
		// a partial message, the receiver can always infer the justification chain from
		// the message phase, round. In a case where justification is invalid, the
		// signature won't match anyway, so it seems harmless to always infer the
		// justification chain.
		//
		// In fact, it probably should have been omitted altogether at the time of
		// protocol design.
		pmsg.Justification.Vote.Value = gpbft.ECChain{}
	}
	return pmsg, nil
}

func (pmm *partialMessageManager) NotifyChainDiscovered(ctx context.Context, key chainexchange.Key, instance uint64, chain gpbft.ECChain) {
	discovery := &discoveredChain{key: key, instance: instance, chain: chain}
	select {
	case <-ctx.Done():
		return
	case pmm.pendingDiscoveredChains <- discovery:
		// TODO: add metrics
	default:
		// The message completion looks up the key on chain exchange anyway. The net
		// effect of this is delay in delivering messages assuming they're re-boradcasted
		// by GPBFT. This is probably the best we can do if the partial message manager
		// is too slow.
		log.Warnw("Dropped chain discovery notification as partial messge manager is too slow.", "instance", instance, "chain", chain)
	}
}

func (pmm *partialMessageManager) bufferPartialMessage(ctx context.Context, msg *PartiallyValidatedMessage) {
	select {
	case <-ctx.Done():
		return
	case pmm.pendingPartialMessages <- msg:
		// TODO: add metrics
	default:
		// Choosing to rely on GPBFT re-boradcast to compensate for a partial message
		// being dropped. The key thing here is that partial message manager should never
		// be too slow. If it is, then there are bigger problems to solve. Hence, the
		// failsafe is to drop the message instead of halting further message processing.
		log.Warnw("Dropped partial message as partial message manager is too slow.", "msg", msg)
	}
}

func (pmm *partialMessageManager) getOrInitPartialMessageBuffer(instance uint64) *lru.Cache[partialMessageKey, *PartiallyValidatedMessage] {
	buffer, found := pmm.pmByInstance[instance]
	if !found {
		// TODO: parameterize this in the manifest?
		// Based on 5 phases, 2K network size at a couple of rounds plus some headroom.
		const maxBufferedMessagesPerInstance = 25_000
		var err error
		buffer, err = lru.New[partialMessageKey, *PartiallyValidatedMessage](maxBufferedMessagesPerInstance)
		if err != nil {
			log.Fatalf("Failed to create buffer for instance %d: %s", instance, err)
			panic(err)
		}
		pmm.pmByInstance[instance] = buffer
	}
	if _, ok := pmm.pmkByInstanceByChainKey[instance]; !ok {
		pmm.pmkByInstanceByChainKey[instance] = make(map[string][]partialMessageKey)
	}
	return buffer
}

func (pmm *partialMessageManager) CompleteMessage(ctx context.Context, pgmsg *PartialGMessage) (*gpbft.GMessage, bool) {
	if pgmsg == nil {
		// For sanity assert that the message isn't nil.
		return nil, false
	}
	if pgmsg.VoteValueKey.IsZero() {
		// A zero VoteValueKey indicates that there's no partial chain value, for
		// example, COMMIT for bottom. Return the message as is.
		return pgmsg.GMessage, true
	}

	chain, found := pmm.chainex.GetChainByInstance(ctx, pgmsg.Vote.Instance, pgmsg.VoteValueKey)
	if !found {
		return nil, false
	}
	pgmsg.Vote.Value = chain
	inferJustificationVoteValue(pgmsg)
	return pgmsg.GMessage, true
}

func inferJustificationVoteValue(pgmsg *PartialGMessage) {
	// Infer what the value of justification should be based on the vote phase. A
	// valid message with non-nil justification must justify the vote value chain
	// at:
	//  * CONVERGE_PHASE, with justification of PREPARE.
	//  * PREPARE_PHASE, with justification of PREPARE.
	//  * COMMIT_PHASE, with justification of PREPARE.
	//  * DECIDE_PHASE, with justification of COMMIT.
	//
	// Future work should get rid of chains in justification entirely. See:
	//  * https://github.com/filecoin-project/go-f3/issues/806
	if pgmsg.Justification != nil {
		switch pgmsg.Vote.Phase {
		case
			gpbft.CONVERGE_PHASE,
			gpbft.PREPARE_PHASE,
			gpbft.COMMIT_PHASE:
			if pgmsg.Justification.Vote.Phase == gpbft.PREPARE_PHASE {
				pgmsg.Justification.Vote.Value = pgmsg.Vote.Value
			}
		case gpbft.DECIDE_PHASE:
			if pgmsg.Justification.Vote.Phase == gpbft.COMMIT_PHASE {
				pgmsg.Justification.Vote.Value = pgmsg.Vote.Value
			}
		default:
			// The message must be invalid. But let the flow proceed and have the validator
			// reject it.
		}
	}
}

func (pmm *partialMessageManager) Shutdown(ctx context.Context) error {
	if pmm.stop != nil {
		pmm.stop()
	}
	return pmm.chainex.Shutdown(ctx)
}
