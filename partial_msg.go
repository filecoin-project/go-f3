package f3

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/chainexchange"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/manifest"
	lru "github.com/hashicorp/golang-lru/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var _ chainexchange.Listener = (*partialMessageManager)(nil)

type PartialGMessage struct {
	*gpbft.GMessage
	VoteValueKey gpbft.ECChainKey `cborgen:"maxlen=32"`
}

type partialMessageKey struct {
	sender  gpbft.ActorID
	instant gpbft.Instant
}

type discoveredChain struct {
	instance uint64
	chain    *gpbft.ECChain
}

type partialMessageManager struct {
	chainex *chainexchange.PubSubChainExchange

	// pmByInstance is a map of instance to a buffer of partial messages that are
	// keyed by sender+instance+round+phase.
	pmByInstance map[uint64]*lru.Cache[partialMessageKey, *PartiallyValidatedMessage]
	// pmkByInstanceByChainKey is used for an auxiliary lookup of all partial
	// messages for a given vote value at an instance.
	pmkByInstanceByChainKey map[uint64]map[gpbft.ECChainKey][]partialMessageKey
	// pendingPartialMessages is a channel of partial messages that are pending to be buffered.
	pendingPartialMessages chan *PartiallyValidatedMessage
	// pendingDiscoveredChains is a channel of chains discovered by chainexchange
	// that are pending to be processed.
	pendingDiscoveredChains chan *discoveredChain
	// pendingChainBroadcasts is a channel of chains that are pending to be broadcasted.
	pendingChainBroadcasts chan chainexchange.Message
	// pendingInstanceRemoval is a channel of instances that are pending to be removed.
	pendingInstanceRemoval chan uint64
	// rebroadcastInterval is the interval at which chains are re-broadcasted.
	rebroadcastInterval time.Duration
	// maxBuffMsgPerInstance is the maximum number of buffered partial messages per instance.
	maxBuffMsgPerInstance int
	// completedMsgsBufSize is the size of the buffer for completed messages channel.
	completedMsgsBufSize int
	clk                  clock.Clock

	stop func()
}

func newPartialMessageManager(progress gpbft.Progress, ps *pubsub.PubSub, m manifest.Manifest, clk clock.Clock) (*partialMessageManager, error) {
	pmm := &partialMessageManager{
		pmByInstance:            make(map[uint64]*lru.Cache[partialMessageKey, *PartiallyValidatedMessage]),
		pmkByInstanceByChainKey: make(map[uint64]map[gpbft.ECChainKey][]partialMessageKey),
		pendingDiscoveredChains: make(chan *discoveredChain, m.PartialMessageManager.PendingDiscoveredChainsBufferSize),
		pendingPartialMessages:  make(chan *PartiallyValidatedMessage, m.PartialMessageManager.PendingPartialMessagesBufferSize),
		pendingChainBroadcasts:  make(chan chainexchange.Message, m.PartialMessageManager.PendingChainBroadcastsBufferSize),
		pendingInstanceRemoval:  make(chan uint64, m.PartialMessageManager.PendingInstanceRemovalBufferSize),
		rebroadcastInterval:     m.ChainExchange.RebroadcastInterval,
		maxBuffMsgPerInstance:   m.PartialMessageManager.MaxBufferedMessagesPerInstance,
		completedMsgsBufSize:    m.PartialMessageManager.CompletedMessagesBufferSize,
		clk:                     clk,
	}
	var err error
	pmm.chainex, err = chainexchange.NewPubSubChainExchange(
		chainexchange.WithCompression(m.PubSub.ChainCompressionEnabled),
		chainexchange.WithListener(pmm),
		chainexchange.WithProgress(progress),
		chainexchange.WithPubSub(ps),
		chainexchange.WithMaxChainLength(m.ChainExchange.MaxChainLength),
		chainexchange.WithMaxDiscoveredChainsPerInstance(m.ChainExchange.MaxDiscoveredChainsPerInstance),
		chainexchange.WithMaxInstanceLookahead(m.ChainExchange.MaxInstanceLookahead),
		chainexchange.WithMaxWantedChainsPerInstance(m.ChainExchange.MaxWantedChainsPerInstance),
		chainexchange.WithMaxTimestampAge(m.ChainExchange.MaxTimestampAge),
		chainexchange.WithSubscriptionBufferSize(m.ChainExchange.SubscriptionBufferSize),
		chainexchange.WithTopicName(manifest.ChainExchangeTopicFromNetworkName(m.NetworkName)),
		chainexchange.WithClock(clk),
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

	completedMessages := make(chan *PartiallyValidatedMessage, pmm.completedMsgsBufSize)
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
				chainkey := discovered.chain.Key()
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
							// The gpbft runner is too slow to consume the completed messages. Drop the
							// earliest unconsumed message to make room for the new one. This is a trade-off
							// between preferring eventual progress versus full message delivery while
							// keeping a capped memory usage.
							log.Warn("The gpbft runner is too slow to consume completed messages.")
							select {
							case <-ctx.Done():
								return
							case dropped := <-completedMessages:
								metrics.partialMessagesDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", "completed_message")))
								select {
								case <-ctx.Done():
									return
								case completedMessages <- pgmsg:
									log.Warnw("Dropped earliest completed message to add the latest as the gpbft runner is too slow to consume them.", "added", pgmsg.GMessage, "dropped", dropped)
								}
							}
						}
						buffer.Remove(messageKey)
						metrics.partialMessages.Add(ctx, -1)
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
				if known, found, _ := buffer.PeekOrAdd(key, pgmsg); !found {
					pmkByChainKey := pmm.pmkByInstanceByChainKey[pgmsg.Vote.Instance]
					pmkByChainKey[pgmsg.VoteValueKey] = append(pmkByChainKey[pgmsg.VoteValueKey], key)
					metrics.partialMessages.Add(ctx, 1)
				} else {
					// The message is a duplicate. This can happen when a message is re-broadcasted.
					// But the vote value key must remain consistent for the same instance, sender,
					// round and phase. If it's not, then it's an equivocation.
					equivocation := known.VoteValueKey != pgmsg.VoteValueKey
					metrics.partialMessageDuplicates.Add(ctx, 1,
						metric.WithAttributes(attribute.Bool("equivocation", equivocation)))
				}
			case instance, ok := <-pmm.pendingInstanceRemoval:
				if !ok {
					return
				}
				for i, pmsgs := range pmm.pmByInstance {
					if i < instance {
						delete(pmm.pmByInstance, i)
						metrics.partialMessageInstances.Add(ctx, -1)
						metrics.partialMessages.Add(ctx, -int64(pmsgs.Len()))
					}
				}
				for i := range pmm.pmkByInstanceByChainKey {
					if i < instance {
						delete(pmm.pmkByInstanceByChainKey, i)
					}
				}
				if err := pmm.chainex.RemoveChainsByInstance(ctx, instance); err != nil {
					log.Errorw("Failed to remove chains by instance form chainexchange.", "instance", instance, "error", err)
				}
			}
		}
	}()

	// Use a dedicated goroutine for chain broadcast to avoid any delay in
	// broadcasting chains as it can fundamentally affect progress across the system.
	go func() {
		ticker := pmm.clk.Ticker(pmm.rebroadcastInterval)
		defer func() {
			ticker.Stop()
			log.Debugw("Partial message manager rebroadcast stopped.")
		}()

		var current *chainexchange.Message
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				if current != nil {
					current.Timestamp = roundDownToUnixMilliTime(t, pmm.rebroadcastInterval)
					if err := pmm.chainex.Broadcast(ctx, *current); err != nil {
						log.Debugw("Failed to re-broadcast chain.", "instance", current.Instance, "chain", current.Chain, "error", err)
					}
				}
			case pending, ok := <-pmm.pendingChainBroadcasts:
				if !ok {
					return
				}
				switch {
				case current == nil, pending.Instance > current.Instance:
					// Either there's no prior chain broadcast or a new instance has started.
					// Broadcast immediately and reset the timer to tick from now onwards. This is to
					// re-align the chain rebroadcast relative to instance start.
					current = &pending
					current.Timestamp = roundDownToUnixMilliTime(pmm.clk.Now(), pmm.rebroadcastInterval)
					if err := pmm.chainex.Broadcast(ctx, *current); err != nil {
						log.Debugw("Failed to immediately re-broadcast chain.", "instance", current.Instance, "chain", current.Chain, "error", err)
					}
					ticker.Reset(pmm.rebroadcastInterval)
				case pending.Instance == current.Instance:
					// When the instance is the same as the current instance, only switch if the
					// current chain doesn't contain the pending chain as a prefix. Because,
					// broadcasting the longer chain offers more information to the network and
					// improves the chances of consensus on a longer chain at the price of slightly
					// higher bandwidth usage.
					if !current.Chain.HasPrefix(pending.Chain) {
						current = &pending
					}
					// TODO: Maybe parameterise this in manifest in case we need to save as much
					//       bandwidth as possible?
				case pending.Instance < current.Instance:
					log.Debugw("Dropped chain broadcast message as it's too old.", "messageInstance", pending.Instance, "latestInstance", current.Instance)
					continue
				}
			}
		}
	}()

	return completedMessages, nil
}

func roundDownToUnixMilliTime(t time.Time, interval time.Duration) int64 {
	intervalMilli := interval.Milliseconds()
	if intervalMilli <= 0 {
		log.Errorw("Invalid rebroadcast interval, must be greater than 1 millisecond.", "interval", intervalMilli)
		return 0
	}
	return (t.UnixMilli() / intervalMilli) * intervalMilli
}

func (pmm *partialMessageManager) BroadcastChain(ctx context.Context, instance uint64, chain *gpbft.ECChain) error {
	if chain.IsZero() {
		return nil
	}
	msg := chainexchange.Message{Instance: instance, Chain: chain}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pmm.pendingChainBroadcasts <- msg:
	default:
		// The chain broadcast is too slow. Drop the earliest unprocessed request and
		// rely on GPBFT re-broadcast to request chain broadcast again instead of
		// blocking. The rationale for dropping the earliest is that the chances are
		// later messages are more informative for the network and later ones. If we are
		// slow in processing, the chances are we are behind.
		log.Debugw("The chain rebroadcast is too slow.", "instance", instance, "chain", chain)
		select {
		case <-ctx.Done():
			return nil
		case dropped := <-pmm.pendingChainBroadcasts:
			metrics.partialMessagesDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", "chain_broadcast")))
			select {
			case <-ctx.Done():
				return nil
			case pmm.pendingChainBroadcasts <- msg:
				log.Warnw("Dropped earliest chain broadcast request to the latest as the chain broadcast is too slow to consume them.", "added", msg, "dropped", dropped)
			}
		}
	}
	return nil
}

func (pmm *partialMessageManager) toPartialGMessage(msg *gpbft.GMessage) (*PartialGMessage, error) {
	msgCopy := *(msg)
	pmsg := &PartialGMessage{
		GMessage: &msgCopy,
	}
	if !pmsg.Vote.Value.IsZero() {
		pmsg.VoteValueKey = pmsg.Vote.Value.Key()
		pmsg.Vote.Value = &gpbft.ECChain{}
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
		pmsg.Justification.Vote.Value = &gpbft.ECChain{}
	}
	return pmsg, nil
}

func (pmm *partialMessageManager) NotifyChainDiscovered(ctx context.Context, instance uint64, chain *gpbft.ECChain) {
	discovery := &discoveredChain{instance: instance, chain: chain}
	select {
	case <-ctx.Done():
		return
	case pmm.pendingDiscoveredChains <- discovery:
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
		var err error
		buffer, err = lru.New[partialMessageKey, *PartiallyValidatedMessage](pmm.maxBuffMsgPerInstance)
		if err != nil {
			log.Fatalf("Failed to create buffer for instance %d: %s", instance, err)
			panic(err)
		}
		pmm.pmByInstance[instance] = buffer
		metrics.partialMessageInstances.Add(context.Background(), 1)
	}
	if _, ok := pmm.pmkByInstanceByChainKey[instance]; !ok {
		pmm.pmkByInstanceByChainKey[instance] = make(map[gpbft.ECChainKey][]partialMessageKey)
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

func (pmm *partialMessageManager) RemoveMessagesBeforeInstance(ctx context.Context, instance uint64) {
	select {
	case <-ctx.Done():
		return
	case pmm.pendingInstanceRemoval <- instance:
	default:
		log.Warnw("Dropped instance removal request as partial message manager is too slow.", "instance", instance)
	}
}

func (pmm *partialMessageManager) Shutdown(ctx context.Context) error {
	if pmm.stop != nil {
		pmm.stop()
	}
	return pmm.chainex.Shutdown(ctx)
}
