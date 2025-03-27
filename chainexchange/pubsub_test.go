package chainexchange_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/chainexchange"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestPubSubChainExchange_Broadcast(t *testing.T) {
	for _, test := range []struct {
		name string
		opts []chainexchange.Option
	}{
		{
			name: "no compression",
			opts: []chainexchange.Option{chainexchange.WithCompression(false)},
		},
		{
			name: "with compression",
			opts: []chainexchange.Option{chainexchange.WithCompression(true)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const topicName = "fish"
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			var testInstant gpbft.InstanceProgress
			var testListener listener
			host, err := libp2p.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				cancel()
				require.NoError(t, host.Close())
			})

			ps, err := pubsub.NewGossipSub(ctx, host, pubsub.WithFloodPublish(true))
			require.NoError(t, err)

			options := []chainexchange.Option{
				chainexchange.WithProgress(func() (instant gpbft.InstanceProgress) {
					return testInstant
				}),
				chainexchange.WithPubSub(ps),
				chainexchange.WithTopicName(topicName),
				chainexchange.WithTopicScoreParams(nil),
				chainexchange.WithMaxTimestampAge(time.Minute),
				chainexchange.WithListener(&testListener),
				chainexchange.WithCompression(true),
			}
			options = append(options, test.opts...)
			subject, err := chainexchange.NewPubSubChainExchange(
				options...,
			)
			require.NoError(t, err)
			require.NotNil(t, subject)

			err = subject.Start(ctx)
			require.NoError(t, err)

			instance := uint64(1)
			ecChain := &gpbft.ECChain{
				TipSets: []*gpbft.TipSet{
					{Epoch: 0, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
					{Epoch: 1, Key: []byte("barreleye"), PowerTable: gpbft.MakeCid([]byte("pt"))},
				},
			}

			key := ecChain.Key()
			chain, found := subject.GetChainByInstance(ctx, instance, key)
			require.False(t, found)
			require.Nil(t, chain)
			require.Empty(t, testListener.getNotifications())

			require.NoError(t, subject.Broadcast(ctx, chainexchange.Message{
				Instance:  instance,
				Chain:     ecChain,
				Timestamp: time.Now().Add(-2 * time.Second).UnixMilli(),
			}))

			require.Eventually(t, func() bool {
				chain, found = subject.GetChainByInstance(ctx, instance, key)
				return found
			}, time.Second, 100*time.Millisecond)
			require.EqualExportedValues(t, ecChain, chain)

			baseChain := ecChain.BaseChain()
			baseKey := baseChain.Key()
			require.Eventually(t, func() bool {
				chain, found = subject.GetChainByInstance(ctx, instance, baseKey)
				return found
			}, time.Second, 100*time.Millisecond)
			require.EqualExportedValues(t, baseChain, chain)

			// Assert that we have received 2 notifications, because ecChain has 2 tipsets.
			// First should be the ecChain, second should be the baseChain.

			notifications := testListener.getNotifications()
			require.Len(t, notifications, 2)
			require.Equal(t, instance, notifications[1].instance)
			require.EqualExportedValues(t, baseChain, notifications[1].chain)
			require.Equal(t, instance, notifications[0].instance)
			require.EqualExportedValues(t, ecChain, notifications[0].chain)

			// Assert instance is no longer found once removed.
			require.NoError(t, subject.RemoveChainsByInstance(ctx, instance+1))
			chain, found = subject.GetChainByInstance(ctx, instance, key)
			require.False(t, found)
			require.Nil(t, chain)

			require.NoError(t, subject.Shutdown(ctx))
		})
	}
}

func TestSwarm(t *testing.T) {
	const (
		topicName = "fish"
		swarmSize = 100
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	currentInstance := gpbft.InstanceProgress{
		Input: &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
			},
		},
	}

	mnet := mocknet.New()
	swarm := make([]*chainexchange.PubSubChainExchange, swarmSize)
	for i := range swarm {
		host, err := mnet.GenPeer()
		require.NoError(t, err)
		ps, err := pubsub.NewGossipSub(ctx, host, pubsub.WithFloodPublish(true), pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign))
		require.NoError(t, err)
		subject, err := chainexchange.NewPubSubChainExchange(
			chainexchange.WithProgress(func() (instant gpbft.InstanceProgress) {
				return currentInstance
			}),
			chainexchange.WithPubSub(ps),
			chainexchange.WithTopicName(topicName),
			chainexchange.WithTopicScoreParams(nil),
			chainexchange.WithMaxTimestampAge(time.Minute),
			chainexchange.WithCompression(true),
			chainexchange.WithMaxWantedChainsPerInstance(6),
			chainexchange.WithMaxDiscoveredChainsPerInstance(6),
		)
		require.NoError(t, err)
		require.NotNil(t, subject)
		require.NoError(t, subject.Start(ctx))
		swarm[i] = subject
	}

	require.NoError(t, mnet.LinkAll())
	require.NoError(t, mnet.ConnectAllButSelf())

	someChain := currentInstance.Input.BaseChain().Append(
		&gpbft.TipSet{Epoch: 1, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
		&gpbft.TipSet{Epoch: 2, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
		&gpbft.TipSet{Epoch: 3, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
		&gpbft.TipSet{Epoch: 4, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
		&gpbft.TipSet{Epoch: 5, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
	)

	// Attempt to get the chain from all the nodes in the swarm to mark it as wanted.
	for _, cx := range swarm {
		_, found := cx.GetChainByInstance(ctx, currentInstance.ID, someChain.Key())
		require.False(t, found)
	}

	require.NoError(t, swarm[0].Broadcast(ctx, chainexchange.Message{
		Instance:  currentInstance.ID,
		Chain:     someChain,
		Timestamp: time.Now().UnixMilli(),
	}))

	require.Eventually(t, func() bool {
		for _, cx := range swarm {
			_, found := cx.GetChainByInstance(ctx, currentInstance.ID, someChain.Key())
			if !found {
				return false
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond)
}

func TestValidation(t *testing.T) {

	var (
		validBaseChain = &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
			},
		}
		anotherValidChain = &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 1, Key: []byte("barreleye"), PowerTable: gpbft.MakeCid([]byte("pt"))},
			},
		}
		validChain   = validBaseChain.Append(anotherValidChain.TipSets...)
		invalidChain = &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 1, Key: []byte("lobster"), PowerTable: gpbft.MakeCid([]byte("pt"))},
				{Epoch: 0, Key: nil, PowerTable: gpbft.MakeCid([]byte("pt"))},
			},
		}
	)

	for _, test := range []struct {
		name          string
		messageAt     func(clock *clock.Mock) chainexchange.Message
		givenProgress gpbft.InstanceProgress
		wantErr       string
	}{
		{
			name: "instance too old",
			messageAt: func(clock *clock.Mock) chainexchange.Message {
				return chainexchange.Message{
					Instance:  10,
					Chain:     validChain,
					Timestamp: clock.Now().UnixMilli(),
				}
			},
			givenProgress: gpbft.InstanceProgress{
				Instant: gpbft.Instant{
					ID: 11,
				},
				Input: validChain,
			},
			wantErr: pubsub.RejectValidationIgnored,
		},
		{
			name: "timestamp too old",
			messageAt: func(clock *clock.Mock) chainexchange.Message {
				ts := clock.Now().UnixMilli()
				clock.Add(time.Hour)
				return chainexchange.Message{
					Instance:  10,
					Chain:     validChain,
					Timestamp: ts,
				}
			},
			givenProgress: gpbft.InstanceProgress{
				Instant: gpbft.Instant{
					ID: 10,
				},
				Input: validChain,
			},
			wantErr: pubsub.RejectValidationIgnored,
		},
		{
			name: "invalid chain",
			messageAt: func(clock *clock.Mock) chainexchange.Message {
				return chainexchange.Message{
					Instance:  10,
					Chain:     invalidChain,
					Timestamp: clock.Now().UnixMilli(),
				}
			},
			givenProgress: gpbft.InstanceProgress{
				Instant: gpbft.Instant{
					ID: 10,
				},
				Input: validChain,
			},
			wantErr: pubsub.RejectValidationFailed,
		},
		{
			name: "unexpected base chain",
			messageAt: func(clock *clock.Mock) chainexchange.Message {
				return chainexchange.Message{
					Instance:  10,
					Chain:     anotherValidChain,
					Timestamp: clock.Now().UnixMilli(),
				}
			},
			givenProgress: gpbft.InstanceProgress{
				Instant: gpbft.Instant{
					ID: 10,
				},
				Input: validChain,
			},
			wantErr: pubsub.RejectValidationFailed,
		},
		{
			name: "fresh enough timestamp",
			messageAt: func(clock *clock.Mock) chainexchange.Message {
				ts := clock.Now().UnixMilli()
				clock.Add(5 * time.Second)
				return chainexchange.Message{
					Instance:  10,
					Chain:     validChain,
					Timestamp: ts,
				}
			},
			givenProgress: gpbft.InstanceProgress{
				Instant: gpbft.Instant{
					ID: 10,
				},
				Input: validChain,
			},
		},
		{
			name: "future instance",
			messageAt: func(clock *clock.Mock) chainexchange.Message {
				return chainexchange.Message{
					Instance:  11,
					Chain:     anotherValidChain,
					Timestamp: clock.Now().UnixMilli(),
				}
			},
			givenProgress: gpbft.InstanceProgress{
				Instant: gpbft.Instant{
					ID: 10,
				},
				Input: validChain,
			},
		},
		{
			name: "too far in future instance",
			messageAt: func(clock *clock.Mock) chainexchange.Message {
				return chainexchange.Message{
					Instance:  110,
					Chain:     anotherValidChain,
					Timestamp: clock.Now().UnixMilli(),
				}
			},
			givenProgress: gpbft.InstanceProgress{
				Instant: gpbft.Instant{
					ID: 10,
				},
				Input: validChain,
			},
			wantErr: pubsub.RejectValidationIgnored,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const topicName = "fish"
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			clck := clock.NewMock()
			host, err := libp2p.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				cancel()
				require.NoError(t, host.Close())
			})

			ps, err := pubsub.NewGossipSub(ctx, host, pubsub.WithFloodPublish(true))
			require.NoError(t, err)

			subject, err := chainexchange.NewPubSubChainExchange(
				chainexchange.WithProgress(func() (instant gpbft.InstanceProgress) { return test.givenProgress }),
				chainexchange.WithPubSub(ps),
				chainexchange.WithTopicName(topicName),
				chainexchange.WithTopicScoreParams(nil),
				chainexchange.WithMaxTimestampAge(10*time.Second),
				chainexchange.WithCompression(true),
				chainexchange.WithClock(clck),
			)
			require.NoError(t, err)
			require.NotNil(t, subject)

			err = subject.Start(ctx)
			require.NoError(t, err)

			err = subject.Broadcast(ctx, test.messageAt(clck))
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, subject.Shutdown(ctx))
		})
	}
}

type notification struct {
	instance uint64
	chain    *gpbft.ECChain
}
type listener struct {
	mu            sync.Mutex
	notifications []notification
}

func (l *listener) NotifyChainDiscovered(_ context.Context, instance uint64, chain *gpbft.ECChain) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.notifications = append(l.notifications, notification{instance: instance, chain: chain})
}

func (l *listener) getNotifications() []notification {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.notifications)
}
